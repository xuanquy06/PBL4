"""
Chương trình Client UAV Độc lập (Standalone Multi-Process UAV Client - PBL4 Client-Server):
- Chạy như một tiến trình Hệ điều hành riêng (PID độc lập) ở Terminal khác hoặc máy tính khác qua mạng LAN/Wi-Fi.
- Kết nối TCP tới Server Trạm Kiểm Lâm (:9000) để bắt tay JOIN và báo cháy FIRE_ALERT.
- Gửi gói tin Telemetry nhị phân (26B Header + IPv4 + TTL + CRC32) định kỳ qua UDP Socket (:9001).
"""
from __future__ import annotations

import argparse
from io import TextIOWrapper
import json
import math
import os
import socket
import struct
import sys
import threading
import time
from typing import Any, Optional

from core.network_stack import (
    FLAG_ACK,
    FLAG_DATA,
    NetworkPacket,
    _recv_exact,
    assign_node_ip,
)

if sys.platform == "win32":
    for _stream in (sys.stdout, sys.stderr):
        if isinstance(_stream, TextIOWrapper):
            _stream.reconfigure(encoding="utf-8", errors="replace")


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
    return r * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


class StandaloneUAVClient:
    """Tiến trình Client UAV độc lập giao tiếp TCP/UDP nhị phân với Server SAGSINS."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        tcp_port: int = 9000,
        udp_port: int = 9001,
        uav_id: str = "UAV_09",
        zone_id: str = "",
        battery: float = 90.0,
    ):
        self.host = host
        self.tcp_port = int(tcp_port)
        self.udp_port = int(udp_port)
        self.raw_id = str(uav_id).strip() or "UAV_09"
        self.uav_id = self.raw_id if self.raw_id.startswith("MÁY_BAY_") else f"MÁY_BAY_{self.raw_id}"
        self.zone_id = str(zone_id).strip()
        self.pid = os.getpid()

        self.ip_address = assign_node_ip(self.uav_id, "MÁY_BAY", self.zone_id or "ZONE_03")
        self.dst_ip = "10.0.0.1"

        self.zone_center = [16.125, 108.255]
        self.station = [16.105, 108.250]
        self.lat = self.zone_center[0]
        self.lon = self.zone_center[1]
        self.alt = 1050.0
        self.speed = 40.0
        self.battery = min(100.0, max(5.0, float(battery)))
        self.battery_capacity_j = 50000.0
        self.power_w = 80.0
        self.is_takeover = False
        self.paused = False
        self.time_scale = 1.0
        self.rf_link = "GROUND_LINK"
        self.rf_hops: list[str] = []
        self.rf_delay_ms = 0.0
        self.rf_per = 0.0
        self.state = "PATROLLING"
        self._orbit_angle = 0.0

        self._seq = 1000
        self._tcp_sock: Optional[socket.socket] = None
        self._udp_sock: Optional[socket.socket] = None
        self._tcp_lock = threading.Lock()
        self._state_lock = threading.RLock()
        self._pending_lock = threading.Lock()
        self._pending_acks: dict[int, tuple[threading.Event, list[dict[str, Any]]]] = {}
        self._running = False
        self._telemetry_thread: Optional[threading.Thread] = None
        self._tcp_reader_thread: Optional[threading.Thread] = None
        self.server_disconnected = False
        self.server_cmd_rx_count = 0
        self.last_server_cmd: Optional[dict[str, Any]] = None
        self._fire_target: Optional[tuple[float, float]] = None

        self.tcp_tx_count = 0
        self.tcp_ack_count = 0
        self.udp_tx_count = 0
        self.interactive_prompt = False
        self._print_lock = threading.Lock()

    def _prompt_label(self) -> str:
        return self.uav_id.replace("MÁY_BAY_", "")

    def _print_async(self, msg: str):
        with self._print_lock:
            print(f"\n{msg}", flush=True)
            if self.interactive_prompt and self._running and not self.server_disconnected:
                print(f"{self._prompt_label()}> ", end="", flush=True)

    def _next_seq(self) -> int:
        with self._tcp_lock:
            self._seq += 1
            return self._seq

    def send_tcp_command(self, cmd: str, extra: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """Đóng gói bản tin JSON vào khung NetworkPacket TCP (CRC32) và chờ gói ACK thật từ Server."""
        if self._tcp_sock is None or self.server_disconnected:
            raise RuntimeError("TCP socket chưa kết nối.")
        body: dict[str, Any] = {
            "cmd": str(cmd).upper(),
            "uav_id": self.uav_id,
            "pid": self.pid,
        }
        if self.zone_id:
            body["zone_id"] = self.zone_id
        if extra:
            body.update(extra)
        payload_bytes = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        prio = 0 if body["cmd"] in ("FIRE_ALERT", "JOIN") else 1

        reader_alive = self._tcp_reader_thread is not None and self._tcp_reader_thread.is_alive()

        with self._tcp_lock:
            self._seq += 1
            seq = self._seq
            pkt = NetworkPacket(
                src_ip=self.ip_address,
                dst_ip=self.dst_ip,
                protocol="TCP",
                priority=prio,
                seq_num=seq,
                ack_num=0,
                ttl=16,
                flags=FLAG_DATA,
                payload=payload_bytes,
            )
            raw = pkt.encode()
            ev: Optional[threading.Event] = None
            box: list[dict[str, Any]] = []
            if reader_alive:
                ev = threading.Event()
                with self._pending_lock:
                    self._pending_acks[seq] = (ev, box)

            self._tcp_sock.sendall(struct.pack("!I", len(raw)) + raw)
            self.tcp_tx_count += 1

            if not reader_alive:
                ack_len_hdr = _recv_exact(self._tcp_sock, 4)
                if not ack_len_hdr:
                    raise ConnectionError("Server đã đóng kết nối TCP.")
                ack_len = struct.unpack("!I", ack_len_hdr)[0]
                ack_raw = _recv_exact(self._tcp_sock, ack_len)
                if not ack_raw:
                    raise ConnectionError("Nhận khung ACK bị gián đoạn.")
                ack_pkt, crc_ok, _ = NetworkPacket.decode_and_verify(ack_raw)
                if ack_pkt is None or not crc_ok or not (ack_pkt.flags & FLAG_ACK):
                    raise RuntimeError("Gói ACK từ Server lỗi Checksum CRC32.")
                self.tcp_ack_count += 1
                if ack_pkt.payload:
                    return json.loads(ack_pkt.payload.decode("utf-8"))
                return {"ok": True}

        assert ev is not None
        if not ev.wait(timeout=5.0):
            with self._pending_lock:
                self._pending_acks.pop(seq, None)
            raise TimeoutError("Hết thời gian chờ gói ACK từ Server.")
        if not box:
            raise ConnectionError("Không nhận được phản hồi ACK từ Server.")
        res = box[0]
        if res.get("_conn_error"):
            raise ConnectionError(str(res.get("error") or "Server đã ngắt kết nối TCP."))
        if res.get("_crc_error"):
            raise RuntimeError("Gói ACK từ Server lỗi Checksum CRC32.")
        return res

    def send_udp_telemetry(self):
        """Đóng gói và truyền 1 datagram Telemetry nhị phân (CRC32, TTL=16) qua UDP Socket."""
        if self._udp_sock is None:
            return
        with self._state_lock:
            body: dict[str, Any] = {
                "cmd": "TELEMETRY",
                "uav_id": self.uav_id,
                "zone_id": self.zone_id,
                "lat": round(self.lat, 5),
                "lon": round(self.lon, 5),
                "alt": round(self.alt, 1),
                "speed": round(self.speed, 1),
                "battery": round(self.battery, 2),
                "state": self.state,
            }
            if self._fire_target is not None and self.state == "CONTAINING":
                body["fire_lat"] = round(self._fire_target[0], 5)
                body["fire_lon"] = round(self._fire_target[1], 5)
        payload_bytes = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        pkt = NetworkPacket(
            src_ip=self.ip_address,
            dst_ip=self.dst_ip,
            protocol="UDP",
            priority=1,
            seq_num=self._next_seq(),
            ack_num=0,
            ttl=16,
            flags=FLAG_DATA,
            payload=payload_bytes,
        )
        raw = pkt.encode()
        self._udp_sock.sendto(raw, (self.host, self.udp_port))
        self.udp_tx_count += 1

    def connect(self) -> dict[str, Any]:
        """Mở TCP Socket + UDP Socket và gửi bắt tay JOIN tới Server."""
        tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp.settimeout(5.0)
        try:
            tcp.connect((self.host, self.tcp_port))
        except Exception:
            tcp.close()
            raise
        self._tcp_sock = tcp
        self._udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        reply = self.send_tcp_command(
            "JOIN",
            {
                "battery": self.battery,
                "alt": self.alt,
            },
        )
        if not reply.get("ok"):
            err_msg = str(reply.get("error") or f"Server từ chối JOIN ({reply.get('cmd')})")
            self.stop(graceful=False)
            raise RuntimeError(err_msg)

        if reply.get("uav_id"):
            self.uav_id = str(reply["uav_id"])
        if reply.get("assigned_ip"):
            self.ip_address = str(reply["assigned_ip"])
        if reply.get("udp_port"):
            self.udp_port = int(reply["udp_port"])
        if reply.get("zone_id"):
            self.zone_id = str(reply["zone_id"])
        if isinstance(reply.get("zone_center"), list) and len(reply["zone_center"]) == 2:
            self.zone_center = [float(reply["zone_center"][0]), float(reply["zone_center"][1])]
        if isinstance(reply.get("station"), list) and len(reply["station"]) == 2:
            self.station = [float(reply["station"][0]), float(reply["station"][1])]
        self.is_takeover = bool(reply.get("is_takeover", False))
        if reply.get("lat") is not None and reply.get("lon") is not None:
            self.lat = float(reply["lat"])
            self.lon = float(reply["lon"])
        else:
            self.lat, self.lon = self.zone_center[0], self.zone_center[1]
        if reply.get("alt") is not None:
            self.alt = float(reply["alt"])
        if reply.get("battery") is not None:
            self.battery = min(100.0, max(0.0, float(reply["battery"])))
        if reply.get("battery_capacity_j") is not None:
            self.battery_capacity_j = max(1000.0, float(reply["battery_capacity_j"]))
        if reply.get("power_w") is not None:
            self.power_w = max(1.0, float(reply["power_w"]))
        if reply.get("state"):
            st = str(reply["state"]).upper()
            self.state = st if st in ("PATROLLING", "TRANSITING", "RETURNING", "CHARGING", "STANDBY", "RESPONDING", "CONTAINING", "OFFLINE") else "PATROLLING"
        if reply.get("paused") is not None:
            self.paused = bool(reply["paused"])
        if reply.get("time_scale") is not None:
            self.time_scale = max(0.1, float(reply["time_scale"]))
        if reply.get("rf_link"):
            self.rf_link = str(reply["rf_link"])
        if isinstance(reply.get("rf_hops"), list):
            self.rf_hops = [str(x) for x in reply["rf_hops"]]
        if reply.get("rf_delay_ms") is not None:
            self.rf_delay_ms = float(reply["rf_delay_ms"])
        if reply.get("rf_per") is not None:
            self.rf_per = float(reply["rf_per"])
        m_lat = 111320.0
        m_lon = 111320.0 * max(0.2, math.cos(math.radians(self.zone_center[0])))
        self._orbit_angle = math.atan2(
            (self.lat - self.zone_center[0]) * m_lat,
            (self.lon - self.zone_center[1]) * m_lon,
        ) % (2.0 * math.pi)
        return reply

    def send_corrupted_udp_test(self):
        """Gửi 1 datagram UDP cố ý đảo 1 bit lỗi CRC32 để kiểm chứng phát hiện lỗi phía Server."""
        if self._udp_sock is None:
            return
        body = {"cmd": "TELEMETRY", "uav_id": self.uav_id, "zone_id": self.zone_id, "test_crc": True}
        payload_bytes = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        pkt = NetworkPacket(
            src_ip=self.ip_address,
            dst_ip=self.dst_ip,
            protocol="UDP",
            priority=1,
            seq_num=self._next_seq(),
            ack_num=0,
            ttl=16,
            flags=FLAG_DATA,
            payload=payload_bytes,
        )
        bad_raw = NetworkPacket.corrupt_wire_bytes(pkt.encode())
        self._udp_sock.sendto(bad_raw, (self.host, self.udp_port))
        self.udp_tx_count += 1

    def _handle_server_command(self, payload_obj: dict[str, Any]):
        """Xử lý lệnh đẩy bất đồng bộ từ Server (mô hình ThreadedHandler 2 chiều)."""
        action = str(payload_obj.get("action", "")).upper()
        should_push_telem = action in ("RTB", "SET_BATTERY", "DISPATCH_FIRE", "RESUME_PATROL")
        async_msg: Optional[str] = None
        with self._state_lock:
            self.server_cmd_rx_count += 1
            self.last_server_cmd = dict(payload_obj)
            if action == "RTB":
                self._fire_target = None
                self.state = "RETURNING"
                self.speed = 45.0
                async_msg = f"[SERVER -> CLIENT] Nhận lệnh RTB -> {self._prompt_label()} đang bay về Trạm sạc pin."
            elif action == "SET_BATTERY" and payload_obj.get("battery") is not None:
                new_bat = min(100.0, max(0.0, float(payload_obj["battery"])))
                self.battery = new_bat
                if new_bat <= 0.0:
                    self._fire_target = None
                    self.state = "OFFLINE"
                    self.speed = 0.0
                elif new_bat < 25.0:
                    self._fire_target = None
                    self.state = "RETURNING"
                    self.speed = 45.0
                elif new_bat >= 100.0:
                    self._fire_target = None
                    dist_st = _haversine_m(self.lat, self.lon, self.station[0], self.station[1])
                    if dist_st <= 120.0:
                        self.state = "STANDBY"
                        self.speed = 0.0
                    else:
                        self.state = "PATROLLING"
                        self.speed = 40.0
                else:
                    if self.state == "OFFLINE":
                        dist_st = _haversine_m(self.lat, self.lon, self.station[0], self.station[1])
                        if dist_st <= 120.0:
                            self.state = "CHARGING"
                            self.speed = 0.0
                        else:
                            self.state = "PATROLLING"
                            self.speed = 40.0
                async_msg = f"[SERVER -> CLIENT] Nhận lệnh SET_BATTERY={new_bat:.1f}% -> State={self.state}."
            elif action == "DISPATCH_FIRE" and payload_obj.get("lat") is not None and payload_obj.get("lon") is not None:
                self._fire_target = (float(payload_obj["lat"]), float(payload_obj["lon"]))
                self.state = "RESPONDING"
                self.speed = 50.0
                async_msg = (
                    f"[SERVER -> CLIENT] Nhận lệnh DISPATCH_FIRE tới "
                    f"({self._fire_target[0]:.4f}, {self._fire_target[1]:.4f})."
                )
            elif action == "RESUME_PATROL":
                if payload_obj.get("zone_id"):
                    self.zone_id = str(payload_obj["zone_id"])
                zc = payload_obj.get("zone_center")
                if isinstance(zc, list) and len(zc) == 2:
                    self.zone_center = [float(zc[0]), float(zc[1])]
                self._fire_target = None
                dist_z = _haversine_m(self.lat, self.lon, self.zone_center[0], self.zone_center[1])
                if dist_z > 500.0:
                    self.state = "TRANSITING"
                    self.speed = 45.0
                else:
                    self.state = "PATROLLING"
                    self.speed = 40.0
                async_msg = f"[SERVER -> CLIENT] Nhận lệnh RESUME_PATROL -> {self.state} tại {self.zone_id}."
            elif action == "SET_CLOCK":
                if payload_obj.get("paused") is not None:
                    self.paused = bool(payload_obj["paused"])
                if payload_obj.get("time_scale") is not None:
                    self.time_scale = max(0.1, float(payload_obj["time_scale"]))
                async_msg = f"[SERVER -> CLIENT] Đồng bộ đồng hồ: Paused={self.paused}, Scale={self.time_scale}x."
            elif action == "RF_STATUS":
                new_link = str(payload_obj.get("link") or self.rf_link)
                changed = (new_link != self.rf_link)
                self.rf_link = new_link
                if isinstance(payload_obj.get("hops"), list):
                    self.rf_hops = [str(x) for x in payload_obj["hops"]]
                if payload_obj.get("delay_ms") is not None:
                    self.rf_delay_ms = float(payload_obj["delay_ms"])
                if payload_obj.get("per") is not None:
                    self.rf_per = float(payload_obj["per"])
                if changed:
                    async_msg = (
                        f"[RF_GATEWAY] Tuyến SAGSINS chuyển sang {self.rf_link} "
                        f"({max(0, len(self.rf_hops) - 1)} hops, delay={self.rf_delay_ms:.2f} ms, PER={self.rf_per:.4f})."
                    )
            elif action == "PING":
                async_msg = "[SERVER -> CLIENT] Nhận lệnh PING kiểm tra kết nối từ Server."
        if async_msg:
            self._print_async(async_msg)
        if should_push_telem:
            try:
                self.send_udp_telemetry()
            except OSError:
                pass

    def _tcp_reader_loop(self):
        """Luồng đọc TCP nền (Client-TCP-Reader): nhận ACK, nhận lệnh đẩy 2 chiều từ Server và phát hiện EOF ngay."""
        sock = self._tcp_sock
        if sock is None:
            return
        try:
            sock.settimeout(0.5)
        except OSError:
            return
        while self._running and self._tcp_sock is not None:
            try:
                len_hdr = _recv_exact(sock, 4)
                if not len_hdr:
                    raise ConnectionError("Server closed TCP connection (EOF)")
                frame_len = struct.unpack("!I", len_hdr)[0]
                if frame_len <= 0 or frame_len > 65536 + 64:
                    raise ConnectionError("Invalid TCP frame length")
                frame = _recv_exact(sock, frame_len)
                if not frame:
                    raise ConnectionError("Incomplete TCP frame (EOF)")
            except socket.timeout:
                continue
            except OSError:
                if self._running:
                    self.server_disconnected = True
                    self._running = False
                    self._print_async("[CLIENT_NET] Mất kết nối TCP với Server (Server đã đóng hoặc ngắt mạng).")
                break

            pkt, crc_ok, _ = NetworkPacket.decode_and_verify(frame)
            if pkt is not None and (pkt.flags & FLAG_ACK):
                with self._pending_lock:
                    waiter = self._pending_acks.pop(pkt.ack_num, None)
                if waiter is not None:
                    ev, box = waiter
                    if not crc_ok:
                        box.append({"_crc_error": True})
                    else:
                        self.tcp_ack_count += 1
                        if pkt.payload:
                            try:
                                box.append(json.loads(pkt.payload.decode("utf-8")))
                            except (UnicodeDecodeError, json.JSONDecodeError):
                                box.append({"ok": True})
                        else:
                            box.append({"ok": True})
                    ev.set()
            elif pkt is not None and crc_ok and pkt.payload:
                try:
                    payload_obj = json.loads(pkt.payload.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    payload_obj = None
                if isinstance(payload_obj, dict) and str(payload_obj.get("cmd", "")).upper() == "SERVER_CMD":
                    self._handle_server_command(payload_obj)

        with self._pending_lock:
            for _, (ev, box) in list(self._pending_acks.items()):
                box.append({"_conn_error": True, "error": "Server disconnected"})
                ev.set()
            self._pending_acks.clear()

    def start_background(self):
        if self._running:
            return
        self._running = True
        self._tcp_reader_thread = threading.Thread(
            target=self._tcp_reader_loop,
            name="Client-TCP-Reader",
            daemon=True,
        )
        self._telemetry_thread = threading.Thread(
            target=self._flight_and_telemetry_loop,
            name="Client-UDP-Telemetry",
            daemon=True,
        )
        self._tcp_reader_thread.start()
        self._telemetry_thread.start()

    def stop(self, graceful: bool = True):
        if graceful and self._tcp_sock is not None and not self.server_disconnected:
            try:
                self.send_tcp_command("LEAVE")
            except Exception:
                pass
        self._running = False
        for th_attr in ("_telemetry_thread", "_tcp_reader_thread"):
            th = getattr(self, th_attr, None)
            if th is not None and th.is_alive():
                th.join(timeout=1.5)
            setattr(self, th_attr, None)
        for attr in ("_tcp_sock", "_udp_sock"):
            s = getattr(self, attr, None)
            if s is not None:
                try:
                    s.close()
                except OSError:
                    pass
                setattr(self, attr, None)

    def step_flight(self, dt: float):
        """Cập nhật động học bay thực & vòng đời Pin (khớp PA 4 và đồng hồ Server) trong 1 bước `dt` giây."""
        with self._state_lock:
            if self.paused or self.state in ("OFFLINE", "STANDBY"):
                self.speed = 0.0
                return

            eff_dt = max(0.0, float(dt)) * self.time_scale
            drain_pct = (self.power_w * eff_dt / max(1000.0, self.battery_capacity_j)) * 100.0

            if self.state == "RESPONDING" and self._fire_target is not None:
                t_lat, t_lon = self._fire_target
                dist_m = _haversine_m(self.lat, self.lon, t_lat, t_lon)
                if dist_m <= 135.0:
                    self.state = "CONTAINING"
                    self.speed = 40.0
                    m_lat = 111320.0
                    m_lon = 111320.0 * max(0.2, math.cos(math.radians(t_lat)))
                    self._orbit_angle = math.atan2(
                        (self.lat - t_lat) * m_lat,
                        (self.lon - t_lon) * m_lon,
                    ) % (2.0 * math.pi)
                else:
                    step_m = min(dist_m, 50.0 * eff_dt)
                    ratio = step_m / dist_m
                    self.lat += (t_lat - self.lat) * ratio
                    self.lon += (t_lon - self.lon) * ratio
                    self.speed = 50.0
                self.alt = 1050.0
                self.battery = max(0.0, self.battery - 1.15 * drain_pct)
                if self.battery < 25.0:
                    self._fire_target = None
                    self.state = "RETURNING"
                    self.speed = 45.0
            elif self.state == "CONTAINING" and self._fire_target is not None:
                t_lat, t_lon = self._fire_target
                m_lat = 111320.0
                m_lon = 111320.0 * max(0.2, math.cos(math.radians(t_lat)))
                self._orbit_angle = (self._orbit_angle + (40.0 * eff_dt / 130.0)) % (2.0 * math.pi)
                tgt_lat = t_lat + (130.0 * math.sin(self._orbit_angle)) / m_lat
                tgt_lon = t_lon + (130.0 * math.cos(self._orbit_angle)) / m_lon
                d_step = _haversine_m(self.lat, self.lon, tgt_lat, tgt_lon)
                if d_step > 1e-3:
                    mv = min(d_step, 40.0 * eff_dt)
                    r = mv / d_step
                    self.lat += (tgt_lat - self.lat) * r
                    self.lon += (tgt_lon - self.lon) * r
                self.alt = 1050.0
                self.speed = 40.0
                self.battery = max(0.0, self.battery - drain_pct)
                if self.battery < 25.0:
                    self._fire_target = None
                    self.state = "RETURNING"
                    self.speed = 45.0
            elif self.state == "TRANSITING":
                dist_m = _haversine_m(self.lat, self.lon, self.zone_center[0], self.zone_center[1])
                if dist_m <= 450.0:
                    m_lat = 111320.0
                    m_lon = 111320.0 * max(0.2, math.cos(math.radians(self.zone_center[0])))
                    self._orbit_angle = math.atan2(
                        (self.lat - self.zone_center[0]) * m_lat,
                        (self.lon - self.zone_center[1]) * m_lon,
                    ) % (2.0 * math.pi)
                    self.alt = 1050.0
                    self.state = "PATROLLING"
                    self.speed = 40.0
                else:
                    step_m = min(dist_m, 45.0 * eff_dt)
                    ratio = step_m / dist_m
                    self.lat += (self.zone_center[0] - self.lat) * ratio
                    self.lon += (self.zone_center[1] - self.lon) * ratio
                    self.alt += (1050.0 - self.alt) * min(1.0, ratio * 2.5)
                    self.speed = 45.0
                self.battery = max(0.0, self.battery - drain_pct)
                if self.battery < 25.0:
                    self.state = "RETURNING"
                    self.speed = 45.0
            elif self.state == "PATROLLING":
                self._orbit_angle = (self._orbit_angle + (40.0 * eff_dt / 420.0)) % (2.0 * math.pi)
                m_lat = 111320.0
                m_lon = 111320.0 * max(0.2, math.cos(math.radians(self.zone_center[0])))
                tgt_lat = self.zone_center[0] + (380.0 * math.sin(self._orbit_angle)) / m_lat
                tgt_lon = self.zone_center[1] + (460.0 * math.cos(self._orbit_angle) + 120.0 * math.sin(2.0 * self._orbit_angle)) / m_lon
                d_step = _haversine_m(self.lat, self.lon, tgt_lat, tgt_lon)
                if d_step > 1e-3:
                    mv = min(d_step, 40.0 * eff_dt)
                    r = mv / d_step
                    self.lat += (tgt_lat - self.lat) * r
                    self.lon += (tgt_lon - self.lon) * r
                self.alt = 1050.0
                self.speed = 40.0
                self.battery = max(0.0, self.battery - drain_pct)
                if self.battery < 25.0:
                    self.state = "RETURNING"
                    self.speed = 45.0
            elif self.state == "RETURNING":
                dist_m = _haversine_m(self.lat, self.lon, self.station[0], self.station[1])
                if dist_m <= 100.0:
                    self.lat, self.lon, self.alt, self.speed = self.station[0], self.station[1], 25.0, 0.0
                    self.state = "CHARGING"
                else:
                    step_m = min(dist_m, 45.0 * eff_dt)
                    ratio = step_m / dist_m
                    self.lat += (self.station[0] - self.lat) * ratio
                    self.lon += (self.station[1] - self.lon) * ratio
                    self.alt += (25.0 - self.alt) * ratio
                    self.speed = 45.0
                self.battery = max(1.0, self.battery - drain_pct)
            elif self.state == "CHARGING":
                self.lat, self.lon, self.alt, self.speed = self.station[0], self.station[1], 25.0, 0.0
                self.battery = min(100.0, self.battery + 15.0 * eff_dt)
                if self.battery >= 100.0:
                    self.battery = 100.0
                    self.state = "STANDBY"
                    self.speed = 0.0

    def _flight_and_telemetry_loop(self):
        interval = 0.8
        while self._running:
            try:
                self.step_flight(interval)
                self.send_udp_telemetry()
            except Exception:
                break
            time.sleep(interval)

    def trigger_fire(self) -> dict[str, Any]:
        with self._state_lock:
            f_lat, f_lon = self.lat, self.lon
        res = self.send_tcp_command("FIRE_ALERT", {"lat": f_lat, "lon": f_lon})
        if res.get("ok"):
            with self._state_lock:
                if res.get("handover_p2p"):
                    self._fire_target = None
                    self.state = "RETURNING"
                    self.speed = 45.0
                else:
                    self._fire_target = (f_lat, f_lon)
                    self.state = "CONTAINING"
                    self.speed = 40.0
                if res.get("rf_link"):
                    self.rf_link = str(res["rf_link"])
                if isinstance(res.get("rf_hops"), list):
                    self.rf_hops = [str(x) for x in res["rf_hops"]]
                if res.get("rf_delay_ms") is not None:
                    self.rf_delay_ms = float(res["rf_delay_ms"])
            self.send_udp_telemetry()
        return res

    def trigger_low_battery(self):
        with self._state_lock:
            self._fire_target = None
            self.battery = 18.0
            self.state = "RETURNING"
            self.speed = 45.0
        self.send_udp_telemetry()

    def trigger_full_charge(self) -> dict[str, Any]:
        with self._state_lock:
            self._fire_target = None
            self.battery = 100.0
            dist_z = _haversine_m(self.lat, self.lon, self.zone_center[0], self.zone_center[1])
            if dist_z > 500.0:
                self.state = "TRANSITING"
                self.speed = 45.0
            else:
                self.state = "PATROLLING"
                self.speed = 40.0
            st = self.state
        self.send_udp_telemetry()
        return {"ok": True, "battery": 100.0, "state": st}

    def route_text(self) -> str:
        with self._state_lock:
            hop_cnt = max(0, len(self.rf_hops) - 1)
            short_hops = [
                h.replace("MÁY_BAY_", "").replace("TRẠM_KIỂM_LÂM_", "").replace("VỆ_TINH_", "")
                for h in self.rf_hops
            ]
            path_str = " -> ".join(short_hops) if short_hops else "(Chưa có tuyến)"
            return (
                f"Kênh: {self.rf_link} | Số chặng: {hop_cnt} | Trễ E2E: {self.rf_delay_ms:.2f} ms | "
                f"PER: {self.rf_per * 100:.2f}%\n"
                f"Lộ trình: {path_str}"
            )

    def format_status_box(self) -> str:
        with self._state_lock:
            mode_str = "TIẾP QUẢN (TAKEOVER)" if self.is_takeover else "MỞ RỘNG (EXTRA)"
            hop_cnt = max(0, len(self.rf_hops) - 1)
            clk_str = "TẠM DỪNG" if self.paused else f"{self.time_scale:.1f}x"
            short_hops = [
                h.replace("MÁY_BAY_", "").replace("TRẠM_KIỂM_LÂM_", "").replace("VỆ_TINH_", "")
                for h in self.rf_hops
            ]
            path_str = " -> ".join(short_hops) if short_hops else "Đang cập nhật"
            return (
                f"--- TRẠNG THÁI {self._prompt_label()} ({mode_str}) ---\n"
                f"  Tiến trình & Mạng : PID={self.pid} | IP={self.ip_address} | Vùng={self.zone_id} | Kết nối={'OK' if not self.server_disconnected else 'MẤT KẾT NỐI'}\n"
                f"  Bay & Năng lượng  : State={self.state} ({self.speed:.0f} m/s) | Pin={self.battery:.1f}% ({self.battery_capacity_j/1000.0:.0f} kJ) | Pos=({self.lat:.5f}, {self.lon:.5f})\n"
                f"  Tuyến vô tuyến RF : {self.rf_link} ({hop_cnt} chặng, {self.rf_delay_ms:.2f} ms, PER={self.rf_per*100:.2f}%) | {path_str}\n"
                f"  Gói tin & Đồng hồ : UDP_TX={self.udp_tx_count} | TCP_TX={self.tcp_tx_count} (ACK={self.tcp_ack_count}) | ServerCmd_RX={self.server_cmd_rx_count} | Clock={clk_str}"
            )

    def status_text(self) -> str:
        with self._state_lock:
            mode_str = "TAKEOVER" if self.is_takeover else "EXTRA"
            hop_cnt = max(0, len(self.rf_hops) - 1)
            clk_str = "PAUSED" if self.paused else f"{self.time_scale:.1f}x"
            return (
                f"PID={self.pid} | ID={self.uav_id} ({mode_str}) | IP={self.ip_address} | Zone={self.zone_id} | "
                f"State={self.state} | Pin={self.battery:.1f}% ({self.battery_capacity_j/1000.0:.0f}kJ) | "
                f"Pos=({self.lat:.5f}, {self.lon:.5f}) | RF={self.rf_link} ({hop_cnt} hops, {self.rf_delay_ms:.1f}ms) | "
                f"Clock={clk_str} | UDP_TX={self.udp_tx_count} | TCP_TX={self.tcp_tx_count} (ACK={self.tcp_ack_count}) | "
                f"ServerCmd_RX={self.server_cmd_rx_count} | Connected={not self.server_disconnected}"
            )


def _print_control_menu(client: StandaloneUAVClient):
    short_id = client._prompt_label()
    mode_lbl = "TIẾP QUẢN ĐỘI HÌNH" if client.is_takeover else "MỞ RỘNG NGOÀI ĐỘI HÌNH"
    leave_desc = "trả quyền bay tự động về Server" if client.is_takeover else "ngắt nút khỏi bản đồ"
    print("\n======================================================================")
    print(f" BẢNG ĐIỀU KHIỂN UAV CLIENT | {short_id} ({mode_lbl})")
    print(f" PID={client.pid} | IP={client.ip_address} | Vùng={client.zone_id} | Pin={client.battery:.1f}% ({client.battery_capacity_j/1000.0:.0f}kJ) | RF={client.rf_link}")
    print("----------------------------------------------------------------------")
    print("  [f] Giả lập cảm biến phát hiện cháy tại vị trí bay (TCP FIRE_ALERT)")
    print("  [b] Giả lập pin yếu 18% & bay về Trạm sạc (RTB -> CHARGING)")
    print("  [c] Giả lập nạp đầy 100% & bay về vùng (kiểm thử phục hồi pin)")
    print("  [e] Gửi thử 1 gói UDP lỗi bit CRC32 (kiểm lỗi phía Server)")
    print("  [r] Xem chi tiết tuyến vô tuyến SAGSINS (A2G / FANET / LEO)")
    print("  [s] Xem trạng thái tiến trình, Socket & Pin")
    print("  [h] Hiện lại bảng điều khiển")
    print(f"  [q] Rời mạng lịch sự ({leave_desc}) | Ctrl+C: Rớt mạng đột ngột")
    print("======================================================================\n")


def main():
    parser = argparse.ArgumentParser(description="SAGSINS Standalone UAV Client (PBL4 Client-Server)")
    parser.add_argument("--host", default="127.0.0.1", help="IP máy chủ Server (mặc định 127.0.0.1)")
    parser.add_argument("--tcp-port", type=int, default=9000, help="Cổng TCP Server (mặc định 9000)")
    parser.add_argument("--udp-port", type=int, default=9001, help="Cổng UDP Server (mặc định 9001)")
    parser.add_argument("--id", default="UAV_09", help="Định danh UAV tiếp quản hoặc UAV mới (mặc định: UAV_09; Z7: UAV_25)")
    parser.add_argument("--zone", default="", help="Phân khu nếu tạo UAV mới ngoài đội hình (mặc định lấy theo UAV trên Server)")
    parser.add_argument("--battery", type=float, default=90.0, help="Mức pin khởi tạo (%%)")
    parser.add_argument("--non-interactive", action="store_true", help="Chạy nền không mở menu nhập phím")
    args = parser.parse_args()

    client = StandaloneUAVClient(
        host=args.host,
        tcp_port=args.tcp_port,
        udp_port=args.udp_port,
        uav_id=args.id,
        zone_id=args.zone,
        battery=args.battery,
    )
    print(f"[UAV_CLIENT] Đang kết nối TCP tới {args.host}:{args.tcp_port} (PID={client.pid})...")
    try:
        ack = client.connect()
    except (OSError, RuntimeError) as exc:
        print(
            f"[LỖI KẾT NỐI] Không thể kết nối TCP tới {args.host}:{args.tcp_port} ({exc}). "
            "Hãy chắc chắn Server main.py đang chạy và kiểm tra IP/Firewall."
        )
        client.stop(graceful=False)
        sys.exit(1)
    mode_lbl = "Tiếp quản UAV đội hình" if client.is_takeover else "UAV mở rộng ngoài đội hình"
    print(
        f"[UAV_CLIENT] Kết nối thành công ({mode_lbl})! ID={client.uav_id} | IP={client.ip_address} | "
        f"Zone={client.zone_id} | Pin={client.battery:.1f}% ({client.battery_capacity_j/1000.0:.0f}kJ) | "
        f"RF={client.rf_link} | UDP={args.host}:{client.udp_port} | ACK={ack.get('cmd')}"
    )
    client.interactive_prompt = not args.non_interactive
    client.start_background()

    if not args.non_interactive:
        _print_control_menu(client)

    try:
        if args.non_interactive:
            while not client.server_disconnected:
                time.sleep(1.0)
        else:
            prompt_lbl = client._prompt_label()
            while not client.server_disconnected:
                cmd = input(f"{prompt_lbl}> ").strip().lower()
                if not cmd:
                    continue
                if cmd == "f":
                    cur_st = client.state
                    if cur_st != "PATROLLING":
                        reason_map = {
                            "OFFLINE": "UAV đang sập nguồn 0% (OFFLINE). Hãy bấm [c] hoặc 'Sạc 100%' trên Web trước.",
                            "STANDBY": "UAV đang trực chiến tại Trạm Kiểm Lâm, chưa bay ra vùng rừng tuần tra.",
                            "CHARGING": "UAV đang sạc pin tại Trạm Kiểm Lâm, chưa thể quét cháy rừng.",
                            "TRANSITING": "UAV đang bay chuyển tiếp từ Trạm ra phân khu, chưa vào vùng tuần tra.",
                            "RETURNING": "UAV đang bay về Trạm do pin yếu (< 25%), không thể nhận nhiệm vụ cháy mới.",
                            "RESPONDING": "UAV đang cơ động xử lý một đám cháy khác.",
                            "CONTAINING": "UAV đang khoanh vùng đám cháy hiện tại (130 m).",
                        }
                        print(f"[CẢNH BÁO] {reason_map.get(cur_st, f'Trạng thái hiện tại ({cur_st}) chưa sẵn sàng báo cháy.')}")
                        continue
                    try:
                        res = client.trigger_fire()
                        if res.get("ok"):
                            if res.get("handover_p2p"):
                                print(
                                    f"[TCP -> FIRE_ACK] Đã báo cháy ({client.lat:.4f}, {client.lon:.4f}) về Trạm "
                                    f"| Pin khứ hồi yếu ({res.get('pin_khu_hoi_pct')}% <= 25%) -> Đã bàn giao P2P cho đồng đội, UAV quay về sạc!"
                                )
                            else:
                                print(
                                    f"[TCP -> FIRE_ACK] Báo cháy & khóa mục tiêu tại ({client.lat:.4f}, {client.lon:.4f}) "
                                    f"| Pin khứ hồi={res.get('pin_khu_hoi_pct')}% | Kênh={res.get('rf_link')} | "
                                    f"Chặng={res.get('rf_hop_count')} | Trễ={res.get('rf_delay_ms')} ms"
                                )
                        else:
                            print(f"[RF_GATEWAY -> FIRE_NACK] Không gửi được báo cháy: {res.get('error')}")
                    except (OSError, RuntimeError, TimeoutError) as exc:
                        print(f"[LỖI TCP] Không thể gửi lệnh tới Server: {exc}")
                elif cmd == "b":
                    if client.state == "OFFLINE":
                        print("[CẢNH BÁO] UAV đang sập nguồn 0% (OFFLINE). Không thể bay về Trạm.")
                        continue
                    client.trigger_low_battery()
                    print("[CLIENT] Pin đặt về 18% -> Đang bay thẳng về Trạm (45 m/s) để sạc pin...")
                elif cmd == "c":
                    res = client.trigger_full_charge()
                    st_new = res.get("state")
                    hanh_vi = "đang bay chuyển tiếp về vùng" if st_new == "TRANSITING" else "đang tuần tra tại vùng"
                    print(f"[UDP -> TELEMETRY] Đã nạp pin 100% ({hanh_vi}, State={st_new}).")
                elif cmd == "e":
                    client.send_corrupted_udp_test()
                    print("[UDP -> CRC32_TEST] Đã gửi 1 gói UDP lỗi bit tới Server (kiểm tra EXT_UDP_CRC_ERR trên Dashboard).")
                elif cmd == "r":
                    print(f"[RF_GATEWAY] {client.route_text()}")
                elif cmd == "s":
                    print(client.format_status_box())
                elif cmd == "h":
                    _print_control_menu(client)
                elif cmd == "q":
                    if client.is_takeover:
                        print(f"[UAV_CLIENT] Đang gửi TCP LEAVE để trả quyền bay {prompt_lbl} về Server...")
                    else:
                        print("[UAV_CLIENT] Đang gửi TCP LEAVE và ngắt kết nối...")
                    client.stop(graceful=True)
                    break
                else:
                    print(f"[CLIENT] Lệnh '{cmd}' không hợp lệ. Gõ [h] để xem danh sách phím.")
    except (KeyboardInterrupt, EOFError):
        print("\n[UAV_CLIENT] Đã dừng tiến trình Client đột ngột.")
        client.stop(graceful=False)


if __name__ == "__main__":
    main()
