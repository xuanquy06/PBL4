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
        uav_id: str = "UAV_EXT_01",
        zone_id: str = "ZONE_03",
        battery: float = 90.0,
    ):
        self.host = host
        self.tcp_port = int(tcp_port)
        self.udp_port = int(udp_port)
        self.raw_id = str(uav_id).strip()
        self.uav_id = self.raw_id if self.raw_id.startswith("MÁY_BAY_") else f"MÁY_BAY_{self.raw_id}"
        self.zone_id = str(zone_id).strip()
        self.pid = os.getpid()

        self.ip_address = assign_node_ip(self.uav_id, "MÁY_BAY", self.zone_id)
        self.dst_ip = "10.0.0.1"

        self.zone_center = [16.125, 108.255]
        self.station = [16.105, 108.250]
        self.lat = self.zone_center[0]
        self.lon = self.zone_center[1]
        self.alt = 1050.0
        self.speed = 40.0
        self.battery = min(100.0, max(5.0, float(battery)))
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
            "zone_id": self.zone_id,
            "pid": self.pid,
        }
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
            body = {
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
        if reply.get("uav_id"):
            self.uav_id = str(reply["uav_id"])
        if reply.get("assigned_ip"):
            self.ip_address = str(reply["assigned_ip"])
        if reply.get("udp_port"):
            self.udp_port = int(reply["udp_port"])
        if isinstance(reply.get("zone_center"), list) and len(reply["zone_center"]) == 2:
            self.zone_center = [float(reply["zone_center"][0]), float(reply["zone_center"][1])]
            self.lat, self.lon = self.zone_center[0], self.zone_center[1]
        if isinstance(reply.get("station"), list) and len(reply["station"]) == 2:
            self.station = [float(reply["station"][0]), float(reply["station"][1])]
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
        with self._state_lock:
            self.server_cmd_rx_count += 1
            self.last_server_cmd = dict(payload_obj)
            if action == "RTB":
                self._fire_target = None
                self.state = "RETURNING"
                self.speed = 45.0
                print(f"\n[SERVER -> CLIENT] Nhận lệnh RTB -> {self.uav_id} đang bay về Trạm sạc pin.")
            elif action == "SET_BATTERY" and payload_obj.get("battery") is not None:
                new_bat = min(100.0, max(0.0, float(payload_obj["battery"])))
                self.battery = new_bat
                if new_bat < 25.0:
                    self._fire_target = None
                    self.state = "RETURNING"
                    self.speed = 45.0
                elif new_bat >= 100.0:
                    self._fire_target = None
                    self.state = "PATROLLING"
                    self.speed = 40.0
                print(f"\n[SERVER -> CLIENT] Nhận lệnh SET_BATTERY={new_bat:.1f}% -> State={self.state}.")
            elif action == "DISPATCH_FIRE" and payload_obj.get("lat") is not None and payload_obj.get("lon") is not None:
                self._fire_target = (float(payload_obj["lat"]), float(payload_obj["lon"]))
                self.state = "RESPONDING"
                self.speed = 50.0
                print(
                    f"\n[SERVER -> CLIENT] Nhận lệnh DISPATCH_FIRE tới "
                    f"({self._fire_target[0]:.4f}, {self._fire_target[1]:.4f})."
                )
            elif action == "RESUME_PATROL":
                self._fire_target = None
                self.state = "PATROLLING"
                self.speed = 40.0
                print(f"\n[SERVER -> CLIENT] Nhận lệnh RESUME_PATROL -> quay lại tuần tra {self.zone_id}.")
            elif action == "PING":
                print(f"\n[SERVER -> CLIENT] Nhận lệnh PING kiểm tra kết nối từ Server.")
        if action in ("RTB", "SET_BATTERY", "DISPATCH_FIRE", "RESUME_PATROL"):
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
                    print("\n[CLIENT_NET] Mất kết nối TCP với Server (Server đã đóng hoặc ngắt mạng).")
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
        """Cập nhật động học bay & vòng đời Pin của Client trong 1 bước `dt` giây."""
        with self._state_lock:
            if self.state == "RESPONDING" and self._fire_target is not None:
                t_lat, t_lon = self._fire_target
                dist_m = _haversine_m(self.lat, self.lon, t_lat, t_lon)
                if dist_m <= 130.0:
                    self._orbit_angle = (self._orbit_angle + 0.14 * dt) % (2.0 * math.pi)
                    m_lat = 111320.0
                    m_lon = 111320.0 * max(0.2, math.cos(math.radians(t_lat)))
                    self.lat = t_lat + (130.0 * math.sin(self._orbit_angle)) / m_lat
                    self.lon = t_lon + (130.0 * math.cos(self._orbit_angle)) / m_lon
                else:
                    step_m = min(dist_m, 140.0 * dt)
                    ratio = step_m / dist_m
                    self.lat += (t_lat - self.lat) * ratio
                    self.lon += (t_lon - self.lon) * ratio
                self.alt = 1050.0
                self.speed = 50.0
                self.battery = max(5.0, self.battery - 0.45 * dt)
                if self.battery < 25.0:
                    self._fire_target = None
                    self.state = "RETURNING"
                    self.speed = 45.0
            elif self.state == "PATROLLING":
                self._orbit_angle = (self._orbit_angle + 0.085 * dt) % (2.0 * math.pi)
                radius_m = 450.0
                m_lat = 111320.0
                m_lon = 111320.0 * max(0.2, math.cos(math.radians(self.zone_center[0])))
                self.lat = self.zone_center[0] + (radius_m * math.sin(self._orbit_angle)) / m_lat
                self.lon = self.zone_center[1] + (radius_m * math.cos(self._orbit_angle)) / m_lon
                self.alt = 1050.0
                self.speed = 40.0
                self.battery = max(5.0, self.battery - 0.35 * dt)
                if self.battery < 25.0:
                    self.state = "RETURNING"
                    self.speed = 45.0
            elif self.state == "RETURNING":
                dist_m = _haversine_m(self.lat, self.lon, self.station[0], self.station[1])
                if dist_m <= 100.0:
                    self.lat, self.lon, self.alt, self.speed = self.station[0], self.station[1], 25.0, 0.0
                    self.state = "CHARGING"
                else:
                    step_m = min(dist_m, 120.0 * dt)
                    ratio = step_m / dist_m
                    self.lat += (self.station[0] - self.lat) * ratio
                    self.lon += (self.station[1] - self.lon) * ratio
                    self.alt += (25.0 - self.alt) * ratio
                    self.speed = 45.0
            elif self.state == "CHARGING":
                self.lat, self.lon, self.alt, self.speed = self.station[0], self.station[1], 25.0, 0.0
                self.battery = min(100.0, self.battery + 15.0 * dt)
                if self.battery >= 100.0:
                    self.battery = 100.0
                    self.lat, self.lon, self.alt = self.zone_center[0], self.zone_center[1], 1050.0
                    self.state = "PATROLLING"
                    self.speed = 40.0

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
        return self.send_tcp_command("FIRE_ALERT", {"lat": f_lat, "lon": f_lon})

    def trigger_low_battery(self):
        with self._state_lock:
            self.battery = 18.0
            self.state = "RETURNING"
            self.speed = 45.0
        self.send_udp_telemetry()

    def trigger_full_charge(self) -> dict[str, Any]:
        with self._state_lock:
            self.battery = 100.0
            self.lat, self.lon, self.alt = self.zone_center[0], self.zone_center[1], 1050.0
            self.state = "PATROLLING"
        self.send_udp_telemetry()
        return {"ok": True, "battery": 100.0, "state": "PATROLLING"}

    def status_text(self) -> str:
        with self._state_lock:
            return (
                f"PID={self.pid} | ID={self.uav_id} | IP={self.ip_address} | Zone={self.zone_id} | "
                f"State={self.state} | Pin={self.battery:.1f}% | Pos=({self.lat:.5f}, {self.lon:.5f}) | "
                f"UDP_TX={self.udp_tx_count} | TCP_TX={self.tcp_tx_count} (ACK={self.tcp_ack_count}) | "
                f"ServerCmd_RX={self.server_cmd_rx_count} | Connected={not self.server_disconnected}"
            )


def main():
    parser = argparse.ArgumentParser(description="SAGSINS Standalone UAV Client (PBL4 Client-Server)")
    parser.add_argument("--host", default="127.0.0.1", help="IP máy chủ Server (mặc định 127.0.0.1)")
    parser.add_argument("--tcp-port", type=int, default=9000, help="Cổng TCP Server (mặc định 9000)")
    parser.add_argument("--udp-port", type=int, default=9001, help="Cổng UDP Server (mặc định 9001)")
    parser.add_argument("--id", default="UAV_EXT_01", help="Định danh UAV Client (VD: UAV_EXT_01)")
    parser.add_argument("--zone", default="ZONE_03", help="Phân khu tuần tra (ZONE_01..ZONE_07)")
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
    print(
        f"[UAV_CLIENT] Kết nối thành công! ID={client.uav_id} | IP={client.ip_address} | "
        f"UDP={args.host}:{client.udp_port} | ACK={ack.get('cmd')}"
    )
    client.start_background()

    print("\n--- BẢNG ĐIỀU KHIỂN UAV CLIENT ĐỘC LẬP ---")
    print("  [f] Báo cháy tại vị trí hiện tại (gửi TCP FIRE_ALERT)")
    print("  [b] Giả lập Pin yếu 18% (bay về Trạm sạc pin)")
    print("  [c] Sạc đầy 100% & quay lại vùng tuần tra")
    print("  [e] Gửi thử 1 gói UDP lỗi bit CRC32 (kiểm chứng phát hiện lỗi phía Server)")
    print("  [s] Xem trạng thái PID / Socket / Gói tin / Pin")
    print("  [q] Ngắt kết nối lịch sự (hoặc Ctrl+C để giả lập rớt mạng đột ngột)\n")

    try:
        if args.non_interactive:
            while not client.server_disconnected:
                time.sleep(1.0)
        else:
            while not client.server_disconnected:
                cmd = input("client> ").strip().lower()
                if cmd == "f":
                    try:
                        res = client.trigger_fire()
                        print(f"[TCP -> FIRE_ACK] Đã báo cháy tại ({client.lat:.4f}, {client.lon:.4f}) -> {res}")
                    except (OSError, RuntimeError, TimeoutError) as exc:
                        print(f"[LỖI TCP] Không thể gửi lệnh tới Server: {exc}")
                elif cmd == "b":
                    client.trigger_low_battery()
                    print("[CLIENT] Pin đặt về 18% -> Đang bay thẳng về Trạm để sạc pin...")
                elif cmd == "c":
                    res = client.trigger_full_charge()
                    print(f"[UDP -> TELEMETRY] Đã sạc 100% & quay lại tuần tra -> {res}")
                elif cmd == "e":
                    client.send_corrupted_udp_test()
                    print("[UDP -> CRC32_TEST] Đã gửi 1 gói UDP lỗi bit tới Server.")
                elif cmd == "s":
                    print(f"[STATUS] {client.status_text()}")
                elif cmd == "q":
                    print("[UAV_CLIENT] Đang gửi TCP LEAVE và ngắt kết nối...")
                    client.stop(graceful=True)
                    break
    except (KeyboardInterrupt, EOFError):
        print("\n[UAV_CLIENT] Đã dừng tiến trình Client đột ngột.")
        client.stop(graceful=False)


if __name__ == "__main__":
    main()
