"""
Tiến trình Client UAV Độc lập (Standalone Multi-Process UAV Client - Pure Python stdlib):
- Chạy như 1 tiến trình Hệ điều hành riêng biệt (PID độc lập) trên Máy 2 hoặc Terminal riêng.
- Giao tiếp Client-Server thuần qua TCP :9000 (JOIN / FIRE_ALERT / LEAVE / nhận SERVER_CMD)
  và UDP :9001 (gửi Telemetry 1s/lần đóng gói khung nhị phân 26B Header + CRC32).
- Tự chủ bay tuần tra trong đa giác phân khu Sơn Trà (Ray-Casting Point-in-Polygon) khi là PATROL
  hoặc trực chiến tại Trạm Kiểm Lâm khi là STANDBY.
"""
from __future__ import annotations

import argparse
from io import TextIOWrapper
import json
import math
import os
from pathlib import Path
import random
import socket
import struct
import sys
import threading
import time
from typing import Any, Optional

CLIENT_DIR = Path(__file__).resolve().parent
if str(CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(CLIENT_DIR))

from protocol import (
    FLAG_ACK,
    FLAG_DATA,
    NetworkPacket,
    assign_node_ip,
    recv_exact,
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


def _normalize_ring(ring: Any) -> list[list[float]]:
    """Chuẩn hóa danh sách đỉnh đa giác [[lon, lat], ...] an toàn từ Polygon/MultiPolygon."""
    if not isinstance(ring, list) or not ring:
        return []
    cur = ring
    while isinstance(cur, list) and cur and isinstance(cur[0], list) and cur[0] and isinstance(cur[0][0], list):
        cur = cur[0]
    return [
        [float(pt[0]), float(pt[1])]
        for pt in cur
        if isinstance(pt, (list, tuple)) and len(pt) >= 2 and isinstance(pt[0], (int, float)) and isinstance(pt[1], (int, float))
    ]


def _point_in_ring(lat: float, lon: float, ring: list[list[float]]) -> bool:
    """Thuật toán Ray-Casting kiểm tra điểm (lat, lon) nằm trong ranh giới đa giác [[lon, lat], ...]."""
    pts = _normalize_ring(ring)
    if len(pts) < 3:
        return True
    inside = False
    for idx, cur in enumerate(pts):
        prev = pts[idx - 1]
        x1, y1 = cur[0], cur[1]
        x2, y2 = prev[0], prev[1]
        if (y1 > lat) != (y2 > lat):
            if lon < ((x2 - x1) * (lat - y1) / (y2 - y1) + x1):
                inside = not inside
    return inside


def _point_inside_with_margin(lat: float, lon: float, ring: list[list[float]], margin_deg: float = 0.00025) -> bool:
    """Kiểm tra điểm (lat, lon) nằm gọn trong đa giác với khoảng lùi biên an toàn."""
    pts = _normalize_ring(ring)
    if len(pts) < 3:
        return True
    if not _point_in_ring(lat, lon, pts):
        return False
    for dlat, dlon in ((margin_deg, 0.0), (-margin_deg, 0.0), (0.0, margin_deg), (0.0, -margin_deg)):
        if not _point_in_ring(lat + dlat, lon + dlon, pts):
            return False
    return True


class StandaloneUAVClient:
    """Tiến trình Client UAV độc lập chạy bằng 100% thư viện chuẩn Python."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        tcp_port: int = 9000,
        udp_port: int = 9001,
        uav_id: str = "UAV_09",
        zone_id: str = "",
        battery: Optional[float] = None,
        managed: bool = False,
    ):
        self.host = host
        self.tcp_port = int(tcp_port)
        self.udp_port = int(udp_port)
        self.raw_id = str(uav_id).strip() or "UAV_09"
        self.uav_id = self.raw_id if self.raw_id.startswith("MÁY_BAY_") else f"MÁY_BAY_{self.raw_id}"
        self.zone_id = str(zone_id).strip()
        self._explicit_battery = battery
        self.battery = min(100.0, max(5.0, float(battery))) if battery is not None else 90.0
        self.pid = os.getpid()
        self.managed = bool(managed)

        self.ip_address = assign_node_ip(self.uav_id, "MÁY_BAY", self.zone_id or "ZONE_03")
        self.dst_ip = "10.0.0.1"
        self.home_zone_id = self.zone_id
        self.role = "PATROL"
        self.zone_center = [16.125, 108.255]
        self.zone_polygon: list[list[float]] = []
        self.station = [16.105, 108.250]
        self.lat = self.zone_center[0]
        self.lon = self.zone_center[1]
        self.alt = 1050.0
        self.speed = 40.0
        self.battery_capacity_j = 50000.0
        self.power_w = 80.0
        self.paused = False
        self.time_scale = 1.0

        self.rf_link = "GROUND_LINK"
        self.rf_hops: list[str] = []
        self.rf_delay_ms = 0.0
        self.rf_per = 0.0
        self.rf_snr_db = 0.0
        self.distance_m = 0.0
        self.state = "PATROLLING"
        self._orbit_angle = 0.0
        self._patrol_waypoint: Optional[tuple[float, float]] = None
        self._fire_target: Optional[tuple[float, float]] = None
        self._rng = random.Random(self.pid ^ hash(self.uav_id))
        _init_ang = self._rng.uniform(0.0, 2.0 * math.pi)
        self._heading: tuple[float, float] = (math.cos(_init_ang), math.sin(_init_ang))
        self._swarm_peers: list[list[float]] = []
        self._swarm_hotspots: list[list[float]] = []
        self._swarm_env_ts: float = 0.0

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

        self.tcp_tx_count = 0
        self.tcp_ack_count = 0
        self.udp_tx_count = 0
        self.udp_muted: bool = False
        self.interactive_prompt = False
        self._print_lock = threading.Lock()
        self._last_status_emit = 0.0

    def _short_id(self) -> str:
        return self.uav_id.replace("MÁY_BAY_", "")

    def _emit_log(self, msg: str):
        with self._print_lock:
            if self.managed:
                print(f"@@LOG@@{msg}", flush=True)
            else:
                print(f"\n{msg}", flush=True)
                if self.interactive_prompt and self._running and not self.server_disconnected:
                    print(f"{self._short_id()}> ", end="", flush=True)

    def _emit_status(self, force: bool = False):
        if not self.managed:
            return
        now = time.time()
        if not force and (now - self._last_status_emit) < 0.45:
            return
        self._last_status_emit = now
        with self._state_lock:
            snap = {
                "uav_id": self.uav_id,
                "short_id": self._short_id(),
                "pid": self.pid,
                "ip": self.ip_address,
                "zone_id": self.zone_id,
                "role": self.role,
                "state": self.state,
                "lat": round(self.lat, 5),
                "lon": round(self.lon, 5),
                "alt": round(self.alt, 1),
                "speed": round(self.speed, 1),
                "battery": round(self.battery, 2),
                "rf_link": self.rf_link,
                "rf_hops": [h.replace("MÁY_BAY_", "").replace("TRẠM_KIỂM_LÂM_", "") for h in self.rf_hops],
                "rf_delay_ms": round(self.rf_delay_ms, 2),
                "rf_per": round(self.rf_per, 4),
                "rf_snr_db": round(self.rf_snr_db, 1),
                "distance_m": round(self.distance_m, 1),
                "tcp_tx": self.tcp_tx_count,
                "tcp_ack": self.tcp_ack_count,
                "udp_tx": self.udp_tx_count,
                "udp_muted": self.udp_muted,
                "cmd_rx": self.server_cmd_rx_count,
                "disconnected": self.server_disconnected,
            }
        with self._print_lock:
            print("@@STATUS@@" + json.dumps(snap, ensure_ascii=False, separators=(",", ":")), flush=True)

    def _next_seq(self) -> int:
        with self._tcp_lock:
            self._seq += 1
            return self._seq

    def _pick_interior_waypoint(self) -> tuple[float, float]:
        """Chọn ngẫu nhiên một điểm mục tiêu tuần tra nằm gọn trong đa giác phân khu."""
        pts = _normalize_ring(self.zone_polygon)
        if len(pts) >= 3:
            self.zone_polygon = pts
            lons = [p[0] for p in pts]
            lats = [p[1] for p in pts]
            min_lon, max_lon = min(lons), max(lons)
            min_lat, max_lat = min(lats), max(lats)
            for _ in range(30):
                cand_lat = self._rng.uniform(min_lat, max_lat)
                cand_lon = self._rng.uniform(min_lon, max_lon)
                if _point_in_ring(cand_lat, cand_lon, pts):
                    return cand_lat, cand_lon
        m_lat = 111320.0
        m_lon = 111320.0 * max(0.2, math.cos(math.radians(self.zone_center[0])))
        ang = self._rng.uniform(0.0, 2.0 * math.pi)
        rad = self._rng.uniform(120.0, 450.0)
        return (
            self.zone_center[0] + rad * math.sin(ang) / m_lat,
            self.zone_center[1] + rad * math.cos(ang) / m_lon,
        )

    def send_tcp_command(self, cmd: str, extra: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """Đóng gói bản tin JSON vào khung TCP NetworkPacket (CRC32) và chờ ACK từ Server."""
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
                ack_len_hdr = recv_exact(self._tcp_sock, 4)
                if not ack_len_hdr:
                    raise ConnectionError("Server đã đóng kết nối TCP.")
                ack_len = struct.unpack("!I", ack_len_hdr)[0]
                ack_raw = recv_exact(self._tcp_sock, ack_len)
                if not ack_raw:
                    raise ConnectionError("Nhận khung ACK bị gián đoạn.")
                ack_pkt, crc_ok, _ = NetworkPacket.decode_and_verify(ack_raw)
                if ack_pkt is None or not crc_ok or not (ack_pkt.flags & FLAG_ACK):
                    raise RuntimeError("Gói ACK từ Server lỗi Checksum CRC32.")
                self.tcp_ack_count += 1
                return json.loads(ack_pkt.payload.decode("utf-8")) if ack_pkt.payload else {"ok": True}

        assert ev is not None
        if not ev.wait(timeout=6.0):
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
        """Đóng gói và truyền 1 datagram Telemetry nhị phân (CRC32) qua UDP Socket."""
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
        self._udp_sock.sendto(pkt.encode(), (self.host, self.udp_port))
        self.udp_tx_count += 1

    def connect(self) -> dict[str, Any]:
        """Mở TCP Socket + UDP Socket và gửi bắt tay JOIN tới Server."""
        tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp.settimeout(6.0)
        try:
            tcp.connect((self.host, self.tcp_port))
        except Exception:
            tcp.close()
            raise
        self._tcp_sock = tcp
        self._udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        join_extra: dict[str, Any] = {"alt": self.alt}
        if self._explicit_battery is not None:
            join_extra["battery"] = self._explicit_battery
        reply = self.send_tcp_command("JOIN", join_extra)
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
        self.home_zone_id = str(reply.get("home_zone_id") or self.zone_id)
        if reply.get("role"):
            self.role = str(reply["role"]).upper()
        if isinstance(reply.get("zone_center"), list) and len(reply["zone_center"]) == 2:
            self.zone_center = [float(reply["zone_center"][0]), float(reply["zone_center"][1])]
        if isinstance(reply.get("zone_polygon"), list):
            self.zone_polygon = list(reply["zone_polygon"])
        if isinstance(reply.get("station"), list) and len(reply["station"]) == 2:
            self.station = [float(reply["station"][0]), float(reply["station"][1])]
        if reply.get("lat") is not None and reply.get("lon") is not None:
            self.lat = float(reply["lat"])
            self.lon = float(reply["lon"])
        if reply.get("alt") is not None:
            self.alt = float(reply["alt"])
        if reply.get("battery") is not None:
            self.battery = min(100.0, max(0.0, float(reply["battery"])))
        if reply.get("battery_capacity_j") is not None:
            self.battery_capacity_j = max(1000.0, float(reply["battery_capacity_j"]))
        if reply.get("power_w") is not None:
            self.power_w = max(1.0, float(reply["power_w"]))
        if reply.get("state"):
            self.state = str(reply["state"]).upper()
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
        if reply.get("rf_snr_db") is not None:
            self.rf_snr_db = float(reply["rf_snr_db"])
        if reply.get("distance_m") is not None:
            self.distance_m = float(reply["distance_m"])

        if self.state == "PATROLLING":
            self._patrol_waypoint = self._pick_interior_waypoint()
        elif self.state == "STANDBY":
            self.speed = 0.0
        self._emit_status(force=True)
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
        self._emit_status(force=True)

    def _handle_server_command(self, payload_obj: dict[str, Any]):
        """Xử lý lệnh điều phối 2 chiều từ Server (RTB, DISPATCH_FIRE, RESUME_PATROL, SET_BATTERY, RF_STATUS, SWARM_ENV)."""
        action = str(payload_obj.get("action", "")).upper()
        if action == "SWARM_ENV":
            with self._state_lock:
                self._swarm_peers = [
                    list(p) for p in (payload_obj.get("peers") or [])
                    if isinstance(p, (list, tuple)) and len(p) >= 3
                ]
                self._swarm_hotspots = [
                    list(h) for h in (payload_obj.get("hotspots") or [])
                    if isinstance(h, (list, tuple)) and len(h) >= 3
                ]
                self._swarm_env_ts = time.time()
            return

        should_push = action in ("RTB", "SET_BATTERY", "DISPATCH_FIRE", "RESUME_PATROL")
        async_msg: Optional[str] = None

        with self._state_lock:
            self.server_cmd_rx_count += 1
            self.last_server_cmd = dict(payload_obj)
            if action == "RTB":
                self._fire_target = None
                self.state = "RETURNING"
                self.speed = 45.0
                async_msg = f"[SERVER -> CLIENT] Nhận lệnh RTB -> {self._short_id()} đang quay về Trạm sạc."
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
                    if _haversine_m(self.lat, self.lon, self.station[0], self.station[1]) <= 120.0:
                        self.state = "STANDBY"
                        self.speed = 0.0
                    else:
                        self.state = "PATROLLING"
                        self.speed = 40.0
                else:
                    if self.state == "OFFLINE":
                        if _haversine_m(self.lat, self.lon, self.station[0], self.station[1]) <= 120.0:
                            self.state = "CHARGING"
                            self.speed = 0.0
                        else:
                            self.state = "PATROLLING"
                            self.speed = 40.0
                async_msg = f"[SERVER -> CLIENT] Đặt pin {new_bat:.1f}% -> Trạng thái {self.state}."
            elif action == "DISPATCH_FIRE":
                f_lat = payload_obj.get("lat")
                f_lon = payload_obj.get("lon")
                if f_lat is not None and f_lon is not None:
                    self._fire_target = (float(f_lat), float(f_lon))
                    self.state = "RESPONDING"
                    self.speed = 50.0
                    if self.alt < 950.0:
                        self.alt = 1050.0
                    async_msg = (
                        f"[SERVER -> CLIENT] Nhận lệnh DISPATCH_FIRE tới ({float(f_lat):.4f}, {float(f_lon):.4f})!"
                    )
            elif action == "RESUME_PATROL":
                req_zid = str(payload_obj.get("zone_id") or "")
                if req_zid and (not self.home_zone_id or req_zid == self.home_zone_id):
                    self.zone_id = req_zid
                    if isinstance(payload_obj.get("zone_center"), list) and len(payload_obj["zone_center"]) == 2:
                        self.zone_center = [float(payload_obj["zone_center"][0]), float(payload_obj["zone_center"][1])]
                    if isinstance(payload_obj.get("zone_polygon"), list):
                        self.zone_polygon = list(payload_obj["zone_polygon"])
                self._fire_target = None
                self._patrol_waypoint = self._pick_interior_waypoint()
                if self.alt < 950.0:
                    self.alt = 1050.0
                dist_z = _haversine_m(self.lat, self.lon, self.zone_center[0], self.zone_center[1])
                self.state = "TRANSITING" if dist_z > 900.0 else "PATROLLING"
                self.speed = 45.0 if self.state == "TRANSITING" else 40.0
                async_msg = f"[SERVER -> CLIENT] Nhận lệnh RESUME_PATROL tại {self.zone_id} ({self.state})."
            elif action == "SET_CLOCK":
                if payload_obj.get("paused") is not None:
                    self.paused = bool(payload_obj["paused"])
                if payload_obj.get("time_scale") is not None:
                    self.time_scale = max(0.1, float(payload_obj["time_scale"]))
            elif action == "RF_STATUS":
                old_link = self.rf_link
                if payload_obj.get("link"):
                    self.rf_link = str(payload_obj["link"])
                if isinstance(payload_obj.get("hops"), list):
                    self.rf_hops = [str(x) for x in payload_obj["hops"]]
                if payload_obj.get("delay_ms") is not None:
                    self.rf_delay_ms = float(payload_obj["delay_ms"])
                if payload_obj.get("per") is not None:
                    self.rf_per = float(payload_obj["per"])
                if payload_obj.get("snr_db") is not None:
                    self.rf_snr_db = float(payload_obj["snr_db"])
                if payload_obj.get("distance_m") is not None:
                    self.distance_m = float(payload_obj["distance_m"])
                if old_link != self.rf_link:
                    async_msg = f"[KÊNH SAGSINS] Chuyển mạch vô tuyến: {old_link} -> {self.rf_link} ({self.rf_delay_ms:.2f} ms)."

        if should_push:
            self.send_udp_telemetry()
        if async_msg:
            self._emit_log(async_msg)
        self._emit_status(force=should_push)

    def _tcp_reader_loop(self):
        sock = self._tcp_sock
        if sock is None:
            return
        sock.settimeout(0.5)
        while self._running:
            try:
                len_hdr = recv_exact(sock, 4)
                if not len_hdr:
                    break
                frame_len = struct.unpack("!I", len_hdr)[0]
                if frame_len <= 0 or frame_len > 65536 + 64:
                    break
                frame = recv_exact(sock, frame_len)
                if not frame:
                    break
            except socket.timeout:
                continue
            except OSError:
                break

            pkt, crc_ok, _ = NetworkPacket.decode_and_verify(frame)
            if pkt is None or not crc_ok:
                if pkt is not None and pkt.ack_num > 0:
                    with self._pending_lock:
                        waiter = self._pending_acks.pop(pkt.ack_num, None)
                    if waiter is not None:
                        waiter[1].append({"_crc_error": True})
                        waiter[0].set()
                continue

            payload_obj: dict[str, Any] = {}
            if pkt.payload:
                try:
                    payload_obj = json.loads(pkt.payload.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    payload_obj = {}

            if pkt.flags & FLAG_ACK:
                self.tcp_ack_count += 1
                with self._pending_lock:
                    waiter = self._pending_acks.pop(pkt.ack_num, None)
                if waiter is not None:
                    waiter[1].append(payload_obj or {"ok": True})
                    waiter[0].set()
            elif isinstance(payload_obj, dict) and str(payload_obj.get("cmd", "")).upper() == "SERVER_CMD":
                self._handle_server_command(payload_obj)

        self.server_disconnected = True
        with self._pending_lock:
            pending_items = list(self._pending_acks.values())
            self._pending_acks.clear()
        for ev, box in pending_items:
            box.append({"_conn_error": True, "error": "Kết nối TCP với Server đã đóng."})
            ev.set()
        self._emit_status(force=True)

    def _step_physics(self, dt_real: float):
        """Mô phỏng chuyển động và năng lượng phía Client trong đa giác Sơn Trà."""
        if self.paused or dt_real <= 0:
            return
        dt_sim = dt_real * self.time_scale
        m_lat = 111320.0
        m_lon = 111320.0 * max(0.2, math.cos(math.radians(self.lat)))

        with self._state_lock:
            if self.state in ("OFFLINE", "STANDBY"):
                self.speed = 0.0
                return
            if self.state == "CHARGING":
                self.speed = 0.0
                self.battery = min(100.0, self.battery + 15.0 * dt_sim)
                if self.battery >= 100.0:
                    self.battery = 100.0
                    self.state = "STANDBY"
                    self._emit_log("[PIN] Đã sạc đầy 100% tại Trạm Kiểm Lâm -> Chuyển trực chiến STANDBY.")
                return

            drain_pct = (self.power_w * dt_sim / self.battery_capacity_j) * 100.0
            self.battery = max(0.0, self.battery - drain_pct)
            if self.battery <= 0.0:
                self.state = "OFFLINE"
                self.speed = 0.0
                self._fire_target = None
                return
            if self.battery < 25.0 and self.state not in ("RETURNING", "CHARGING", "STANDBY"):
                self._fire_target = None
                self.state = "RETURNING"
                self.speed = 45.0
                self._emit_log(f"[PIN] Pin xuống {self.battery:.1f}% (<25%) -> Tự động quay về Trạm (RETURNING).")

            if self.state == "RETURNING":
                dist_st = _haversine_m(self.lat, self.lon, self.station[0], self.station[1])
                if dist_st <= 80.0:
                    self.lat, self.lon, self.alt = self.station[0], self.station[1], 25.0
                    self.state = "CHARGING"
                    self.speed = 0.0
                    self._emit_log("[PIN] Đã hạ cánh tại Trạm Kiểm Lâm -> Bắt đầu CHARGING (+15%/s).")
                else:
                    dlat_m = (self.station[0] - self.lat) * m_lat
                    dlon_m = (self.station[1] - self.lon) * m_lon
                    step = min(dist_st, 45.0 * dt_sim)
                    self.lat += (dlat_m / dist_st) * step / m_lat
                    self.lon += (dlon_m / dist_st) * step / m_lon
                    self.speed = 45.0
                return

            if self.state == "RESPONDING" and self._fire_target is not None:
                f_lat, f_lon = self._fire_target
                dist_f = _haversine_m(self.lat, self.lon, f_lat, f_lon)
                if dist_f <= 150.0:
                    self.state = "CONTAINING"
                    self.speed = 35.0
                    self._orbit_angle = math.atan2((self.lat - f_lat) * m_lat, (self.lon - f_lon) * m_lon)
                    self._emit_log(f"[KHẨN CẤP] Tới tâm cháy ({f_lat:.4f}, {f_lon:.4f}) -> Khoanh vùng 130m (CONTAINING).")
                else:
                    dlat_m = (f_lat - self.lat) * m_lat
                    dlon_m = (f_lon - self.lon) * m_lon
                    step = min(dist_f, 50.0 * dt_sim)
                    self.lat += (dlat_m / dist_f) * step / m_lat
                    self.lon += (dlon_m / dist_f) * step / m_lon
                    self.speed = 50.0
                return

            if self.state == "CONTAINING" and self._fire_target is not None:
                f_lat, f_lon = self._fire_target
                self._orbit_angle = (self._orbit_angle + (35.0 / 130.0) * dt_sim) % (2.0 * math.pi)
                self.lat = f_lat + 130.0 * math.sin(self._orbit_angle) / m_lat
                self.lon = f_lon + 130.0 * math.cos(self._orbit_angle) / m_lon
                self.speed = 35.0
                return

            if self.state == "TRANSITING":
                wp = self._patrol_waypoint or (self.zone_center[0], self.zone_center[1])
                dist_w = _haversine_m(self.lat, self.lon, wp[0], wp[1])
                in_poly = (len(self.zone_polygon) < 3) or _point_in_ring(self.lat, self.lon, self.zone_polygon)
                if (dist_w <= 120.0 and in_poly) or dist_w <= 40.0:
                    self.state = "PATROLLING"
                    self.speed = 40.0
                    self._patrol_waypoint = self._pick_interior_waypoint()
                else:
                    dlat_m = (wp[0] - self.lat) * m_lat
                    dlon_m = (wp[1] - self.lon) * m_lon
                    step = min(dist_w, 45.0 * dt_sim)
                    self.lat += (dlat_m / dist_w) * step / m_lat
                    self.lon += (dlon_m / dist_w) * step / m_lon
                    self.speed = 45.0
                return

            # PATROLLING: Trường lực MAS giống bản gốc (Virtual Pheromone + Hút Heatmap + Đẩy chống tụ cụm + Quán tính + Trượt biên GeoJSON)
            now_ts = time.time()
            hotspots = self._swarm_hotspots if (self._swarm_hotspots and (now_ts - self._swarm_env_ts) <= 6.0) else []
            peers = self._swarm_peers if (now_ts - self._swarm_env_ts) <= 6.0 else []
            n_sub = max(1, min(5, int(math.ceil(dt_sim))))
            dt_step = dt_sim / n_sub
            for _ in range(n_sub):
                cur_m_lon = 111320.0 * max(0.2, math.cos(math.radians(self.lat)))
                fx_attr, fy_attr = 0.0, 0.0
                ux_max, uy_max = 0.0, 0.0
                if hotspots:
                    def _eff_score(item: list[float]) -> tuple[float, float]:
                        h_la, h_lo, rk = float(item[0]), float(item[1]), float(item[2])
                        d_s = _haversine_m(self.lat, self.lon, h_la, h_lo)
                        closer = any(
                            p[2] == 1 and _haversine_m(float(p[0]), float(p[1]), h_la, h_lo) < d_s
                            for p in peers
                        )
                        ck = 0.2 if closer else 1.0
                        return (round(rk * ck, 4), -d_s)

                    node_max = max(hotspots, key=_eff_score)
                    dx_m = (float(node_max[1]) - self.lon) * cur_m_lon
                    dy_m = (float(node_max[0]) - self.lat) * m_lat
                    d_max = max(1.0, math.hypot(dx_m, dy_m))
                    ux_max, uy_max = dx_m / d_max, dy_m / d_max

                    for item in hotspots:
                        h_la, h_lo, rk = float(item[0]), float(item[1]), float(item[2])
                        dx = (h_lo - self.lon) * cur_m_lon
                        dy = (h_la - self.lat) * m_lat
                        d_k = math.hypot(dx, dy)
                        if d_k < 1.0:
                            continue
                        closer = any(
                            p[2] == 1 and _haversine_m(float(p[0]), float(p[1]), h_la, h_lo) < d_k
                            for p in peers
                        )
                        ck = 0.2 if closer else 1.0
                        w_k = ((rk ** 2) / max(150.0, d_k)) * ck
                        fx_attr += (dx / d_k) * w_k
                        fy_attr += (dy / d_k) * w_k
                    mag_attr = math.hypot(fx_attr, fy_attr)
                    if mag_attr > 1e-9:
                        fx_attr /= mag_attr
                        fy_attr /= mag_attr
                else:
                    if self._patrol_waypoint is None or _haversine_m(self.lat, self.lon, self._patrol_waypoint[0], self._patrol_waypoint[1]) <= 90.0:
                        self._patrol_waypoint = self._pick_interior_waypoint()
                    dx_c = (self._patrol_waypoint[1] - self.lon) * cur_m_lon
                    dy_c = (self._patrol_waypoint[0] - self.lat) * m_lat
                    d_c = max(1.0, math.hypot(dx_c, dy_c))
                    ux_max, uy_max = dx_c / d_c, dy_c / d_c

                heat_mag = 3.2
                fx_heat = ux_max * 2.4 + fx_attr * 0.8
                fy_heat = uy_max * 2.4 + fy_attr * 0.8
                mag_heat = math.hypot(fx_heat, fy_heat)
                if mag_heat > 1e-9:
                    fx_heat = (fx_heat / mag_heat) * heat_mag
                    fy_heat = (fy_heat / mag_heat) * heat_mag

                fx_tot, fy_tot = fx_heat, fy_heat
                for p_lat, p_lon, same_zone in peers:
                    rx = (self.lon - float(p_lon)) * cur_m_lon
                    ry = (self.lat - float(p_lat)) * m_lat
                    dist = math.hypot(rx, ry)
                    if dist <= 0.1:
                        ang_f = (abs(hash(self.uav_id)) % 360) * math.pi / 180.0
                        ux_r, uy_r = math.cos(ang_f), math.sin(ang_f)
                        dist_eff = 1.0
                    else:
                        ux_r, uy_r = rx / dist, ry / dist
                        dist_eff = dist
                    if same_zone == 1 and dist_eff < 1200.0:
                        ty_le = (1200.0 - dist_eff) / 1200.0
                        rep_mag = (2.5 * heat_mag) * (ty_le ** 1.15)
                        fx_tot += (ux_r - 0.32 * uy_r) * rep_mag
                        fy_tot += (uy_r + 0.32 * ux_r) * rep_mag
                    elif same_zone == 0 and dist_eff < 650.0:
                        ty_le = (650.0 - dist_eff) / 650.0
                        rep_mag = (0.85 * heat_mag) * ty_le
                        fx_tot += (ux_r - 0.32 * uy_r) * rep_mag
                        fy_tot += (uy_r + 0.32 * ux_r) * rep_mag

                mag_tot = math.hypot(fx_tot, fy_tot)
                nx, ny = (fx_tot / mag_tot, fy_tot / mag_tot) if mag_tot > 1e-9 else (ux_max, uy_max)
                hx = 0.30 * self._heading[0] + 0.70 * nx
                hy = 0.30 * self._heading[1] + 0.70 * ny
                hnorm = math.hypot(hx, hy)
                hx, hy = (hx / hnorm, hy / hnorm) if hnorm > 1e-9 else (nx, ny)
                self._heading = (hx, hy)

                step = 40.0 * dt_step
                next_lat = self.lat + (hy * step) / m_lat
                next_lon = self.lon + (hx * step) / cur_m_lon
                if len(self.zone_polygon) >= 3:
                    in_zone = _point_in_ring(self.lat, self.lon, self.zone_polygon)
                    if not _point_inside_with_margin(next_lat, next_lon, self.zone_polygon, margin_deg=0.00025):
                        dx_c = (self.zone_center[1] - self.lon) * cur_m_lon
                        dy_c = (self.zone_center[0] - self.lat) * m_lat
                        dc = max(1.0, math.hypot(dx_c, dy_c))
                        ux_c, uy_c = dx_c / dc, dy_c / dc
                        cands = []
                        if in_zone:
                            cands.extend([
                                (hx, 0.0),
                                (0.0, hy),
                                (hx * 0.65 + ux_c * 0.35, hy * 0.65 + uy_c * 0.35),
                                (hx * 0.30 + ux_c * 0.70, hy * 0.30 + uy_c * 0.70),
                            ])
                        cands.extend([(ux_max, uy_max), (ux_c, uy_c)])
                        found = False
                        for cx, cy in cands:
                            cmag = math.hypot(cx, cy)
                            if cmag <= 1e-6:
                                continue
                            c_lat = self.lat + ((cy / cmag) * step) / m_lat
                            c_lon = self.lon + ((cx / cmag) * step) / cur_m_lon
                            if not in_zone or _point_in_ring(c_lat, c_lon, self.zone_polygon):
                                next_lat, next_lon = c_lat, c_lon
                                self._heading = (cx / cmag, cy / cmag)
                                found = True
                                break
                        if not found:
                            next_lat = self.lat + (uy_c * step) / m_lat
                            next_lon = self.lon + (ux_c * step) / cur_m_lon
                            self._heading = (ux_c, uy_c)
                self.lat, self.lon = next_lat, next_lon
            self.speed = 40.0

    def _telemetry_loop(self):
        last_tick = time.time()
        while self._running and not self.server_disconnected:
            now = time.time()
            dt = now - last_tick
            last_tick = now
            self._step_physics(dt)
            if not self.udp_muted:
                try:
                    self.send_udp_telemetry()
                except OSError:
                    pass
            self._emit_status()
            time.sleep(1.0)

    def start_background_loops(self):
        self._running = True
        self._tcp_reader_thread = threading.Thread(
            target=self._tcp_reader_loop,
            name=f"Client-TCP-{self._short_id()}",
            daemon=True,
        )
        self._tcp_reader_thread.start()
        self._telemetry_thread = threading.Thread(
            target=self._telemetry_loop,
            name=f"Client-UDP-{self._short_id()}",
            daemon=True,
        )
        self._telemetry_thread.start()

    def execute_command(self, cmd: str) -> bool:
        """Thực thi 1 lệnh từ bàn phím hoặc từ stdin (`client_app.py`). Trả False nếu thoát (`q`)."""
        c = cmd.strip().lower()
        if not c:
            return True
        if c == "q":
            self._emit_log("[CLIENT] Đang gửi TCP LEAVE rời mạng lịch sự...")
            self.stop(graceful=True)
            return False
        if self.server_disconnected:
            self._emit_log("[CẢNH BÁO] Mất kết nối TCP với Server.")
            return False
        if c == "f":
            with self._state_lock:
                if self.state in ("STANDBY", "CHARGING", "RETURNING", "OFFLINE"):
                    self._emit_log(f"[TỪ CHỐI] UAV đang ở trạng thái {self.state}, chưa thể báo cháy.")
                    return True
                f_lat, f_lon = self.lat, self.lon
            res = self.send_tcp_command("FIRE_ALERT", {"lat": f_lat, "lon": f_lon})
            if not res.get("ok"):
                self._emit_log(f"[FIRE_NACK] Server từ chối báo cháy: {res.get('error')}")
            else:
                if res.get("handover"):
                    with self._state_lock:
                        self._fire_target = None
                        self.state = "RETURNING"
                        self.speed = 45.0
                    self._emit_log(
                        f"[FIRE_ACK] Đã báo cháy ({f_lat:.4f}, {f_lon:.4f}) qua {res.get('rf_link')} | "
                        f"Pin khứ hồi yếu ({res.get('pin_khu_hoi_pct')}%) -> Server bàn giao cho UAV khỏe."
                    )
                else:
                    with self._state_lock:
                        self._fire_target = (f_lat, f_lon)
                        self.state = "CONTAINING"
                        self.speed = 35.0
                    self._emit_log(
                        f"[FIRE_ACK] Đã báo cháy ({f_lat:.4f}, {f_lon:.4f}) qua {res.get('rf_link')} "
                        f"({res.get('rf_delay_ms')} ms) -> Khoanh vùng 130m."
                    )
                self.send_udp_telemetry()
                self._emit_status(force=True)
            return True
        if c == "b":
            with self._state_lock:
                self.battery = 15.0
                self._fire_target = None
                self.state = "RETURNING"
                self.speed = 45.0
            self.send_udp_telemetry()
            self._emit_log("[PIN] Giả lập pin yếu 15% -> Đang quay về Trạm sạc (RETURNING).")
            self._emit_status(force=True)
            return True
        if c == "c":
            with self._state_lock:
                self.battery = 100.0
                self._fire_target = None
                if _haversine_m(self.lat, self.lon, self.station[0], self.station[1]) <= 120.0:
                    self.state = "STANDBY"
                    self.speed = 0.0
                else:
                    self.state = "PATROLLING"
                    self.speed = 40.0
            self.send_udp_telemetry()
            self._emit_log(f"[PIN] Sạc đầy 100% -> Trạng thái {self.state}.")
            self._emit_status(force=True)
            return True
        if c == "e":
            self.send_corrupted_udp_test()
            self._emit_log("[CRC32] Đã gửi 1 datagram UDP lỗi bit để kiểm chứng bộ lọc Checksum phía Server.")
            return True
        if c == "u":
            with self._state_lock:
                self.udp_muted = not self.udp_muted
                muted = self.udp_muted
            if muted:
                self._emit_log("[UDP] Đã TẠM NGƯNG luồng Telemetry UDP (mô phỏng mất tín hiệu đường truyền).")
            else:
                self._emit_log("[UDP] Đã MỞ LẠI luồng Telemetry UDP (1 gói/giây).")
                try:
                    self.send_udp_telemetry()
                except OSError:
                    pass
            self._emit_status(force=True)
            return True
        if c == "r":
            with self._state_lock:
                hops_txt = " -> ".join(h.replace("MÁY_BAY_", "").replace("TRẠM_KIỂM_LÂM_", "") for h in self.rf_hops) or "N/A"
                self._emit_log(
                    f"[KÊNH SAGSINS] Link={self.rf_link} | Tuyến=[{hops_txt}] | Cự ly={self.distance_m:.0f}m | "
                    f"SNR={self.rf_snr_db:.1f} dB | PER={self.rf_per:.4f} | Trễ={self.rf_delay_ms:.2f} ms"
                )
            return True
        if c == "s":
            with self._state_lock:
                self._emit_log(
                    f"[TRẠNG THÁI] PID={self.pid} | ID={self._short_id()} | IP={self.ip_address} | "
                    f"Zone={self.zone_id} ({self.role}) | State={self.state} | Pin={self.battery:.1f}% | "
                    f"Kênh={self.rf_link} | TCP={self.tcp_tx_count}/{self.tcp_ack_count} | UDP={self.udp_tx_count}"
                )
            self._emit_status(force=True)
            return True
        if c == "h":
            self._print_help()
            return True
        self._emit_log(f"[?] Lệnh không hợp lệ '{cmd}'. Gõ [h] xem hướng dẫn.")
        return True

    @staticmethod
    def _print_help():
        print(
            "\n  LỆNH TƯƠNG TÁC UAV CLIENT:\n"
            "  [f] Báo cháy khẩn cấp tại vị trí hiện tại (TCP FIRE_ALERT)\n"
            "  [b] Giả lập pin yếu 15% để kiểm tra RTB + đánh thức STANDBY\n"
            "  [c] Sạc nhanh pin lên 100%\n"
            "  [e] Gửi 1 gói UDP lỗi bit CRC32 kiểm tra bộ lọc phía Server\n"
            "  [u] Tạm ngưng / mở lại luồng gửi Telemetry UDP\n"
            "  [r] Xem thông số kênh vô tuyến A2G / LEO hiện tại\n"
            "  [s] Xem trạng thái chi tiết & bộ đếm TCP/UDP\n"
            "  [h] Hiện trợ giúp | [q] Rời mạng lịch sự (TCP LEAVE)\n",
            flush=True,
        )

    def stop(self, graceful: bool = True):
        self._running = False
        if graceful and self._tcp_sock is not None and not self.server_disconnected:
            try:
                self.send_tcp_command("LEAVE", {})
            except Exception:
                pass
        for sock_attr in ("_tcp_sock", "_udp_sock"):
            sock = getattr(self, sock_attr, None)
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
                setattr(self, sock_attr, None)


def main():
    parser = argparse.ArgumentParser(description="SAGSINS Standalone UAV Client (Pure Python stdlib)")
    parser.add_argument("--host", default="127.0.0.1", help="IP Server (Trạm Kiểm Lâm)")
    parser.add_argument("--tcp-port", type=int, default=9000, help="Cổng TCP Server (mặc định 9000)")
    parser.add_argument("--udp-port", type=int, default=9001, help="Cổng UDP Server (mặc định 9001)")
    parser.add_argument("--id", default="UAV_09", help="Mã UAV (VD: UAV_01..UAV_35 hoặc UAV_EXT_01)")
    parser.add_argument("--zone", default="", help="Phân khu mục tiêu (chỉ dùng cho UAV ngoài đội hình)")
    parser.add_argument("--battery", type=float, default=None, help="Pin ban đầu (%%)")
    parser.add_argument("--managed", action="store_true", help="Chế độ chạy dưới quyền quản lý của client_app.py")
    args = parser.parse_args()

    client = StandaloneUAVClient(
        host=args.host,
        tcp_port=args.tcp_port,
        udp_port=args.udp_port,
        uav_id=args.id,
        zone_id=args.zone,
        battery=args.battery,
        managed=args.managed,
    )

    try:
        ack = client.connect()
    except Exception as exc:
        if args.managed:
            print(f"@@LOG@@[LỖI KẾT NỐI] {exc}", flush=True)
        else:
            print(f"[CLIENT ERROR] Không thể kết nối Server {args.host}:{args.tcp_port} -> {exc}", flush=True)
        sys.exit(1)

    client.start_background_loops()

    if not args.managed:
        print("=" * 72, flush=True)
        print(f"  SAGSINS STANDALONE UAV CLIENT | PID={client.pid} | ID={client._short_id()}", flush=True)
        print(f"  Server: {client.host} (TCP :{client.tcp_port} / UDP :{client.udp_port})", flush=True)
        print(f"  IPv4 ảo: {client.ip_address} | Phân khu: {client.zone_id} ({client.role})", flush=True)
        print(f"  Kênh ban đầu: {ack.get('rf_link')} | Pin: {client.battery:.1f}% | Trạng thái: {client.state}", flush=True)
        print("=" * 72, flush=True)
        client._print_help()
        client.interactive_prompt = True
    else:
        client._emit_log(
            f"[JOIN_ACK] PID={client.pid} kết nối {client.host}:{client.tcp_port} tại {client.zone_id} "
            f"({client.role}, Kênh={client.rf_link}, Pin={client.battery:.1f}%)."
        )

    try:
        while client._running and not client.server_disconnected:
            if client.interactive_prompt:
                print(f"{client._short_id()}> ", end="", flush=True)
            line = sys.stdin.readline()
            if not line:
                break
            if not client.execute_command(line):
                break
    except KeyboardInterrupt:
        print("\n[CLIENT] Nhận Ctrl+C -> Đóng tiến trình UAV Client...", flush=True)
        client.stop(graceful=True)


if __name__ == "__main__":
    main()
