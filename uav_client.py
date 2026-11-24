"""
Chương trình Client UAV Độc lập (Standalone Multi-Process UAV Client - PBL4 Client-Server):
- Chạy như một tiến trình Hệ điều hành riêng (PID độc lập) ở Terminal khác hoặc máy tính khác qua mạng LAN/Wi-Fi.
- Kết nối TCP tới Server Trạm Kiểm Lâm (:9000) để bắt tay JOIN, báo cháy FIRE_ALERT và xin/nhả cổng sạc Semaphore(3).
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
        self._running = False
        self._telemetry_thread: Optional[threading.Thread] = None
        self._last_dock_poll = 0.0

        self.tcp_tx_count = 0
        self.tcp_ack_count = 0
        self.udp_tx_count = 0

    def _next_seq(self) -> int:
        with self._tcp_lock:
            self._seq += 1
            return self._seq

    def send_tcp_command(self, cmd: str, extra: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """Đóng gói bản tin JSON vào khung NetworkPacket TCP (CRC32) và chờ gói ACK thật từ Server."""
        if self._tcp_sock is None:
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
        prio = 0 if body["cmd"] in ("FIRE_ALERT", "DOCK_REQUEST", "DOCK_RELEASE", "JOIN") else 1

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
            self._tcp_sock.sendall(struct.pack("!I", len(raw)) + raw)
            self.tcp_tx_count += 1

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
        tcp.connect((self.host, self.tcp_port))
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

    def start_background(self):
        if self._running:
            return
        self._running = True
        self._telemetry_thread = threading.Thread(
            target=self._flight_and_telemetry_loop,
            name="Client-UDP-Telemetry",
            daemon=True,
        )
        self._telemetry_thread.start()

    def stop(self, graceful: bool = True):
        self._running = False
        if graceful and self._tcp_sock is not None:
            try:
                self.send_tcp_command("LEAVE")
            except Exception:
                pass
        if self._telemetry_thread is not None and self._telemetry_thread.is_alive():
            self._telemetry_thread.join(timeout=1.5)
        for attr in ("_tcp_sock", "_udp_sock"):
            s = getattr(self, attr, None)
            if s is not None:
                try:
                    s.close()
                except OSError:
                    pass
                setattr(self, attr, None)

    def step_flight(self, dt: float):
        """Cập nhật động học bay & vòng đời Pin/Semaphore của Client trong 1 bước `dt` giây."""
        dock_req_needed = False
        dock_rel_needed = False
        with self._state_lock:
            if self.state == "PATROLLING":
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
                    dock_req_needed = True
                else:
                    step_m = min(dist_m, 120.0 * dt)
                    ratio = step_m / dist_m
                    self.lat += (self.station[0] - self.lat) * ratio
                    self.lon += (self.station[1] - self.lon) * ratio
                    self.alt += (25.0 - self.alt) * ratio
                    self.speed = 45.0
            elif self.state == "WAITING_DOCK":
                self.lat, self.lon, self.alt, self.speed = self.station[0], self.station[1], 25.0, 0.0
                if time.monotonic() - self._last_dock_poll >= 1.5:
                    dock_req_needed = True
            elif self.state == "CHARGING":
                self.lat, self.lon, self.alt, self.speed = self.station[0], self.station[1], 25.0, 0.0
                self.battery = min(100.0, self.battery + 15.0 * dt)
                if self.battery >= 100.0:
                    self.battery = 100.0
                    dock_rel_needed = True

        if dock_req_needed:
            self._last_dock_poll = time.monotonic()
            try:
                res = self.send_tcp_command("DOCK_REQUEST")
                with self._state_lock:
                    self.state = str(res.get("state") or ("CHARGING" if res.get("acquired") else "WAITING_DOCK"))
            except Exception:
                pass
        elif dock_rel_needed:
            try:
                self.send_tcp_command("DOCK_RELEASE")
                with self._state_lock:
                    self.lat, self.lon, self.alt = self.zone_center[0], self.zone_center[1], 1050.0
                    self.state = "PATROLLING"
            except Exception:
                pass

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
        res = self.send_tcp_command("DOCK_RELEASE")
        with self._state_lock:
            self.battery = 100.0
            self.lat, self.lon, self.alt = self.zone_center[0], self.zone_center[1], 1050.0
            self.state = "PATROLLING"
        self.send_udp_telemetry()
        return res

    def status_text(self) -> str:
        with self._state_lock:
            return (
                f"PID={self.pid} | ID={self.uav_id} | IP={self.ip_address} | Zone={self.zone_id} | "
                f"State={self.state} | Pin={self.battery:.1f}% | Pos=({self.lat:.5f}, {self.lon:.5f}) | "
                f"UDP_TX={self.udp_tx_count} | TCP_TX={self.tcp_tx_count} (ACK={self.tcp_ack_count})"
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
    ack = client.connect()
    print(
        f"[UAV_CLIENT] Kết nối thành công! ID={client.uav_id} | IP={client.ip_address} | "
        f"UDP={args.host}:{client.udp_port} | ACK={ack.get('cmd')}"
    )
    client.start_background()

    print("\n--- BẢNG ĐIỀU KHIỂN UAV CLIENT ĐỘC LẬP ---")
    print("  [f] Báo cháy tại vị trí hiện tại (gửi TCP FIRE_ALERT)")
    print("  [b] Giả lập Pin yếu 18% (bay về Trạm & xin cổng Semaphore)")
    print("  [c] Sạc đầy 100% & nhả cổng Semaphore (TCP DOCK_RELEASE)")
    print("  [s] Xem trạng thái PID / Socket / Gói tin / Pin")
    print("  [q] Ngắt kết nối lịch sự (hoặc Ctrl+C để giả lập rớt mạng đột ngột)\n")

    try:
        if args.non_interactive:
            while True:
                time.sleep(1.0)
        else:
            while True:
                cmd = input("client> ").strip().lower()
                if cmd == "f":
                    res = client.trigger_fire()
                    print(f"[TCP -> FIRE_ACK] Đã báo cháy tại ({client.lat:.4f}, {client.lon:.4f}) -> {res}")
                elif cmd == "b":
                    client.trigger_low_battery()
                    print("[CLIENT] Pin đặt về 18% -> Đang bay thẳng về Trạm xin cổng sạc Semaphore...")
                elif cmd == "c":
                    res = client.trigger_full_charge()
                    print(f"[TCP -> DOCK_RELEASE_ACK] Đã sạc 100% & nhả cổng Semaphore -> {res}")
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
