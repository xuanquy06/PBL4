"""
Lõi Mạng máy tính (TCP/IP Transport & Packet Framing Stack) cho SAGSINS PBL4:
1. Cấp phát IPv4 theo Subnet (Trạm, 7 phân khu UAV, UAV dự bị, Vệ tinh LEO).
2. Đóng gói khung nhị phân chuẩn 26-byte Header + Checksum CRC32 + TTL + Seq/Ack.
3. Truyền nhận qua Socket TCP (tin cậy, có ACK) và Socket UDP thật của HĐH trên 127.0.0.1.
4. Bộ đệm vòng Packet Sniffer phục vụ quan sát trực tiếp cấu trúc gói tin trên Dashboard.
"""
from __future__ import annotations

from collections import deque
import json
import socket
import struct
import threading
import time
import zlib
from typing import Any, Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from core.os_kernel import OSKernelManager

PROTO_TCP = 6
PROTO_UDP = 17

FLAG_SYN = 0x01
FLAG_ACK = 0x02
FLAG_DATA = 0x04
FLAG_RETRANS = 0x10

# Header nhị phân 26 bytes:
# !BBIIBB4s4sHI = ver_proto(1) + priority(1) + seq(4) + ack(4) + ttl(1) + flags(1)
#               + src_ip(4) + dst_ip(4) + payload_len(2) + crc32(4)
_HEADER_BODY_FMT = "!BBIIBB4s4sH"
_HEADER_FULL_FMT = "!BBIIBB4s4sHI"
HEADER_BODY_SIZE = struct.calcsize(_HEADER_BODY_FMT)  # 22 bytes
HEADER_SIZE = struct.calcsize(_HEADER_FULL_FMT)       # 26 bytes

TCP_RELIABLE_TYPES = frozenset({
    "FIRE_ALERT",
    "FIRE_DETECTION_REPORT",
    "GIAO_NHIEM_VU_DU_LIEU",
    "YEU_CAU_HO_TRO_CHAY_P2P",
    "YEU_CAU_BAN_GIAO_VUNG",
    "FIRE_CONTAINMENT_TELEMETRY",
})


def _extract_trailing_int(node_id: str, default: int = 1) -> int:
    digits = "".join(ch for ch in str(node_id).rsplit("_", 1)[-1] if ch.isdigit())
    return int(digits) if digits else default


def assign_node_ip(
    node_id: str,
    loai_tac_nhan: str,
    zone_id: Optional[str] = None
) -> str:
    """
    Cấp phát địa chỉ IPv4 theo Subnet riêng cho từng tầng và phân khu:
    - Trạm Kiểm Lâm: 10.0.0.1
    - Vệ tinh LEO: 10.0.99.<1..3>
    - UAV tuần tra phân khu Z1..Z7 (UAV_01..28): 10.0.<zone>.<1..4>
    - UAV dự bị khởi tạo (UAV_29..35): 10.0.8.<1..7>
    """
    kind = str(loai_tac_nhan or "").upper()
    if kind == "TRẠM_MẶT_ĐẤT":
        return "10.0.0.1"
    if "VỆ_TINH" in kind:
        sat_num = max(1, min(254, _extract_trailing_int(node_id, 1)))
        return f"10.0.99.{sat_num}"

    uav_num = _extract_trailing_int(node_id, 1)
    if "EXT" in str(node_id).upper():
        host = max(1, min(254, uav_num))
        return f"10.0.10.{host}"
    if uav_num >= 29:
        host = max(1, min(254, uav_num - 28))
        return f"10.0.8.{host}"

    z_num = _extract_trailing_int(zone_id or "", 0)
    if z_num <= 0:
        z_num = ((uav_num - 1) // 4) + 1
    host = ((uav_num - 1) % 4) + 1
    return f"10.0.{max(1, min(254, z_num))}.{host}"


class NetworkPacket:
    """Gói tin tầng Mạng/Giao vận có Header nhị phân 26 bytes, TTL và Checksum CRC32."""

    def __init__(
        self,
        src_ip: str,
        dst_ip: str,
        protocol: str = "TCP",
        priority: int = 1,
        seq_num: int = 1,
        ack_num: int = 0,
        ttl: int = 16,
        flags: int = FLAG_DATA,
        payload: Optional[bytes] = None,
        crc32: int = 0,
    ):
        self.src_ip = src_ip
        self.dst_ip = dst_ip
        self.protocol = "TCP" if str(protocol).upper() == "TCP" else "UDP"
        self.priority = max(0, min(255, int(priority)))
        self.seq_num = int(seq_num) & 0xFFFFFFFF
        self.ack_num = int(ack_num) & 0xFFFFFFFF
        self.ttl = max(0, min(255, int(ttl)))
        self.flags = int(flags) & 0xFF
        self.payload = payload if payload is not None else b""
        self.crc32 = int(crc32) & 0xFFFFFFFF

    def encode(self) -> bytes:
        proto_num = PROTO_TCP if self.protocol == "TCP" else PROTO_UDP
        ver_proto = (4 << 4) | proto_num
        payload_bytes = self.payload[:65535]
        src_bytes = socket.inet_aton(self.src_ip)
        dst_bytes = socket.inet_aton(self.dst_ip)
        head_body = struct.pack(
            _HEADER_BODY_FMT,
            ver_proto,
            self.priority,
            self.seq_num,
            self.ack_num,
            self.ttl,
            self.flags,
            src_bytes,
            dst_bytes,
            len(payload_bytes),
        )
        self.crc32 = zlib.crc32(head_body + payload_bytes) & 0xFFFFFFFF
        full_header = head_body + struct.pack("!I", self.crc32)
        return full_header + payload_bytes

    @staticmethod
    def corrupt_wire_bytes(raw: bytes) -> bytes:
        """Đảo 1 bit trên khung dây truyền để mô phỏng lỗi nhiễu kênh vật lý thật."""
        if not raw:
            return raw
        buf = bytearray(raw)
        idx = len(buf) - 1 if len(buf) > HEADER_SIZE else 0
        buf[idx] ^= 0x01
        return bytes(buf)

    @staticmethod
    def decode_and_verify(raw: bytes) -> tuple[Optional["NetworkPacket"], bool, int]:
        """Giải mã khung nhị phân và xác thực toàn vẹn dữ liệu bằng Checksum CRC32."""
        if len(raw) < HEADER_SIZE:
            return None, False, 0

        (
            ver_proto,
            priority,
            seq_num,
            ack_num,
            ttl,
            flags,
            src_bytes,
            dst_bytes,
            payload_len,
            pkt_crc,
        ) = struct.unpack(_HEADER_FULL_FMT, raw[:HEADER_SIZE])

        if len(raw) < HEADER_SIZE + payload_len:
            return None, False, 0

        payload_bytes = raw[HEADER_SIZE:HEADER_SIZE + payload_len]
        expected_crc = zlib.crc32(raw[:HEADER_BODY_SIZE] + payload_bytes) & 0xFFFFFFFF
        crc_ok = (pkt_crc == expected_crc)

        proto_num = ver_proto & 0x0F
        protocol = "TCP" if proto_num == PROTO_TCP else "UDP"
        pkt = NetworkPacket(
            src_ip=socket.inet_ntoa(src_bytes),
            dst_ip=socket.inet_ntoa(dst_bytes),
            protocol=protocol,
            priority=priority,
            seq_num=seq_num,
            ack_num=ack_num,
            ttl=ttl,
            flags=flags,
            payload=payload_bytes,
            crc32=pkt_crc,
        )
        return pkt, crc_ok, expected_crc


def _recv_exact(conn: socket.socket, n_bytes: int) -> Optional[bytes]:
    chunks = []
    received = 0
    while received < n_bytes:
        chunk = conn.recv(n_bytes - received)
        if not chunk:
            return None
        chunks.append(chunk)
        received += len(chunk)
    return b"".join(chunks)


def detect_local_lan_ip() -> str:
    """Tìm địa chỉ IP LAN/Wi-Fi nội bộ của máy chủ (dự phòng 127.0.0.1 nếu offline)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        return str(ip) if ip else "127.0.0.1"
    except OSError:
        return "127.0.0.1"
    finally:
        try:
            sock.close()
        except OSError:
            pass


class RealSocketTransport:
    """
    Lớp giao vận Multi-threaded TCP/UDP Socket Server + Client Wire cho SAGSINS:
    - TCP Server (mặc định 0.0.0.0:9000): sinh 1 OS Thread riêng phục vụ từng Client kết nối.
    - UDP Server (mặc định 0.0.0.0:9001): nhận datagram Telemetry không kết nối.
    - Hỗ trợ đồng thời cả gói nội bộ 35 UAV mô phỏng lẫn tiến trình `uav_client.py` độc lập ngoài mạng LAN.
    """

    def __init__(
        self,
        os_kernel: Optional["OSKernelManager"] = None,
        bind_host: str = "0.0.0.0",
        preferred_tcp_port: int = 9000,
        preferred_udp_port: int = 9001,
    ):
        self._os_kernel = os_kernel
        self._lock = os_kernel.net_state_lock if os_kernel is not None else threading.RLock()
        self._tcp_client_lock = threading.Lock()
        self._udp_client_lock = threading.Lock()

        self.bind_host = bind_host
        self.preferred_tcp_port = int(preferred_tcp_port)
        self.preferred_udp_port = int(preferred_udp_port)

        self.running = False
        self.socket_mode = "IN_PROCESS_WIRE"
        self.tcp_port = 0
        self.udp_port = 0

        self.on_external_tcp_cmd: Optional[Callable[..., Any]] = None
        self.on_external_udp_telemetry: Optional[Callable[..., Any]] = None
        self.on_external_disconnect: Optional[Callable[..., Any]] = None
        self.external_clients: dict[str, dict[str, Any]] = {}

        self._tcp_server_sock: Optional[socket.socket] = None
        self._udp_server_sock: Optional[socket.socket] = None
        self._tcp_client_sock: Optional[socket.socket] = None
        self._udp_client_sock: Optional[socket.socket] = None
        self._tcp_thread: Optional[threading.Thread] = None
        self._udp_thread: Optional[threading.Thread] = None

        self._seq_counter = 100
        self._ext_udp_sniff_counter = 0
        self.packet_sniffer: deque[dict[str, Any]] = deque(maxlen=40)

        self.tcp_sent = 0
        self.tcp_acked = 0
        self.tcp_retransmissions = 0
        self.tcp_rx_packets = 0
        self.udp_sent = 0
        self.udp_delivered = 0
        self.udp_rx_packets = 0
        self.crc_errors = 0
        self.ttl_drops = 0
        self.tcp_bytes = 0
        self.udp_bytes = 0

    @staticmethod
    def determine_protocol(loai_tin: Optional[str]) -> str:
        kind = str(loai_tin or "UNKNOWN").upper()
        return "TCP" if kind in TCP_RELIABLE_TYPES else "UDP"

    def next_seq(self) -> int:
        with self._lock:
            self._seq_counter += 1
            return self._seq_counter

    def start(self):
        if self.running:
            return
        try:
            tcp_srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                tcp_srv.bind((self.bind_host, self.preferred_tcp_port))
            except OSError:
                tcp_srv.bind((self.bind_host, 0))
            tcp_srv.listen(32)
            tcp_srv.settimeout(0.5)

            udp_srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                udp_srv.bind((self.bind_host, self.preferred_udp_port))
            except OSError:
                udp_srv.bind((self.bind_host, 0))
            udp_srv.settimeout(0.5)

            self._tcp_server_sock = tcp_srv
            self._udp_server_sock = udp_srv
            self.tcp_port = int(tcp_srv.getsockname()[1])
            self.udp_port = int(udp_srv.getsockname()[1])
            self._udp_client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

            self.running = True
            self.socket_mode = "REAL_OS_SOCKET"

            self._tcp_thread = threading.Thread(
                target=self._tcp_server_loop,
                name="SAGSINS-TCP-Server",
                daemon=True,
            )
            self._udp_thread = threading.Thread(
                target=self._udp_server_loop,
                name="SAGSINS-UDP-Server",
                daemon=True,
            )
            self._tcp_thread.start()
            self._udp_thread.start()
        except OSError:
            # Nếu môi trường sandbox chặn mở cổng OS, vẫn giữ toàn vẹn đóng gói & CRC32 in-process
            self.stop()
            self.socket_mode = "IN_PROCESS_WIRE"

    def stop(self):
        self.running = False
        with self._lock:
            self.external_clients.clear()
        for sock_attr in ("_tcp_client_sock", "_udp_client_sock", "_tcp_server_sock", "_udp_server_sock"):
            sock_obj = getattr(self, sock_attr, None)
            if sock_obj is not None:
                try:
                    sock_obj.close()
                except OSError:
                    pass
                setattr(self, sock_attr, None)
        for th_attr in ("_tcp_thread", "_udp_thread"):
            th = getattr(self, th_attr, None)
            if th is not None and th.is_alive():
                th.join(timeout=1.0)
            setattr(self, th_attr, None)
        self.tcp_port = 0
        self.udp_port = 0

    def _tcp_server_loop(self):
        srv = self._tcp_server_sock
        if srv is None:
            return
        while self.running:
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            worker = threading.Thread(
                target=self._handle_tcp_client,
                args=(conn, addr),
                name=f"SAGSINS-TCP-Client-{addr[1]}",
                daemon=True,
            )
            worker.start()

    def _handle_tcp_client(self, conn: socket.socket, addr: tuple[str, int]):
        conn.settimeout(0.5)
        bound_uav_id: Optional[str] = None
        try:
            while self.running:
                try:
                    len_hdr = _recv_exact(conn, 4)
                    if not len_hdr:
                        break
                    frame_len = struct.unpack("!I", len_hdr)[0]
                    if frame_len <= 0 or frame_len > 65536 + HEADER_SIZE:
                        break
                    frame = _recv_exact(conn, frame_len)
                    if not frame:
                        break
                except socket.timeout:
                    continue
                except OSError:
                    break

                pkt, crc_ok, _ = NetworkPacket.decode_and_verify(frame)
                with self._lock:
                    self.tcp_rx_packets += 1

                ack_payload = b""
                should_close = False
                if pkt is not None and crc_ok and pkt.payload:
                    try:
                        payload_obj = json.loads(pkt.payload.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        payload_obj = None

                    if isinstance(payload_obj, dict) and "cmd" in payload_obj:
                        cmd = str(payload_obj["cmd"]).upper()
                        uav_id = str(payload_obj.get("uav_id") or bound_uav_id or f"MÁY_BAY_UAV_EXT_{addr[1]}")
                        if cmd == "JOIN":
                            bound_uav_id = uav_id
                            with self._lock:
                                if self._os_kernel is not None:
                                    self._os_kernel.record_mutex_acquire()
                                self.external_clients[uav_id] = {
                                    "uav_id": uav_id,
                                    "addr": f"{addr[0]}:{addr[1]}",
                                    "pid": payload_obj.get("pid"),
                                    "ip": pkt.src_ip,
                                    "connected_at": time.strftime("%H:%M:%S"),
                                    "thread_name": threading.current_thread().name,
                                }
                        reply_dict: dict[str, Any] = {"ok": True, "cmd": f"{cmd}_ACK"}
                        if self.on_external_tcp_cmd is not None:
                            try:
                                res = self.on_external_tcp_cmd(pkt, payload_obj, addr)
                                if isinstance(res, dict):
                                    reply_dict = res
                            except Exception as exc:
                                reply_dict = {"ok": False, "error": str(exc)}
                        if cmd == "JOIN" and isinstance(reply_dict, dict) and reply_dict.get("assigned_ip"):
                            with self._lock:
                                if uav_id in self.external_clients:
                                    self.external_clients[uav_id]["ip"] = str(reply_dict["assigned_ip"])
                        self.record_sniffer(pkt, f"EXT_{cmd}", uav_id, "TRẠM_KIỂM_LÂM_SƠN_TRÀ", f"EXT_TCP_{cmd}")
                        ack_payload = json.dumps(reply_dict, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                        if cmd == "LEAVE":
                            should_close = True

                ack_num = pkt.seq_num if (pkt is not None and crc_ok) else 0
                ack_pkt = NetworkPacket(
                    src_ip=pkt.dst_ip if pkt else "10.0.0.1",
                    dst_ip=pkt.src_ip if pkt else "10.0.0.1",
                    protocol="TCP",
                    priority=0,
                    seq_num=0,
                    ack_num=ack_num,
                    ttl=16,
                    flags=FLAG_ACK if crc_ok else 0,
                    payload=ack_payload,
                )
                ack_wire = ack_pkt.encode()
                conn.sendall(struct.pack("!I", len(ack_wire)) + ack_wire)
                if should_close:
                    break
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass
            if bound_uav_id:
                with self._lock:
                    if self._os_kernel is not None:
                        self._os_kernel.record_mutex_acquire()
                    self.external_clients.pop(bound_uav_id, None)
                if self.on_external_disconnect is not None:
                    try:
                        self.on_external_disconnect(bound_uav_id, addr)
                    except Exception:
                        pass

    def _udp_server_loop(self):
        srv = self._udp_server_sock
        if srv is None:
            return
        while self.running:
            try:
                data, addr = srv.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                break
            if not data:
                continue
            pkt, crc_ok, _ = NetworkPacket.decode_and_verify(data)
            with self._lock:
                self.udp_rx_packets += 1
            if pkt is not None and crc_ok and pkt.payload:
                try:
                    payload_obj = json.loads(pkt.payload.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    payload_obj = None
                if isinstance(payload_obj, dict) and str(payload_obj.get("cmd", "")).upper() == "TELEMETRY":
                    uav_id = str(payload_obj.get("uav_id") or f"MÁY_BAY_UAV_EXT_{addr[1]}")
                    if self.on_external_udp_telemetry is not None:
                        try:
                            self.on_external_udp_telemetry(pkt, payload_obj, addr)
                        except Exception:
                            pass
                    with self._lock:
                        self.udp_bytes += len(data)
                        self._ext_udp_sniff_counter += 1
                        sample_sniff = (self._ext_udp_sniff_counter % 4 == 1)
                    if sample_sniff:
                        self.record_sniffer(pkt, "EXT_TELEM", uav_id, "TRẠM_KIỂM_LÂM_SƠN_TRÀ", "EXT_UDP_OK")

    def _ensure_tcp_client(self) -> socket.socket:
        if self._tcp_client_sock is None:
            cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cli.settimeout(2.0)
            cli.connect(("127.0.0.1", self.tcp_port))
            self._tcp_client_sock = cli
        return self._tcp_client_sock

    def send_over_wire(
        self,
        packet: NetworkPacket,
        inject_bit_error: bool = False
    ) -> tuple[bool, bool, Optional[int]]:
        """
        Đóng gói nhị phân + truyền qua Socket OS thật (hoặc wire verifier khi server chưa chạy).
        Trả về `(delivered_ok, crc_ok, ack_num)`.
        """
        raw = packet.encode()
        wire_bytes = NetworkPacket.corrupt_wire_bytes(raw) if inject_bit_error else raw
        wire_len = len(wire_bytes)

        if packet.protocol == "TCP":
            with self._lock:
                self.tcp_sent += 1
                self.tcp_bytes += wire_len
                if packet.flags & FLAG_RETRANS:
                    self.tcp_retransmissions += 1

            if self.running and self.tcp_port > 0 and self.socket_mode == "REAL_OS_SOCKET":
                try:
                    with self._tcp_client_lock:
                        for attempt in range(2):
                            try:
                                cli = self._ensure_tcp_client()
                                cli.sendall(struct.pack("!I", wire_len) + wire_bytes)
                                ack_len_hdr = _recv_exact(cli, 4)
                                if not ack_len_hdr:
                                    raise OSError("TCP connection closed by peer")
                                ack_len = struct.unpack("!I", ack_len_hdr)[0]
                                ack_raw = _recv_exact(cli, ack_len)
                                if not ack_raw:
                                    raise OSError("Incomplete ACK frame")
                                ack_pkt, ack_crc_ok, _ = NetworkPacket.decode_and_verify(ack_raw)
                                valid_ack = (
                                    ack_pkt is not None
                                    and ack_crc_ok
                                    and bool(ack_pkt.flags & FLAG_ACK)
                                    and ack_pkt.ack_num == packet.seq_num
                                )
                                if valid_ack:
                                    with self._lock:
                                        self.tcp_acked += 1
                                    return True, True, ack_pkt.ack_num
                                with self._lock:
                                    self.crc_errors += 1
                                return False, False, None
                            except OSError:
                                if self._tcp_client_sock is not None:
                                    try:
                                        self._tcp_client_sock.close()
                                    except OSError:
                                        pass
                                    self._tcp_client_sock = None
                                if attempt == 1:
                                    raise
                except OSError:
                    with self._lock:
                        self.socket_mode = "IN_PROCESS_WIRE"

            # Fallback in-process verification khi không ở chế độ server loop hoặc sandbox chặn loopback
            pkt_chk, crc_ok, _ = NetworkPacket.decode_and_verify(wire_bytes)
            if crc_ok and pkt_chk is not None:
                with self._lock:
                    self.tcp_acked += 1
                return True, True, pkt_chk.seq_num
            with self._lock:
                self.crc_errors += 1
            return False, False, None

        # Giao thức UDP: gửi 1 lần, không ACK, tự hủy nếu CRC lỗi
        with self._lock:
            self.udp_sent += 1
            self.udp_bytes += wire_len

        if self.running and self.udp_port > 0 and self._udp_client_sock is not None and self.socket_mode == "REAL_OS_SOCKET":
            try:
                with self._udp_client_lock:
                    self._udp_client_sock.sendto(wire_bytes, ("127.0.0.1", self.udp_port))
            except OSError:
                with self._lock:
                    self.socket_mode = "IN_PROCESS_WIRE"

        _, crc_ok, _ = NetworkPacket.decode_and_verify(wire_bytes)
        if crc_ok:
            with self._lock:
                self.udp_delivered += 1
            return True, True, None
        with self._lock:
            self.crc_errors += 1
        return False, False, None

    def record_ttl_drop(self):
        with self._lock:
            self.ttl_drops += 1

    def record_sniffer(
        self,
        packet: NetworkPacket,
        msg_type: str,
        src_node: str,
        dst_node: str,
        status: str,
        retrans_count: int = 0,
    ):
        entry = {
            "time": time.strftime("%H:%M:%S"),
            "seq": packet.seq_num,
            "ack": packet.ack_num,
            "proto": packet.protocol,
            "priority": f"P{packet.priority}",
            "msg_type": msg_type,
            "src": src_node.replace("MÁY_BAY_", "").replace("TRẠM_KIỂM_LÂM_", "").replace("VỆ_TINH_", ""),
            "dst": dst_node.replace("MÁY_BAY_", "").replace("TRẠM_KIỂM_LÂM_", "").replace("VỆ_TINH_", ""),
            "src_ip": packet.src_ip,
            "dst_ip": packet.dst_ip,
            "ttl": packet.ttl,
            "crc32": f"0x{packet.crc32:08X}",
            "size_bytes": HEADER_SIZE + len(packet.payload),
            "retrans": retrans_count,
            "status": status,
        }
        with self._lock:
            if self._os_kernel is not None:
                self._os_kernel.record_mutex_acquire()
            self.packet_sniffer.appendleft(entry)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "socket_mode": self.socket_mode,
                "tcp_port": self.tcp_port,
                "udp_port": self.udp_port,
                "tcp_sent": self.tcp_sent,
                "tcp_acked": self.tcp_acked,
                "tcp_retransmissions": self.tcp_retransmissions,
                "tcp_rx_packets": self.tcp_rx_packets,
                "udp_sent": self.udp_sent,
                "udp_delivered": self.udp_delivered,
                "udp_rx_packets": self.udp_rx_packets,
                "crc_errors": self.crc_errors,
                "ttl_drops": self.ttl_drops,
                "tcp_bytes": self.tcp_bytes,
                "udp_bytes": self.udp_bytes,
                "external_clients_count": len(self.external_clients),
                "external_clients": list(self.external_clients.values()),
                "sniffer": list(self.packet_sniffer),
            }


def encode_payload_json(thong_diep: dict[str, Any]) -> bytes:
    """Đóng gói metadata payload nhẹ làm phần thân của NetworkPacket."""
    compact = {
        "t": thong_diep.get("loai_tin", "UNKNOWN"),
        "kb": thong_diep.get("kich_thuoc_kb", 1.0),
        "z": thong_diep.get("zone_id"),
    }
    return json.dumps(compact, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
