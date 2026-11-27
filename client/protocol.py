"""
Giao thức nhị phân SAGSINS (100% Python Standard Library - Zero dependencies):
- Đóng gói khung nhị phân 26-byte Header + Checksum CRC32 (IEEE 802.3) đồng bộ byte-to-byte với Server.
- Có thể sao chép độc lập thư mục `client/` sang máy tính thứ 2 để kết nối tới Server qua LAN.
"""
from __future__ import annotations

import socket
import struct
import zlib
from typing import Optional

PROTO_TCP = 6
PROTO_UDP = 17

FLAG_SYN = 0x01
FLAG_ACK = 0x02
FLAG_DATA = 0x04
FLAG_RETRANS = 0x10

# !BBIIBB4s4sHI = ver_proto(1) + priority(1) + seq(4) + ack(4) + ttl(1) + flags(1)
#               + src_ip(4) + dst_ip(4) + payload_len(2) + crc32(4) = 26 bytes
_HEADER_BODY_FMT = "!BBIIBB4s4sH"
_HEADER_FULL_FMT = "!BBIIBB4s4sHI"
HEADER_BODY_SIZE = struct.calcsize(_HEADER_BODY_FMT)  # 22 bytes
HEADER_SIZE = struct.calcsize(_HEADER_FULL_FMT)       # 26 bytes


def _extract_trailing_int(node_id: str, default: int = 1) -> int:
    digits = "".join(ch for ch in str(node_id).rsplit("_", 1)[-1] if ch.isdigit())
    return int(digits) if digits else default


def assign_node_ip(
    node_id: str,
    loai_tac_nhan: str = "MÁY_BAY",
    zone_id: Optional[str] = None,
) -> str:
    """Cấp phát địa chỉ IPv4 logic theo dải 10.0.x.y đồng bộ với Server."""
    kind = str(loai_tac_nhan or "").upper()
    if kind == "TRẠM_MẶT_ĐẤT":
        return "10.0.0.1"
    if "VỆ_TINH" in kind:
        sat_num = max(1, min(254, _extract_trailing_int(node_id, 1)))
        return f"10.0.99.{sat_num}"

    uav_num = _extract_trailing_int(node_id, 1)
    if "EXT" in str(node_id).upper():
        return f"10.0.10.{max(1, min(254, uav_num))}"
    if uav_num >= 29:
        return f"10.0.8.{max(1, min(254, uav_num - 28))}"

    z_num = _extract_trailing_int(zone_id or "", 0)
    if z_num <= 0:
        z_num = ((uav_num - 1) // 4) + 1
    host = ((uav_num - 1) % 4) + 1
    return f"10.0.{max(1, min(254, z_num))}.{host}"


class NetworkPacket:
    """Gói tin tầng Giao vận nhị phân 26-byte Header + Checksum CRC32."""

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
        return head_body + struct.pack("!I", self.crc32) + payload_bytes

    @staticmethod
    def corrupt_wire_bytes(raw: bytes) -> bytes:
        """Đảo 1 bit trên khung dây truyền để kiểm chứng phát hiện lỗi CRC32 phía Server."""
        if not raw:
            return raw
        buf = bytearray(raw)
        idx = len(buf) - 1 if len(buf) > HEADER_SIZE else 0
        buf[idx] ^= 0x01
        return bytes(buf)

    @staticmethod
    def decode_and_verify(raw: bytes) -> tuple[Optional["NetworkPacket"], bool, int]:
        """Giải mã khung nhị phân và xác thực toàn vẹn Checksum CRC32."""
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


def recv_exact(conn: socket.socket, n_bytes: int) -> Optional[bytes]:
    chunks = []
    received = 0
    while received < n_bytes:
        try:
            chunk = conn.recv(n_bytes - received)
        except socket.timeout:
            if received == 0:
                raise
            continue
        if not chunk:
            return None
        chunks.append(chunk)
        received += len(chunk)
    return b"".join(chunks)
