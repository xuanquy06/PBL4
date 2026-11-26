import asyncio
import heapq
import json
import math
import random
import time
from typing import Any, Dict, List, TYPE_CHECKING, Optional
from fastapi import WebSocket
from core.os_kernel import OSKernelManager, phan_loai_do_uu_tien
from core.network_stack import (
    FLAG_DATA,
    FLAG_RETRANS,
    NetworkPacket,
    RealSocketTransport,
    assign_node_ip,
    encode_payload_json,
)

if TYPE_CHECKING:
    from core.base_agent import BaseAgent

RouteChannel = dict[str, Any]
RouteState = tuple[float, str, tuple[str, ...], tuple[RouteChannel, ...]]


class MessageBroker:
    """
    Môi trường truyền dẫn vô tuyến trung tâm (Wireless Channel & Event Broker):
    - Kiểm tra điều kiện vật lý (Bán kính phủ sóng Communication Range, SNR, PER theo khoảng cách, Delay).
    - Cập nhật tri thức láng giềng cục bộ cho nút nhận khi gói tin truyền qua kênh vật lý.
    - Đồng bộ trạng thái mạng, liên kết, độ phủ 7 phân khu và số liệu QoS ra giao diện Web.
    """
    def __init__(self, physics_engine: Any, scenario_manager: Any):
        self.physics: Any = physics_engine
        self.scenario: Any = scenario_manager
        if self.scenario is not None:
            try:
                setattr(self.scenario, "_broker_ref", self)
            except AttributeError:
                pass
        self.danh_sach_tac_nhan: Dict[str, Any] = {}
        self.danh_sach_client_ws: List[WebSocket] = []
        self.tuyen_e2e_gan_nhat: Optional[dict[str, Any]] = None
        self.tuyen_e2e_chay_hien_tai: Optional[dict[str, Any]] = None
        self.danh_sach_tuyen_e2e_chay: List[dict[str, Any]] = []
        self.nhat_ky_gan_day: List[dict[str, Any]] = []
        self.ground_station_server: Any = None
        self.thong_ke_lien_tuc: dict[str, Any] = {}
        self.muc_tieu_chay_da_khoa: List[dict[str, Any]] = []
        self.os_kernel = OSKernelManager()
        self.network_stack = RealSocketTransport(os_kernel=self.os_kernel)
        self._beacon_sniff_counter = 0

        self.thong_ke_toan_mang: dict[str, Any] = {
            "tong_goi_tin_phat": 0,
            "tong_goi_tin_thanh_cong": 0,
            "tong_goi_tin_rot_per": 0,
            "tong_goi_tin_ngoai_vung_phu": 0,
            "tong_goi_tin_mat_los": 0,
            "do_tre_trung_binh_ms": 0.0,
            "nang_luong_tieu_hao_joule": 0.0
        }

    @staticmethod
    def _khoang_cach_mat_dat_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        r_earth = 6371000.0
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp = p2 - p1
        dl = math.radians(lon2 - lon1)
        a = math.sin(dp / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
        return r_earth * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

    def kiem_tra_muc_tieu_da_khoa(
        self,
        lat: float,
        lon: float,
        ban_kinh_khoa_m: float = 850.0
    ) -> Optional[str]:
        """Kiểm tra xem tâm cháy (lat, lon) đã được một UAV hoạt động khác tiếp nhận khóa mục tiêu chưa."""
        con_hieu_luc: List[dict[str, Any]] = []
        uav_khoa: Optional[str] = None
        for entry in self.muc_tieu_chay_da_khoa:
            uav = self.danh_sach_tac_nhan.get(entry["uav_id"])
            if not uav or getattr(uav.trang_thai, "value", "") == "NGẮT_KẾT_NỐI":
                continue
            con_hieu_luc.append(entry)
            if self._khoang_cach_mat_dat_m(lat, lon, entry["lat"], entry["lon"]) <= ban_kinh_khoa_m:
                uav_khoa = entry["uav_id"]
        self.muc_tieu_chay_da_khoa = con_hieu_luc
        return uav_khoa

    def khoa_muc_tieu_chay(
        self,
        lat: float,
        lon: float,
        uav_id: str,
        ban_kinh_khoa_m: float = 850.0
    ) -> bool:
        """
        Khóa mục tiêu đám cháy tại (lat, lon) cho `uav_id` theo luật điều phối MAS,
        tránh việc nhiều UAV cùng dồn vào một đám cháy.
        """
        da_khoa_boi = self.kiem_tra_muc_tieu_da_khoa(lat, lon, ban_kinh_khoa_m=ban_kinh_khoa_m)
        if da_khoa_boi and da_khoa_boi != uav_id:
            return False
        if not da_khoa_boi:
            self.muc_tieu_chay_da_khoa.append({
                "lat": float(lat),
                "lon": float(lon),
                "uav_id": uav_id,
                "timestamp": time.time(),
            })
        heat_nodes = getattr(self.scenario, "heat_nodes", None) or []
        for node in heat_nodes:
            if self._khoang_cach_mat_dat_m(lat, lon, node.lat, node.lon) <= ban_kinh_khoa_m:
                node.locked_by = uav_id
        return True

    def mo_khoa_muc_tieu_chay(self, uav_id: str, ha_nhiet: bool = False):
        """Giải phóng khóa mục tiêu của `uav_id` trên bản đồ sự cố; không hạ nhiệt nếu còn active_fires."""
        cac_tam = [e for e in self.muc_tieu_chay_da_khoa if e["uav_id"] == uav_id]
        self.muc_tieu_chay_da_khoa = [e for e in self.muc_tieu_chay_da_khoa if e["uav_id"] != uav_id]
        has_active_fires = bool(getattr(self.scenario, "active_fires", None))
        cho_phep_ha_nhiet = bool(ha_nhiet and not has_active_fires)
        heat_nodes = getattr(self.scenario, "heat_nodes", None) or []
        giu_lai = []
        for node in heat_nodes:
            gan_tam_chay = (
                getattr(node, "locked_by", None) == uav_id
                or any(self._khoang_cach_mat_dat_m(tam["lat"], tam["lon"], node.lat, node.lon) <= 850.0 for tam in cac_tam)
            )
            if cho_phep_ha_nhiet and gan_tam_chay and getattr(node, "is_fire_hotspot", False):
                continue
            if getattr(node, "locked_by", None) == uav_id:
                node.locked_by = None
                if cho_phep_ha_nhiet:
                    node.risk_score = 0.15
                    node.fire_center = None
                    node.fire_status = None
                    node.fire_id = None
            elif cho_phep_ha_nhiet and gan_tam_chay:
                node.risk_score = 0.15
                node.locked_by = None
                node.fire_center = None
                node.fire_status = None
                node.fire_id = None
            giu_lai.append(node)
        if cho_phep_ha_nhiet and hasattr(self.scenario, "heat_nodes"):
            self.scenario.heat_nodes = giu_lai

    def dang_ky_tac_nhan(self, tac_nhan: Any):
        """Đăng ký một Node mạng (UAV, Vệ tinh, Trạm mặt đất) vào không gian mô phỏng"""
        with self.os_kernel.net_state_lock:
            self.danh_sach_tac_nhan[tac_nhan.ma_dinh_danh] = tac_nhan
        print(f"[WIRELESS MEDIUM] Đã đăng ký Node: {tac_nhan.ma_dinh_danh} ({tac_nhan.loai_tac_nhan})")

    def lay_tac_nhan(self, ma_dinh_danh: str) -> Optional[Any]:
        with self.os_kernel.net_state_lock:
            return self.danh_sach_tac_nhan.get(ma_dinh_danh)

    def cap_nhat_nang_luong_tong(self):
        self.thong_ke_toan_mang["nang_luong_tieu_hao_joule"] = round(
            sum(
                getattr(agent, "thong_ke", {}).get("nang_luong_tieu_hao_joule", 0.0)
                for agent in self.danh_sach_tac_nhan.values()
            ),
            3
        )

    def _tim_tuyen_toan_cuc_tham_chieu(
        self,
        ma_bat_dau: str,
        kich_thuoc_kb: float = 250.0,
        loai_tru: Optional[set[str]] = None
    ) -> Optional[dict[str, Any]]:
        """
        Tìm tuyến khả dụng có chi phí QoS + Năng lượng thấp nhất từ một node tới trạm mặt đất.
        Đã khắc phục lỗi tự khóa trạng thái RELAYING: cho phép nhiều UAV cùng định tuyến
        qua một UAV trung chuyển và cân bằng tải dựa trên số luồng relay + mức pin + bộ đệm.
        """
        bat_dau = self.danh_sach_tac_nhan.get(ma_bat_dau)
        if not bat_dau:
            return None
        cac_node_loai_tru = loai_tru or set()
        if bat_dau.loai_tac_nhan == "TRẠM_MẶT_ĐẤT":
            return {"nodes": [ma_bat_dau], "channels": [], "cost": 0.0, "energy_j": 0.0, "delay_ms": 0.0}
        if bat_dau.loai_tac_nhan == "MÁY_BAY" and getattr(bat_dau, "_dang_o_tai_tram", lambda: False)():
            tram_id = next(
                (mid for mid, ag in self.danh_sach_tac_nhan.items() if ag.loai_tac_nhan == "TRẠM_MẶT_ĐẤT"),
                None
            )
            if tram_id:
                return {"nodes": [ma_bat_dau, tram_id], "channels": [], "cost": 0.0, "energy_j": 0.0, "delay_ms": 0.0}

        hang_doi: List[RouteState] = [(0.0, ma_bat_dau, (ma_bat_dau,), ())]
        chi_phi_tot_nhat = {ma_bat_dau: 0.0}
        while hang_doi:
            chi_phi, ma_hien_tai, nodes, channels = heapq.heappop(hang_doi)
            if chi_phi > chi_phi_tot_nhat.get(ma_hien_tai, float("inf")):
                continue
            hien_tai = self.danh_sach_tac_nhan[ma_hien_tai]
            if hien_tai.loai_tac_nhan == "TRẠM_MẶT_ĐẤT":
                ket_qua = {
                    "nodes": list(nodes),
                    "channels": list(channels),
                    "cost": round(chi_phi, 3),
                    "energy_j": round(sum(ch["energy_j"] for ch in channels), 3),
                    "delay_ms": round(sum(ch["do_tre_ms"] for ch in channels), 3)
                }
                if hasattr(bat_dau, "tuyen_ve_tram_cuc_bo"):
                    bat_dau.tuyen_ve_tram_cuc_bo = list(nodes)
                    bat_dau.chi_phi_ve_tram_cuc_bo = round(chi_phi, 3)
                return ket_qua

            for ma_ke_tiep, ke_tiep in self.danh_sach_tac_nhan.items():
                kenh: RouteChannel
                if (
                    ma_ke_tiep in nodes
                    or ma_ke_tiep in cac_node_loai_tru
                    or ke_tiep.trang_thai.value == "NGẮT_KẾT_NỐI"
                    or (
                        ke_tiep.loai_tac_nhan == "MÁY_BAY"
                        and (
                            ke_tiep.trang_thai.value in ("ĐANG_PHẢN_ỨNG", "ĐANG_SẠC")
                            or getattr(ke_tiep, "_dang_o_tai_tram", lambda: False)()
                        )
                    )
                ):
                    continue
                if ke_tiep.loai_tac_nhan in ("MÁY_BAY", "VỆ_TINH_LEO") and ke_tiep.phan_tram_pin < ke_tiep.nguong_pin_yeu_pct:
                    continue
                if (
                    hien_tai.loai_tac_nhan == "MÁY_BAY"
                    and hien_tai.trang_thai.value == "ĐANG_SẠC"
                    and ke_tiep.loai_tac_nhan == "TRẠM_MẶT_ĐẤT"
                    and self.physics.tinh_khoang_cach_3d(hien_tai.toa_do, ke_tiep.toa_do) <= 1.0
                ):
                    kenh = {
                        "kha_dung": True,
                        "do_tre_ms": 0.0,
                        "dung_luong_mbps": max(
                            0.5,
                            hien_tai.radio_config["bang_thong_hz"] / 1_000_000.0
                        ),
                        "per": 0.0,
                        "snr_db": 100.0
                    }
                else:
                    kenh = self.physics.danh_gia_chat_luong_kenh_truyen(
                        pos1=hien_tai.toa_do,
                        pos2=ke_tiep.toa_do,
                        tan_so_hz=hien_tai.radio_config["tan_so_hz"],
                        cong_suat_phat_dbm=hien_tai.radio_config["cong_suat_phat_dbm"],
                        bang_thong_hz=hien_tai.radio_config["bang_thong_hz"]
                    )
                if not kenh["kha_dung"] or kenh["per"] >= 0.25 or kenh["dung_luong_mbps"] <= 0:
                    continue

                energy_j = self._uoc_tinh_nang_luong_truyen(hien_tai, kich_thuoc_kb)
                kenh_co_nang_luong: RouteChannel = {**kenh, "energy_j": energy_j}

                so_luong_relay = len(getattr(ke_tiep, "_relay_requesters", ()))
                weight = self._tinh_trong_so_lien_ket(
                    do_tre_ms=kenh["do_tre_ms"],
                    dung_luong_mbps=kenh["dung_luong_mbps"],
                    energy_j=energy_j,
                    loai_ke_tiep=ke_tiep.loai_tac_nhan,
                    tai_hang_doi_pct=ke_tiep.tai_hang_doi_pct,
                    pin_pct=ke_tiep.phan_tram_pin,
                    so_luong_relay=so_luong_relay,
                )
                next_cost = chi_phi + weight
                if next_cost < chi_phi_tot_nhat.get(ma_ke_tiep, float("inf")):
                    chi_phi_tot_nhat[ma_ke_tiep] = next_cost
                    heapq.heappush(
                        hang_doi,
                        (next_cost, ma_ke_tiep, (*nodes, ma_ke_tiep), (*channels, kenh_co_nang_luong))
                    )

        if hasattr(bat_dau, "tuyen_ve_tram_cuc_bo"):
            bat_dau.tuyen_ve_tram_cuc_bo = []
            bat_dau.chi_phi_ve_tram_cuc_bo = float("inf")
        return None

    def tim_tuyen_den_tram_mat_dat(
        self,
        ma_bat_dau: str,
        kich_thuoc_kb: float = 250.0,
        loai_tru: Optional[set[str]] = None
    ) -> Optional[dict[str, Any]]:
        """Chọn tuyến từ quảng bá sink của các láng giềng 1-hop, không quét toàn đồ thị."""
        bat_dau = self.danh_sach_tac_nhan.get(ma_bat_dau)
        if not bat_dau:
            return None
        if bat_dau.loai_tac_nhan == "TRẠM_MẶT_ĐẤT":
            return {
                "nodes": [ma_bat_dau],
                "channels": [],
                "cost": 0.0,
                "energy_j": 0.0,
                "delay_ms": 0.0,
                "capacity_mbps": float("inf")
            }
        if bat_dau.loai_tac_nhan == "MÁY_BAY" and getattr(bat_dau, "_dang_o_tai_tram", lambda: False)():
            tram_id = next(
                (mid for mid, ag in self.danh_sach_tac_nhan.items() if ag.loai_tac_nhan == "TRẠM_MẶT_ĐẤT"),
                None
            )
            if tram_id:
                route_tram = {
                    "nodes": [ma_bat_dau, tram_id],
                    "channels": [],
                    "cost": 0.0,
                    "energy_j": 0.0,
                    "delay_ms": 0.0,
                    "capacity_mbps": float("inf")
                }
                bat_dau.tuyen_ve_tram_cuc_bo = list(route_tram["nodes"])
                bat_dau.chi_phi_ve_tram_cuc_bo = 0.0
                return route_tram

        excluded = loai_tru or set()
        now = time.time()
        candidates = []
        for neighbor_id, info in bat_dau.bang_lang_gieng.items():
            neighbor = self.danh_sach_tac_nhan.get(neighbor_id)
            if (
                not neighbor
                or neighbor_id in excluded
                or now - info.get("thoi_diem_cap_nhat", 0.0)
                > getattr(bat_dau, "NEIGHBOR_TIMEOUT_SEC", 15.0)
                or info.get("trang_thai") in (
                    "NGẮT_KẾT_NỐI",
                    "ĐANG_PHẢN_ỨNG",
                    "ĐANG_SẠC"
                )
                or (
                    neighbor.loai_tac_nhan == "MÁY_BAY"
                    and getattr(neighbor, "_dang_o_tai_tram", lambda: False)()
                )
                or (
                    info.get("loai_tac_nhan") in ("MÁY_BAY", "VỆ_TINH_LEO")
                    and info.get("pin_pct", 0.0) < getattr(neighbor, "nguong_pin_yeu_pct", 20.0)
                )
            ):
                continue

            if info.get("loai_tac_nhan") == "TRẠM_MẶT_ĐẤT":
                downstream_path = [neighbor_id]
                downstream_cost = 0.0
                downstream_energy = 0.0
                downstream_delay = 0.0
                downstream_capacity = float("inf")
            else:
                downstream_path = list(info.get("path_to_sink", []))
                if (
                    not downstream_path
                    or downstream_path[0] != neighbor_id
                    or ma_bat_dau in downstream_path
                    or excluded.intersection(downstream_path)
                    or len(downstream_path) != len(set(downstream_path))
                    or downstream_path[-1] not in self.danh_sach_tac_nhan
                    or self.danh_sach_tac_nhan[downstream_path[-1]].loai_tac_nhan
                    != "TRẠM_MẶT_ĐẤT"
                ):
                    continue
                downstream_cost = float(info.get("cost_to_sink", float("inf")))
                downstream_energy = float(info.get("energy_to_sink_j", 0.0))
                downstream_delay = float(info.get("delay_to_sink_ms", 0.0))
                downstream_capacity = float(info.get("capacity_to_sink_mbps", 0.0))
                if not math.isfinite(downstream_cost):
                    continue

            if (
                bat_dau.loai_tac_nhan == "MÁY_BAY"
                and bat_dau.trang_thai.value == "ĐANG_SẠC"
                and info.get("loai_tac_nhan") == "TRẠM_MẶT_ĐẤT"
                and self.physics.tinh_khoang_cach_3d(bat_dau.toa_do, neighbor.toa_do) <= 1.0
            ):
                channel = {
                    "kha_dung": True,
                    "do_tre_ms": 0.0,
                    "dung_luong_mbps": max(
                        0.5,
                        bat_dau.radio_config["bang_thong_hz"] / 1_000_000.0
                    ),
                    "per": 0.0,
                    "snr_db": 100.0
                }
            else:
                channel = self.physics.danh_gia_chat_luong_kenh_truyen(
                    pos1=bat_dau.toa_do,
                    pos2=neighbor.toa_do,
                    tan_so_hz=bat_dau.radio_config["tan_so_hz"],
                    cong_suat_phat_dbm=bat_dau.radio_config["cong_suat_phat_dbm"],
                    bang_thong_hz=bat_dau.radio_config["bang_thong_hz"]
                )
            if (
                not channel["kha_dung"]
                or channel["per"] >= 0.25
                or channel["dung_luong_mbps"] <= 0
            ):
                continue

            energy_j = self._uoc_tinh_nang_luong_truyen(bat_dau, kich_thuoc_kb)
            so_luong_relay = len(getattr(neighbor, "_relay_requesters", ()))
            link_cost = self._tinh_trong_so_lien_ket(
                do_tre_ms=channel["do_tre_ms"],
                dung_luong_mbps=channel["dung_luong_mbps"],
                energy_j=energy_j,
                loai_ke_tiep=str(info.get("loai_tac_nhan", "")),
                tai_hang_doi_pct=float(info.get("tai_hang_doi_pct", 0.0)),
                pin_pct=float(info.get("pin_pct", 100.0)),
                so_luong_relay=so_luong_relay,
            )

            path = [ma_bat_dau, *downstream_path]
            if len(path) != len(set(path)):
                continue
            capacity = min(channel["dung_luong_mbps"], downstream_capacity)
            candidates.append((
                link_cost + downstream_cost,
                neighbor_id,
                {
                    "nodes": path,
                    "channels": [{
                        **channel,
                        "energy_j": energy_j,
                        "dung_luong_mbps": capacity
                    }],
                    "cost": round(link_cost + downstream_cost, 3),
                    "energy_j": round(energy_j + downstream_energy, 3),
                    "delay_ms": round(channel["do_tre_ms"] + downstream_delay, 3),
                    "capacity_mbps": capacity
                }
            ))

        if not candidates:
            bat_dau.tuyen_ve_tram_cuc_bo = []
            bat_dau.chi_phi_ve_tram_cuc_bo = float("inf")
            bat_dau.nang_luong_ve_tram_cuc_bo = 0.0
            bat_dau.do_tre_ve_tram_cuc_bo_ms = 0.0
            bat_dau.dung_luong_ve_tram_cuc_bo_mbps = 0.0
            return None

        _, _, route = min(candidates, key=lambda item: (item[0], item[1]))
        bat_dau.tuyen_ve_tram_cuc_bo = list(route["nodes"])
        bat_dau.chi_phi_ve_tram_cuc_bo = route["cost"]
        bat_dau.nang_luong_ve_tram_cuc_bo = route["energy_j"]
        bat_dau.do_tre_ve_tram_cuc_bo_ms = route["delay_ms"]
        bat_dau.dung_luong_ve_tram_cuc_bo_mbps = route["capacity_mbps"]
        return route

    @staticmethod
    def _tinh_trong_so_lien_ket(
        do_tre_ms: float,
        dung_luong_mbps: float,
        energy_j: float,
        loai_ke_tiep: str,
        tai_hang_doi_pct: float,
        pin_pct: float,
        so_luong_relay: int = 0,
    ) -> float:
        """
        Hàm chi phí đa mục tiêu chuẩn hóa không thứ nguyên (Weighted Sum Normalization) cho Dijkstra.
        Các ngưỡng tham chiếu (D_ref=50 ms, B_ref=10 Mbps, E_ref=5 J, R_ref=5 luồng)
        và hệ số trọng số ưu tiên là tham số giả lập thực nghiệm của mô hình mô phỏng.
        """
        d_norm = min(1.0, max(0.0, float(do_tre_ms)) / 50.0)
        b_norm = min(1.0, 10.0 / max(0.5, float(dung_luong_mbps)))
        e_norm = min(1.0, max(0.0, float(energy_j)) / 5.0)
        q_norm = min(1.0, max(0.0, float(tai_hang_doi_pct)) / 100.0)
        is_uav = (loai_ke_tiep == "MÁY_BAY")
        r_norm = min(1.0, max(0, int(so_luong_relay)) / 5.0) if is_uav else 0.0
        p_norm = ((100.0 - min(100.0, max(0.0, float(pin_pct)))) / 100.0) if is_uav else 0.0
        leo_penalty = 1.50 if loai_ke_tiep == "VỆ_TINH_LEO" else 0.0

        return (
            0.20 * d_norm
            + 0.20 * b_norm
            + 0.10 * e_norm
            + 0.10 * q_norm
            + 0.20 * r_norm
            + 0.20 * p_norm
            + leo_penalty
        )

    @staticmethod
    def _uoc_tinh_nang_luong_truyen(agent, kich_thuoc_kb: float) -> float:
        cong_suat_w = 10.0 ** ((agent.radio_config["cong_suat_phat_dbm"] - 30.0) / 10.0)
        return round(cong_suat_w * 0.05 * (kich_thuoc_kb / 10.0), 4)

    def tim_tuyen_dijkstra_ve_tram(
        self,
        ma_bat_dau: str,
        kich_thuoc_kb: float = 250.0,
        loai_tru: Optional[set[str]] = None
    ) -> Optional[dict[str, Any]]:
        """Tìm tuyến ngắn nhất đa mục tiêu về Trạm mặt đất bằng thuật toán Dijkstra."""
        return self._tim_tuyen_toan_cuc_tham_chieu(
            ma_bat_dau=ma_bat_dau,
            kich_thuoc_kb=kich_thuoc_kb,
            loai_tru=loai_tru,
        )

    async def gui_chang_tiep_ve_tram(
        self,
        ma_node: str,
        thong_diep: dict[str, Any],
        lo_trinh_da_di: Optional[list[str]] = None
    ) -> Optional[dict[str, Any]]:
        """Gửi một chặng kế tiếp qua định tuyến láng giềng hoặc Dijkstra; khi chặng lỗi, thử láng giềng thay thế."""
        kich_thuoc_kb = float(thong_diep.get("kich_thuoc_kb", 12.0))
        lo_trinh = list(lo_trinh_da_di or thong_diep.get("lo_trinh_di_qua", []))
        if not lo_trinh or lo_trinh[-1] != ma_node:
            if ma_node in lo_trinh:
                lo_trinh = lo_trinh[:lo_trinh.index(ma_node) + 1]
            else:
                lo_trinh.append(ma_node)

        da_thu = set()
        for _ in range(len(self.danh_sach_tac_nhan)):
            loai_tru = (set(lo_trinh) - {ma_node}) | da_thu
            tuyen = self.tim_tuyen_den_tram_mat_dat(
                ma_node,
                kich_thuoc_kb=kich_thuoc_kb,
                loai_tru=loai_tru
            )
            if not tuyen or len(tuyen.get("nodes", [])) < 2:
                tuyen = self._tim_tuyen_toan_cuc_tham_chieu(
                    ma_node,
                    kich_thuoc_kb=kich_thuoc_kb,
                    loai_tru=loai_tru
                )
            if not tuyen or len(tuyen["nodes"]) < 2:
                return None

            ma_ke_tiep = tuyen["nodes"][1]
            da_gui = await self.gui_don_huong(
                ma_node,
                ma_ke_tiep,
                {
                    **thong_diep,
                    "lo_trinh_di_qua": [*lo_trinh, ma_ke_tiep],
                    "tuyen_du_kien": list(tuyen["nodes"])
                }
            )
            if da_gui:
                return tuyen
            da_thu.add(ma_ke_tiep)
        return None

    # =========================================================================
    # 1. CƠ CHẾ TRUYỀN GÓI TIN QUA KÊNH VẬT LÝ (UNICAST & BROADCAST)
    # =========================================================================
    async def gui_don_huong(self, ma_nguoi_gui: str, ma_nguoi_nhan: str, thong_diep: dict) -> bool:
        """
        Truyền gói tin điểm-nối-điểm (Unicast) trên Mặt phẳng Mạng vô tuyến mô phỏng:
        - Giảm TTL ở mỗi hop, hủy gói nếu TTL <= 0 (TTL_DROP).
        - Đóng gói khung nhị phân 26-byte Header + Checksum CRC32 (IEEE 802.3) trong mô phỏng kênh PHY.
        - Kênh tin cậy mô phỏng cơ chế Stop-and-Wait RDT/ARQ (Kurose & Ross, Ch. 3, tối đa 3 lần khi lỗi PER);
          kênh Telemetry không tin cậy (UDP) gửi 1 lần và hủy ngay nếu sai CRC32.
        """
        nguoi_gui = self.danh_sach_tac_nhan.get(ma_nguoi_gui)
        nguoi_nhan = self.danh_sach_tac_nhan.get(ma_nguoi_nhan)

        if not nguoi_gui or not nguoi_nhan:
            return False

        loai_tin = thong_diep.get("loai_tin", "UNKNOWN")
        la_beacon = (loai_tin == "HELLO_BEACON")
        kich_thuoc_kb = float(thong_diep.get("kich_thuoc_kb", 10.0))
        ttl = int(thong_diep.get("_ttl", 16)) - 1
        if ttl <= 0:
            self.network_stack.record_ttl_drop()
            if not la_beacon:
                await self.gui_log_ra_web(
                    "WARN",
                    f"[TTL_DROP] Hết TTL (0) tại {ma_nguoi_gui} -> {ma_nguoi_nhan} ({loai_tin}) ➔ Hủy gói chống lặp tuyến."
                )
            return False

        proto = self.network_stack.determine_protocol(loai_tin)
        prio = phan_loai_do_uu_tien(loai_tin)
        src_ip = getattr(nguoi_gui, "ip_address", None) or assign_node_ip(
            ma_nguoi_gui, nguoi_gui.loai_tac_nhan, getattr(nguoi_gui, "zone_id", None)
        )
        dst_ip = getattr(nguoi_nhan, "ip_address", None) or assign_node_ip(
            ma_nguoi_nhan, nguoi_nhan.loai_tac_nhan, getattr(nguoi_nhan, "zone_id", None)
        )

        # Tính toán thông số Lớp Vật lý (PHY Layer) tại thời điểm phát sóng
        thong_so_kenh = self.physics.danh_gia_chat_luong_kenh_truyen(
            pos1=nguoi_gui.toa_do,
            pos2=nguoi_nhan.toa_do,
            tan_so_hz=nguoi_gui.radio_config["tan_so_hz"],
            cong_suat_phat_dbm=nguoi_gui.radio_config["cong_suat_phat_dbm"],
            bang_thong_hz=nguoi_gui.radio_config["bang_thong_hz"]
        )

        # Với bản tin quảng bá láng giềng 1-hop (HELLO_BEACON), các node ngoài bán kính phủ sóng tự nhiên không thu được sóng
        if la_beacon and not thong_so_kenh["kha_dung"]:
            return False

        # Khấu trừ năng lượng phát sóng (Tx) của người gửi (nếu chưa khấu trừ ở mức quảng bá)
        if not thong_diep.get("_da_khau_tru_tx") and hasattr(nguoi_gui, "tieu_hao_nang_luong_truyen_tin"):
            nang_luong_tx = nguoi_gui.tieu_hao_nang_luong_truyen_tin(
                kich_thuoc_kb, la_phat_song=True
            )
        else:
            nang_luong_tx = float(thong_diep.get("_nang_luong_tx", 0.0))
        self.cap_nhat_nang_luong_tong()

        if not la_beacon:
            self.thong_ke_toan_mang["tong_goi_tin_phat"] += 1

        thong_so_kenh["nang_luong_tx_joule"] = nang_luong_tx
        thong_so_kenh["do_kha_dung_pct"] = self.thong_ke_lien_tuc.get("do_kha_dung_pct", 100.0)
        thong_so_kenh["thoi_gian_mat_lien_lac_giay"] = self.thong_ke_lien_tuc.get("thoi_gian_mat_lien_lac_giay", 0.0)
        thong_so_kenh["nang_luong_toan_mang_joule"] = self.thong_ke_toan_mang["nang_luong_tieu_hao_joule"]

        # Kiểm tra 1: Nằm ngoài Bán kính phủ sóng vô tuyến (d > R_max)
        if not thong_so_kenh["kha_dung"]:
            self.thong_ke_toan_mang["tong_goi_tin_ngoai_vung_phu"] += 1
            self.thong_ke_toan_mang["tong_goi_tin_mat_los"] = self.thong_ke_toan_mang["tong_goi_tin_ngoai_vung_phu"]
            if hasattr(self.scenario, "ghi_log_thuc_nghiem_qos"):
                self.scenario.ghi_log_thuc_nghiem_qos(
                    ma_nguoi_gui, ma_nguoi_nhan, loai_tin, thong_so_kenh, "NGOAI_VUNG_PHU_SONG"
                )
            return False

        # Kiểm tra 2: Đóng gói khung nhị phân + truyền Socket TCP/UDP thật kèm kiểm chứng CRC32 & ARQ Retransmission
        payload_bytes = encode_payload_json({**thong_diep, "loai_tin": loai_tin})
        per = float(thong_so_kenh.get("per", 0.0))
        final_pkt: Optional[NetworkPacket] = None
        hop_ok = False
        retrans_count = 0

        if proto == "TCP":
            for attempt in range(3):
                seq = self.network_stack.next_seq()
                flags = FLAG_DATA | (FLAG_RETRANS if attempt > 0 else 0)
                pkt = NetworkPacket(
                    src_ip=src_ip,
                    dst_ip=dst_ip,
                    protocol="TCP",
                    priority=prio,
                    seq_num=seq,
                    ack_num=0,
                    ttl=ttl,
                    flags=flags,
                    payload=payload_bytes,
                )
                bit_err = (random.random() < per)
                delivered_ok, crc_ok, ack_num = self.network_stack.send_over_wire(pkt, inject_bit_error=bit_err)
                if delivered_ok and crc_ok:
                    pkt.ack_num = ack_num or pkt.seq_num
                    final_pkt = pkt
                    hop_ok = True
                    retrans_count = attempt
                    status_lbl = f"RETRANS_OK(#{attempt})" if attempt > 0 else "TCP_ACK"
                    self.network_stack.record_sniffer(pkt, loai_tin, ma_nguoi_gui, ma_nguoi_nhan, status_lbl, retrans_count=attempt)
                    if attempt > 0 and not la_beacon:
                        await self.gui_log_ra_web(
                            "WARN",
                            f"[TCP_ARQ] 🔄 Truyền lại #{attempt} thành công ({ma_nguoi_gui} -> {ma_nguoi_nhan}) | Seq={seq} Ack={pkt.ack_num} CRC=0x{pkt.crc32:08X}"
                        )
                    break
                final_pkt = pkt
            if not hop_ok and final_pkt is not None:
                self.network_stack.record_sniffer(final_pkt, loai_tin, ma_nguoi_gui, ma_nguoi_nhan, "TCP_DROP_CRC", retrans_count=2)
        else:
            seq = self.network_stack.next_seq()
            pkt = NetworkPacket(
                src_ip=src_ip,
                dst_ip=dst_ip,
                protocol="UDP",
                priority=prio,
                seq_num=seq,
                ack_num=0,
                ttl=ttl,
                flags=FLAG_DATA,
                payload=payload_bytes,
            )
            bit_err = (random.random() < per)
            delivered_ok, crc_ok, _ = self.network_stack.send_over_wire(pkt, inject_bit_error=bit_err)
            final_pkt = pkt
            hop_ok = bool(delivered_ok and crc_ok)
            if not la_beacon:
                self.network_stack.record_sniffer(
                    pkt, loai_tin, ma_nguoi_gui, ma_nguoi_nhan,
                    "UDP_OK" if hop_ok else "UDP_CRC_ERR", retrans_count=0
                )
            else:
                self._beacon_sniff_counter += 1
                if self._beacon_sniff_counter % 10 == 0:
                    self.network_stack.record_sniffer(
                        pkt, loai_tin, ma_nguoi_gui, ma_nguoi_nhan,
                        "UDP_OK" if hop_ok else "UDP_CRC_ERR", retrans_count=0
                    )

        if not hop_ok or final_pkt is None:
            if not la_beacon:
                self.thong_ke_toan_mang["tong_goi_tin_rot_per"] += 1
                if hasattr(self.scenario, "ghi_log_thuc_nghiem_qos"):
                    self.scenario.ghi_log_thuc_nghiem_qos(
                        ma_nguoi_gui, ma_nguoi_nhan, loai_tin, thong_so_kenh, "ROT_GOI_DO_NHIEU_PER"
                    )
                crc_hex = f"0x{final_pkt.crc32:08X}" if final_pkt else "0x00000000"
                await self.gui_log_ra_web(
                    "WARN",
                    f"[CRC32_ERR] Rớt {proto} {ma_nguoi_gui} -> {ma_nguoi_nhan} ({loai_tin}) | CRC={crc_hex} | SNR={thong_so_kenh['snr_db']} dB"
                )
            return False

        # Tính tổng độ trễ = Độ trễ lan truyền sóng (Propagation) + Độ trễ truyền dữ liệu (Serialization)
        dung_luong_mbps = max(0.1, thong_so_kenh["dung_luong_mbps"])
        do_tre_truyen_tai_ms = (kich_thuoc_kb * 8.0 / 1000.0) / dung_luong_mbps * 1000.0
        tong_do_tre_ms = round(thong_so_kenh["do_tre_ms"] + do_tre_truyen_tai_ms, 3)
        thong_so_kenh["do_tre_ms"] = tong_do_tre_ms

        # Gửi dữ liệu liên kết vô tuyến để hiển thị tia sóng trên bản đồ
        if not la_beacon:
            await self.phat_su_kien_ra_web("RF_LINK", {
                "tu_node": ma_nguoi_gui,
                "den_node": ma_nguoi_nhan,
                "loai_tin": loai_tin,
                "protocol": proto,
                "priority": f"P{prio}",
                "ttl": ttl,
                "seq": final_pkt.seq_num,
                "crc32": f"0x{final_pkt.crc32:08X}",
                "retrans": retrans_count,
                "toa_do_dau": nguoi_gui.toa_do,
                "toa_do_cuoi": nguoi_nhan.toa_do,
                "snr_db": thong_so_kenh["snr_db"],
                "do_tre_ms": tong_do_tre_ms,
                "dung_luong_mbps": thong_so_kenh["dung_luong_mbps"]
            })

        await asyncio.sleep(min(0.25, max(0.01, tong_do_tre_ms / 150.0)))

        # Đính kèm Metadata lớp Vật lý và thông tin Beacon người gửi để người nhận cập nhật Bảng láng giềng cục bộ
        goi_tin_hoan_chinh = {
            k: v for k, v in thong_diep.items()
            if not k.startswith("_")
        }
        goi_tin_hoan_chinh.update({
            "nguoi_gui": ma_nguoi_gui,
            "nguoi_nhan": ma_nguoi_nhan,
            "loai_nguoi_gui": nguoi_gui.loai_tac_nhan,
            "toa_do_nguoi_gui": dict(nguoi_gui.toa_do),
            "pin_pct": getattr(nguoi_gui, "phan_tram_pin", 100.0),
            "tai_hang_doi_pct": getattr(nguoi_gui, "tai_hang_doi_pct", 0.0),
            "trang_thai_nguoi_gui": getattr(nguoi_gui.trang_thai, "value", str(nguoi_gui.trang_thai)),
            "zone_id": getattr(nguoi_gui, "zone_id", None),
            "cost_to_sink": getattr(nguoi_gui, "chi_phi_ve_tram_cuc_bo", float("inf")),
            "path_to_sink": list(getattr(nguoi_gui, "tuyen_ve_tram_cuc_bo", [])),
            "energy_to_sink_j": getattr(nguoi_gui, "nang_luong_ve_tram_cuc_bo", 0.0),
            "delay_to_sink_ms": getattr(nguoi_gui, "do_tre_ve_tram_cuc_bo_ms", 0.0),
            "capacity_to_sink_mbps": getattr(
                nguoi_gui,
                "dung_luong_ve_tram_cuc_bo_mbps",
                float("inf")
            ),
            "protocol": proto,
            "ttl": ttl,
            "crc32": f"0x{final_pkt.crc32:08X}",
            "thoi_diem_gui": thong_diep.get("thoi_diem_gui", time.time()),
            "thong_so_kenh": thong_so_kenh
        })

        if hasattr(nguoi_nhan, "cap_nhat_lang_gieng_tu_goi_tin"):
            nguoi_nhan.cap_nhat_lang_gieng_tu_goi_tin(goi_tin_hoan_chinh)

        await nguoi_nhan.hang_doi_thong_diep.put(goi_tin_hoan_chinh)

        # Cập nhật thống kê toàn mạng & ghi vào file CSV thực nghiệm cho các bản tin dữ liệu/điều khiển
        if not la_beacon:
            self.thong_ke_toan_mang["tong_goi_tin_thanh_cong"] += 1
            n_ok = self.thong_ke_toan_mang["tong_goi_tin_thanh_cong"]
            cur_avg = self.thong_ke_toan_mang["do_tre_trung_binh_ms"]
            self.thong_ke_toan_mang["do_tre_trung_binh_ms"] = round(((cur_avg * (n_ok - 1)) + tong_do_tre_ms) / n_ok, 3)
            self.cap_nhat_nang_luong_tong()

            if hasattr(self.scenario, "ghi_log_thuc_nghiem_qos"):
                self.scenario.ghi_log_thuc_nghiem_qos(
                    ma_nguoi_gui, ma_nguoi_nhan, loai_tin, thong_so_kenh, "THANH_CONG"
                )
        return True

    async def phat_song_quang_ba(
        self,
        ma_nguoi_gui: str,
        thong_diep: dict,
        danh_sach_nguoi_nhan: Optional[set[str]] = None
    ):
        """
        Phát sóng quảng bá (Broadcast) đến tất cả các Node lân cận nằm trong Bán kính phủ sóng (Communication Range).
        Chỉ khấu trừ năng lượng phát sóng 1 lần cho mỗi bản tin quảng bá vô hướng.
        """
        nguoi_gui = self.danh_sach_tac_nhan.get(ma_nguoi_gui)
        kich_thuoc_kb = float(thong_diep.get("kich_thuoc_kb", 10.0))
        nang_luong_tx = 0.0
        if nguoi_gui and hasattr(nguoi_gui, "tieu_hao_nang_luong_truyen_tin"):
            nang_luong_tx = nguoi_gui.tieu_hao_nang_luong_truyen_tin(kich_thuoc_kb, la_phat_song=True)

        goi_quang_ba = {
            **thong_diep,
            "_da_khau_tru_tx": True,
            "_nang_luong_tx": nang_luong_tx
        }
        cac_tac_vu = []
        for ma_dich in self.danh_sach_tac_nhan:
            if (
                ma_dich != ma_nguoi_gui
                and (
                    danh_sach_nguoi_nhan is None
                    or ma_dich in danh_sach_nguoi_nhan
                )
            ):
                cac_tac_vu.append(
                    self.gui_don_huong(ma_nguoi_gui, ma_dich, goi_quang_ba.copy())
                )
        if cac_tac_vu:
            await asyncio.gather(*cac_tac_vu)

    async def gui_goi_tin_da_chang_ve_tram(self, ma_nguon: str, thong_diep: dict) -> bool:
        """
        Định tuyến và truyền gói tin đa chặng (Multi-hop E2E) từ UAV nguồn về Trạm mặt đất:
        - Chọn từng chặng từ quảng bá láng giềng và thử tuyến thay thế khi truyền thất bại.
        - Phát từng chặng qua `gui_don_huong` để kích hoạt hiệu ứng tia `RF_LINK` và thống kê QoS.
        - Phát sự kiện `E2E_ROUTE_COMPLETED` khi gói tin tới Trạm mặt đất.
        """
        source = self.danh_sach_tac_nhan.get(ma_nguon)
        if not source or source.loai_tac_nhan == "TRẠM_MẶT_ĐẤT":
            return False

        kich_thuoc_kb = float(thong_diep.get("kich_thuoc_kb", 12.0))
        loai_tin = thong_diep.get("loai_tin") or thong_diep.get("loai_thong_diep") or "TELEMETRY_REPORT"
        nodes = [ma_nguon]
        channels = []
        current_id = ma_nguon
        ground_station = None

        for _ in range(len(self.danh_sach_tac_nhan)):
            current = self.danh_sach_tac_nhan.get(current_id)
            if not current:
                return False
            if current.loai_tac_nhan == "TRẠM_MẶT_ĐẤT":
                ground_station = current
                break

            selected_route = await self.gui_chang_tiep_ve_tram(
                current_id,
                {
                    **thong_diep,
                    "loai_tin": loai_tin,
                    "kich_thuoc_kb": kich_thuoc_kb,
                    "_ttl": max(1, 16 - (len(nodes) - 1)),
                },
                nodes
            )
            if not selected_route:
                return False

            next_id = selected_route["nodes"][1]
            if next_id in nodes:
                return False
            nodes.append(next_id)
            if selected_route.get("channels"):
                channels.append(selected_route["channels"][0])
            current_id = next_id

        if not ground_station:
            return False

        is_fire_route = bool(
            loai_tin in ("FIRE_ALERT", "FIRE_DETECTION", "GIAO_NHIEM_VU_DU_LIEU")
            or getattr(getattr(source, "uav_state", None), "value", getattr(source, "uav_state", None)) == "CONTAINING"
            or getattr(source, "_tam_khoanh_vung_chay", None) is not None
        )
        last_ch = channels[-1] if channels else {}
        tong_tre_ms = round(sum(float(channel.get("do_tre_ms", 0.0)) for channel in channels), 2)
        tong_nang_luong = round(sum(float(channel.get("energy_j", 0.0)) for channel in channels), 3)
        proto_e2e = self.network_stack.determine_protocol(loai_tin)
        ket_qua_e2e = {
            "ma_phien": thong_diep.get("ma_phien_dau_gia", "TELEM"),
            "uav_id": ma_nguon,
            "loai_su_co": loai_tin,
            "protocol": proto_e2e,
            "ttl_remaining": max(1, 16 - (len(nodes) - 1)),
            "is_fire_route": is_fire_route,
            "lo_trinh": nodes,
            "lo_trinh_hop": nodes,
            "so_chang_hop": len(nodes) - 1,
            "e2e_delay_ms": tong_tre_ms,
            "tong_do_tre_ms": tong_tre_ms,
            "snr_chang_cuoi_db": last_ch.get("snr_db", 0.0),
            "dung_luong_chang_cuoi_mbps": last_ch.get("dung_luong_mbps", 0.0),
            "nang_luong_du_kien_joule": tong_nang_luong,
            "thoi_gian": time.strftime("%H:%M:%S"),
            "trang_thai_phu_song": "KET_NOI",
        }
        if is_fire_route or not self.tuyen_e2e_chay_hien_tai:
            await self.phat_su_kien_ra_web("E2E_ROUTE_COMPLETED", ket_qua_e2e)
        return True

    # =========================================================================
    # 2. QUẢN LÝ KẾT NỐI WEBSOCKET & ĐỒNG BỘ GIAO DIỆN
    # =========================================================================
    ALLOWED_SWARM_LOG_TAGS = (
        "[HEATMAP_ANOMALY]",
        "[GRADIENT_PULL]",
        "[FIRE_DETECTED]",
        "[TARGET_LOCKED]",
        "[P2P_HANDOVER]",
        "[DIJKSTRA_REPORT]",
        "[OS_SYNC]",
        "[OS_SCHED]",
        "[MUTEX]",
        "[TCP_ARQ]",
        "[CRC32_ERR]",
        "[TTL_DROP]",
        "[CLIENT_NET]",
    )

    async def phat_su_kien_ra_web(self, loai_su_kien: str, du_lieu):
        """Đẩy gói tin JSON thời gian thực xuống tất cả trình duyệt đang mở"""
        thoi_diem = time.strftime("%H:%M:%S")
        if loai_su_kien == "E2E_ROUTE_COMPLETED":
            if isinstance(du_lieu, dict) and (
                du_lieu.get("is_fire_route") or not self.tuyen_e2e_chay_hien_tai
            ):
                self.tuyen_e2e_gan_nhat = du_lieu
        elif loai_su_kien == "LOG" and isinstance(du_lieu, dict):
            noi_dung_log = str(du_lieu.get("noi_dung", ""))
            if not any(tag in noi_dung_log for tag in self.ALLOWED_SWARM_LOG_TAGS):
                return
            self.nhat_ky_gan_day.append({
                "timestamp": thoi_diem,
                "cap_do": du_lieu.get("cap_do", "INFO"),
                "noi_dung": noi_dung_log
            })
            if len(self.nhat_ky_gan_day) > 60:
                self.nhat_ky_gan_day = self.nhat_ky_gan_day[-60:]

        if not self.danh_sach_client_ws:
            return

        payload = json.dumps({
            "type": loai_su_kien,
            "timestamp": thoi_diem,
            "data": du_lieu
        }, ensure_ascii=False)

        client_loi = []
        for ws in self.danh_sach_client_ws:
            try:
                await ws.send_text(payload)
            except Exception:
                client_loi.append(ws)

        for ws in client_loi:
            if ws in self.danh_sach_client_ws:
                self.danh_sach_client_ws.remove(ws)

    async def gui_log_ra_web(self, loai_log: str, noi_dung: str):
        """Hàm tiện ích in nhật ký sự kiện lên bảng điều khiển Web (chỉ lọc 6 bản tin giao thức Bầy đàn)"""
        if not any(tag in str(noi_dung) for tag in self.ALLOWED_SWARM_LOG_TAGS):
            return
        await self.phat_su_kien_ra_web("LOG", {
            "cap_do": loai_log,
            "noi_dung": noi_dung
        })

    def tinh_thong_ke_phu_song_vung(self) -> Dict[str, dict]:
        """Tính trạng thái độ phủ sóng thực tế của 7 phân khu rừng Sơn Trà (định mức 4/4 UAV tuần tra/vùng)."""
        vung_stats: Dict[str, dict] = {}
        for idx in range(1, 8):
            zone_id = f"ZONE_{idx:02d}"
            vung_stats[zone_id] = {
                "zone_id": zone_id,
                "tong_uav": 4,
                "uav_hoat_dong": 0,
                "uav_dang_sac_hoac_rtb": 0,
                "uav_nhan_ban_giao": 0,
                "trang_thai": "DU_PHU_SONG"
            }

        active_air_states = ("PATROLLING", "RELAYING", "CONTAINING", "RESPONDING")
        with self.os_kernel.net_state_lock:
            agents_snapshot = list(self.danh_sach_tac_nhan.values())
        for agent in agents_snapshot:
            if agent.loai_tac_nhan != "MÁY_BAY":
                continue
            z_id = getattr(agent, "zone_id", None)
            if not z_id or z_id not in vung_stats:
                continue
            entry = vung_stats[z_id]
            fsm_val = getattr(getattr(agent, "trang_thai", None), "value", str(getattr(agent, "trang_thai", "")))
            uav_state_obj = getattr(agent, "uav_state", None)
            uav_state_val = str(getattr(uav_state_obj, "value", uav_state_obj or "")).upper()
            at_station = callable(getattr(agent, "_dang_o_tai_tram", None)) and agent._dang_o_tai_tram()

            if fsm_val == "NGẮT_KẾT_NỐI" or uav_state_val in ("RETURNING", "CHARGING", "OFFLINE") or fsm_val in (
                "PIN_YẾU_RTB", "ĐANG_QUAY_VỀ_SẠC", "ĐANG_SẠC"
            ):
                entry["uav_dang_sac_hoac_rtb"] += 1
            elif uav_state_val in active_air_states and not at_station:
                entry["uav_hoat_dong"] += 1

            vung_phu = getattr(agent, "_vung_tuan_tra_phu", None)
            if vung_phu:
                entry["uav_nhan_ban_giao"] += len(vung_phu)

        for z_id, entry in vung_stats.items():
            nguong_du = 4
            if entry["uav_hoat_dong"] == 0:
                entry["trang_thai"] = "MAT_PHU_SONG"
            elif entry["uav_hoat_dong"] < nguong_du:
                entry["trang_thai"] = "BAN_GIAO_TAM_THOI"
            else:
                entry["trang_thai"] = "DU_PHU_SONG"
        return vung_stats

    def cap_nhat_tuyen_e2e_dam_chay_lien_tuc(self) -> List[dict[str, Any]]:
        """
        Tại mỗi tick mô phỏng, nếu vẫn còn active_fire đang được UAV khoanh vùng (CONTAINING),
        tính toán lại tuyến Dijkstra đa chặng từ UAV đó về TRẠM_KIỂM_LÂM và ưu tiên giữ tuyến này lên trên cùng.
        """
        active_fires = getattr(self.scenario, "active_fires", None) or []
        if not active_fires:
            self.tuyen_e2e_chay_hien_tai = None
            self.danh_sach_tuyen_e2e_chay = []
            if isinstance(self.tuyen_e2e_gan_nhat, dict) and self.tuyen_e2e_gan_nhat.get("is_fire_route"):
                self.tuyen_e2e_gan_nhat = None
            return []

        fire_routes: List[dict[str, Any]] = []
        for agent in self.danh_sach_tac_nhan.values():
            if getattr(agent, "loai_tac_nhan", None) != "MÁY_BAY":
                continue
            if agent.trang_thai.value in ("NGẮT_KẾT_NỐI", "PIN_YẾU_RTB", "ĐANG_QUAY_VỀ_SẠC", "ĐANG_SẠC"):
                continue
            uav_state_val = getattr(getattr(agent, "uav_state", None), "value", getattr(agent, "uav_state", None))
            tam_chay = getattr(agent, "_tam_khoanh_vung_chay", None)
            if uav_state_val != "CONTAINING" and tam_chay is None:
                continue

            tuyen = self.tim_tuyen_dijkstra_ve_tram(agent.ma_dinh_danh, kich_thuoc_kb=250.0)
            if not tuyen or len(tuyen.get("nodes", [])) < 2:
                continue

            nodes = list(tuyen["nodes"])
            channels = tuyen.get("channels", [])
            last_ch = channels[-1] if channels else {}
            tong_tre_ms = round(
                float(tuyen.get("delay_ms", sum(float(c.get("do_tre_ms", 0.0)) for c in channels))),
                2,
            )
            tong_nang_luong = round(
                float(tuyen.get("energy_j", sum(float(c.get("energy_j", 0.0)) for c in channels))),
                3,
            )
            tam = tam_chay or {}
            fire_lat = tam.get("vi_do")
            fire_lon = tam.get("kinh_do")
            route_dict = {
                "ma_phien": tam.get("ma_phien") or f"FIRE_{agent.ma_dinh_danh}",
                "uav_id": agent.ma_dinh_danh,
                "loai_su_co": "FIRE_CONTAINMENT_TELEMETRY",
                "protocol": "TCP",
                "ttl_remaining": max(1, 16 - (len(nodes) - 1)),
                "is_fire_route": True,
                "lo_trinh": nodes,
                "lo_trinh_hop": nodes,
                "so_chang_hop": len(nodes) - 1,
                "e2e_delay_ms": tong_tre_ms,
                "tong_do_tre_ms": tong_tre_ms,
                "snr_chang_cuoi_db": round(float(last_ch.get("snr_db", 0.0)), 2),
                "dung_luong_chang_cuoi_mbps": round(float(last_ch.get("dung_luong_mbps", 0.0)), 2),
                "nang_luong_du_kien_joule": tong_nang_luong,
                "thoi_gian": time.strftime("%H:%M:%S"),
                "trang_thai_phu_song": "KET_NOI",
                "fire_lat": fire_lat,
                "fire_lon": fire_lon,
            }
            fire_routes.append(route_dict)

            if self.ground_station_server and fire_lat is not None and fire_lon is not None:
                record = self.ground_station_server.tim_su_co_gan_nhat(float(fire_lat), float(fire_lon))
                if record is not None:
                    record["tuyen_bao_cao_e2e"] = nodes

        self.danh_sach_tuyen_e2e_chay = fire_routes
        if fire_routes:
            self.tuyen_e2e_chay_hien_tai = fire_routes[0]
            self.tuyen_e2e_gan_nhat = fire_routes[0]
        else:
            self.tuyen_e2e_chay_hien_tai = None
        return fire_routes

    async def dong_bo_trang_thai_toan_mang(self, heatmap_nodes: Optional[list] = None):
        """
        Thu thập tọa độ, pin, tuyến truyền, danh sách điểm nhiệt (heatmap), độ phủ 7 phân khu và thống kê QoS đẩy ra giao diện.
        """
        fire_routes = self.cap_nhat_tuyen_e2e_dam_chay_lien_tuc()
        with self.os_kernel.net_state_lock:
            agents_snapshot = list(self.danh_sach_tac_nhan.values())
        danh_sach_nodes = [
            agent.xuat_ho_so_trang_thai()
            for agent in agents_snapshot
        ]
        if heatmap_nodes is None:
            heatmap_nodes = (
                self.scenario.xuat_heatmap()
                if hasattr(self.scenario, "xuat_heatmap")
                else []
            )
        swarm_state = (
            self.ground_station_server.xuat_trang_thai_phan_ung()
            if self.ground_station_server and hasattr(self.ground_station_server, "xuat_trang_thai_phan_ung")
            else None
        )
        await self.phat_su_kien_ra_web("WORLD_STATE", {
            "nodes": danh_sach_nodes,
            "heatmap": heatmap_nodes,
            "heatmap_nodes": heatmap_nodes,
            "active_fires": list(getattr(self.scenario, "active_fires", []) or []),
            "fire_e2e_routes": fire_routes,
            "zone_coverage": self.tinh_thong_ke_phu_song_vung(),
            "network_metrics": self.thong_ke_toan_mang,
            "connectivity_metrics": getattr(self, "thong_ke_lien_tuc", {}),
            "os_metrics": self.os_kernel.snapshot(),
            "transport_metrics": self.network_stack.snapshot(),
            "swarm_response": swarm_state,
            "last_auction": swarm_state,
            "last_e2e_route": self.tuyen_e2e_chay_hien_tai or self.tuyen_e2e_gan_nhat
        })
