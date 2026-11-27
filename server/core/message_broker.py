import asyncio
import json
import math
import random
import time
from typing import Any, Dict, List, Optional
from fastapi import WebSocket
from core.os_kernel import OSKernelManager
from core.network_stack import RealSocketTransport


class MessageBroker:
    """
    Môi trường truyền dẫn vô tuyến & điều phối trung tâm SAGSINS (Client-Server 2 kênh A2G / LEO):
    - Đánh giá trực tiếp kênh Không-Đất (A2G <= 6.0 km) hoặc chuyển tiếp qua Vệ tinh LEO (<= 2500 km).
    - Khóa/mở khóa mục tiêu cháy và đồng bộ bản đồ, Dashboard theo thời gian thực qua WebSocket.
    """

    ALLOWED_SWARM_LOG_TAGS = (
        "[HEATMAP_ANOMALY]",
        "[GRADIENT_PULL]",
        "[FIRE_DETECTED]",
        "[TARGET_LOCKED]",
        "[SWARM_HANDOVER]",
        "[SAGSINS_REPORT]",
        "[RF_GATEWAY]",
        "[OS_SYNC]",
        "[OS_SCHED]",
        "[MUTEX]",
        "[CRC32_ERR]",
        "[CLIENT_NET]",
    )

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

        self.thong_ke_toan_mang: dict[str, Any] = {
            "tong_goi_tin_phat": 0,
            "tong_goi_tin_thanh_cong": 0,
            "tong_goi_tin_rot_per": 0,
            "tong_goi_tin_ngoai_vung_phu": 0,
            "tong_goi_tin_mat_los": 0,
            "do_tre_trung_binh_ms": 0.0,
            "nang_luong_tieu_hao_joule": 0.0,
        }

    @staticmethod
    def _khoang_cach_mat_dat_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        r_earth = 6371000.0
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp = p2 - p1
        dl = math.radians(lon2 - lon1)
        a = math.sin(dp / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
        return r_earth * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

    def dang_ky_tac_nhan(self, tac_nhan: Any):
        """Đăng ký một Node mạng (Trạm, Vệ tinh LEO, UAV Client) vào hệ thống."""
        with self.os_kernel.net_state_lock:
            self.danh_sach_tac_nhan[tac_nhan.ma_dinh_danh] = tac_nhan

    def huy_dang_ky_tac_nhan(self, ma_dinh_danh: str) -> Optional[Any]:
        """Xóa một tác nhân (khi UAV Client chủ động LEAVE) khỏi bảng nút hoạt động."""
        with self.os_kernel.net_state_lock:
            return self.danh_sach_tac_nhan.pop(ma_dinh_danh, None)

    def lay_tac_nhan(self, ma_dinh_danh: str) -> Optional[Any]:
        with self.os_kernel.net_state_lock:
            return self.danh_sach_tac_nhan.get(ma_dinh_danh)

    def cap_nhat_nang_luong_tong(self):
        self.thong_ke_toan_mang["nang_luong_tieu_hao_joule"] = round(
            sum(
                getattr(agent, "thong_ke", {}).get("nang_luong_tieu_hao_joule", 0.0)
                for agent in self.danh_sach_tac_nhan.values()
            ),
            3,
        )

    def _lay_tram_mat_dat(self) -> Optional[Any]:
        for agent in self.danh_sach_tac_nhan.values():
            if getattr(agent, "loai_tac_nhan", None) == "TRẠM_MẶT_ĐẤT":
                return agent
        return None

    def danh_gia_kenh_sagsins(
        self,
        uav_agent_or_pos: Any,
        kich_thuoc_kb: float = 2.0,
        ma_nguon: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Đánh giá 2 kênh truyền Client-Server SAGSINS (KHÔNG dùng FANET/Dijkstra):
        1. Kênh Không-Đất trực tiếp (GROUND_LINK / A2G): UAV <-> Trạm Kiểm Lâm (<= 6.0 km).
        2. Kênh Vệ tinh LEO (LEO_SATELLITE): UAV -> Vệ tinh LEO (SNR cao nhất) -> Trạm (<= 2500 km).
        3. Ngoài vùng phủ sóng (OUT_OF_RANGE): khi mất cả A2G lẫn A2S/S2G.
        """
        if isinstance(uav_agent_or_pos, dict):
            pos = dict(uav_agent_or_pos)
            uav_id = ma_nguon or str(pos.get("node") or "UAV_CLIENT")
            radio_cfg = {
                "tan_so_hz": 2.4e9,
                "cong_suat_phat_dbm": 35.0,
                "bang_thong_hz": 20e6,
            }
            at_station_flag = False
            is_offline = False
        else:
            pos = dict(uav_agent_or_pos.toa_do)
            uav_id = ma_nguon or uav_agent_or_pos.ma_dinh_danh
            radio_cfg = dict(uav_agent_or_pos.radio_config)
            at_station_flag = bool(
                hasattr(uav_agent_or_pos, "_dang_o_tai_tram") and uav_agent_or_pos._dang_o_tai_tram()
            )
            is_offline = getattr(getattr(uav_agent_or_pos, "trang_thai", None), "value", "") == "NGẮT_KẾT_NỐI"

        tram = self._lay_tram_mat_dat()
        if tram is None:
            return {
                "ok": False,
                "link": "OUT_OF_RANGE",
                "hops": [],
                "hop_count": 0,
                "relay_satellite": None,
                "distance_m": 0.0,
                "snr_db": -100.0,
                "per": 1.0,
                "delay_ms": 9999.0,
                "capacity_mbps": 0.0,
                "energy_j": 0.0,
                "reason": "RF_OUT_OF_RANGE_DROP",
            }

        dist_ground_m = self.physics.tinh_khoang_cach_3d(pos, tram.toa_do)
        if is_offline:
            return {
                "ok": False,
                "link": "OUT_OF_RANGE",
                "hops": [],
                "hop_count": 0,
                "relay_satellite": None,
                "distance_m": round(dist_ground_m, 1),
                "snr_db": -100.0,
                "per": 1.0,
                "delay_ms": 9999.0,
                "capacity_mbps": 0.0,
                "energy_j": 0.0,
                "reason": "RF_OUT_OF_RANGE_DROP",
            }

        if at_station_flag or dist_ground_m <= 100.0:
            return {
                "ok": True,
                "link": "GROUND_LINK",
                "hops": [uav_id, tram.ma_dinh_danh],
                "hop_count": 1,
                "relay_satellite": None,
                "distance_m": round(dist_ground_m, 1),
                "snr_db": 60.0,
                "per": 0.0,
                "delay_ms": 0.01,
                "capacity_mbps": 54.0,
                "energy_j": 0.01,
                "reason": "EXT_UDP_A2G",
            }

        # Kênh 1: Không-Đất trực tiếp (A2G <= 6.0 km)
        a2g = self.physics.danh_gia_chat_luong_kenh_truyen(
            pos1=pos,
            pos2=tram.toa_do,
            tan_so_hz=radio_cfg["tan_so_hz"],
            cong_suat_phat_dbm=radio_cfg["cong_suat_phat_dbm"],
            bang_thong_hz=radio_cfg["bang_thong_hz"],
        )
        if a2g.get("kha_dung") and float(a2g.get("per", 1.0)) < 0.25:
            bw_mbps = max(0.5, float(a2g["dung_luong_mbps"]))
            tx_ms = (kich_thuoc_kb * 8.0 / 1000.0) / bw_mbps * 1000.0
            delay_ms = round(float(a2g["do_tre_ms"]) + tx_ms, 2)
            p_tx_w = 10.0 ** ((radio_cfg["cong_suat_phat_dbm"] - 30.0) / 10.0)
            energy_j = round(p_tx_w * 0.05 * (kich_thuoc_kb / 10.0), 4)
            return {
                "ok": True,
                "link": "GROUND_LINK",
                "hops": [uav_id, tram.ma_dinh_danh],
                "hop_count": 1,
                "relay_satellite": None,
                "distance_m": round(dist_ground_m, 1),
                "snr_db": round(float(a2g["snr_db"]), 2),
                "per": round(float(a2g["per"]), 4),
                "delay_ms": delay_ms,
                "capacity_mbps": round(bw_mbps, 2),
                "energy_j": energy_j,
                "reason": "EXT_UDP_A2G",
            }

        # Kênh 2: Vệ tinh LEO (A2S + S2G <= 2500 km), chọn vệ tinh có SNR thắt cổ chai cao nhất
        best_leo: Optional[dict[str, Any]] = None
        for sat in self.danh_sach_tac_nhan.values():
            if getattr(sat, "loai_tac_nhan", None) != "VỆ_TINH_LEO":
                continue
            if getattr(getattr(sat, "trang_thai", None), "value", "") == "NGẮT_KẾT_NỐI":
                continue
            a2s = self.physics.danh_gia_chat_luong_kenh_truyen(
                pos1=pos,
                pos2=sat.toa_do,
                tan_so_hz=radio_cfg["tan_so_hz"],
                cong_suat_phat_dbm=radio_cfg["cong_suat_phat_dbm"],
                bang_thong_hz=radio_cfg["bang_thong_hz"],
            )
            if not a2s.get("kha_dung") or float(a2s.get("per", 1.0)) >= 0.25:
                continue
            s2g = self.physics.danh_gia_chat_luong_kenh_truyen(
                pos1=sat.toa_do,
                pos2=tram.toa_do,
                tan_so_hz=sat.radio_config["tan_so_hz"],
                cong_suat_phat_dbm=sat.radio_config["cong_suat_phat_dbm"],
                bang_thong_hz=sat.radio_config["bang_thong_hz"],
            )
            if not s2g.get("kha_dung") or float(s2g.get("per", 1.0)) >= 0.25:
                continue

            bottleneck_snr = min(float(a2s["snr_db"]), float(s2g["snr_db"]))
            bottleneck_bw = max(0.5, min(float(a2s["dung_luong_mbps"]), float(s2g["dung_luong_mbps"])))
            tx_ms = (kich_thuoc_kb * 8.0 / 1000.0) / bottleneck_bw * 1000.0 * 2.0
            total_delay = round(float(a2s["do_tre_ms"]) + float(s2g["do_tre_ms"]) + tx_ms, 2)
            combined_per = round(1.0 - (1.0 - float(a2s["per"])) * (1.0 - float(s2g["per"])), 4)
            p_tx_w = 10.0 ** ((radio_cfg["cong_suat_phat_dbm"] - 30.0) / 10.0)
            energy_j = round((p_tx_w + 15.0) * 0.05 * (kich_thuoc_kb / 10.0), 4)

            cand = {
                "ok": True,
                "link": "LEO_SATELLITE",
                "hops": [uav_id, sat.ma_dinh_danh, tram.ma_dinh_danh],
                "hop_count": 2,
                "relay_satellite": sat.ma_dinh_danh,
                "distance_m": round(dist_ground_m, 1),
                "snr_db": round(bottleneck_snr, 2),
                "per": combined_per,
                "delay_ms": total_delay,
                "capacity_mbps": round(bottleneck_bw, 2),
                "energy_j": energy_j,
                "reason": "EXT_UDP_LEO",
            }
            if best_leo is None or cand["snr_db"] > best_leo["snr_db"]:
                best_leo = cand

        if best_leo is not None:
            return best_leo

        return {
            "ok": False,
            "link": "OUT_OF_RANGE",
            "hops": [],
            "hop_count": 0,
            "relay_satellite": None,
            "distance_m": round(dist_ground_m, 1),
            "snr_db": -100.0,
            "per": 1.0,
            "delay_ms": 9999.0,
            "capacity_mbps": 0.0,
            "energy_j": 0.0,
            "reason": "RF_OUT_OF_RANGE_DROP",
        }

    def kiem_tra_muc_tieu_da_khoa(
        self,
        lat: float,
        lon: float,
        ban_kinh_khoa_m: float = 850.0
    ) -> Optional[str]:
        """Kiểm tra tâm cháy (lat, lon) đã được một UAV hoạt động khác khóa mục tiêu chưa."""
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
        """Khóa mục tiêu đám cháy tại (lat, lon) cho `uav_id`, tránh dồn nhiều UAV vào một đám cháy."""
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
        """Giải phóng khóa mục tiêu của `uav_id` trên bản đồ sự cố."""
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

    async def gui_bao_cao_ve_tram(self, ma_nguon: str, thong_diep: dict) -> bool:
        """Gửi báo cáo sự cố/telemetry từ UAV Client về Trạm Kiểm Lâm qua kênh A2G hoặc LEO."""
        source = self.danh_sach_tac_nhan.get(ma_nguon)
        tram = self._lay_tram_mat_dat()
        if not source or not tram:
            return False

        kich_thuoc_kb = float(thong_diep.get("kich_thuoc_kb", 250.0))
        loai_tin = str(thong_diep.get("loai_tin") or "FIRE_DETECTION_REPORT")
        kenh = self.danh_gia_kenh_sagsins(source, kich_thuoc_kb=kich_thuoc_kb, ma_nguon=ma_nguon)

        self.thong_ke_toan_mang["tong_goi_tin_phat"] += 1
        if not kenh["ok"]:
            self.thong_ke_toan_mang["tong_goi_tin_ngoai_vung_phu"] += 1
            self.thong_ke_toan_mang["tong_goi_tin_mat_los"] = self.thong_ke_toan_mang["tong_goi_tin_ngoai_vung_phu"]
            return False

        nodes = list(kenh["hops"])
        for idx in range(len(nodes) - 1):
            n1 = self.danh_sach_tac_nhan.get(nodes[idx])
            n2 = self.danh_sach_tac_nhan.get(nodes[idx + 1])
            if n1 and n2:
                await self.phat_su_kien_ra_web("RF_LINK", {
                    "tu_node": nodes[idx],
                    "den_node": nodes[idx + 1],
                    "loai_tin": loai_tin,
                    "protocol": "TCP" if loai_tin.startswith("FIRE") else "UDP",
                    "channel": kenh["link"],
                    "toa_do_dau": n1.toa_do,
                    "toa_do_cuoi": n2.toa_do,
                    "snr_db": kenh["snr_db"],
                    "do_tre_ms": kenh["delay_ms"],
                    "dung_luong_mbps": kenh["capacity_mbps"],
                })

        if hasattr(source, "tieu_hao_nang_luong_truyen_tin"):
            source.tieu_hao_nang_luong_truyen_tin(kich_thuoc_kb, la_phat_song=True)

        self.thong_ke_toan_mang["tong_goi_tin_thanh_cong"] += 1
        n_ok = self.thong_ke_toan_mang["tong_goi_tin_thanh_cong"]
        cur_avg = self.thong_ke_toan_mang["do_tre_trung_binh_ms"]
        self.thong_ke_toan_mang["do_tre_trung_binh_ms"] = round(((cur_avg * (n_ok - 1)) + kenh["delay_ms"]) / n_ok, 2)
        self.cap_nhat_nang_luong_tong()

        if hasattr(self.scenario, "ghi_log_thuc_nghiem_qos"):
            self.scenario.ghi_log_thuc_nghiem_qos(
                ma_nguon,
                nodes[1],
                loai_tin,
                {
                    "khoang_cach_km": round(kenh["distance_m"] / 1000.0, 2),
                    "goc_ngang_do": 45.0,
                    "do_tre_ms": kenh["delay_ms"],
                    "fspl_db": 95.0 if kenh["link"] == "GROUND_LINK" else 178.0,
                    "snr_db": kenh["snr_db"],
                    "dung_luong_mbps": kenh["capacity_mbps"],
                    "per": kenh["per"],
                    "nang_luong_tx_joule": kenh["energy_j"],
                    "do_kha_dung_pct": self.thong_ke_lien_tuc.get("do_kha_dung_pct", 100.0),
                    "thoi_gian_mat_lien_lac_giay": self.thong_ke_lien_tuc.get("thoi_gian_mat_lien_lac_giay", 0.0),
                    "nang_luong_toan_mang_joule": self.thong_ke_toan_mang["nang_luong_tieu_hao_joule"],
                },
                "THANH_CONG",
            )

        is_fire = loai_tin.startswith("FIRE")
        route_payload = {
            "ma_phien": thong_diep.get("ma_phien_su_co") or thong_diep.get("ma_su_co") or "FIRE",
            "uav_id": ma_nguon,
            "loai_su_co": loai_tin,
            "protocol": "TCP" if is_fire else "UDP",
            "channel": kenh["link"],
            "is_fire_route": is_fire,
            "lo_trinh": nodes,
            "lo_trinh_hop": nodes,
            "so_chang_hop": kenh["hop_count"],
            "e2e_delay_ms": kenh["delay_ms"],
            "tong_do_tre_ms": kenh["delay_ms"],
            "snr_chang_cuoi_db": kenh["snr_db"],
            "dung_luong_chang_cuoi_mbps": kenh["capacity_mbps"],
            "nang_luong_du_kien_joule": kenh["energy_j"],
            "thoi_gian": time.strftime("%H:%M:%S"),
            "trang_thai_phu_song": "KET_NOI",
        }
        await self.phat_su_kien_ra_web("E2E_ROUTE_COMPLETED", route_payload)

        if self.ground_station_server and is_fire and thong_diep.get("vi_do") is not None:
            await self.ground_station_server.ghi_nhan_bao_cao_truyen_tin(
                float(thong_diep["vi_do"]),
                float(thong_diep["kinh_do"]),
                ma_nguon,
                nodes,
                do_tre_ms=kenh["delay_ms"],
                link_type=kenh["link"],
            )
        return True

    async def phat_su_kien_ra_web(self, loai_su_kien: str, du_lieu: Any):
        """Đẩy gói tin JSON thời gian thực xuống tất cả trình duyệt đang mở."""
        thoi_diem = time.strftime("%H:%M:%S")
        if loai_su_kien == "E2E_ROUTE_COMPLETED":
            if isinstance(du_lieu, dict) and (du_lieu.get("is_fire_route") or not self.tuyen_e2e_chay_hien_tai):
                self.tuyen_e2e_gan_nhat = du_lieu
        elif loai_su_kien == "LOG" and isinstance(du_lieu, dict):
            noi_dung_log = str(du_lieu.get("noi_dung", ""))
            if not any(tag in noi_dung_log for tag in self.ALLOWED_SWARM_LOG_TAGS):
                return
            self.nhat_ky_gan_day.append({
                "timestamp": thoi_diem,
                "cap_do": du_lieu.get("cap_do", "INFO"),
                "noi_dung": noi_dung_log,
            })
            if len(self.nhat_ky_gan_day) > 60:
                self.nhat_ky_gan_day = self.nhat_ky_gan_day[-60:]

        if not self.danh_sach_client_ws:
            return

        payload = json.dumps({
            "type": loai_su_kien,
            "timestamp": thoi_diem,
            "data": du_lieu,
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
        if not any(tag in str(noi_dung) for tag in self.ALLOWED_SWARM_LOG_TAGS):
            return
        await self.phat_su_kien_ra_web("LOG", {
            "cap_do": loai_log,
            "noi_dung": noi_dung,
        })

    def tinh_thong_ke_phu_song_vung(self) -> Dict[str, dict]:
        """Tính độ phủ sóng của 7 phân khu rừng Sơn Trà (định mức 4 UAV tuần tra/vùng)."""
        vung_stats: Dict[str, dict] = {}
        for idx in range(1, 8):
            zone_id = f"ZONE_{idx:02d}"
            vung_stats[zone_id] = {
                "zone_id": zone_id,
                "tong_uav": 4,
                "uav_hoat_dong": 0,
                "uav_dang_sac_hoac_rtb": 0,
                "uav_nhan_ban_giao": 0,
                "trang_thai": "MAT_PHU_SONG",
            }

        active_air_states = ("PATROLLING", "TRANSITING", "CONTAINING", "RESPONDING")
        with self.os_kernel.net_state_lock:
            agents_snapshot = list(self.danh_sach_tac_nhan.values())
        for agent in agents_snapshot:
            if getattr(agent, "loai_tac_nhan", None) != "MÁY_BAY":
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

        for entry in vung_stats.values():
            if entry["uav_hoat_dong"] == 0:
                entry["trang_thai"] = "MAT_PHU_SONG"
            elif entry["uav_hoat_dong"] < 4:
                entry["trang_thai"] = "BAN_GIAO_TAM_THOI"
            else:
                entry["trang_thai"] = "DU_PHU_SONG"
        return vung_stats

    def cap_nhat_tuyen_e2e_dam_chay_lien_tuc(self) -> List[dict[str, Any]]:
        """Cập nhật tuyến truyền cháy 2 kênh (A2G hoặc LEO) cho mọi UAV đang CONTAINING."""
        active_fires = getattr(self.scenario, "active_fires", None) or []
        if not active_fires:
            self.tuyen_e2e_chay_hien_tai = None
            self.danh_sach_tuyen_e2e_chay = []
            if isinstance(self.tuyen_e2e_gan_nhat, dict) and self.tuyen_e2e_gan_nhat.get("is_fire_route"):
                self.tuyen_e2e_gan_nhat = None
            return []

        fire_routes: List[dict[str, Any]] = []
        with self.os_kernel.net_state_lock:
            agents_snapshot = list(self.danh_sach_tac_nhan.values())
        for agent in agents_snapshot:
            if getattr(agent, "loai_tac_nhan", None) != "MÁY_BAY":
                continue
            if getattr(agent.trang_thai, "value", "") in ("NGẮT_KẾT_NỐI", "PIN_YẾU_RTB", "ĐANG_QUAY_VỀ_SẠC", "ĐANG_SẠC"):
                continue
            uav_state_val = getattr(getattr(agent, "uav_state", None), "value", getattr(agent, "uav_state", None))
            tam_chay = getattr(agent, "_tam_khoanh_vung_chay", None)
            if uav_state_val != "CONTAINING" and tam_chay is None:
                continue

            kenh = self.danh_gia_kenh_sagsins(agent, kich_thuoc_kb=250.0, ma_nguon=agent.ma_dinh_danh)
            if not kenh["ok"]:
                continue

            nodes = list(kenh["hops"])
            tam = tam_chay or {}
            fire_lat = tam.get("vi_do")
            fire_lon = tam.get("kinh_do")
            route_dict = {
                "ma_phien": tam.get("ma_phien") or f"FIRE_{agent.ma_dinh_danh}",
                "uav_id": agent.ma_dinh_danh,
                "loai_su_co": "FIRE_CONTAINMENT_TELEMETRY",
                "protocol": "TCP",
                "channel": kenh["link"],
                "is_fire_route": True,
                "lo_trinh": nodes,
                "lo_trinh_hop": nodes,
                "so_chang_hop": kenh["hop_count"],
                "e2e_delay_ms": kenh["delay_ms"],
                "tong_do_tre_ms": kenh["delay_ms"],
                "snr_chang_cuoi_db": kenh["snr_db"],
                "dung_luong_chang_cuoi_mbps": kenh["capacity_mbps"],
                "nang_luong_du_kien_joule": kenh["energy_j"],
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
        """Thu thập trạng thái các nút mạng, Heatmap, độ phủ và thống kê đẩy ra giao diện Web."""
        fire_routes = self.cap_nhat_tuyen_e2e_dam_chay_lien_tuc()
        with self.os_kernel.net_state_lock:
            agents_snapshot = list(self.danh_sach_tac_nhan.values())
        danh_sach_nodes = [agent.xuat_ho_so_trang_thai() for agent in agents_snapshot]
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
            "last_e2e_route": self.tuyen_e2e_chay_hien_tai or self.tuyen_e2e_gan_nhat,
        })
