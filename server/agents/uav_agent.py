"""Server-side digital twin for connected external UAV Clients (2-channel A2G/LEO SAGSINS + MAS)."""
import asyncio
import math
import time
import uuid
from enum import Enum
from typing import Any, Optional
from core.base_agent import AgentState, BaseAgent

LOW_BATTERY_THRESHOLD = 25.0
SCAN_RADIUS_M = 600.0
COOLING_RADIUS_M = 600.0
FIRE_DETECTION_RADIUS_M = 600.0
FIRE_RISK_THRESHOLD = 0.85
FIRE_CONTAINMENT_RADIUS_M = 130.0


class UAVState(str, Enum):
    STANDBY = "STANDBY"
    PATROLLING = "PATROLLING"
    TRANSITING = "TRANSITING"
    RESPONDING = "RESPONDING"
    CONTAINING = "CONTAINING"
    RETURNING = "RETURNING"
    CHARGING = "CHARGING"
    OFFLINE = "OFFLINE"


class UAVAgent(BaseAgent):
    """
    Đại diện phía Server cho mỗi tiến trình Client UAV thật kết nối qua TCP :9000 / UDP :9001.
    Client tự bay tuần tra trong đa giác phân khu và báo cáo vị trí/pin qua UDP Telemetry;
    Server đánh giá kênh truyền A2G/LEO, phát hiện cháy <= 600m và đẩy lệnh điều phối 2 chiều qua TCP.
    """

    def __init__(
        self,
        bo_dinh_tuyen: Any,
        cong_suat_bay_w: float = 80.0,
        zone_id: Optional[str] = None,
        home_zone_id: Optional[str] = None,
        role: str = "PATROL",
        **kwargs,
    ):
        kwargs.setdefault("loai_tac_nhan", "MÁY_BAY")
        kwargs.setdefault("pin_max_joule", 50000.0)
        kwargs.setdefault("cong_suat_dbm", 35.0)
        kwargs.setdefault("tan_so_hz", 2.4e9)
        kwargs.setdefault("bang_thong_hz", 20e6)
        self.is_external_client = bool(kwargs.pop("is_external_client", True))
        self.client_pid = kwargs.pop("client_pid", None)
        self.client_addr = kwargs.pop("client_addr", None)
        super().__init__(bo_dinh_tuyen=bo_dinh_tuyen, zone_id=zone_id, **kwargs)

        self.cong_suat_bay_w = max(1.0, float(cong_suat_bay_w))
        self.zone_id = zone_id
        self.home_zone_id = home_zone_id or zone_id
        self.role = str(role or "PATROL").upper()
        self.nguong_pin_yeu_pct = LOW_BATTERY_THRESHOLD
        self.so_nhiem_vu_da_nhan = 0
        self.toa_do_muc_tieu: Optional[dict] = None
        self._tam_khoanh_vung_chay: Optional[dict] = None
        self._da_goi_du_bi_thay_ca = False
        self._da_phat_hien_dam_chay: set[tuple[float, float]] = set()
        self._su_co_da_nho_dong_doi: set[tuple[float, float]] = set()
        self.uav_state = UAVState.STANDBY
        self.current_link = "GROUND_LINK"
        self.in_comm_range = True
        self.direct_los = True
        self.relay_required = False
        self.relay_target_id: Optional[str] = None
        self.link_metrics: dict[str, Any] = {
            "distance_m": 0.0,
            "snr_db": 60.0,
            "per": 0.0,
            "delay_ms": 0.01,
            "capacity_mbps": 54.0,
        }
        self.last_external_heartbeat = time.time()
        self._last_pushed_rf_link: Optional[str] = None
        self._rf_udp_cnt: int = 0
        self._last_oor_push_ts: float = 0.0

        if self.role == "STANDBY" or self._dang_o_tai_tram():
            self.chuyen_trang_thai(AgentState.IDLE)
            self.uav_state = UAVState.STANDBY
        else:
            self.chuyen_trang_thai(AgentState.PATROLLING)
            self.uav_state = UAVState.PATROLLING

    @property
    def lat(self) -> float:
        return float(self.toa_do["vi_do"])

    @lat.setter
    def lat(self, value: float):
        self.toa_do["vi_do"] = float(value)

    @property
    def lon(self) -> float:
        return float(self.toa_do["kinh_do"])

    @lon.setter
    def lon(self, value: float):
        self.toa_do["kinh_do"] = float(value)

    @staticmethod
    def _khoang_cach_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        r_earth = 6371000.0
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp = p2 - p1
        dl = math.radians(lon2 - lon1)
        a = math.sin(dp / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
        return r_earth * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

    def _dang_o_tai_tram(self) -> bool:
        tac_nhan_map = getattr(self.bo_dinh_tuyen, "danh_sach_tac_nhan", {}) or {}
        station_near = any(
            getattr(agent, "loai_tac_nhan", None) == "TRẠM_MẶT_ĐẤT"
            and self._khoang_cach_m(
                self.lat,
                self.lon,
                agent.toa_do["vi_do"],
                agent.toa_do["kinh_do"],
            ) <= 100.0
            for agent in tac_nhan_map.values()
        )
        if not station_near:
            return False
        uav_st = getattr(self, "uav_state", None)
        if uav_st in (UAVState.STANDBY, UAVState.CHARGING):
            return True
        return self.trang_thai in (AgentState.IDLE, AgentState.CHARGING) and float(self.toa_do.get("do_cao", 25.0)) <= 100.0

    def chuyen_trang_thai(self, trang_thai_moi: AgentState):
        super().chuyen_trang_thai(trang_thai_moi)
        if not hasattr(self, "uav_state"):
            return
        if self._tam_khoanh_vung_chay is not None and trang_thai_moi in (AgentState.PATROLLING, AgentState.RESPONDING):
            self.uav_state = UAVState.CONTAINING
            return
        mapping = {
            AgentState.IDLE: UAVState.STANDBY,
            AgentState.CHARGING: UAVState.CHARGING,
            AgentState.PATROLLING: UAVState.PATROLLING,
            AgentState.RESPONDING: UAVState.RESPONDING,
            AgentState.RETURNING: UAVState.RETURNING,
            AgentState.LOW_BATTERY_RTB: UAVState.RETURNING,
            AgentState.OFFLINE: UAVState.OFFLINE,
        }.get(trang_thai_moi)
        if mapping is not None:
            self.uav_state = mapping

    def evaluate_communication_link(self) -> dict[str, Any]:
        """Đánh giá 2 kênh truyền Client-Server (GROUND_LINK A2G hoặc LEO_SATELLITE)."""
        kenh = self.bo_dinh_tuyen.danh_gia_kenh_sagsins(self, kich_thuoc_kb=2.0, ma_nguon=self.ma_dinh_danh)
        self.current_link = kenh["link"]
        self.in_comm_range = (kenh["link"] == "GROUND_LINK")
        self.direct_los = self.in_comm_range
        self.relay_required = (kenh["link"] == "LEO_SATELLITE")
        self.relay_target_id = kenh.get("relay_satellite")
        self.tuyen_ve_tram_cuc_bo = list(kenh.get("hops", []))
        self.do_tre_ve_tram_cuc_bo_ms = float(kenh.get("delay_ms", 0.0))
        self.dung_luong_ve_tram_cuc_bo_mbps = float(kenh.get("capacity_mbps", 0.0))
        self.link_metrics = {
            "distance_m": float(kenh.get("distance_m", 0.0)),
            "snr_db": float(kenh.get("snr_db", 0.0)),
            "per": float(kenh.get("per", 0.0)),
            "delay_ms": float(kenh.get("delay_ms", 0.0)),
            "capacity_mbps": float(kenh.get("capacity_mbps", 0.0)),
        }
        return kenh

    def xuat_ho_so_trang_thai(self) -> dict:
        ho_so = super().xuat_ho_so_trang_thai()
        ho_so["state"] = self.uav_state.value
        ho_so["zone_id"] = self.zone_id
        ho_so["home_zone_id"] = self.home_zone_id
        ho_so["role"] = self.role
        ho_so["current_link"] = self.current_link
        ho_so["in_comm_range"] = self.in_comm_range
        ho_so["direct_los"] = self.in_comm_range
        ho_so["relay_required"] = self.relay_required
        ho_so["relay_target_id"] = self.relay_target_id
        ho_so["tuyen_ve_tram"] = list(self.tuyen_ve_tram_cuc_bo)
        ho_so["distance_to_station_m"] = self.link_metrics.get("distance_m", 0.0)
        ho_so["snr_db"] = self.link_metrics.get("snr_db", 0.0)
        ho_so["per"] = self.link_metrics.get("per", 0.0)
        ho_so["delay_ms"] = self.link_metrics.get("delay_ms", 0.0)
        ho_so["capacity_mbps"] = self.link_metrics.get("capacity_mbps", 0.0)
        ho_so["is_reserve"] = bool(self.uav_state == UAVState.STANDBY and self._dang_o_tai_tram())
        ho_so["is_external_client"] = self.is_external_client
        ho_so["client_pid"] = self.client_pid
        ho_so["client_addr"] = self.client_addr
        ho_so["scan_radius_m"] = FIRE_DETECTION_RADIUS_M
        ho_so["so_nhiem_vu"] = self.so_nhiem_vu_da_nhan
        if self.toa_do_muc_tieu:
            ho_so["target_lat"] = round(float(self.toa_do_muc_tieu["vi_do"]), 5)
            ho_so["target_lng"] = round(float(self.toa_do_muc_tieu["kinh_do"]), 5)
        return ho_so

    def _ket_thuc_khoanh_vung_chay(self):
        self._tam_khoanh_vung_chay = None
        self.toa_do_muc_tieu = None
        if self.uav_state == UAVState.CONTAINING and self.trang_thai == AgentState.PATROLLING:
            self.uav_state = UAVState.PATROLLING

    def _lay_tam_chay_dang_phu_trach(self) -> Optional[tuple[float, float]]:
        """Trả tọa độ (lat, lon) đám cháy mà UAV này đang giữ nhiệm vụ (nếu có)."""
        tam = getattr(self, "_tam_khoanh_vung_chay", None)
        if isinstance(tam, dict) and tam.get("vi_do") is not None and tam.get("kinh_do") is not None:
            return (float(tam["vi_do"]), float(tam["kinh_do"]))
        for entry in (getattr(self.bo_dinh_tuyen, "muc_tieu_chay_da_khoa", None) or []):
            if entry.get("uav_id") == self.ma_dinh_danh and entry.get("lat") is not None and entry.get("lon") is not None:
                return (float(entry["lat"]), float(entry["lon"]))
        return None

    async def ban_giao_chay_khi_roi_nhiem_vu(
        self,
        fire_pos: Optional[tuple[float, float]] = None,
    ) -> Optional["UAVAgent"]:
        """
        Khi UAV đang giữ nhiệm vụ cháy (RESPONDING/CONTAINING) phải rời vị trí (hết pin/RTB/OFFLINE/LEAVE),
        mở khóa đám cháy và điều phối ngay UAV khỏe gần nhất trong cùng phân khu vào thay thế.
        """
        pos = fire_pos or self._lay_tam_chay_dang_phu_trach()
        self.bo_dinh_tuyen.mo_khoa_muc_tieu_chay(self.ma_dinh_danh, ha_nhiet=False)
        self._tam_khoanh_vung_chay = None
        self.toa_do_muc_tieu = None
        if pos is None:
            return None

        scenario = getattr(self.bo_dinh_tuyen, "scenario", None)
        active_fires = getattr(scenario, "active_fires", None) or []
        fire_active = any(
            self._khoang_cach_m(pos[0], pos[1], float(f.get("lat", 0.0)), float(f.get("lon", 0.0))) <= 850.0
            for f in active_fires
        )
        if not fire_active:
            return None

        pin_khu_hoi = self._tinh_pin_du_kien_khu_hoi_pct(pos[0], pos[1])
        uav_thay = await self.dieu_phoi_ban_giao_nhiem_vu(pos[0], pos[1], pin_khu_hoi, force=True)
        if hasattr(self.bo_dinh_tuyen, "cap_nhat_tuyen_e2e_dam_chay_lien_tuc"):
            self.bo_dinh_tuyen.cap_nhat_tuyen_e2e_dam_chay_lien_tuc()
        return uav_thay

    def _tinh_pin_du_kien_khu_hoi_pct(self, target_lat: float, target_lon: float) -> float:
        muc_tieu = {
            "vi_do": float(target_lat),
            "kinh_do": float(target_lon),
            "do_cao": float(self.toa_do.get("do_cao", 1000.0)),
        }
        kc_toi_chay_m = self.bo_dinh_tuyen.physics.tinh_khoang_cach_3d(self.toa_do, muc_tieu)
        van_toc = max(1.0, float(self.toa_do.get("van_toc_m_s") or 40.0))
        thoi_gian_khu_hoi_s = (2.0 * kc_toi_chay_m) / van_toc
        nang_luong_tieu_hao_j = thoi_gian_khu_hoi_s * self.cong_suat_bay_w
        pin_con_lai_j = self.pin_hien_tai - nang_luong_tieu_hao_j
        if self.pin_toi_da <= 0:
            return 0.0
        return round((pin_con_lai_j / self.pin_toi_da) * 100.0, 2)

    async def dat_pin_gia_lap(self, phan_tram_pin: float):
        """Áp dụng thay đổi mức pin từ UI Server và đẩy lệnh TCP SET_BATTERY tương ứng xuống Client."""
        fire_pos = self._lay_tam_chay_dang_phu_trach()
        self.pin_hien_tai = self.pin_toi_da * min(100.0, max(0.0, float(phan_tram_pin))) / 100.0
        if self.pin_hien_tai <= 0.0:
            self.chuyen_trang_thai(AgentState.OFFLINE)
            self.uav_state = UAVState.OFFLINE
            await self.ban_giao_chay_khi_roi_nhiem_vu(fire_pos)
            await self._goi_uav_du_bi_thay_ca()
        elif float(phan_tram_pin) < LOW_BATTERY_THRESHOLD:
            self._da_goi_du_bi_thay_ca = False
            self.chuyen_trang_thai(AgentState.RETURNING)
            self.uav_state = UAVState.RETURNING
            await self.ban_giao_chay_khi_roi_nhiem_vu(fire_pos)
            await self._goi_uav_du_bi_thay_ca()
        else:
            self._da_goi_du_bi_thay_ca = False
            self._ket_thuc_khoanh_vung_chay()
            if self._dang_o_tai_tram():
                if float(phan_tram_pin) >= 100.0:
                    self.chuyen_trang_thai(AgentState.IDLE)
                    self.uav_state = UAVState.STANDBY
                else:
                    self.chuyen_trang_thai(AgentState.CHARGING)
                    self.uav_state = UAVState.CHARGING
            else:
                self.chuyen_trang_thai(AgentState.PATROLLING)
                self.uav_state = UAVState.PATROLLING

    async def cap_nhat_vat_ly(self, dt_giay: float):
        if dt_giay <= 0 or self.trang_thai == AgentState.OFFLINE or self.uav_state == UAVState.OFFLINE:
            return
        self.evaluate_communication_link()
        if (
            self.uav_state in (UAVState.PATROLLING, UAVState.TRANSITING)
            and self.trang_thai == AgentState.PATROLLING
            and self._tam_khoanh_vung_chay is None
            and not self._dang_o_tai_tram()
        ):
            await self._kiem_tra_va_xu_ly_dam_chay_tu_chu()

    async def _kiem_tra_va_xu_ly_dam_chay_tu_chu(self):
        """Quét điểm cháy risk >= 0.85 trong bán kính <= 600m quanh Client đang tuần tra."""
        scenario = getattr(self.bo_dinh_tuyen, "scenario", None)
        heat_nodes = getattr(scenario, "heat_nodes", None) or []
        if not heat_nodes:
            return

        cac_diem_chay_gan = []
        for node in heat_nodes:
            if float(node.risk_score) < FIRE_RISK_THRESHOLD or getattr(node, "locked_by", None):
                continue
            if getattr(node, "zone_id", None) and self.zone_id and node.zone_id != self.zone_id:
                continue
            kc_m = self._khoang_cach_m(self.lat, self.lon, node.lat, node.lon)
            if kc_m <= FIRE_DETECTION_RADIUS_M:
                cac_diem_chay_gan.append((kc_m, node))

        if not cac_diem_chay_gan:
            return

        cac_diem_chay_gan.sort(key=lambda item: (-float(item[1].risk_score), item[0]))
        kc_min_m, fire_node = cac_diem_chay_gan[0]
        fire_center = getattr(fire_node, "fire_center", None)
        fire_lat, fire_lon = (
            (float(fire_center[0]), float(fire_center[1]))
            if fire_center
            else (float(fire_node.lat), float(fire_node.lon))
        )

        if self.bo_dinh_tuyen.kiem_tra_muc_tieu_da_khoa(fire_lat, fire_lon):
            return

        det_key = (round(fire_lat, 4), round(fire_lon, 4))
        gcs_server = getattr(self.bo_dinh_tuyen, "ground_station_server", None)
        if det_key not in self._da_phat_hien_dam_chay:
            self._da_phat_hien_dam_chay.add(det_key)
            if gcs_server and hasattr(gcs_server, "ghi_nhan_phat_hien_dam_chay"):
                await gcs_server.ghi_nhan_phat_hien_dam_chay(
                    fire_lat,
                    fire_lon,
                    self.ma_dinh_danh,
                    kc_min_m,
                    zone_id=self.zone_id,
                )

        pin_du_kien_pct = self._tinh_pin_du_kien_khu_hoi_pct(fire_lat, fire_lon)
        if self.phan_tram_pin > LOW_BATTERY_THRESHOLD and pin_du_kien_pct > LOW_BATTERY_THRESHOLD:
            await self._giao_nhiem_vu_khoanh_vung(
                fire_lat,
                fire_lon,
                pin_du_kien_pct=pin_du_kien_pct,
                la_p2p=False,
                uav_phat_hien_goc=self.ma_dinh_danh,
            )
        else:
            await self.dieu_phoi_ban_giao_nhiem_vu(fire_lat, fire_lon, pin_du_kien_pct)

    async def _giao_nhiem_vu_khoanh_vung(
        self,
        fire_lat: float,
        fire_lon: float,
        pin_du_kien_pct: Optional[float] = None,
        ma_phien: Optional[str] = None,
        la_p2p: bool = False,
        uav_phat_hien_goc: Optional[str] = None,
    ):
        if not self.bo_dinh_tuyen.khoa_muc_tieu_chay(fire_lat, fire_lon, self.ma_dinh_danh):
            return

        phien_id = ma_phien or str(uuid.uuid4())
        pin_khu_hoi = (
            pin_du_kien_pct
            if pin_du_kien_pct is not None
            else self._tinh_pin_du_kien_khu_hoi_pct(fire_lat, fire_lon)
        )

        gcs_server = getattr(self.bo_dinh_tuyen, "ground_station_server", None)
        if gcs_server and hasattr(gcs_server, "ghi_nhan_khoa_muc_tieu"):
            await gcs_server.ghi_nhan_khoa_muc_tieu(
                fire_lat,
                fire_lon,
                self.ma_dinh_danh,
                pin_khu_hoi,
                zone_id=self.zone_id,
                la_p2p=la_p2p,
                uav_phat_hien_goc=uav_phat_hien_goc or self.ma_dinh_danh,
            )

        self.so_nhiem_vu_da_nhan += 1
        self._su_co_da_nho_dong_doi.discard((round(float(fire_lat), 4), round(float(fire_lon), 4)))
        self.toa_do_muc_tieu = {"vi_do": float(fire_lat), "kinh_do": float(fire_lon)}
        self._tam_khoanh_vung_chay = {"vi_do": float(fire_lat), "kinh_do": float(fire_lon), "ma_phien": phien_id}
        self.chuyen_trang_thai(AgentState.RESPONDING)
        self.uav_state = UAVState.RESPONDING

        net_stack = getattr(self.bo_dinh_tuyen, "network_stack", None)
        if net_stack is not None:
            net_stack.send_command_to_external_client(
                self.ma_dinh_danh,
                "DISPATCH_FIRE",
                {"lat": float(fire_lat), "lon": float(fire_lon), "fire_id": phien_id, "zone_id": self.zone_id},
            )

        await self.bo_dinh_tuyen.gui_bao_cao_ve_tram(
            ma_nguon=self.ma_dinh_danh,
            thong_diep={
                "loai_tin": "FIRE_DETECTION_REPORT",
                "ma_su_co": phien_id,
                "ma_phien_su_co": phien_id,
                "uav_phat_hien": uav_phat_hien_goc or self.ma_dinh_danh,
                "loai_su_co": "CHÁY_RỪNG",
                "vi_do": float(fire_lat),
                "kinh_do": float(fire_lon),
                "thoi_diem_phat_hien": time.time(),
                "kich_thuoc_kb": 250.0,
            },
        )
        if hasattr(self.bo_dinh_tuyen, "cap_nhat_tuyen_e2e_dam_chay_lien_tuc"):
            self.bo_dinh_tuyen.cap_nhat_tuyen_e2e_dam_chay_lien_tuc()

    async def dieu_phoi_ban_giao_nhiem_vu(
        self,
        fire_lat: float,
        fire_lon: float,
        pin_du_kien_pct: float,
        force: bool = False,
    ) -> Optional["UAVAgent"]:
        """
        Khi UAV phát hiện hoặc đang giữ nhiệm vụ bị pin yếu (<= 25% / RTB / OFFLINE),
        Server chọn UAV Client khỏe gần tâm cháy nhất trong cùng phân khu và đẩy TCP DISPATCH_FIRE thay thế.
        """
        key_su_co = (round(fire_lat, 4), round(fire_lon, 4))
        if not force and key_su_co in self._su_co_da_nho_dong_doi:
            return None

        def _eligible(u: "UAVAgent", same_zone: bool) -> bool:
            if getattr(u, "loai_tac_nhan", None) != "MÁY_BAY" or u.ma_dinh_danh == self.ma_dinh_danh:
                return False
            if same_zone and getattr(u, "zone_id", None) != self.zone_id:
                return False
            if u._dang_o_tai_tram() or u.trang_thai in (AgentState.OFFLINE, AgentState.IDLE, AgentState.LOW_BATTERY_RTB, AgentState.RETURNING, AgentState.CHARGING):
                return False
            if getattr(u, "uav_state", None) in (UAVState.STANDBY, UAVState.CHARGING, UAVState.RETURNING, UAVState.OFFLINE, UAVState.CONTAINING, UAVState.RESPONDING):
                return False
            return u._tinh_pin_du_kien_khu_hoi_pct(fire_lat, fire_lon) > LOW_BATTERY_THRESHOLD

        with self.bo_dinh_tuyen.os_kernel.net_state_lock:
            all_agents = [a for a in self.bo_dinh_tuyen.danh_sach_tac_nhan.values() if isinstance(a, UAVAgent)]
        # Chỉ bàn giao nội bộ trong đúng phân khu của UAV (khóa chặt vùng hoạt động)
        ung_vien = [u for u in all_agents if _eligible(u, same_zone=True)]
        if not ung_vien:
            return None

        self._su_co_da_nho_dong_doi.add(key_su_co)
        ung_vien.sort(
            key=lambda u: (
                0 if u._tinh_pin_du_kien_khu_hoi_pct(fire_lat, fire_lon) >= 35.0 else 1,
                self._khoang_cach_m(fire_lat, fire_lon, u.lat, u.lon),
                -u.phan_tram_pin,
            )
        )
        uav_chon = ung_vien[0]
        pin_tiep_ung = uav_chon._tinh_pin_du_kien_khu_hoi_pct(fire_lat, fire_lon)

        gcs_server = getattr(self.bo_dinh_tuyen, "ground_station_server", None)
        if gcs_server and hasattr(gcs_server, "ghi_nhan_ban_giao_nhiem_vu"):
            await gcs_server.ghi_nhan_ban_giao_nhiem_vu(
                fire_lat,
                fire_lon,
                self.ma_dinh_danh,
                uav_chon.ma_dinh_danh,
                pin_du_kien_pct,
                pin_tiep_ung,
                zone_id=self.zone_id,
            )

        await uav_chon._giao_nhiem_vu_khoanh_vung(
            fire_lat,
            fire_lon,
            pin_du_kien_pct=pin_tiep_ung,
            la_p2p=True,
            uav_phat_hien_goc=self.ma_dinh_danh,
        )
        return uav_chon

    async def _goi_uav_du_bi_thay_ca(self) -> Optional[str]:
        """Khi UAV rời vùng tuần tra (RTB / sập kết nối), đánh thức 1 Client STANDBY qua TCP RESUME_PATROL."""
        if self._da_goi_du_bi_thay_ca or not self.zone_id:
            return None
        with self.bo_dinh_tuyen.os_kernel.net_state_lock:
            standby_candidates = [
                a for a in self.bo_dinh_tuyen.danh_sach_tac_nhan.values()
                if isinstance(a, UAVAgent)
                and a.ma_dinh_danh != self.ma_dinh_danh
                and a.trang_thai != AgentState.OFFLINE
                and a.uav_state == UAVState.STANDBY
                and a._dang_o_tai_tram()
                and a.phan_tram_pin >= 90.0
            ]
        if not standby_candidates:
            return None

        # Khóa chặt theo home_zone_id: phân khu nào chỉ dùng UAV dự bị của đúng phân khu đó
        same_home = [u for u in standby_candidates if (u.home_zone_id or u.zone_id) == self.zone_id]
        if not same_home:
            return None
        same_home.sort(key=lambda u: (-u.phan_tram_pin, u.ma_dinh_danh))
        u_du_bi = same_home[0]

        self._da_goi_du_bi_thay_ca = True
        u_du_bi.zone_id = self.zone_id
        u_du_bi.chuyen_trang_thai(AgentState.PATROLLING)
        u_du_bi.uav_state = UAVState.TRANSITING

        sim_loop = getattr(self.bo_dinh_tuyen, "simulation_loop", None)
        zone_center = list(sim_loop._get_zone_center(self.zone_id)) if sim_loop else [16.125, 108.255]
        zone_polygon = sim_loop._get_zone_polygon(self.zone_id) if sim_loop else []

        net_stack = getattr(self.bo_dinh_tuyen, "network_stack", None)
        if net_stack is not None:
            net_stack.send_command_to_external_client(
                u_du_bi.ma_dinh_danh,
                "RESUME_PATROL",
                {
                    "zone_id": self.zone_id,
                    "zone_center": zone_center,
                    "zone_polygon": zone_polygon,
                },
            )

        await self.bo_dinh_tuyen.gui_log_ra_web(
            "INFO",
            f"[SWARM_HANDOVER] Điều động Client dự bị [{u_du_bi.ma_dinh_danh}] (STANDBY) "
            f"cất cánh thay ca tại {self.zone_id}.",
        )
        return u_du_bi.ma_dinh_danh

    async def xu_ly_thong_diep(self, thong_diep: dict):
        return
