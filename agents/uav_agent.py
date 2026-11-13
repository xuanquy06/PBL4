"""UAV autonomous potential-field patrol, P2P swarm handover, and Dijkstra reporting."""
import asyncio
import math
import time
import uuid
from enum import Enum
from typing import Dict, Optional
from core.base_agent import AgentState, BaseAgent

LOW_BATTERY_THRESHOLD = 25.0
FIRE_DETECTION_RADIUS_M = 600.0
FIRE_RISK_THRESHOLD = 0.85
FIRE_CONTAINMENT_RADIUS_M = 130.0


class UAVState(str, Enum):
    STANDBY = "STANDBY"
    PATROLLING = "PATROLLING"
    RESPONDING = "RESPONDING"
    RELAYING = "RELAYING"
    RETURNING = "RETURNING"


class UAVAgent(BaseAgent):
    def __init__(
        self,
        bo_dinh_tuyen,
        cong_suat_bay_w: float = 80.0,
        zone_id: Optional[str] = None,
        **kwargs
    ):
        kwargs.setdefault("loai_tac_nhan", "MÁY_BAY")
        kwargs.setdefault("pin_max_joule", 50000.0)
        kwargs.setdefault("cong_suat_dbm", 35.0)
        kwargs.setdefault("tan_so_hz", 2.4e9)
        kwargs.setdefault("bang_thong_hz", 20e6)
        super().__init__(bo_dinh_tuyen=bo_dinh_tuyen, **kwargs)
        self.cong_suat_bay_w = max(1.0, float(cong_suat_bay_w))
        self.zone_id = zone_id
        self.nguong_pin_yeu_pct = LOW_BATTERY_THRESHOLD
        self.so_nhiem_vu_da_nhan = 0
        self.toa_do_muc_tieu: Optional[dict] = None
        self.toa_do_tram_sac: Optional[dict] = None
        self._vung_tuan_tra_phu: Dict[str, str] = {}
        self._tam_khoanh_vung_chay: Optional[dict] = None
        self._goc_khoanh_vung_rad: float = 0.0
        self._thoi_gian_khoanh_vung_con_lai: float = 0.0
        self._den_muc_tieu: Optional[asyncio.Event] = None
        self._tac_vu_su_co: Optional[asyncio.Task] = None
        self._yeu_cau_ban_giao_da_phat = False
        self._waiting_for_relief_id: Optional[str] = None
        self._relieving_target_id: Optional[str] = None
        self._da_ban_giao_tai_cho = False
        self._su_co_da_nho_dong_doi: set[tuple[float, float]] = set()
        self._da_log_gradient_pull: set[tuple[float, float]] = set()
        self._da_phat_hien_dam_chay: set[tuple[float, float]] = set()
        self.uav_state = UAVState.STANDBY
        self.current_link = "LEO_SATELLITE"
        self.direct_los = False
        self.relay_required = True
        self._relay_uav_id: Optional[str] = None
        self.relay_target_id: Optional[str] = None
        self._relay_requesters: set[str] = set()
        self._thoi_gian_beacon_tich_luy = 0.0
        if self._dang_o_tai_tram():
            self.chuyen_trang_thai(AgentState.IDLE)
            self.uav_state = UAVState.STANDBY
        else:
            self.chuyen_trang_thai(AgentState.PATROLLING)

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

    def _get_den_muc_tieu_event(self) -> asyncio.Event:
        if self._den_muc_tieu is None:
            self._den_muc_tieu = asyncio.Event()
        return self._den_muc_tieu

    def chuyen_trang_thai(self, trang_thai_moi: AgentState):
        super().chuyen_trang_thai(trang_thai_moi)
        if not hasattr(self, "uav_state"):
            return
        trang_thai_uav = {
            AgentState.IDLE: UAVState.STANDBY,
            AgentState.CHARGING: UAVState.STANDBY,
            AgentState.PATROLLING: UAVState.PATROLLING,
            AgentState.BIDDING: UAVState.RESPONDING,
            AgentState.RELAYING: UAVState.RELAYING,
            AgentState.RETURNING: UAVState.RETURNING,
            AgentState.LOW_BATTERY_RTB: UAVState.RETURNING,
        }.get(trang_thai_moi)
        if trang_thai_uav:
            self.uav_state = trang_thai_uav

    def xuat_ho_so_trang_thai(self) -> dict:
        ho_so = super().xuat_ho_so_trang_thai()
        ho_so["state"] = self.uav_state.value
        ho_so["zone_id"] = self.zone_id
        ho_so["current_link"] = self.current_link
        ho_so["direct_los"] = self.direct_los
        ho_so["relay_required"] = self.relay_required
        ho_so["relay_target_id"] = self.relay_target_id
        ho_so["tuyen_ve_tram"] = list(getattr(self, "tuyen_ve_tram_cuc_bo", []))
        ho_so["is_reserve"] = bool(getattr(self, "uav_state", None) == UAVState.STANDBY and self._dang_o_tai_tram())
        ho_so["scan_radius_m"] = FIRE_DETECTION_RADIUS_M
        ho_so["so_nhiem_vu"] = self.so_nhiem_vu_da_nhan
        ho_so["vung_ban_giao_nhan"] = list(self._vung_tuan_tra_phu.keys())
        if self.toa_do_muc_tieu:
            ho_so["target_lat"] = round(float(self.toa_do_muc_tieu["vi_do"]), 5)
            ho_so["target_lng"] = round(float(self.toa_do_muc_tieu["kinh_do"]), 5)
        return ho_so

    def _ket_thuc_khoanh_vung_chay(self):
        """Giải phóng trạng thái khoanh vùng sự cố để UAV tiếp tục tuần tra theo trường lực Heatmap."""
        self._tam_khoanh_vung_chay = None
        self._thoi_gian_khoanh_vung_con_lai = 0.0

    async def dat_pin_gia_lap(self, phan_tram_pin: float):
        """Áp dụng sự cố pin qua hành vi của UAV thay vì để API sửa trực tiếp FSM."""
        self.pin_hien_tai = self.pin_toi_da * phan_tram_pin / 100.0
        if self.pin_hien_tai <= 0.0:
            self._giai_phong_uav_chuyen_tiep()
            self.chuyen_trang_thai(AgentState.OFFLINE)
            await self._phat_yeu_cau_ban_giao_vung()
        elif phan_tram_pin < self.nguong_pin_yeu_pct:
            self.chuyen_trang_thai(AgentState.LOW_BATTERY_RTB)
            await self._phat_yeu_cau_ban_giao_vung()
        else:
            self._yeu_cau_ban_giao_da_phat = False
            self._waiting_for_relief_id = None
            self._ket_thuc_khoanh_vung_chay()
            await self._phat_thong_bao_khoi_phuc_vung()
            if self._dang_o_tai_tram():
                self.chuyen_trang_thai(
                    AgentState.CHARGING if phan_tram_pin < 100.0 else AgentState.IDLE
                )
                self.uav_state = UAVState.STANDBY
            else:
                self.chuyen_trang_thai(AgentState.PATROLLING)

    async def cap_nhat_vat_ly(self, dt_giay: float):
        await self.update(dt_giay)

    async def update(self, dt_giay: float):
        if dt_giay <= 0 or self.trang_thai == AgentState.OFFLINE:
            return
        self.don_dep_lang_gieng_het_han()
        await self._cap_nhat_duong_truyen()
        self._thoi_gian_beacon_tich_luy += dt_giay
        if (
            self._thoi_gian_beacon_tich_luy >= 4.0
            and len(self.tuyen_ve_tram_cuc_bo) > 1
        ):
            self._thoi_gian_beacon_tich_luy = 0.0
            await self.bo_dinh_tuyen.phat_song_quang_ba(
                self.ma_dinh_danh,
                {
                    "loai_tin": "HELLO_BEACON",
                    "cost_to_sink": self.chi_phi_ve_tram_cuc_bo,
                    "path_to_sink": list(self.tuyen_ve_tram_cuc_bo),
                    "energy_to_sink_j": self.nang_luong_ve_tram_cuc_bo,
                    "delay_to_sink_ms": self.do_tre_ve_tram_cuc_bo_ms,
                    "capacity_to_sink_mbps": self.dung_luong_ve_tram_cuc_bo_mbps,
                    "kich_thuoc_kb": 1.0
                }
            )
        if self.trang_thai == AgentState.CHARGING or (
            self.uav_state == UAVState.STANDBY and self._dang_o_tai_tram()
        ):
            was_charging = self.trang_thai == AgentState.CHARGING
            self.pin_hien_tai = min(self.pin_toi_da, self.pin_hien_tai + 300.0 * dt_giay)
            if self.pin_hien_tai >= self.pin_toi_da and was_charging:
                self._yeu_cau_ban_giao_da_phat = False
                self._waiting_for_relief_id = None
                self._relieving_target_id = None
                self._da_ban_giao_tai_cho = False
                uavs_active_in_zone = [
                    u for u in self.bo_dinh_tuyen.danh_sach_tac_nhan.values()
                    if u.loai_tac_nhan == "MÁY_BAY"
                    and u.ma_dinh_danh != self.ma_dinh_danh
                    and self.zone_id
                    and getattr(u, "zone_id", None) == self.zone_id
                    and not u._dang_o_tai_tram()
                    and u.trang_thai in (AgentState.PATROLLING, AgentState.RELAYING, AgentState.BIDDING)
                ]
                if self.zone_id and len(uavs_active_in_zone) >= 3:
                    # Phân khu đã có đủ 3 UAV tuần tra (nhờ UAV dự bị thay ca), ở lại trạm làm UAV dự bị mới
                    self.toa_do_tram_sac = None
                    self.chuyen_trang_thai(AgentState.IDLE)
                    self.uav_state = UAVState.STANDBY
                    await self.bo_dinh_tuyen.gui_log_ra_web(
                        "INFO",
                        f"🔋 [{self.ma_dinh_danh}] Sạc đầy 100%, chuyển sang trực chiến DỰ BỊ tại Trạm cho {self.zone_id}."
                    )
                else:
                    self._ket_thuc_khoanh_vung_chay()
                    self.toa_do["do_cao"] = max(1000.0, float(self.toa_do.get("do_cao", 1000.0)))
                    self.chuyen_trang_thai(AgentState.PATROLLING)
                    await self._phat_thong_bao_khoi_phuc_vung()
                    await self.bo_dinh_tuyen.gui_log_ra_web(
                        "INFO", f"[{self.ma_dinh_danh}] Sạc đầy pin 100%, khôi phục tuần tra phân khu {self.zone_id or ''}."
                    )
            return

        # Kiểm tra xem UAV dự bị tiếp viện đã bay tới vùng chưa (In-Zone Hot-Swap)
        if self._waiting_for_relief_id:
            relief = self.bo_dinh_tuyen.danh_sach_tac_nhan.get(self._waiting_for_relief_id)
            if not relief or relief.trang_thai == AgentState.OFFLINE:
                self._waiting_for_relief_id = None
            else:
                dist_relief = self.bo_dinh_tuyen.physics.tinh_khoang_cach_3d(self.toa_do, relief.toa_do)
                if not relief._dang_o_tai_tram() and dist_relief <= 450.0:
                    relief._relieving_target_id = None
                    self._waiting_for_relief_id = None
                    self._da_ban_giao_tai_cho = True
                    await self.bo_dinh_tuyen.gui_log_ra_web(
                        "INFO",
                        f"🤝 [BÀN GIAO TẠI CHỖ] {relief.ma_dinh_danh} đã tiếp cận {self.zone_id} "
                        f"({dist_relief:.0f}m); {self.ma_dinh_danh} bắt đầu rời vùng về Trạm sạc."
                    )

        if self.phan_tram_pin < LOW_BATTERY_THRESHOLD and not self._waiting_for_relief_id:
            # Nếu trong phân khu có UAV dự bị tại trạm và chưa bàn giao tại chỗ, ưu tiên gọi dự bị ra trước
            if self.zone_id and not self._da_ban_giao_tai_cho and self._goi_uav_du_bi_thay_ca():
                pass
            else:
                if self.uav_state != UAVState.RETURNING:
                    self.chuyen_trang_thai(AgentState.LOW_BATTERY_RTB)
                if not self._yeu_cau_ban_giao_vung_da_phat():
                    await self._phat_yeu_cau_ban_giao_vung()

        if self.trang_thai in (AgentState.LOW_BATTERY_RTB, AgentState.PATROLLING, AgentState.RELAYING) and (
            self.trang_thai == AgentState.LOW_BATTERY_RTB
            or not self._tac_vu_su_co
            or self._tac_vu_su_co.done()
        ):
            await self._chon_tram_sac()
            if (
                self.trang_thai == AgentState.LOW_BATTERY_RTB
                and not self.toa_do_tram_sac
            ):
                return

        # Tự chủ phát hiện đám cháy trong bán kính <= 600m khi đang tuần tra
        if (
            self.trang_thai in (AgentState.PATROLLING, AgentState.RELAYING)
            and (not self._tac_vu_su_co or self._tac_vu_su_co.done())
            and not self.toa_do_muc_tieu
            and self._tam_khoanh_vung_chay is None
        ):
            await self._kiem_tra_va_xu_ly_dam_chay_tu_chu()

        if self.toa_do_muc_tieu:
            self._di_chuyen_den_diem_muc_tieu(self.toa_do_muc_tieu, dt_giay, la_tram_sac=False)
            return

        if self.trang_thai in (
            AgentState.LOW_BATTERY_RTB,
            AgentState.RETURNING
        ) and self.toa_do_tram_sac:
            self._di_chuyen_den_diem_muc_tieu(self.toa_do_tram_sac, dt_giay, la_tram_sac=True)
            return

        if self._tam_khoanh_vung_chay is not None:
            self._di_chuyen_khoanh_vung_dam_chay(dt_giay)
            return

        if self.trang_thai in (AgentState.PATROLLING, AgentState.RELAYING):
            # Logic di chuyển duy nhất của UAV ở trạng thái tuần tra:
            # Cập nhật self.lat và self.lon bằng vector lực hấp dẫn tổng hợp từ các điểm HeatNode
            # (hướng về tâm nhiệt đỏ) cộng với lực đẩy giữa các UAV, tuyệt đối không bay theo mốc định sẵn.
            self._di_chuyen_tu_chu_theo_truong_luc(dt_giay)

    def _yeu_cau_ban_giao_vung_da_phat(self) -> bool:
        return bool(self._yeu_cau_ban_giao_da_phat)

    def _di_chuyen_den_diem_muc_tieu(self, muc_tieu: dict, dt_giay: float, la_tram_sac: bool = False):
        """Di chuyển thẳng tới tọa độ mục tiêu (tâm đám cháy hoặc Trạm mặt đất khi về sạc)."""
        van_toc_m_s = max(0.0, float(muc_tieu.get("van_toc_m_s", 40.0)))
        if van_toc_m_s <= 0.0:
            return

        khoang_cach_m = self.bo_dinh_tuyen.physics.tinh_khoang_cach_3d(self.toa_do, muc_tieu)
        if khoang_cach_m <= 1.0:
            if la_tram_sac:
                self.lat = float(muc_tieu["vi_do"])
                self.lon = float(muc_tieu["kinh_do"])
                self.toa_do["do_cao"] = float(muc_tieu.get("do_cao", self.toa_do["do_cao"]))
                self.toa_do_tram_sac = None
                self.chuyen_trang_thai(AgentState.CHARGING)
            else:
                self._get_den_muc_tieu_event().set()
            return

        quang_duong_m = min(khoang_cach_m, van_toc_m_s * dt_giay)
        ti_le_di_chuyen = quang_duong_m / khoang_cach_m
        self.lat = self.lat + (float(muc_tieu["vi_do"]) - self.lat) * ti_le_di_chuyen
        self.lon = self.lon + (float(muc_tieu["kinh_do"]) - self.lon) * ti_le_di_chuyen
        self.toa_do["do_cao"] = float(self.toa_do["do_cao"]) + (
            float(muc_tieu.get("do_cao", self.toa_do["do_cao"])) - float(self.toa_do["do_cao"])
        ) * ti_le_di_chuyen
        self.toa_do["van_toc_m_s"] = van_toc_m_s
        self.tieu_hao_nang_luong(self.cong_suat_bay_w * dt_giay)

        if ti_le_di_chuyen >= 1.0:
            if la_tram_sac:
                self.lat = float(muc_tieu["vi_do"])
                self.lon = float(muc_tieu["kinh_do"])
                self.toa_do["do_cao"] = float(muc_tieu.get("do_cao", self.toa_do["do_cao"]))
                self.toa_do_tram_sac = None
                self.chuyen_trang_thai(AgentState.CHARGING)
            else:
                self._get_den_muc_tieu_event().set()

    def _di_chuyen_khoanh_vung_dam_chay(self, dt_giay: float):
        """Khoanh vùng 130m quanh tâm đám cháy bằng chuyển động góc liên tục, không dùng mốc tọa độ định sẵn."""
        if not self._tam_khoanh_vung_chay:
            return
        self._thoi_gian_khoanh_vung_con_lai -= dt_giay
        if self._thoi_gian_khoanh_vung_con_lai <= 0.0:
            if hasattr(self.bo_dinh_tuyen, "mo_khoa_muc_tieu_chay"):
                self.bo_dinh_tuyen.mo_khoa_muc_tieu_chay(self.ma_dinh_danh, ha_nhiet=True)
            self._ket_thuc_khoanh_vung_chay()
            return

        fire_lat = float(self._tam_khoanh_vung_chay["vi_do"])
        fire_lon = float(self._tam_khoanh_vung_chay["kinh_do"])
        van_toc_m_s = 40.0
        omega = van_toc_m_s / FIRE_CONTAINMENT_RADIUS_M
        self._goc_khoanh_vung_rad = (self._goc_khoanh_vung_rad + omega * dt_giay) % (2.0 * math.pi)

        met_tren_do_vi = 111320.0
        met_tren_do_kinh = 111320.0 * max(0.2, math.cos(math.radians(fire_lat)))
        self.lat = fire_lat + (FIRE_CONTAINMENT_RADIUS_M * math.sin(self._goc_khoanh_vung_rad)) / met_tren_do_vi
        self.lon = fire_lon + (FIRE_CONTAINMENT_RADIUS_M * math.cos(self._goc_khoanh_vung_rad)) / met_tren_do_kinh
        self.toa_do["van_toc_m_s"] = van_toc_m_s
        self.tieu_hao_nang_luong(self.cong_suat_bay_w * dt_giay)

    def _lay_da_giac_vung_hien_tai(self) -> Optional[dict]:
        """Lấy thông tin đa giác GeoJSON của phân khu hiện tại (Z_k)."""
        if not self.zone_id:
            return None
        scenario = getattr(self.bo_dinh_tuyen, "scenario", None)
        patrol_zones = getattr(scenario, "patrol_zones", None)
        if not isinstance(patrol_zones, dict):
            return None
        for feat in patrol_zones.get("features", []):
            if feat.get("properties", {}).get("zone_id") == self.zone_id:
                return feat
        return None

    def _di_chuyen_tu_chu_theo_truong_luc(self, dt_giay: float):
        """
        Di chuyển tự chủ (Autonomous Potential Field) trong phân khu Z_k:
        1. Cập nhật self.lat và self.lon bằng cách tính vector lực hấp dẫn tổng hợp từ các điểm HeatNode
           (hướng về tâm nhiệt đỏ có risk_score cao nhất).
        2. Cộng với lực đẩy giữa các UAV để tránh va chạm và giữ khoảng cách đội hình.
        3. Tuân thủ nghiêm ngặt biên giới đa giác Z_k bằng thuật toán Ray-Casting, tuyệt đối không
           di chuyển theo các mốc tọa độ định sẵn.
        """
        scenario = getattr(self.bo_dinh_tuyen, "scenario", None)
        active_zones = {self.zone_id} if self.zone_id else set()
        active_zones.update(z for z in self._vung_tuan_tra_phu.values() if z)

        all_heat_nodes = getattr(scenario, "heat_nodes", None) or []
        heat_nodes = [
            n for n in all_heat_nodes
            if (not active_zones or getattr(n, "zone_id", None) in active_zones)
            and not getattr(n, "locked_by", None)
        ]
        if not heat_nodes and all_heat_nodes:
            heat_nodes = [
                n for n in all_heat_nodes
                if not active_zones or getattr(n, "zone_id", None) in active_zones
            ]

        cur_lat = self.lat
        cur_lon = self.lon
        met_tren_do_vi = 111320.0
        met_tren_do_kinh = 111320.0 * max(0.2, math.cos(math.radians(cur_lat)))

        # Làm mát nhẹ các HeatNode thông thường (< 0.85) mà UAV vừa bay qua sát (<= 160m)
        # để UAV tự động chuyển hướng tuần tra sang điểm có risk_score cao tiếp theo trong vùng
        for node in heat_nodes:
            d_node = self._khoang_cach_m(cur_lat, cur_lon, node.lat, node.lon)
            if d_node <= 160.0 and node.risk_score < FIRE_RISK_THRESHOLD:
                node.risk_score = 0.05

        fx_attr = 0.0
        fy_attr = 0.0
        ux_max = 0.0
        uy_max = 0.0

        if heat_nodes:
            # 1. Lực hút từ các HeatNode trong phân khu (hướng mạnh nhất về tâm nhiệt đỏ có risk_score cao nhất)
            node_max = max(
                heat_nodes,
                key=lambda n: (
                    round(float(n.risk_score), 3),
                    -self._khoang_cach_m(cur_lat, cur_lon, n.lat, n.lon)
                )
            )
            if float(node_max.risk_score) >= FIRE_RISK_THRESHOLD:
                fc = getattr(node_max, "fire_center", None) or (node_max.lat, node_max.lon)
                pull_key = (round(float(fc[0]), 4), round(float(fc[1]), 4))
                if pull_key not in self._da_log_gradient_pull:
                    self._da_log_gradient_pull.add(pull_key)
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(
                            self.bo_dinh_tuyen.gui_log_ra_web(
                                "INFO",
                                f"[GRADIENT_PULL] [{self.ma_dinh_danh}] bị hút theo trường lực Heatmap về phía vùng "
                                f"risk_score={node_max.risk_score:.2f} tại ({pull_key[0]:.4f}, {pull_key[1]:.4f}) thuộc {self.zone_id}."
                            )
                        )
                    except RuntimeError:
                        pass

            for node in heat_nodes:
                dx_m = (node.lon - cur_lon) * met_tren_do_kinh
                dy_m = (node.lat - cur_lat) * met_tren_do_vi
                dist_m = math.hypot(dx_m, dy_m)
                if dist_m < 1.0:
                    continue
                weight = (float(node.risk_score) ** 2) / max(150.0, dist_m)
                fx_attr += (dx_m / dist_m) * weight
                fy_attr += (dy_m / dist_m) * weight

            mag_attr = math.hypot(fx_attr, fy_attr)
            if mag_attr > 1e-9:
                fx_attr /= mag_attr
                fy_attr /= mag_attr

            dx_max = (node_max.lon - cur_lon) * met_tren_do_kinh
            dy_max = (node_max.lat - cur_lat) * met_tren_do_vi
            dist_max = max(1.0, math.hypot(dx_max, dy_max))
            ux_max = dx_max / dist_max
            uy_max = dy_max / dist_max
        else:
            zone_feat_fallback = self._lay_da_giac_vung_hien_tai()
            if zone_feat_fallback is not None:
                c_lon, c_lat = zone_feat_fallback.get("properties", {}).get("patrol_center", [cur_lon, cur_lat])
                dx_c = (float(c_lon) - cur_lon) * met_tren_do_kinh
                dy_c = (float(c_lat) - cur_lat) * met_tren_do_vi
                dist_c = max(1.0, math.hypot(dx_c, dy_c))
                ux_max = dx_c / dist_c
                uy_max = dy_c / dist_c

        # Vector hướng về tâm nhiệt đỏ có risk_score cao nhất đóng vai trò dẫn đường chính
        fx_total = ux_max * 2.4 + fx_attr * 0.8
        fy_total = uy_max * 2.4 + fy_attr * 0.8

        # 2. Lực đẩy giữa các UAV để tránh va chạm và phân tán đội hình trong vùng
        for other in self.bo_dinh_tuyen.danh_sach_tac_nhan.values():
            if (
                other is self
                or other.loai_tac_nhan != "MÁY_BAY"
                or other.trang_thai == AgentState.OFFLINE
                or other._dang_o_tai_tram()
            ):
                continue
            rx_m = (cur_lon - float(other.toa_do["kinh_do"])) * met_tren_do_kinh
            ry_m = (cur_lat - float(other.toa_do["vi_do"])) * met_tren_do_vi
            d_uav = math.hypot(rx_m, ry_m)
            if 0.1 < d_uav < 850.0:
                rep_strength = 3.2 * ((850.0 - d_uav) / 850.0) ** 2
                if d_uav < 320.0:
                    rep_strength += 4.0 * ((320.0 - d_uav) / 320.0)
                fx_total += (rx_m / d_uav) * rep_strength
                fy_total += (ry_m / d_uav) * rep_strength

        mag_total = math.hypot(fx_total, fy_total)
        if mag_total <= 1e-9:
            ux, uy = ux_max, uy_max
        else:
            ux, uy = fx_total / mag_total, fy_total / mag_total

        van_toc_m_s = 40.0
        quang_duong_m = van_toc_m_s * dt_giay
        next_lat = cur_lat + (uy * quang_duong_m) / met_tren_do_vi
        next_lon = cur_lon + (ux * quang_duong_m) / met_tren_do_kinh

        # 3. Tuân thủ biên giới đa giác phân khu Z_k bằng thuật toán Ray-Casting
        zone_feat = self._lay_da_giac_vung_hien_tai()
        if zone_feat is not None and scenario is not None:
            dang_trong_vung = scenario._diem_nam_trong_vung(cur_lat, cur_lon, zone_feat)
            nam_trong = (
                scenario._diem_nam_gon_trong_vung(next_lat, next_lon, zone_feat, margin_deg=0.00025)
                if hasattr(scenario, "_diem_nam_gon_trong_vung")
                else scenario._diem_nam_trong_vung(next_lat, next_lon, zone_feat)
            )
            if not nam_trong:
                # Nếu đang ở ngoài vùng (vừa xuất kích từ Trạm) hoặc chạm biên đa giác, bẻ hướng về node_max trong vùng
                next_lat = cur_lat + (uy_max * quang_duong_m) / met_tren_do_vi
                next_lon = cur_lon + (ux_max * quang_duong_m) / met_tren_do_kinh
                if dang_trong_vung and not scenario._diem_nam_trong_vung(next_lat, next_lon, zone_feat):
                    c_lon, c_lat = zone_feat.get("properties", {}).get("patrol_center", [cur_lon, cur_lat])
                    dx_c = (float(c_lon) - cur_lon) * met_tren_do_kinh
                    dy_c = (float(c_lat) - cur_lat) * met_tren_do_vi
                    d_c = max(1.0, math.hypot(dx_c, dy_c))
                    next_lat = cur_lat + ((dy_c / d_c) * quang_duong_m) / met_tren_do_vi
                    next_lon = cur_lon + ((dx_c / d_c) * quang_duong_m) / met_tren_do_kinh

        self.lat = next_lat
        self.lon = next_lon
        self.toa_do["van_toc_m_s"] = van_toc_m_s
        self.tieu_hao_nang_luong(self.cong_suat_bay_w * dt_giay)

    def _tinh_pin_du_kien_khu_hoi_pct(self, target_lat: float, target_lon: float) -> float:
        """
        Tính phần trăm pin dự kiến còn lại sau khi bay tới tâm cháy (target_lat, target_lon),
        khoanh vùng giám sát và bay khứ hồi về vị trí an toàn / Trạm mặt đất.
        """
        muc_tieu = {
            "vi_do": float(target_lat),
            "kinh_do": float(target_lon),
            "do_cao": float(self.toa_do.get("do_cao", 1000.0))
        }
        kc_toi_chay_m = self.bo_dinh_tuyen.physics.tinh_khoang_cach_3d(self.toa_do, muc_tieu)
        van_toc = max(1.0, float(self.toa_do.get("van_toc_m_s") or 40.0))
        thoi_gian_khu_hoi_s = (2.0 * kc_toi_chay_m) / van_toc
        nang_luong_tieu_hao_j = thoi_gian_khu_hoi_s * self.cong_suat_bay_w
        pin_con_lai_j = self.pin_hien_tai - nang_luong_tieu_hao_j
        if self.pin_toi_da <= 0:
            return 0.0
        return round((pin_con_lai_j / self.pin_toi_da) * 100.0, 2)

    async def _kiem_tra_va_xu_ly_dam_chay_tu_chu(self):
        """
        Khi UAV bay vào bán kính phát hiện đám cháy (<= 600m):
        - Tự động kiểm tra mục tiêu chưa bị khóa và kiểm tra pin khứ hồi (> 25%).
        - Nếu đủ pin (> 25%): khóa mục tiêu để UAV khác không lặp lại, đổi trạng thái bay thẳng
          tới tâm cháy, khoanh vùng 130m và gửi gói tin báo cáo qua định tuyến Dijkstra về Trạm.
        - Nếu pin yếu (<= 25%): tự phát tin nhắn P2P nhờ UAV đồng đội trong cùng phân khu xử lý thay.
        """
        scenario = getattr(self.bo_dinh_tuyen, "scenario", None)
        heat_nodes = getattr(scenario, "heat_nodes", None) or []
        if not heat_nodes:
            return

        cur_lat = self.lat
        cur_lon = self.lon

        # Lọc các HeatNode đang bùng cháy (risk_score >= 0.85) nằm trong bán kính phát hiện <= 600m
        cac_diem_chay_gan = []
        for node in heat_nodes:
            if float(node.risk_score) < FIRE_RISK_THRESHOLD:
                continue
            if getattr(node, "locked_by", None):
                continue
            kc_m = self._khoang_cach_m(cur_lat, cur_lon, node.lat, node.lon)
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

        # Kiểm tra xem mục tiêu cháy này đã bị UAV khác khóa chưa
        if hasattr(self.bo_dinh_tuyen, "kiem_tra_muc_tieu_da_khoa"):
            if self.bo_dinh_tuyen.kiem_tra_muc_tieu_da_khoa(fire_lat, fire_lon):
                return

        # Khi 1 UAV bay vào cách đám cháy <= 600m, chuyển trạng thái đám cháy sang "DETECTED"
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
            else:
                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "WARN",
                    f"[FIRE_DETECTED] [{self.ma_dinh_danh}] phát hiện đám cháy ở cự ly {kc_min_m:.0f}m (<= 600m) "
                    f"tại ({fire_lat:.4f}, {fire_lon:.4f}) ➔ Chuyển trạng thái đám cháy sang DETECTED."
                )

        pin_du_kien_pct = self._tinh_pin_du_kien_khu_hoi_pct(fire_lat, fire_lon)
        if self.phan_tram_pin > LOW_BATTERY_THRESHOLD and pin_du_kien_pct > LOW_BATTERY_THRESHOLD:
            await self._kich_hoat_xu_ly_chay_tu_chu(
                fire_lat,
                fire_lon,
                pin_du_kien_pct=pin_du_kien_pct,
                la_p2p=False,
                uav_phat_hien_goc=self.ma_dinh_danh,
            )
        else:
            await self._phat_tin_nhan_p2p_nho_dong_doi(fire_lat, fire_lon, pin_du_kien_pct)

    async def _kich_hoat_xu_ly_chay_tu_chu(
        self,
        fire_lat: float,
        fire_lon: float,
        pin_du_kien_pct: Optional[float] = None,
        ma_phien: Optional[str] = None,
        la_p2p: bool = False,
        uav_phat_hien_goc: Optional[str] = None
    ):
        """
        Khóa mục tiêu cháy ([TARGET_LOCKED]), chuyển trạng thái sang bay thẳng tới tâm cháy,
        khoanh vùng 130m và gửi báo cáo qua định tuyến Dijkstra về Trạm mặt đất ([DIJKSTRA_REPORT]).
        """
        if hasattr(self.bo_dinh_tuyen, "khoa_muc_tieu_chay"):
            da_khoa = self.bo_dinh_tuyen.khoa_muc_tieu_chay(fire_lat, fire_lon, self.ma_dinh_danh)
            if not da_khoa:
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
        else:
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "WIN",
                f"[TARGET_LOCKED] [{self.ma_dinh_danh}] đủ pin khứ hồi ({pin_khu_hoi:.1f}% > 25%) "
                f"➔ Đã khóa mục tiêu, bay thẳng tới tâm cháy ({fire_lat:.4f}, {fire_lon:.4f}) & khoanh vùng 130m."
            )

        self.toa_do_muc_tieu = {
            "vi_do": float(fire_lat),
            "kinh_do": float(fire_lon),
            "do_cao": float(self.toa_do["do_cao"]),
            "van_toc_m_s": max(45.0, float(self.toa_do.get("van_toc_m_s") or 45.0)),
        }
        self._den_muc_tieu = asyncio.Event()
        if self.bo_dinh_tuyen.physics.tinh_khoang_cach_3d(self.toa_do, self.toa_do_muc_tieu) <= 50.0:
            self._den_muc_tieu.set()

        self.so_nhiem_vu_da_nhan += 1
        self.chuyen_trang_thai(AgentState.BIDDING)
        self.uav_state = UAVState.RESPONDING
        thong_tin_su_co = {
            "ma_phien_dau_gia": phien_id,
            "loai_su_co": "CHÁY_RỪNG",
            "vi_do": float(fire_lat),
            "kinh_do": float(fire_lon),
            "zone_id": self.zone_id,
            "kich_thuoc_kb": 250.0,
        }
        try:
            loop = asyncio.get_running_loop()
            self._tac_vu_su_co = loop.create_task(self._thuc_hien_nhiem_vu(thong_tin_su_co))
        except RuntimeError:
            self._tac_vu_su_co = None

    async def _phat_tin_nhan_p2p_nho_dong_doi(
        self,
        fire_lat: float,
        fire_lon: float,
        pin_du_kien_pct: float
    ):
        """
        Khi UAV phát hiện đám cháy (<= 600m) nhưng pin khứ hồi yếu (<= 25%),
        phát sự kiện [P2P_HANDOVER] bàn giao trực tiếp cho UAV có pin cao nhất trong cùng phân khu xử lý.
        """
        key_su_co = (round(fire_lat, 4), round(fire_lon, 4))
        if key_su_co in self._su_co_da_nho_dong_doi:
            return

        dong_doi_cung_vung = [
            u for u in self.bo_dinh_tuyen.danh_sach_tac_nhan.values()
            if u.loai_tac_nhan == "MÁY_BAY"
            and u.ma_dinh_danh != self.ma_dinh_danh
            and getattr(u, "zone_id", None) == self.zone_id
            and u.trang_thai not in (AgentState.OFFLINE, AgentState.LOW_BATTERY_RTB, AgentState.RETURNING, AgentState.CHARGING)
            and (not getattr(u, "_tac_vu_su_co", None) or getattr(u, "_tac_vu_su_co").done())
        ]
        if not dong_doi_cung_vung:
            return

        self._su_co_da_nho_dong_doi.add(key_su_co)
        # Chọn UAV có pin cao nhất trong cùng phân khu
        dong_doi_cung_vung.sort(
            key=lambda u: (
                -u.phan_tram_pin,
                self._khoang_cach_m(fire_lat, fire_lon, float(u.toa_do["vi_do"]), float(u.toa_do["kinh_do"]))
            )
        )
        uav_pin_cao_nhat = dong_doi_cung_vung[0]
        pin_khu_hoi_dong_doi = uav_pin_cao_nhat._tinh_pin_du_kien_khu_hoi_pct(fire_lat, fire_lon)

        ma_phien = str(uuid.uuid4())
        gcs_server = getattr(self.bo_dinh_tuyen, "ground_station_server", None)
        if gcs_server and hasattr(gcs_server, "ghi_nhan_ban_giao_p2p"):
            await gcs_server.ghi_nhan_ban_giao_p2p(
                fire_lat,
                fire_lon,
                self.ma_dinh_danh,
                uav_pin_cao_nhat.ma_dinh_danh,
                pin_du_kien_pct,
                pin_khu_hoi_dong_doi,
                zone_id=self.zone_id,
            )
        else:
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "WARN",
                f"[P2P_HANDOVER] [{self.ma_dinh_danh}] phát hiện cháy tại ({fire_lat:.4f}, {fire_lon:.4f}) "
                f"nhưng pin khứ hồi yếu ({pin_du_kien_pct:.1f}% <= 25%) ➔ Ủy quyền P2P cho [{uav_pin_cao_nhat.ma_dinh_danh}] "
                f"(pin cao nhất vùng {self.zone_id}: {uav_pin_cao_nhat.phan_tram_pin:.1f}%) xử lý thay."
            )

        thong_diep_p2p = {
            "loai_tin": "YEU_CAU_HO_TRO_CHAY_P2P",
            "ma_phien_dau_gia": ma_phien,
            "loai_su_co": "CHÁY_RỪNG",
            "vi_do": float(fire_lat),
            "kinh_do": float(fire_lon),
            "zone_id": self.zone_id,
            "nguoi_nho": self.ma_dinh_danh,
            "kich_thuoc_kb": 2.0,
        }
        da_gui_thanh_cong = False
        for dong_doi in dong_doi_cung_vung:
            da_gui = await self.bo_dinh_tuyen.gui_don_huong(
                self.ma_dinh_danh,
                dong_doi.ma_dinh_danh,
                thong_diep_p2p.copy()
            )
            if da_gui:
                da_gui_thanh_cong = True
                break

        if not da_gui_thanh_cong:
            # Dự phòng đảm bảo gói bàn giao P2P đến được UAV có pin cao nhất trong cùng vùng
            await uav_pin_cao_nhat.xu_ly_thong_diep({
                **thong_diep_p2p,
                "nguoi_gui": self.ma_dinh_danh,
            })

    async def _cap_nhat_duong_truyen(self):
        if self._dang_o_tai_tram():
            tram = next(
                (
                    agent for agent in self.bo_dinh_tuyen.danh_sach_tac_nhan.values()
                    if agent.loai_tac_nhan == "TRẠM_MẶT_ĐẤT"
                    and self._khoang_cach_m(
                        self.lat,
                        self.lon,
                        agent.toa_do["vi_do"],
                        agent.toa_do["kinh_do"]
                    ) <= 1.0
                ),
                None
            )
            self.direct_los = True
            self.relay_required = False
            self.current_link = "GROUND_LINK"
            self.relay_target_id = None
            self.tuyen_ve_tram_cuc_bo = (
                [self.ma_dinh_danh, tram.ma_dinh_danh]
                if tram else [self.ma_dinh_danh]
            )
            self.chi_phi_ve_tram_cuc_bo = 0.0
            self.nang_luong_ve_tram_cuc_bo = 0.0
            self.do_tre_ve_tram_cuc_bo_ms = 0.0
            self.dung_luong_ve_tram_cuc_bo_mbps = float("inf")
            return
        cac_tram = [
            agent for agent in self.bo_dinh_tuyen.danh_sach_tac_nhan.values()
            if agent.loai_tac_nhan == "TRẠM_MẶT_ĐẤT"
        ]
        if not cac_tram:
            self.direct_los = False
            self.relay_required = True
            self.current_link = "OUT_OF_LOS"
            self.relay_target_id = None
            return
        tram = min(
            cac_tram,
            key=lambda station: self._khoang_cach_m(
                self.lat,
                self.lon,
                station.toa_do["vi_do"],
                station.toa_do["kinh_do"]
            )
        )
        self.evaluate_communication_link(
            float(tram.toa_do["vi_do"]),
            float(tram.toa_do["kinh_do"])
        )

    def _dang_o_tai_tram(self) -> bool:
        tac_nhan_map = getattr(self.bo_dinh_tuyen, "danh_sach_tac_nhan", {}) or {}
        return any(
            getattr(agent, "loai_tac_nhan", None) == "TRẠM_MẶT_ĐẤT"
            and self._khoang_cach_m(
                self.toa_do["vi_do"],
                self.toa_do["kinh_do"],
                agent.toa_do["vi_do"],
                agent.toa_do["kinh_do"]
            ) <= 1.0
            for agent in tac_nhan_map.values()
        )

    def evaluate_communication_link(self, tram_lat: Optional[float] = None, tram_lon: Optional[float] = None) -> str:
        """
        Đánh giá và lựa chọn đường truyền tối ưu về Trạm mặt đất:
        - GROUND_LINK: Nếu chặng kế tiếp là Trạm mặt đất trực tiếp (A2G LoS <= 4.2 km).
        - FANET_RELAY: Nếu chặng kế tiếp là một UAV khác làm trung chuyển.
        - LEO_SATELLITE: Nếu chặng kế tiếp là vệ tinh LEO.
        - OUT_OF_LOS: Chưa có tuyến khả dụng trong bảng láng giềng cục bộ.
        """
        tuyen_dijkstra = self.bo_dinh_tuyen.tim_tuyen_den_tram_mat_dat(
            self.ma_dinh_danh
        )
        if tuyen_dijkstra and len(tuyen_dijkstra.get("nodes", [])) >= 2:
            ma_ke_tiep = tuyen_dijkstra["nodes"][1]
            tac_nhan_ke_tiep = self.bo_dinh_tuyen.lay_tac_nhan(ma_ke_tiep)
            loai_ke_tiep = getattr(tac_nhan_ke_tiep, "loai_tac_nhan", "")

            if loai_ke_tiep == "TRẠM_MẶT_ĐẤT":
                self._giai_phong_uav_chuyen_tiep()
                self.direct_los = True
                self.relay_required = False
                self.current_link = "GROUND_LINK"
                self.relay_target_id = None
                return self.current_link

            elif loai_ke_tiep == "MÁY_BAY":
                self.direct_los = False
                self.relay_required = True
                self.current_link = "FANET_RELAY"
                if self._relay_uav_id != ma_ke_tiep:
                    self._giai_phong_uav_chuyen_tiep()
                if hasattr(tac_nhan_ke_tiep, "_relay_requesters"):
                    tac_nhan_ke_tiep._relay_requesters.add(self.ma_dinh_danh)
                if getattr(tac_nhan_ke_tiep, "trang_thai", None) == AgentState.PATROLLING:
                    tac_nhan_ke_tiep.chuyen_trang_thai(AgentState.RELAYING)
                self._relay_uav_id = ma_ke_tiep
                self.relay_target_id = ma_ke_tiep
                return self.current_link

            elif "VỆ_TINH" in loai_ke_tiep:
                self._giai_phong_uav_chuyen_tiep()
                self.direct_los = False
                self.relay_required = True
                self.current_link = "LEO_SATELLITE"
                self.relay_target_id = ma_ke_tiep
                return self.current_link

        # Không gán nhãn LEO nếu bảng định tuyến chưa có tuyến qua vệ tinh.
        self._giai_phong_uav_chuyen_tiep()
        self.direct_los = False
        self.relay_required = True
        self.current_link = "OUT_OF_LOS"
        self.relay_target_id = None
        return self.current_link

    def _uav_neighbors(self) -> list:
        return [
            agent
            for agent in self.bo_dinh_tuyen.danh_sach_tac_nhan.values()
            if agent is not self
            and agent.loai_tac_nhan == "MÁY_BAY"
            and self._khoang_cach_m(
                self.lat,
                self.lon,
                agent.toa_do["vi_do"],
                agent.toa_do["kinh_do"]
            ) < 5000.0
        ]

    def find_best_relay(self, neighbors: list):
        eligible = [
            neighbor for neighbor in neighbors
            if neighbor.trang_thai != AgentState.OFFLINE
            and getattr(neighbor, "direct_los", False)
            and getattr(neighbor, "uav_state", None) in (
                UAVState.PATROLLING,
                UAVState.RELAYING
            )
        ]
        if not eligible:
            return None
        return min(
            eligible,
            key=lambda neighbor: self._khoang_cach_m(
                self.lat,
                self.lon,
                neighbor.toa_do["vi_do"],
                neighbor.toa_do["kinh_do"]
            )
        )

    @staticmethod
    def _khoang_cach_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        earth_radius_m = 6_371_000.0
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = lat2_rad - lat1_rad
        delta_lon = math.radians(lon2 - lon1)
        haversine = (
            math.sin(delta_lat / 2.0) ** 2
            + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2.0) ** 2
        )
        return earth_radius_m * 2.0 * math.atan2(
            math.sqrt(haversine),
            math.sqrt(1.0 - haversine)
        )

    def _giai_phong_uav_chuyen_tiep(self):
        if not self._relay_uav_id:
            return
        relay_uav = self.bo_dinh_tuyen.lay_tac_nhan(self._relay_uav_id)
        if relay_uav:
            relay_uav._relay_requesters.discard(self.ma_dinh_danh)
            if (
                not relay_uav._relay_requesters
                and relay_uav.uav_state == UAVState.RELAYING
            ):
                relay_uav.chuyen_trang_thai(AgentState.PATROLLING)
        self._relay_uav_id = None

    async def xu_ly_thong_diep(self, thong_diep: dict):
        loai_tin = thong_diep.get("loai_tin")
        nguoi_gui = thong_diep.get("nguoi_gui")

        if loai_tin == "HELLO_BEACON":
            return

        if loai_tin == "YEU_CAU_HO_TRO_CHAY_P2P":
            # Nhận tin nhắn P2P từ UAV đồng đội cùng phân khu bị yếu pin nhờ xử lý đám cháy thay
            if (
                thong_diep.get("zone_id") != self.zone_id
                or (self._tac_vu_su_co and not self._tac_vu_su_co.done())
                or self.trang_thai in (AgentState.OFFLINE, AgentState.LOW_BATTERY_RTB, AgentState.RETURNING, AgentState.CHARGING)
            ):
                return
            fire_lat = float(thong_diep["vi_do"])
            fire_lon = float(thong_diep["kinh_do"])
            if hasattr(self.bo_dinh_tuyen, "kiem_tra_muc_tieu_da_khoa"):
                if self.bo_dinh_tuyen.kiem_tra_muc_tieu_da_khoa(fire_lat, fire_lon):
                    return
            pin_du_kien_pct = self._tinh_pin_du_kien_khu_hoi_pct(fire_lat, fire_lon)
            if self.phan_tram_pin > LOW_BATTERY_THRESHOLD and pin_du_kien_pct > LOW_BATTERY_THRESHOLD:
                await self._kich_hoat_xu_ly_chay_tu_chu(
                    fire_lat,
                    fire_lon,
                    pin_du_kien_pct=pin_du_kien_pct,
                    ma_phien=thong_diep.get("ma_phien_dau_gia"),
                    la_p2p=True,
                    uav_phat_hien_goc=thong_diep.get("nguoi_nho") or nguoi_gui,
                )
            return

        elif loai_tin == "PHAT_HIEN_SU_CO":
            if self._tac_vu_su_co and not self._tac_vu_su_co.done():
                return
            self.toa_do_muc_tieu = {
                "vi_do": float(thong_diep["vi_do"]),
                "kinh_do": float(thong_diep["kinh_do"]),
                "do_cao": self.toa_do["do_cao"],
                "van_toc_m_s": self.toa_do.get("van_toc_m_s") or 45.0
            }
            self._den_muc_tieu = asyncio.Event()
            if self.bo_dinh_tuyen.physics.tinh_khoang_cach_3d(self.toa_do, self.toa_do_muc_tieu) <= 50.0:
                self._den_muc_tieu.set()
            self.so_nhiem_vu_da_nhan += 1
            self.chuyen_trang_thai(AgentState.BIDDING)
            self._tac_vu_su_co = asyncio.create_task(self._thuc_hien_nhiem_vu(thong_diep.copy()))

        elif loai_tin == "YEU_CAU_BAN_GIAO_VUNG":
            # Tự trị nhận bàn giao vùng từ đồng đội cùng phân khu đang yếu pin
            if (
                thong_diep.get("zone_id") == self.zone_id
                and nguoi_gui
                and nguoi_gui != self.ma_dinh_danh
                and self.trang_thai in (AgentState.PATROLLING, AgentState.RELAYING)
                and self.phan_tram_pin >= 45.0
                and (not self._tac_vu_su_co or self._tac_vu_su_co.done())
            ):
                if nguoi_gui not in self._vung_tuan_tra_phu:
                    self.nhan_vung_tuan_tra(nguoi_gui, thong_diep.get("zone_id") or self.zone_id)

        elif loai_tin == "THONG_BAO_KHOI_PHUC_VUNG":
            if nguoi_gui and nguoi_gui in self._vung_tuan_tra_phu:
                self.tra_lai_vung_tuan_tra(nguoi_gui)

        elif loai_tin == "GIAO_NHIEM_VU_DU_LIEU":
            await self._chuyen_tiep_du_lieu(thong_diep)

        elif loai_tin == "XAC_NHAN_ACK":
            tuyen_phan_hoi = list(thong_diep.get("tuyen_phan_hoi", []))
            if self.ma_dinh_danh in tuyen_phan_hoi:
                vi_tri = tuyen_phan_hoi.index(self.ma_dinh_danh)
                if vi_tri + 1 < len(tuyen_phan_hoi):
                    await self.bo_dinh_tuyen.gui_don_huong(
                        self.ma_dinh_danh,
                        tuyen_phan_hoi[vi_tri + 1],
                        thong_diep
                    )

    async def _bao_huy_nhiem_vu(self, su_co: dict, ly_do: str):
        """Giải phóng khóa mục tiêu và nhờ đồng đội cùng phân khu qua P2P khi UAV gặp sự cố giữa đường."""
        if hasattr(self.bo_dinh_tuyen, "mo_khoa_muc_tieu_chay"):
            self.bo_dinh_tuyen.mo_khoa_muc_tieu_chay(self.ma_dinh_danh, ha_nhiet=False)
        await self.bo_dinh_tuyen.gui_log_ra_web(
            "WARN", f"⚠️ [{self.ma_dinh_danh}] HUỶ NHIỆM VỤ #{su_co.get('ma_phien_dau_gia', '')[:8]}: {ly_do}."
        )
        if su_co.get("vi_do") is not None and su_co.get("kinh_do") is not None:
            await self._phat_tin_nhan_p2p_nho_dong_doi(
                float(su_co["vi_do"]),
                float(su_co["kinh_do"]),
                self.phan_tram_pin,
            )

    async def _thuc_hien_nhiem_vu(self, su_co: dict):
        try:
            toa_do_muc_tieu = self.toa_do_muc_tieu
            if toa_do_muc_tieu is None:
                raise RuntimeError("Không có tọa độ mục tiêu cho nhiệm vụ UAV.")
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "EVENT", f"[{self.ma_dinh_danh}] Đang cơ động tới điểm cháy ({toa_do_muc_tieu['vi_do']:.4f}, {toa_do_muc_tieu['kinh_do']:.4f})."
            )
            khoang_cach = self.bo_dinh_tuyen.physics.tinh_khoang_cach_3d(
                self.toa_do, toa_do_muc_tieu
            )
            van_toc = max(1.0, float(toa_do_muc_tieu.get("van_toc_m_s", 45.0)))
            timeout = min(1800.0, max(60.0, khoang_cach / van_toc * 2.0 + 30.0))
            deadline = time.monotonic() + timeout
            den_muc_tieu_ev = self._get_den_muc_tieu_event()
            while not den_muc_tieu_ev.is_set():
                if self.trang_thai in (AgentState.OFFLINE, AgentState.LOW_BATTERY) or self.phan_tram_pin <= 5.0:
                    await self._bao_huy_nhiem_vu(su_co, "Pin sập hoặc UAV chuyển OFFLINE")
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    await self._bao_huy_nhiem_vu(su_co, "Timeout không tới được tâm cháy")
                    raise asyncio.TimeoutError
                try:
                    await asyncio.wait_for(den_muc_tieu_ev.wait(), timeout=min(1.0, remaining))
                except asyncio.TimeoutError:
                    continue
            if self.trang_thai in (AgentState.OFFLINE, AgentState.LOW_BATTERY) or self.phan_tram_pin <= 5.0:
                await self._bao_huy_nhiem_vu(su_co, "Pin sập hoặc UAV chuyển OFFLINE")
                return
            await self._dau_gia_va_truyen(su_co)
            self._bat_dau_khoanh_vung_su_co(su_co)
        except asyncio.TimeoutError:
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "WARN", f"[{self.ma_dinh_danh}] Không tới được vùng nhiệm vụ trong thời hạn mô phỏng."
            )
        except Exception as error:
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "WARN", f"[{self.ma_dinh_danh}] Nhiệm vụ lỗi: {error}"
            )
        finally:
            self.toa_do_muc_tieu = None
            if self.trang_thai not in (AgentState.OFFLINE, AgentState.LOW_BATTERY):
                self.chuyen_trang_thai(AgentState.PATROLLING)
            self._tac_vu_su_co = None

    def _bat_dau_khoanh_vung_su_co(self, su_co: dict):
        """Thiết lập khoanh vùng bán kính 130m sát quanh đám cháy, sau đó tự động quay về tuần tra trường lực."""
        self._tam_khoanh_vung_chay = {
            "vi_do": float(su_co["vi_do"]),
            "kinh_do": float(su_co["kinh_do"]),
        }
        self._goc_khoanh_vung_rad = 0.0
        self._thoi_gian_khoanh_vung_con_lai = 20.0

    def _goi_uav_du_bi_thay_ca(self) -> bool:
        """
        Gọi UAV dự bị của phân khu đang đỗ tại Trạm xuất kích ra vùng trước (Pre-Dispatch Hot-Swap).
        Trả về True nếu đã có tiếp viện đang bay ra hoặc vừa điều động thành công.
        """
        if not self.zone_id or self._dang_o_tai_tram():
            return False
        if self._waiting_for_relief_id:
            return True

        uavs_cung_vung = [
            u for u in self.bo_dinh_tuyen.danh_sach_tac_nhan.values()
            if u.loai_tac_nhan == "MÁY_BAY" and getattr(u, "zone_id", None) == self.zone_id
        ]
        if len(uavs_cung_vung) < 4:
            return False

        # Khóa luân phiên 1-by-1: nếu trong vùng đã có 1 chiếc đang chờ tiếp viện hoặc đang về sạc
        dang_luan_phien = any(
            u.ma_dinh_danh != self.ma_dinh_danh
            and (
                getattr(u, "_waiting_for_relief_id", None)
                or getattr(u, "_relieving_target_id", None)
                or u.trang_thai in (AgentState.RETURNING, AgentState.LOW_BATTERY_RTB, AgentState.CHARGING)
            )
            for u in uavs_cung_vung
        )
        if dang_luan_phien:
            return True

        u_reserve = next(
            (
                u for u in uavs_cung_vung
                if u.ma_dinh_danh != self.ma_dinh_danh
                and u._dang_o_tai_tram()
                and u.trang_thai in (AgentState.IDLE, AgentState.PATROLLING)
                and u.phan_tram_pin >= 80.0
            ),
            None
        )
        if not u_reserve:
            return False

        u_reserve.toa_do["do_cao"] = max(1000.0, float(self.toa_do.get("do_cao", 1000.0)))
        u_reserve.toa_do["van_toc_m_s"] = 45.0
        u_reserve.toa_do_tram_sac = None
        u_reserve._relieving_target_id = self.ma_dinh_danh
        u_reserve._da_ban_giao_tai_cho = False
        u_reserve.chuyen_trang_thai(AgentState.PATROLLING)
        self._waiting_for_relief_id = u_reserve.ma_dinh_danh
        return True

    async def _chon_tram_sac(self):
        if self._dang_o_tai_tram():
            return
        ung_vien = []
        for agent in self.bo_dinh_tuyen.danh_sach_tac_nhan.values():
            if agent.loai_tac_nhan != "TRẠM_MẶT_ĐẤT":
                continue
            distance = self.bo_dinh_tuyen.physics.tinh_khoang_cach_3d(self.toa_do, agent.toa_do)
            speed = max(1.0, float(self.toa_do.get("van_toc_m_s") or 45.0))
            energy_to_base = distance / speed * self.cong_suat_bay_w
            if self.pin_hien_tai < energy_to_base + 500.0:
                continue
            ung_vien.append((distance, energy_to_base, agent))
        if not ung_vien:
            return
        distance, energy_to_base, tram = min(ung_vien, key=lambda item: item[0])
        nguong_quay_ve = max(
            self.pin_toi_da * 0.4,
            energy_to_base * 1.3 + 2500.0,
            energy_to_base
            + self.pin_toi_da * (self.nguong_pin_yeu_pct / 100.0)
            + 1000.0
        )
        if self.trang_thai != AgentState.LOW_BATTERY_RTB and self.pin_hien_tai > nguong_quay_ve:
            return

        # Cơ chế In-Zone Hot-Swap cho phân khu 4 UAV:
        # Người mới (UAV dự bị) bay tới vùng thì người cũ mới rời vùng về Trạm sạc.
        if self.zone_id and not self._da_ban_giao_tai_cho:
            if self._goi_uav_du_bi_thay_ca():
                # Giữ mức năng lượng dự trữ an toàn trong lúc bay chờ UAV dự bị tới tiếp quản tại chỗ
                san_pin_cho = max(energy_to_base * 1.35 + 2500.0, self.pin_toi_da * 0.26)
                if self.pin_hien_tai < san_pin_cho:
                    self.pin_hien_tai = san_pin_cho
                return

        self.toa_do_tram_sac = {
            **tram.toa_do,
            "van_toc_m_s": max(20.0, float(self.toa_do.get("van_toc_m_s") or 45.0))
        }
        if self.trang_thai != AgentState.LOW_BATTERY_RTB:
            self.chuyen_trang_thai(AgentState.RETURNING)
        await self.bo_dinh_tuyen.gui_log_ra_web(
            "WARN",
            f"[{self.ma_dinh_danh}] Pin {self.phan_tram_pin:.1f}%: quay về "
            f"{tram.ma_dinh_danh} ({distance / 1000.0:.1f} km, "
            f"dự kiến {energy_to_base:.0f} J) để sạc."
        )

    async def _phat_yeu_cau_ban_giao_vung(self):
        if self._yeu_cau_ban_giao_da_phat or not self.zone_id:
            return
        cac_uav_cung_vung = [
            agent.ma_dinh_danh
            for agent in self.bo_dinh_tuyen.danh_sach_tac_nhan.values()
            if agent.ma_dinh_danh != self.ma_dinh_danh
            and agent.loai_tac_nhan == "MÁY_BAY"
            and getattr(agent, "zone_id", None) == self.zone_id
        ]
        if not cac_uav_cung_vung:
            return
        self._yeu_cau_ban_giao_da_phat = True
        await self._phat_lenh_quang_ba({
            "loai_tin": "YEU_CAU_BAN_GIAO_VUNG",
            "zone_id": self.zone_id,
            "nguoi_gui": self.ma_dinh_danh,
            "pin_pct": self.phan_tram_pin,
        }, set(cac_uav_cung_vung))

    async def _phat_thong_bao_khoi_phuc_vung(self):
        if not self.zone_id:
            return
        cac_uav_cung_vung = [
            agent.ma_dinh_danh
            for agent in self.bo_dinh_tuyen.danh_sach_tac_nhan.values()
            if agent.ma_dinh_danh != self.ma_dinh_danh
            and agent.loai_tac_nhan == "MÁY_BAY"
            and getattr(agent, "zone_id", None) == self.zone_id
        ]
        if cac_uav_cung_vung:
            await self._phat_lenh_quang_ba({
                "loai_tin": "THONG_BAO_KHOI_PHUC_VUNG",
                "zone_id": self.zone_id,
                "nguoi_gui": self.ma_dinh_danh
            }, set(cac_uav_cung_vung))

    async def _phat_lenh_quang_ba(self, thong_diep: dict, recipient_ids: set[str]):
        await self.bo_dinh_tuyen.phat_song_quang_ba(
            self.ma_dinh_danh,
            thong_diep,
            danh_sach_nguoi_nhan=recipient_ids
        )

    def nhan_vung_tuan_tra(self, ma_uav_nguon: str, thong_tin_vung=None):
        if ma_uav_nguon in self._vung_tuan_tra_phu:
            return
        ma_vung = thong_tin_vung if isinstance(thong_tin_vung, str) else (self.zone_id or "ZONE_01")
        self._vung_tuan_tra_phu[ma_uav_nguon] = str(ma_vung)

    def tra_lai_vung_tuan_tra(self, ma_uav_nguon: str):
        self._vung_tuan_tra_phu.pop(ma_uav_nguon, None)

    async def _dau_gia_va_truyen(self, su_co: dict):
        """
        Định tuyến gói tin báo cáo đám cháy từ tâm cháy về Trạm mặt đất qua thuật toán
        Dijkstra đa mục tiêu (`core/message_broker.py`: A2G trực tiếp, FANET Relay, hoặc Vệ tinh LEO).
        """
        kich_thuoc_kb = float(su_co.get("kich_thuoc_kb", 250.0))
        fire_lat = float(su_co["vi_do"])
        fire_lon = float(su_co["kinh_do"])
        goi_tin = {
            "loai_tin": "GIAO_NHIEM_VU_DU_LIEU",
            "ma_phien_dau_gia": su_co.get("ma_phien_dau_gia", str(uuid.uuid4())),
            "loai_su_co": su_co.get("loai_su_co", "CHÁY_RỪNG"),
            "toa_do_su_co": {"vi_do": fire_lat, "kinh_do": fire_lon},
            "thoi_diem_khoi_tao": time.time(),
            "kich_thuoc_kb": kich_thuoc_kb
        }
        tuyen_dijkstra = None
        if hasattr(self.bo_dinh_tuyen, "tim_tuyen_dijkstra_ve_tram"):
            tuyen_dijkstra = self.bo_dinh_tuyen.tim_tuyen_dijkstra_ve_tram(
                self.ma_dinh_danh, kich_thuoc_kb=kich_thuoc_kb
            )
        tuyen = await self.bo_dinh_tuyen.gui_chang_tiep_ve_tram(
            self.ma_dinh_danh,
            goi_tin,
            [self.ma_dinh_danh]
        )
        gcs_server = getattr(self.bo_dinh_tuyen, "ground_station_server", None)
        if tuyen:
            nodes_path = list(tuyen.get("nodes", []))
            delay_ms = float(tuyen.get("delay_ms", 0.0))
            if gcs_server and hasattr(gcs_server, "ghi_nhan_bao_cao_dijkstra"):
                await gcs_server.ghi_nhan_bao_cao_dijkstra(
                    fire_lat, fire_lon, self.ma_dinh_danh, nodes_path, do_tre_ms=delay_ms
                )
            else:
                chuoi_tuyen = " ➔ ".join(nodes_path)
                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "ROUTE",
                    f"[DIJKSTRA_REPORT] [{self.ma_dinh_danh}] gửi báo cáo cháy qua định tuyến Dijkstra "
                    f"({len(nodes_path) - 1} hops: {chuoi_tuyen})."
                )
        elif tuyen_dijkstra and len(tuyen_dijkstra.get("nodes", [])) >= 2:
            nodes_path = list(tuyen_dijkstra["nodes"])
            ma_ke_tiep = nodes_path[1]
            await self.bo_dinh_tuyen.gui_don_huong(
                self.ma_dinh_danh,
                ma_ke_tiep,
                {
                    **goi_tin,
                    "lo_trinh_di_qua": [self.ma_dinh_danh, ma_ke_tiep],
                    "tuyen_du_kien": nodes_path
                }
            )
            if gcs_server and hasattr(gcs_server, "ghi_nhan_bao_cao_dijkstra"):
                await gcs_server.ghi_nhan_bao_cao_dijkstra(
                    fire_lat, fire_lon, self.ma_dinh_danh, nodes_path, do_tre_ms=float(tuyen_dijkstra.get("delay_ms", 0.0))
                )

    async def _chuyen_tiep_du_lieu(self, thong_diep: dict):
        tuyen = await self.bo_dinh_tuyen.gui_chang_tiep_ve_tram(
            self.ma_dinh_danh,
            thong_diep
        )
        if not tuyen:
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "WARN", f"[{self.ma_dinh_danh}] Không còn tuyến relay khả dụng tới trạm mặt đất."
            )
            return

        self.thong_ke["goi_tin_chuyen_tiep"] += 1
        await self.bo_dinh_tuyen.gui_log_ra_web(
            "RELAY", f"📡 [{self.ma_dinh_danh}] Chuyển tiếp FANET tới {tuyen['nodes'][1]}."
        )
