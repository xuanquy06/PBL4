"""UAV patrol, energy-aware scheduling, and CNP mission behavior."""
import asyncio
import math
import time
import uuid
from enum import Enum
from typing import Dict, List, Optional
from core.base_agent import AgentState, BaseAgent

LOW_BATTERY_THRESHOLD = 25.0


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
        waypoints: Optional[List[dict]] = None,
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
        self.waypoints = [dict(wp) for wp in (waypoints or [])]
        self.waypoints_goc = [dict(wp) for wp in self.waypoints]
        self.waypoint_index = 0
        self.cong_suat_bay_w = max(1.0, float(cong_suat_bay_w))
        self.zone_id = zone_id
        self.nguong_pin_yeu_pct = LOW_BATTERY_THRESHOLD
        self.danh_sach_bao_gia: Dict[str, list[dict]] = {}
        self.so_nhiem_vu_da_nhan = 0
        self.toa_do_muc_tieu: Optional[dict] = None
        self.toa_do_tram_sac: Optional[dict] = None
        self._vung_tuan_tra_phu: Dict[str, list[dict]] = {}
        self._so_diem_tuan_tra_su_co_con_lai = 0
        self._den_muc_tieu: Optional[asyncio.Event] = None
        self._tac_vu_su_co: Optional[asyncio.Task] = None
        self._yeu_cau_ban_giao_da_phat = False
        self._waiting_for_relief_id: Optional[str] = None
        self._relieving_target_id: Optional[str] = None
        self._da_ban_giao_tai_cho = False
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
        ho_so["so_nhiem_vu"] = self.so_nhiem_vu_da_nhan
        ho_so["vung_ban_giao_nhan"] = list(self._vung_tuan_tra_phu.keys())
        if self.toa_do_muc_tieu:
            ho_so["target_lat"] = round(float(self.toa_do_muc_tieu["vi_do"]), 5)
            ho_so["target_lng"] = round(float(self.toa_do_muc_tieu["kinh_do"]), 5)
        return ho_so

    def _khoi_phuc_quy_dao_tuan_tra_goc(self):
        """Khôi phục lộ trình tuần tra phân khu gốc cộng với các vùng đang nhận bàn giao hộ."""
        if not self.waypoints_goc:
            return
        self._so_diem_tuan_tra_su_co_con_lai = 0
        lo_trinh_moi = [dict(wp) for wp in self.waypoints_goc]
        for vung_phu in self._vung_tuan_tra_phu.values():
            lo_trinh_moi.extend(vung_phu)
        self.waypoints = lo_trinh_moi
        self.waypoint_index = self.waypoint_index % max(1, len(self.waypoints))

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
            if self.waypoints_goc:
                self._khoi_phuc_quy_dao_tuan_tra_goc()
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
                    self._khoi_phuc_quy_dao_tuan_tra_goc()
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
                if not self._yeu_cau_ban_giao_da_phat:
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

        if self.toa_do_muc_tieu:
            waypoint = self.toa_do_muc_tieu
        elif self.trang_thai in (
            AgentState.LOW_BATTERY_RTB,
            AgentState.RETURNING
        ) and self.toa_do_tram_sac:
            waypoint = self.toa_do_tram_sac
        elif self.waypoints:
            waypoint = self.waypoints[self.waypoint_index]
        else:
            return

        van_toc_m_s = max(0.0, float(waypoint.get("van_toc_m_s", 0.0)))
        if van_toc_m_s == 0.0:
            return

        khoang_cach_m = self.bo_dinh_tuyen.physics.tinh_khoang_cach_3d(self.toa_do, waypoint)
        if khoang_cach_m <= 1.0:
            if self.trang_thai in (AgentState.RETURNING, AgentState.LOW_BATTERY_RTB):
                self.toa_do.update(waypoint)
                self.toa_do_tram_sac = None
                self.chuyen_trang_thai(AgentState.CHARGING)
            elif self.toa_do_muc_tieu:
                self._get_den_muc_tieu_event().set()
            elif self.waypoints:
                self._chuyen_sang_waypoint_ke_tiep()
            return

        quang_duong_m = min(khoang_cach_m, van_toc_m_s * dt_giay)
        ti_le_di_chuyen = quang_duong_m / khoang_cach_m
        for truong in ("vi_do", "kinh_do", "do_cao"):
            hien_tai = self.toa_do[truong]
            self.toa_do[truong] = hien_tai + (float(waypoint[truong]) - hien_tai) * ti_le_di_chuyen
        self.toa_do["van_toc_m_s"] = van_toc_m_s
        self.tieu_hao_nang_luong(self.cong_suat_bay_w * dt_giay)

        if ti_le_di_chuyen >= 1.0:
            if self.trang_thai in (AgentState.RETURNING, AgentState.LOW_BATTERY_RTB):
                self.toa_do.update(waypoint)
                self.toa_do_tram_sac = None
                self.chuyen_trang_thai(AgentState.CHARGING)
            elif self.toa_do_muc_tieu:
                self._get_den_muc_tieu_event().set()
            elif self.waypoints:
                self._chuyen_sang_waypoint_ke_tiep()

    def _chuyen_sang_waypoint_ke_tiep(self):
        """Di chuyển sang waypoint kế tiếp; nếu đã hoàn tất vòng quét đám cháy thì quay về phân khu gốc."""
        if not self.waypoints:
            return
        if self._so_diem_tuan_tra_su_co_con_lai > 0:
            self._so_diem_tuan_tra_su_co_con_lai -= 1
            if self._so_diem_tuan_tra_su_co_con_lai <= 0 and self.waypoints_goc:
                self._khoi_phuc_quy_dao_tuan_tra_goc()
                return
        self.waypoint_index = (self.waypoint_index + 1) % len(self.waypoints)

    async def _cap_nhat_duong_truyen(self):
        if self._dang_o_tai_tram():
            tram = next(
                (
                    agent for agent in self.bo_dinh_tuyen.danh_sach_tac_nhan.values()
                    if agent.loai_tac_nhan == "TRẠM_MẶT_ĐẤT"
                    and self._khoang_cach_m(
                        self.toa_do["vi_do"],
                        self.toa_do["kinh_do"],
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
                self.toa_do["vi_do"],
                self.toa_do["kinh_do"],
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
                self.toa_do["vi_do"],
                self.toa_do["kinh_do"],
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
                self.toa_do["vi_do"],
                self.toa_do["kinh_do"],
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

        if loai_tin == "CFP_FIRE_EVENT":
            # Xử lý kêu gọi thầu sự cố cháy rừng qua FIPA-CNP
            trang_thai_uav = getattr(self, "uav_state", None)
            if (
                trang_thai_uav not in (UAVState.PATROLLING, UAVState.STANDBY, UAVState.RELAYING)
                and self.trang_thai not in (AgentState.PATROLLING, AgentState.IDLE, AgentState.RELAYING)
            ):
                return
            if self._tac_vu_su_co and not self._tac_vu_su_co.done():
                return

            tam_chan = {
                "vi_do": float(thong_diep["vi_do"]),
                "kinh_do": float(thong_diep["kinh_do"]),
                "do_cao": float(self.toa_do.get("do_cao", 1000.0))
            }
            # 1. Khoảng cách 3D đến điểm cháy
            kc_m = self.bo_dinh_tuyen.physics.tinh_khoang_cach_3d(self.toa_do, tam_chan)
            van_toc = max(1.0, float(self.toa_do.get("van_toc_m_s") or 45.0))

            # 2. Mức hao hụt pin dự kiến sau chuyến bay khứ hồi (2 chiều)
            thoi_gian_bay_khu_hoi_s = (2.0 * kc_m) / van_toc
            nang_luong_tieu_hao_j = thoi_gian_bay_khu_hoi_s * self.cong_suat_bay_w
            pin_con_lai_j = self.pin_hien_tai - nang_luong_tieu_hao_j
            pin_du_kien_pct = (pin_con_lai_j / self.pin_toi_da) * 100.0

            gcs_server = getattr(self.bo_dinh_tuyen, "ground_station_server", None)

            # 3. Ràng buộc cứng: Pin dự kiến sau khứ hồi phải > 25%
            if pin_du_kien_pct > LOW_BATTERY_THRESHOLD:
                # Hàm chi phí đa mục tiêu: Cự ly + Mức hao hụt pin + Cân bằng tải số nhiệm vụ + Trọng số đang gánh vùng phụ
                # Ưu tiên tuyệt đối UAV nội vùng: Phạt 12,000 nếu UAV từ phân khu khác
                fire_zone = thong_diep.get("zone_id")
                phat_khac_vung = 0.0 if (not fire_zone or self.zone_id == fire_zone) else 12000.0
                he_so_ganh_vung = len(self._vung_tuan_tra_phu) * 1500.0
                chi_phi = round(
                    kc_m
                    + (100.0 - self.phan_tram_pin) * 40.0
                    + self.so_nhiem_vu_da_nhan * 3000.0
                    + he_so_ganh_vung
                    + phat_khac_vung,
                    2
                )
                propose_msg = {
                    "loai_tin": "PROPOSE",
                    "type": "PROPOSE",
                    "ma_phien_dau_gia": thong_diep.get("ma_phien_dau_gia"),
                    "uav_id": self.ma_dinh_danh,
                    "zone_id": self.zone_id,
                    "nguoi_gui": self.ma_dinh_danh,
                    "chi_phi": chi_phi,
                    "khoang_cach_m": round(kc_m, 2),
                    "pin_hien_tai_pct": round(self.phan_tram_pin, 2),
                    "pin_du_kien_pct": round(pin_du_kien_pct, 2),
                    "current_link": self.current_link,
                    "toa_do": self.toa_do,
                    "thoi_diem_gui": time.time(),
                    "kich_thuoc_kb": 2.0
                }

                # Chuyển gói PROPOSE qua tuyến mạng SAGSINS về Trạm mặt đất
                da_chuyen_qua_mang = False
                if self.tuyen_ve_tram_cuc_bo and len(self.tuyen_ve_tram_cuc_bo) >= 2:
                    hop_ke_tiep = self.tuyen_ve_tram_cuc_bo[1]
                    da_chuyen_qua_mang = await self.bo_dinh_tuyen.gui_don_huong(
                        self.ma_dinh_danh,
                        hop_ke_tiep,
                        {
                            **propose_msg,
                            "lo_trinh_di_qua": [self.ma_dinh_danh],
                            "tuyen_du_kien": list(self.tuyen_ve_tram_cuc_bo)
                        }
                    )
                else:
                    for agent in self.bo_dinh_tuyen.danh_sach_tac_nhan.values():
                        if agent.loai_tac_nhan == "TRẠM_MẶT_ĐẤT":
                            if await self.bo_dinh_tuyen.gui_don_huong(self.ma_dinh_danh, agent.ma_dinh_danh, propose_msg):
                                da_chuyen_qua_mang = True

                # Đảm bảo tính tương thích với các bài kiểm thử gọi trực tiếp không chạy vòng lặp lắng nghe
                if gcs_server and (da_chuyen_qua_mang or not any(
                    a.loai_tac_nhan == "TRẠM_MẶT_ĐẤT" for a in self.bo_dinh_tuyen.danh_sach_tac_nhan.values()
                )):
                    gcs_server.nhan_propose(propose_msg)

                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "BID",
                    f"[{self.ma_dinh_danh}] PROPOSE phiên #{thong_diep.get('ma_phien_dau_gia', '')[:8]}: "
                    f"Chi phí={chi_phi}, cự ly={kc_m/1000.0:.2f} km, pin sau khứ hồi={pin_du_kien_pct:.1f}%."
                )
            else:
                refuse_msg = {
                    "loai_tin": "REFUSE",
                    "type": "REFUSE",
                    "ma_phien_dau_gia": thong_diep.get("ma_phien_dau_gia"),
                    "uav_id": self.ma_dinh_danh,
                    "zone_id": self.zone_id,
                    "nguoi_gui": self.ma_dinh_danh,
                    "khoang_cach_m": round(kc_m, 2),
                    "pin_hien_tai_pct": round(self.phan_tram_pin, 2),
                    "pin_du_kien_pct": round(pin_du_kien_pct, 2),
                    "ly_do": f"Pin khứ hồi dự kiến ({pin_du_kien_pct:.1f}%) < ngưỡng an toàn {LOW_BATTERY_THRESHOLD:.0f}%",
                    "thoi_diem_gui": time.time(),
                    "kich_thuoc_kb": 1.5
                }
                if gcs_server:
                    gcs_server.nhan_refuse(refuse_msg)

        elif loai_tin == "ACCEPT_PROPOSAL":
            if thong_diep.get("uav_id") != self.ma_dinh_danh:
                return
            if self._tac_vu_su_co and not self._tac_vu_su_co.done():
                return
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "WIN",
                f"🏆 [{self.ma_dinh_danh}] Nhận lệnh ACCEPT_PROPOSAL phiên #{thong_diep.get('ma_phien_dau_gia', '')[:8]}! "
                f"Chuyển sang RESPONDING bay tới điểm cháy."
            )
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
            self.uav_state = UAVState.RESPONDING
            self._tac_vu_su_co = asyncio.create_task(self._thuc_hien_nhiem_vu(thong_diep.copy()))

        elif loai_tin == "REJECT_PROPOSAL":
            pass

        elif loai_tin == "PHAT_HIEN_SU_CO":
            if self._tac_vu_su_co and not self._tac_vu_su_co.done():
                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "WARN", f"[{self.ma_dinh_danh}] Đang xử lý nhiệm vụ khác; từ chối nhiệm vụ mới."
                )
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

        elif loai_tin == "CFP_GOI_THAU":
            await self._tra_bao_gia(thong_diep, nguoi_gui)

        elif loai_tin == "PROPOSE_BAO_GIA":
            ma_phien = thong_diep.get("ma_phien_dau_gia")
            if ma_phien:
                self.danh_sach_bao_gia.setdefault(ma_phien, []).append(thong_diep)

        elif loai_tin == "REFUSE_BAO_GIA":
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "WARN",
                f"[{self.ma_dinh_danh}] {nguoi_gui} từ chối CNP: "
                f"{thong_diep.get('ly_do', 'không đủ năng lượng')}."
            )

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
                waypoints_ban_giao = thong_diep.get("waypoints") or []
                if waypoints_ban_giao and nguoi_gui not in self._vung_tuan_tra_phu:
                    self.nhan_vung_tuan_tra(nguoi_gui, waypoints_ban_giao)
                    await self.bo_dinh_tuyen.gui_log_ra_web(
                        "INFO",
                        f"🤝 [{self.ma_dinh_danh}] Tự động tiếp quản tuyến tuần tra của "
                        f"{nguoi_gui} tại phân khu {self.zone_id}."
                    )

        elif loai_tin == "THONG_BAO_KHOI_PHUC_VUNG":
            if nguoi_gui and nguoi_gui in self._vung_tuan_tra_phu:
                self.tra_lai_vung_tuan_tra(nguoi_gui)
                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "INFO",
                    f"🔄 [{self.ma_dinh_danh}] Đã hoàn trả tuyến tuần tra phân khu {self.zone_id} cho {nguoi_gui}."
                )

        elif loai_tin in ("GIAO_NHIEM_VU_DU_LIEU", "PROPOSE", "REFUSE"):
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
                else:
                    await self.bo_dinh_tuyen.gui_log_ra_web(
                        "SUCCESS",
                        f"[{self.ma_dinh_danh}] Nhận ACK từ {nguoi_gui}; hoàn tất truyền dữ liệu sự cố."
                    )

    async def _tra_bao_gia(self, thong_diep: dict, ma_nguon: Optional[str]):
        if (
            not ma_nguon
            or ma_nguon == self.ma_dinh_danh
            or self.trang_thai != AgentState.PATROLLING
        ):
            return
        target = thong_diep.get("toa_do_su_co")
        flight_energy = 0.0
        if target:
            target_position = {
                "vi_do": float(target["vi_do"]),
                "kinh_do": float(target["kinh_do"]),
                "do_cao": self.toa_do["do_cao"]
            }
            distance = self.bo_dinh_tuyen.physics.tinh_khoang_cach_3d(
                self.toa_do, target_position
            )
            speed = max(1.0, float(self.toa_do.get("van_toc_m_s") or 45.0))
            flight_energy = distance / speed * self.cong_suat_bay_w
            required_energy = flight_energy * 1.5 + 5000.0
            if self.pin_hien_tai < required_energy:
                refusal = {
                    "loai_tin": "REFUSE_BAO_GIA",
                    "ma_phien_dau_gia": thong_diep.get("ma_phien_dau_gia"),
                    "ly_do": "Không đủ năng lượng dự trữ để bay tới tâm cháy.",
                    "nang_luong_can_thiet_joule": round(required_energy, 2),
                    "nang_luong_con_lai_joule": round(self.pin_hien_tai, 2)
                }
                await self.bo_dinh_tuyen.gui_don_huong(
                    self.ma_dinh_danh, ma_nguon, refusal
                )
                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "WARN",
                    f"[{self.ma_dinh_danh}] REFUSE: cần {required_energy:.0f} J để tới tâm cháy, "
                    f"chỉ còn {self.pin_hien_tai:.0f} J."
                )
                return
        elif self.phan_tram_pin < self.nguong_pin_yeu_pct:
            return
        kich_thuoc_kb = float(thong_diep.get("kich_thuoc_kb", 250.0))
        bao_gia = self.bo_dinh_tuyen.tao_bao_gia(
            self.ma_dinh_danh,
            ma_nguon,
            thong_diep.get("thong_so_kenh", {}),
            kich_thuoc_kb
        )
        if not bao_gia:
            return
        await self.bo_dinh_tuyen.gui_don_huong(
            self.ma_dinh_danh,
            ma_nguon,
            {
                "loai_tin": "PROPOSE_BAO_GIA",
                "ma_phien_dau_gia": thong_diep.get("ma_phien_dau_gia"),
                "chi_phi": bao_gia["chi_phi"],
                "loai_ung_vien": self.loai_tac_nhan,
                "tuyen": bao_gia["tuyen"],
                "nang_luong_du_kien_joule": bao_gia["nang_luong_du_kien_joule"],
                "do_tre_du_kien_ms": bao_gia["do_tre_du_kien_ms"],
                "dung_luong_du_kien_mbps": bao_gia["dung_luong_du_kien_mbps"],
                "pin_pct": self.phan_tram_pin,
                "nang_luong_tiep_can_joule": round(flight_energy, 2),
                "kich_thuoc_kb": 2.0
            }
        )
        await self.bo_dinh_tuyen.gui_log_ra_web(
            "BID", f"[{self.ma_dinh_danh}] Báo giá tuyến {bao_gia['tuyen']} · chi phí {bao_gia['chi_phi']}"
        )

    async def _bao_huy_nhiem_vu(self, su_co: dict, ly_do: str):
        """Thông báo khẩn cấp về trạm khi UAV gặp sự cố giữa đường bay làm nhiệm vụ cháy."""
        await self.bo_dinh_tuyen.gui_log_ra_web(
            "WARN", f"⚠️ [{self.ma_dinh_danh}] HUỶ NHIỆM VỤ #{su_co.get('ma_phien_dau_gia', '')[:8]}: {ly_do}."
        )
        msg = {
            "loai_tin": "MISSION_ABORT",
            "ma_phien_dau_gia": su_co.get("ma_phien_dau_gia"),
            "uav_id": self.ma_dinh_danh,
            "zone_id": self.zone_id,
            "vi_do": su_co.get("vi_do"),
            "kinh_do": su_co.get("kinh_do"),
            "ly_do": ly_do,
            "thoi_gian": time.time(),
        }
        gcs_server = getattr(self.bo_dinh_tuyen, "ground_station_server", None)
        if gcs_server and hasattr(gcs_server, "nhan_mission_abort"):
            await gcs_server.nhan_mission_abort(msg)
        else:
            await self.bo_dinh_tuyen.gui_don_huong(self.ma_dinh_danh, "TRẠM_KIỂM_LÂM_SƠN_TRÀ", msg)

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
            self._bat_dau_tuan_tra_vung_su_co(su_co)
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

    def _bat_dau_tuan_tra_vung_su_co(self, su_co: dict):
        """Thiết lập 1 vòng quét nhỏ (~130m) sát quanh đám cháy, sau đó tự động khôi phục về phân khu gốc."""
        vi_do = float(su_co["vi_do"])
        kinh_do = float(su_co["kinh_do"])
        do_cao = float(self.toa_do["do_cao"])
        delta_lat = 0.0012
        delta_lon = delta_lat / max(0.2, abs(math.cos(math.radians(vi_do))))
        self.waypoints = [
            {"vi_do": vi_do - delta_lat, "kinh_do": kinh_do - delta_lon, "do_cao": do_cao, "van_toc_m_s": 40.0},
            {"vi_do": vi_do - delta_lat, "kinh_do": kinh_do + delta_lon, "do_cao": do_cao, "van_toc_m_s": 40.0},
            {"vi_do": vi_do + delta_lat, "kinh_do": kinh_do + delta_lon, "do_cao": do_cao, "van_toc_m_s": 40.0},
            {"vi_do": vi_do + delta_lat, "kinh_do": kinh_do - delta_lon, "do_cao": do_cao, "van_toc_m_s": 40.0}
        ]
        self.waypoint_index = 0
        self._so_diem_tuan_tra_su_co_con_lai = 4

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

        wps = [dict(wp) for wp in (self.waypoints_goc or self.waypoints)]
        if not wps:
            return False
        for wp in wps:
            wp["van_toc_m_s"] = max(45.0, float(wp.get("van_toc_m_s", 45.0)))
        u_reserve.waypoints_goc = list(wps)
        u_reserve.waypoints = list(wps)
        u_reserve.waypoint_index = self.waypoint_index % len(wps)
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
        waypoints_goc = self.waypoints_goc or self.waypoints
        await self._phat_lenh_quang_ba({
            "loai_tin": "YEU_CAU_BAN_GIAO_VUNG",
            "zone_id": self.zone_id,
            "nguoi_gui": self.ma_dinh_danh,
            "pin_pct": self.phan_tram_pin,
            "waypoints": waypoints_goc
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

    def nhan_vung_tuan_tra(self, ma_uav_nguon: str, waypoints: List[dict]):
        if ma_uav_nguon in self._vung_tuan_tra_phu:
            return
        vung = [{**waypoint, "_chu_so_huu_vung": ma_uav_nguon} for waypoint in waypoints]
        self._vung_tuan_tra_phu[ma_uav_nguon] = vung
        self.waypoints.extend(vung)

    def tra_lai_vung_tuan_tra(self, ma_uav_nguon: str):
        vung = self._vung_tuan_tra_phu.pop(ma_uav_nguon, [])
        if not vung:
            return
        ids_vung = {id(waypoint) for waypoint in vung}
        so_truoc = sum(1 for waypoint in self.waypoints[:self.waypoint_index] if id(waypoint) in ids_vung)
        self.waypoints = [waypoint for waypoint in self.waypoints if id(waypoint) not in ids_vung]
        if not self.waypoints and self.waypoints_goc:
            self.waypoints = [dict(wp) for wp in self.waypoints_goc]
        self.waypoint_index = max(0, min(len(self.waypoints) - 1, self.waypoint_index - so_truoc))

    async def _dau_gia_va_truyen(self, su_co: dict):
        """
        Định tuyến gói tin dữ liệu sự cố từ điểm cháy về Trạm mặt đất qua mạng SAGSINS
        (Trực tiếp A2G, qua UAV FANET Relay, hoặc qua Vệ tinh LEO).
        """
        kich_thuoc_kb = float(su_co.get("kich_thuoc_kb", 250.0))
        goi_tin = {
            "loai_tin": "GIAO_NHIEM_VU_DU_LIEU",
            "ma_phien_dau_gia": su_co.get("ma_phien_dau_gia", str(uuid.uuid4())),
            "loai_su_co": su_co.get("loai_su_co", "CHÁY_RỪNG"),
            "toa_do_su_co": {"vi_do": su_co["vi_do"], "kinh_do": su_co["kinh_do"]},
            "thoi_diem_khoi_tao": time.time(),
            "kich_thuoc_kb": kich_thuoc_kb
        }
        tuyen = await self.bo_dinh_tuyen.gui_chang_tiep_ve_tram(
            self.ma_dinh_danh,
            goi_tin,
            [self.ma_dinh_danh]
        )
        if tuyen:
            ma_ke_tiep = tuyen["nodes"][1]
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "RELAY", f"[{self.ma_dinh_danh}] Đã truyền gói tin sự cố tới [{ma_ke_tiep}]."
            )
        else:
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "WARN", f"[{self.ma_dinh_danh}] Không tìm được tuyến truyền gói tin sự cố tới trạm mặt đất."
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
