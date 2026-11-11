import asyncio
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from environment.physics_engine import PhysicsEngine
from environment.scenario_manager import ScenarioManager
from core.message_broker import MessageBroker
from core.ground_station_server import GroundStationServer
from core.base_agent import AgentState
from agents.ground_station import GroundStationAgent
from agents.satellite_agent import SatelliteAgent
from agents.uav_agent import LOW_BATTERY_THRESHOLD, UAVAgent


class SimulationLoop:
    """
    Bộ điều phối đồng hồ vật lý (Simulation Engine & World Clock):
    - Quản lý khởi tạo kịch bản từ CSV / CelesTrak.
    - Điều khiển bước nhảy thời gian vật lý cho các tác nhân tự trị SAGSINS.
    - Cung cấp giao diện điều khiển tốc độ mô phỏng (Time Scale) và kích hoạt sự cố.
    """
    def __init__(
        self,
        tick_interval_sec: float = 1.0
    ):
        self.tick_interval = tick_interval_sec
        self.time_scale = 1.0
        self.dang_chay = False
        self.tam_dung = False
        self.so_buoc_thoi_gian = 0

        project_data_dir = Path(__file__).resolve().parents[1] / "data"
        self.scenario: Any = ScenarioManager(data_dir=project_data_dir)
        self.physics: Any = PhysicsEngine()
        self.broker: Any = MessageBroker(physics_engine=self.physics, scenario_manager=self.scenario)
        self.ground_station_server: Any = GroundStationServer(self.broker)
        setattr(self.broker, "ground_station_server", self.ground_station_server)
        setattr(self.broker, "simulation_loop", self)
        self._active_fire_missions: Dict[str, dict] = {}

        self._cac_tac_vu_nen: List[asyncio.Task] = []
        self.ban_do_waypoints_uav: Dict[str, List[dict]] = {}
        self._so_nhiem_vu_dieu_phoi = 0
        self._tong_mau_lien_lac = 0
        self._mau_lien_lac_thanh_cong = 0
        self._tong_thoi_gian_mat_lien_lac = 0.0
        self._thoi_gian_mau_lien_lac = 0.0
        self._thoi_gian_mat_lien_lac_theo_uav: Dict[str, float] = {}
        self._mat_lien_lac_lien_tuc_theo_uav: Dict[str, float] = {}
        self._mat_lien_lac_lien_tuc_toi_da = 0.0
        self._trang_thai_lien_lac: Dict[str, bool] = {}
        self._ban_giao_vung_uav: Dict[str, str] = {}
        self._chi_so_lien_lac = {
            "uav_ket_noi": 0,
            "tong_uav": 0,
            "uav_ground_link": 0,
            "uav_fanet_relay": 0,
            "uav_leo_satellite": 0,
            "do_kha_dung_pct": 100.0,
            "thoi_gian_mat_lien_lac_giay": 0.0,
            "mat_lien_lac_lien_tuc_toi_da_giay": 0.0,
            "nang_luong_tieu_thu_joule": 0.0,
            "nhiem_vu_da_dieu_phoi": 0,
            "vung_tuan_tra_ban_giao": 0
        }
        self._thoi_gian_tich_luy_kiem_tra_lien_lac = 0.0
        setattr(self.broker, "thong_ke_lien_tuc", self._chi_so_lien_lac)

    def khoi_tao_kich_ban(self):
        """Đọc dữ liệu từ ScenarioManager và khởi tạo toàn bộ các thực thể mạng"""
        print("\n================ KHỞI TẠO KỊCH BẢN MẠNG SAGSINS ================")

        danh_sach_tle = self.scenario.dong_bo_ve_tinh_celestrak(so_luong_toi_da=3)
        self.physics.nap_danh_sach_tle(danh_sach_tle)

        for sat_cfg in danh_sach_tle:
            ma_sat = sat_cfg["ma_ve_tinh"]
            SatelliteAgent(
                ma_dinh_danh=ma_sat,
                ten_quoc_te=sat_cfg.get("ten_quoc_te", ma_sat),
                cong_suat_dbm=sat_cfg["cong_suat_dbm"],
                tan_so_hz=sat_cfg["tan_so_hz"],
                bang_thong_hz=sat_cfg["bang_thong_hz"],
                bo_dinh_tuyen=self.broker
            )

        danh_sach_nodes, self.ban_do_waypoints_uav = self.scenario.tai_hoac_sinh_kich_ban_mang()

        for node_cfg in danh_sach_nodes:
            loai = node_cfg["loai_tac_nhan"]
            ma_node = node_cfg["ma_dinh_danh"]

            if loai == "TRẠM_MẶT_ĐẤT":
                GroundStationAgent(**node_cfg, bo_dinh_tuyen=self.broker)
            elif loai == "MÁY_BAY":
                waypoints = self.ban_do_waypoints_uav.get(ma_node, [])
                UAVAgent(
                    **node_cfg,
                    waypoints=waypoints,
                    bo_dinh_tuyen=self.broker
                )

        # Khởi tạo trạng thái liên kết ban đầu cho tất cả Vệ tinh và UAV
        for agent in self.broker.danh_sach_tac_nhan.values():
            if isinstance(agent, SatelliteAgent):
                dich, thong_so, loai_kn = agent._tim_duong_xuong_tram_mat_dat()
                agent.relay_target_id = dich
                agent.current_link = "S2G_LINK" if loai_kn == "S2G" else "ISL_LINK"
            elif isinstance(agent, UAVAgent):
                agent.evaluate_communication_link()

        print(f"[SIMULATION] Đã khởi tạo thành công {len(self.broker.danh_sach_tac_nhan)} tác nhân!")
        print("================================================================\n")

    async def bat_dau(self):
        """Khởi chạy vòng lặp lắng nghe của từng Agent và vòng lặp Vật lý trung tâm"""
        if self.dang_chay:
            return

        self.khoi_tao_kich_ban()
        await self._kiem_tra_lien_lac_lien_tuc(0.0)
        self.dang_chay = True

        for tac_nhan in self.broker.danh_sach_tac_nhan.values():
            setattr(tac_nhan, "_dang_lang_nghe", True)
            task = asyncio.create_task(tac_nhan.lang_nghe_lien_tuc())
            self._cac_tac_vu_nen.append(task)

        self._cac_tac_vu_nen.append(asyncio.create_task(self._vong_lap_vat_ly_chinh()))

    async def dung(self):
        self.dang_chay = False
        for agent in self.broker.danh_sach_tac_nhan.values():
            setattr(agent, "_dang_lang_nghe", False)
        for task in self._cac_tac_vu_nen:
            task.cancel()
        if self._cac_tac_vu_nen:
            await asyncio.gather(*self._cac_tac_vu_nen, return_exceptions=True)
        self._cac_tac_vu_nen.clear()

    async def dieu_khien_thoi_gian(
        self,
        time_scale: Optional[float] = None,
        tam_dung: Optional[bool] = None
    ) -> dict:
        if time_scale is not None:
            self.time_scale = time_scale
        if tam_dung is not None:
            self.tam_dung = tam_dung

        trang_thai = (
            "TẠM DỪNG"
            if self.tam_dung
            else f"ĐANG CHẠY ({self.time_scale}x)"
        )
        await self.broker.gui_log_ra_web(
            "INFO",
            f"⏱️ Điều chỉnh đồng hồ mô phỏng: {trang_thai}"
        )
        return {
            "time_scale": self.time_scale,
            "paused": self.tam_dung
        }

    async def cap_nhat_pin_uav(self, ma_uav: str, phan_tram_pin: float) -> dict:
        agent = self.broker.lay_tac_nhan(ma_uav)
        if not isinstance(agent, UAVAgent):
            raise ValueError(f"Không tìm thấy UAV {ma_uav}")

        await agent.dat_pin_gia_lap(phan_tram_pin)
        for node in self.broker.danh_sach_tac_nhan.values():
            if isinstance(node, UAVAgent):
                node.evaluate_communication_link()
        await self._kiem_tra_lien_lac_lien_tuc(0.0)

        thong_bao = (
            f"🔋 Đã cập nhật năng lượng [{ma_uav}] ➔ {agent.phan_tram_pin}% "
            f"(Trạng thái FSM: {agent.trang_thai.value})"
        )
        await self.broker.gui_log_ra_web("WARN", thong_bao)
        await self.broker.dong_bo_trang_thai_toan_mang()
        return {"thanh_cong": True, "thong_bao": thong_bao}

    async def _vong_lap_vat_ly_chinh(self):
        """Cập nhật vị trí tác tử, pin và trạng thái mạng qua WebSocket."""
        thoi_diem_truoc = time.time()

        while self.dang_chay:
            await asyncio.sleep(self.tick_interval)
            thoi_diem_hien_tai = time.time()
            dt_thuc = thoi_diem_hien_tai - thoi_diem_truoc
            thoi_diem_truoc = thoi_diem_hien_tai

            if self.tam_dung:
                continue

            self.so_buoc_thoi_gian += 1
            dt_mo_phong = dt_thuc * self.time_scale

            # Cập nhật bản đồ điểm nhiệt (HeatNode) tự động tăng nhẹ risk_score theo thời gian
            self.update_heatmap(dt_mo_phong)

            cac_tac_vu_vat_ly = [
                agent.cap_nhat_vat_ly(dt_mo_phong)
                for agent in self.broker.danh_sach_tac_nhan.values()
            ]
            await asyncio.gather(*cac_tac_vu_vat_ly)

            # Watchdog kiểm tra các nhiệm vụ cháy: nếu UAV trúng thầu gặp nạn/OFFLINE thì kích hoạt tái đấu thầu ngay
            for ma_phien, mission in list(self._active_fire_missions.items()):
                wid = mission.get("winner_id")
                w_agent = self.broker.danh_sach_tac_nhan.get(wid)
                if w_agent and (w_agent.trang_thai == AgentState.OFFLINE or w_agent.phan_tram_pin <= 5.0):
                    if wid not in mission.get("failed_uavs", set()):
                        asyncio.create_task(self.xu_ly_uav_gap_su_co_nhiem_vu(ma_phien, wid, "UAV sập nguồn/OFFLINE"))

            self._thoi_gian_tich_luy_kiem_tra_lien_lac += dt_mo_phong
            if self._thoi_gian_tich_luy_kiem_tra_lien_lac >= 5.0:
                await self._kiem_tra_lien_lac_lien_tuc(self._thoi_gian_tich_luy_kiem_tra_lien_lac)
                self._thoi_gian_tich_luy_kiem_tra_lien_lac = 0.0

            await self.broker.dong_bo_trang_thai_toan_mang()

    async def _kiem_tra_lien_lac_lien_tuc(self, khoang_thoi_gian_giay: float):
        """Đo định kỳ khả năng mỗi UAV tạo được tuyến tới ít nhất một trạm mặt đất."""
        cac_uav = [
            agent for agent in self.broker.danh_sach_tac_nhan.values()
            if agent.loai_tac_nhan == "MÁY_BAY"
        ]
        await self._lap_lich_ban_giao_vung(cac_uav)
        ket_noi = 0
        dem_ground = 0
        dem_fanet = 0
        dem_leo = 0

        for uav in cac_uav:
            ma_uav = uav.ma_dinh_danh
            tuyen = (
                self.broker.tim_tuyen_den_tram_mat_dat(ma_uav)
                if uav.trang_thai not in (AgentState.OFFLINE, AgentState.LOW_BATTERY)
                else None
            )
            co_tuyen = tuyen is not None
            self._tong_mau_lien_lac += 1
            if co_tuyen:
                ket_noi += 1
                self._mau_lien_lac_thanh_cong += 1
                self._mat_lien_lac_lien_tuc_theo_uav[ma_uav] = 0.0
                if hasattr(uav, "evaluate_communication_link"):
                    uav.evaluate_communication_link()
                link_kind = getattr(uav, "current_link", "GROUND_LINK")
                if link_kind == "GROUND_LINK":
                    dem_ground += 1
                elif link_kind == "FANET_RELAY":
                    dem_fanet += 1
                else:
                    dem_leo += 1
            else:
                self._tong_thoi_gian_mat_lien_lac += khoang_thoi_gian_giay
                self._thoi_gian_mat_lien_lac_theo_uav[ma_uav] = (
                    self._thoi_gian_mat_lien_lac_theo_uav.get(ma_uav, 0.0) + khoang_thoi_gian_giay
                )
                self._mat_lien_lac_lien_tuc_theo_uav[ma_uav] = (
                    self._mat_lien_lac_lien_tuc_theo_uav.get(ma_uav, 0.0) + khoang_thoi_gian_giay
                )
                self._mat_lien_lac_lien_tuc_toi_da = max(
                    self._mat_lien_lac_lien_tuc_toi_da,
                    self._mat_lien_lac_lien_tuc_theo_uav[ma_uav]
                )
            if self._trang_thai_lien_lac.get(ma_uav) != co_tuyen:
                await self.broker.gui_log_ra_web(
                    "INFO" if co_tuyen else "WARN",
                    f"[LIÊN LẠC] {ma_uav}: {'Có tuyến tới trạm' if co_tuyen else 'Mất tuyến tới trạm'}."
                )
            self._trang_thai_lien_lac[ma_uav] = co_tuyen

        tong_nang_luong = round(sum(
            agent.thong_ke["nang_luong_tieu_hao_joule"]
            for agent in self.broker.danh_sach_tac_nhan.values()
        ), 3)
        so_ban_giao = sum(
            len(getattr(uav, "_vung_tuan_tra_phu", {}))
            for uav in cac_uav
        )
        self._chi_so_lien_lac.update({
            "uav_ket_noi": ket_noi,
            "tong_uav": len(cac_uav),
            "uav_ground_link": dem_ground,
            "uav_fanet_relay": dem_fanet,
            "uav_leo_satellite": dem_leo,
            "do_kha_dung_pct": round(
                100.0 * self._mau_lien_lac_thanh_cong / self._tong_mau_lien_lac, 2
            ) if self._tong_mau_lien_lac else 100.0,
            "thoi_gian_mat_lien_lac_giay": round(self._tong_thoi_gian_mat_lien_lac, 2),
            "mat_lien_lac_lien_tuc_toi_da_giay": round(self._mat_lien_lac_lien_tuc_toi_da, 2),
            "nang_luong_tieu_thu_joule": tong_nang_luong,
            "nhiem_vu_da_dieu_phoi": self._so_nhiem_vu_dieu_phoi,
            "vung_tuan_tra_ban_giao": max(len(self._ban_giao_vung_uav), so_ban_giao)
        })
        self.broker.thong_ke_toan_mang["nang_luong_tieu_hao_joule"] = tong_nang_luong
        self.scenario.ghi_log_lien_lac_lien_tuc(self._chi_so_lien_lac)

        # Luân phiên gửi telemetry qua broker để đường truyền và QoS phản ánh cùng một mô hình.
        uav_ung_vien = [
            u for u in cac_uav
            if u.trang_thai not in (AgentState.OFFLINE, AgentState.LOW_BATTERY, AgentState.CHARGING)
        ]
        if uav_ung_vien:
            idx_chon = (self.so_buoc_thoi_gian // 5) % len(uav_ung_vien)
            uav_phat = uav_ung_vien[idx_chon]
            da_gui_telemetry = await self.broker.gui_goi_tin_da_chang_ve_tram(
                ma_nguon=uav_phat.ma_dinh_danh,
                thong_diep={
                    "loai_tin": "TELEMETRY_REPORT",
                    "kich_thuoc_kb": 12.0,
                    "noi_dung": {
                        "zone_id": getattr(uav_phat, "zone_id", None),
                        "pin_pct": round(uav_phat.phan_tram_pin, 1),
                        "trang_thai": uav_phat.trang_thai.value,
                    },
                },
            )
            if not da_gui_telemetry:
                await self.broker.gui_log_ra_web(
                    "WARN",
                    f"[TELEMETRY] Không chuyển được dữ liệu {uav_phat.ma_dinh_danh} về trạm."
                )

    async def _lap_lich_ban_giao_vung(self, cac_uav: list):
        """Đồng bộ trạng thái bàn giao vùng giữa các UAV trong cùng phân khu."""
        by_id = {uav.ma_dinh_danh: uav for uav in cac_uav}
        for ma_chu_vung, ma_uav_nhan in list(self._ban_giao_vung_uav.items()):
            chu_vung = by_id.get(ma_chu_vung)
            uav_nhan = by_id.get(ma_uav_nhan)
            if (
                chu_vung
                and uav_nhan
                and chu_vung.trang_thai in (AgentState.PATROLLING, AgentState.RELAYING)
                and chu_vung.phan_tram_pin >= 95.0
            ):
                uav_nhan.tra_lai_vung_tuan_tra(ma_chu_vung)
                del self._ban_giao_vung_uav[ma_chu_vung]
                await self.broker.gui_log_ra_web(
                    "INFO", f"[LỊCH UAV] {ma_chu_vung} nhận lại tuyến tuần tra từ {ma_uav_nhan}."
                )

        for chu_vung in cac_uav:
            if (
                chu_vung is None
                or chu_vung.ma_dinh_danh in self._ban_giao_vung_uav
                or chu_vung.trang_thai not in (
                    AgentState.RETURNING, AgentState.CHARGING, AgentState.LOW_BATTERY, AgentState.OFFLINE
                )
                or not getattr(chu_vung, "waypoints", None)
            ):
                continue
            # Kiểm tra xem đã có UAV cùng vùng nào tự trị nhận bàn giao qua gói tin YEU_CAU_BAN_GIAO_VUNG chưa
            uav_da_nhan = next(
                (
                    uav for uav in cac_uav
                    if chu_vung.ma_dinh_danh in getattr(uav, "_vung_tuan_tra_phu", {})
                ),
                None
            )
            if uav_da_nhan:
                self._ban_giao_vung_uav[chu_vung.ma_dinh_danh] = uav_da_nhan.ma_dinh_danh
                continue

            # 1. Ưu tiên điều động UAV dự bị của phân khu đang đỗ tại trạm (pin >= 90%)
            u_reserve = next(
                (
                    u for u in cac_uav
                    if u.ma_dinh_danh != chu_vung.ma_dinh_danh
                    and getattr(u, "zone_id", None) == getattr(chu_vung, "zone_id", None)
                    and u._dang_o_tai_tram()
                    and u.phan_tram_pin >= 90.0
                ),
                None
            )
            if u_reserve:
                waypoints = getattr(chu_vung, "waypoints_goc", None) or chu_vung.waypoints
                u_reserve.waypoints_goc = list(waypoints)
                u_reserve.waypoints = list(waypoints)
                u_reserve.waypoint_index = 0
                u_reserve.toa_do_tram_sac = None
                u_reserve.chuyen_trang_thai(AgentState.PATROLLING)
                self._ban_giao_vung_uav[chu_vung.ma_dinh_danh] = u_reserve.ma_dinh_danh
                await self.broker.gui_log_ra_web(
                    "INFO",
                    f"🔄 [LUÂN PHIÊN TAG-TEAM] Điều động UAV dự phòng {u_reserve.ma_dinh_danh} (Pin {u_reserve.phan_tram_pin:.0f}%) "
                    f"từ trạm ra thay thế {chu_vung.ma_dinh_danh} tại phân khu {chu_vung.zone_id}."
                )
                continue

            # 2. Nếu trạm chưa kịp sạc xong UAV dự phòng, bàn giao tạm thời cho UAV tuần tra cùng phân khu
            cac_ung_vien = [
                uav for uav in cac_uav
                if uav.ma_dinh_danh != chu_vung.ma_dinh_danh
                and getattr(uav, "zone_id", None) == getattr(chu_vung, "zone_id", None)
                and uav.trang_thai in (AgentState.PATROLLING, AgentState.RELAYING)
                and uav.phan_tram_pin >= 45.0
                and not getattr(uav, "_tac_vu_su_co", None)
            ]
            if not cac_ung_vien:
                continue
            waypoints = getattr(chu_vung, "waypoints_goc", None) or chu_vung.waypoints
            uav_nhan = min(
                cac_ung_vien,
                key=lambda uav: (
                    len(getattr(uav, "_vung_tuan_tra_phu", {})) * 10000.0
                    + min(
                        self.physics.tinh_khoang_cach_3d(uav.toa_do, waypoint)
                        for waypoint in waypoints
                    )
                )
            )
            uav_nhan.nhan_vung_tuan_tra(chu_vung.ma_dinh_danh, waypoints)
            self._ban_giao_vung_uav[chu_vung.ma_dinh_danh] = uav_nhan.ma_dinh_danh
            await self.broker.gui_log_ra_web(
                "WARN",
                f"[LỊCH UAV] {uav_nhan.ma_dinh_danh} tạm nhận tuyến tuần tra của "
                f"{chu_vung.ma_dinh_danh} trong lúc UAV quay về sạc."
            )

    async def xu_ly_uav_gap_su_co_nhiem_vu(self, ma_phien: str, uav_id: str, ly_do: str):
        """Kích hoạt cơ chế tự động tái đấu giá (Re-auction) khi UAV đang xử lý cháy gặp sự cố / hết pin."""
        if ma_phien not in self._active_fire_missions:
            return
        mission = self._active_fire_missions[ma_phien]
        if mission.get("retry_count", 0) >= 3:
            await self.broker.gui_log_ra_web(
                "WARN", f"🚨 [SỰ CỐ CHÁY] Đám cháy #{ma_phien[:8]} tại ({mission['vi_do']:.4f}, {mission['kinh_do']:.4f}) không thể xử lý sau 3 lần tái điều động!"
            )
            return

        mission["retry_count"] += 1
        mission.setdefault("failed_uavs", set()).add(uav_id)
        await self.broker.gui_log_ra_web(
            "WARN",
            f"⚠️ [SỰ CỐ NHIỆM VỤ] Tác tử {uav_id} gặp sự cố ({ly_do}). "
            f"Kích hoạt tái đấu thầu tự động (Lần {mission['retry_count']}/3)..."
        )

        thong_diep_cfp = {
            "loai_tin": "CFP_FIRE_EVENT",
            "ma_phien_dau_gia": ma_phien,
            "loai_su_co": mission["loai_su_co"],
            "vi_do": mission["vi_do"],
            "kinh_do": mission["kinh_do"],
            "zone_id": mission["zone_id"],
            "thoi_diem_khoi_tao": time.time(),
            "nguoi_gui": "TRẠM_KIỂM_LÂM_SƠN_TRÀ"
        }

        for tac_nhan in self.broker.danh_sach_tac_nhan.values():
            if tac_nhan.loai_tac_nhan == "MÁY_BAY" and tac_nhan.ma_dinh_danh not in mission["failed_uavs"]:
                t_state = getattr(tac_nhan, "uav_state", None) or tac_nhan.trang_thai
                t_name = getattr(t_state, "name", str(t_state))
                if t_name in ("PATROLLING", "STANDBY", "IDLE", "RELAYING"):
                    tac_vu = getattr(tac_nhan, "_tac_vu_su_co", None)
                    if tac_vu is None or tac_vu.done():
                        await tac_nhan.hang_doi_thong_diep.put(thong_diep_cfp.copy())
                        if hasattr(tac_nhan, "xu_ly_thong_diep"):
                            await tac_nhan.xu_ly_thong_diep(thong_diep_cfp.copy())

        ket_qua = await self.ground_station_server.mo_phien_dau_gia_chay_rung(
            ma_phien=ma_phien,
            thong_diep_cfp=thong_diep_cfp,
            thoi_gian_cho_s=0.6,
            exclude_uavs=mission["failed_uavs"]
        )
        if ket_qua and (ket_qua.get("winner_id") or ket_qua.get("uav_id")):
            new_wid = ket_qua.get("winner_id") or ket_qua.get("uav_id")
            mission["winner_id"] = new_wid
            await self.broker.gui_log_ra_web(
                "WIN", f"🔄 [TÁI ĐẤU GIÁ THÀNH CÔNG] Đã bàn giao đám cháy #{ma_phien[:8]} cho tác tử ứng cứu [{new_wid}]!"
            )

    def _thong_tin_uav_gan_nhat(self, vi_do: float, kinh_do: float) -> dict:
        uavs = [
            agent for agent in self.broker.danh_sach_tac_nhan.values()
            if agent.loai_tac_nhan == "MÁY_BAY"
        ]
        if not uavs:
            return {}

        vi_tri_muc_tieu = {"vi_do": vi_do, "kinh_do": kinh_do, "do_cao": 0.0}
        uav_gan_nhat = min(
            uavs,
            key=lambda agent: self.physics.tinh_khoang_cach_3d(
                agent.toa_do, vi_tri_muc_tieu
            )
        )
        khoang_cach_m = self.physics.tinh_khoang_cach_3d(
            uav_gan_nhat.toa_do, vi_tri_muc_tieu
        )
        van_toc = max(1.0, float(uav_gan_nhat.toa_do.get("van_toc_m_s") or 45.0))
        cong_suat_bay_w = max(1.0, float(getattr(uav_gan_nhat, "cong_suat_bay_w", 80.0)))
        nang_luong_khu_hoi = (2.0 * khoang_cach_m / van_toc) * cong_suat_bay_w
        pin_toi_da = max(0.0, float(uav_gan_nhat.pin_toi_da))
        pin_hien_tai = max(0.0, float(uav_gan_nhat.pin_hien_tai))
        nang_luong_du_tru = pin_toi_da * LOW_BATTERY_THRESHOLD / 100.0
        pin_du_kien_pct = (
            (pin_hien_tai - nang_luong_khu_hoi) / pin_toi_da * 100.0
            if pin_toi_da > 0.0 else 0.0
        )
        return {
            "uav_gan_nhat": uav_gan_nhat.ma_dinh_danh,
            "khoang_cach_gan_nhat_km": round(khoang_cach_m / 1000.0, 2),
            "nang_luong_khu_hoi_du_kien_joule": round(nang_luong_khu_hoi, 2),
            "nang_luong_du_tru_yeu_cau_joule": round(nang_luong_du_tru, 2),
            "pin_du_kien_pct": round(pin_du_kien_pct, 2)
        }

    async def _gui_bao_cao_phat_hien_chay(
        self,
        ma_phien: str,
        loai_su_co: str,
        vi_do: float,
        kinh_do: float
    ) -> Optional[str]:
        """Chọn UAV gần nhất có thể truyền báo cáo sự cố về trạm qua broker."""
        target = {"vi_do": vi_do, "kinh_do": kinh_do, "do_cao": 0.0}
        uavs_phat_hien = [
            agent
            for agent in self.broker.danh_sach_tac_nhan.values()
            if agent.loai_tac_nhan == "MÁY_BAY"
            and agent.trang_thai != AgentState.OFFLINE
            and agent.pin_hien_tai > 0.0
        ]
        uavs_phat_hien.sort(
            key=lambda agent: self.physics.tinh_khoang_cach_3d(agent.toa_do, target)
        )

        for uav in uavs_phat_hien:
            bao_cao = {
                "loai_tin": "FIRE_DETECTION_REPORT",
                "ma_su_co": ma_phien,
                "ma_phien_dau_gia": ma_phien,
                "uav_phat_hien": uav.ma_dinh_danh,
                "loai_su_co": loai_su_co,
                "vi_do": vi_do,
                "kinh_do": kinh_do,
                "thoi_diem_phat_hien": time.time(),
                "kich_thuoc_kb": 1.0
            }
            if await self.broker.gui_goi_tin_da_chang_ve_tram(
                ma_nguon=uav.ma_dinh_danh,
                thong_diep=bao_cao
            ):
                return uav.ma_dinh_danh
        return None

    def update_heatmap(self, dt: float) -> list:
        """
        Cập nhật các điểm nhiệt (HeatNode) tự động tăng nhẹ risk_score theo thời gian dt.
        """
        scenario = getattr(self, "scenario", None)
        if scenario is not None and hasattr(scenario, "update_heatmap"):
            return scenario.update_heatmap(dt)
        return []

    def trigger_fire(self, lat: float, lon: float, radius_m: float = 800.0) -> list:
        """
        Khi có sự kiện cháy tại (lat, lon):
        - Tìm các HeatNode xung quanh tâm cháy trong bán kính `radius_m` và đẩy risk_score lên 1.0
          (tạo điểm nóng cực đại).
        - KHÔNG kích hoạt cơ chế đấu thầu của Trạm Kiểm Lâm nữa.
        """
        scenario = getattr(self, "scenario", None)
        if scenario is not None and hasattr(scenario, "trigger_fire"):
            return scenario.trigger_fire(lat, lon, radius_m=radius_m)
        return []

    async def kich_hoat_su_co_khan_cap(
        self,
        loai_su_co: str,
        vi_do: float,
        kinh_do: float,
        ban_kinh_m: float = 800.0
    ) -> dict:
        """
        Kích hoạt sự kiện cháy rừng bằng cách đẩy risk_score của các HeatNode xung quanh
        tâm cháy lên 1.0 thông qua `trigger_fire(lat, lon)`.
        KHÔNG kích hoạt cơ chế đấu thầu FIPA-CNP của Trạm Kiểm Lâm nữa.
        """
        ma_phien = str(uuid.uuid4())
        fire_lat = float(vi_do)
        fire_lon = float(kinh_do)

        fire_zone_id = None
        scenario = getattr(self, "scenario", None)
        if scenario and hasattr(scenario, "patrol_zones") and isinstance(scenario.patrol_zones, dict):
            for feat in scenario.patrol_zones.get("features", []):
                if hasattr(scenario, "_diem_nam_trong_vung") and scenario._diem_nam_trong_vung(fire_lat, fire_lon, feat):
                    fire_zone_id = feat.get("properties", {}).get("zone_id")
                    break

        affected_nodes = self.trigger_fire(fire_lat, fire_lon, radius_m=ban_kinh_m)
        if not fire_zone_id and affected_nodes:
            fire_zone_id = getattr(affected_nodes[0], "zone_id", None)

        so_diem_nong = len(affected_nodes)
        thong_bao_kq = (
            f"🔥 [HEATMAP] Đã kích hoạt điểm cháy [{loai_su_co}] tại ({fire_lat:.4f}, {fire_lon:.4f}) "
            f"{f'thuộc {fire_zone_id} ' if fire_zone_id else ''}"
            f"— Đẩy {so_diem_nong} HeatNode trong bán kính {ban_kinh_m:.0f}m lên risk_score = 1.0 "
            f"(Không kích hoạt đấu thầu Trạm Kiểm Lâm)."
        )

        if hasattr(self, "broker") and self.broker is not None:
            if hasattr(self.broker, "gui_log_ra_web"):
                await self.broker.gui_log_ra_web("EVENT", thong_bao_kq)
            if hasattr(self.broker, "dong_bo_trang_thai_toan_mang"):
                await self.broker.dong_bo_trang_thai_toan_mang()

        return {
            "thanh_cong": True,
            "ma_phien": ma_phien,
            "loai_su_co": loai_su_co,
            "vi_do": fire_lat,
            "kinh_do": fire_lon,
            "zone_id": fire_zone_id,
            "so_diem_nhiet_kich_hoat": so_diem_nong,
            "affected_nodes": [
                n.to_dict() if hasattr(n, "to_dict") else dict(n)
                for n in affected_nodes
            ],
            "auction_triggered": False,
            "thong_bao": thong_bao_kq,
        }

    def lay_du_lieu_khoi_tao_ban_do(self) -> dict:
        """
        Trả về trạng thái node, waypoint tuần tra, bản đồ nhiệt HeatNode, độ phủ 7 phân khu và chỉ số mạng.
        """
        return {
            "nodes": [a.xuat_ho_so_trang_thai() for a in self.broker.danh_sach_tac_nhan.values()],
            "uav_waypoints": self.ban_do_waypoints_uav,
            "patrol_zones": self.scenario.patrol_zones,
            "heatmap": (
                self.scenario.xuat_heatmap()
                if hasattr(self.scenario, "xuat_heatmap")
                else []
            ),
            "zone_coverage": self.broker.tinh_thong_ke_phu_song_vung(),
            "deployment_plan": self.scenario.ke_hoach_bo_tri_ban_dau,
            "network_metrics": self.broker.thong_ke_toan_mang,
            "connectivity_metrics": self._chi_so_lien_lac,
            "last_auction": self.broker.phien_dau_gia_gan_nhat,
            "last_e2e_route": self.broker.tuyen_e2e_gan_nhat,
            "recent_logs": list(getattr(self.broker, "nhat_ky_gan_day", [])),
            "uav_energy_model": {
                agent.ma_dinh_danh: {
                    "power_w": getattr(agent, "cong_suat_bay_w", 0.0),
                    "energy_used_joule": agent.thong_ke["nang_luong_tieu_hao_joule"],
                    "missions": getattr(agent, "so_nhiem_vu_da_nhan", 0)
                }
                for agent in self.broker.danh_sach_tac_nhan.values()
                if agent.loai_tac_nhan == "MÁY_BAY"
            },
            "time_scale": self.time_scale,
            "paused": self.tam_dung
        }
