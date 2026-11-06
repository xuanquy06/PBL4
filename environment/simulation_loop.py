import asyncio
import time
import uuid
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
        tick_interval_sec: float = 1.0,
        uav_ws_server_url: Optional[str] = None
    ):
        self.tick_interval = tick_interval_sec
        self.time_scale = 1.0
        self.dang_chay = False
        self.tam_dung = False
        self.so_buoc_thoi_gian = 0

        self.scenario: Any = ScenarioManager(data_dir="data")
        self.physics: Any = PhysicsEngine()
        self.broker: Any = MessageBroker(physics_engine=self.physics, scenario_manager=self.scenario)
        self.ground_station_server: Any = GroundStationServer(self.broker)
        setattr(self.broker, "ground_station_server", self.ground_station_server)
        self.uav_ws_server_url = uav_ws_server_url

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
                    ws_server_url=self.uav_ws_server_url,
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
            if isinstance(agent, UAVAgent):
                await agent.dong_ket_noi_websocket()
        for task in self._cac_tac_vu_nen:
            task.cancel()
        if self._cac_tac_vu_nen:
            await asyncio.gather(*self._cac_tac_vu_nen, return_exceptions=True)
        self._cac_tac_vu_nen.clear()

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

            cac_tac_vu_vat_ly = [
                agent.cap_nhat_vat_ly(dt_mo_phong)
                for agent in self.broker.danh_sach_tac_nhan.values()
            ]
            await asyncio.gather(*cac_tac_vu_vat_ly)

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

        # Phát gói tin Telemetry đa chặng luân phiên từ các UAV (ưu tiên UAV đa chặng FANET/LEO) để trực quan hóa tia truyền gói tin E2E
        uav_ung_vien = [
            u for u in cac_uav
            if u.trang_thai not in (AgentState.OFFLINE, AgentState.LOW_BATTERY, AgentState.CHARGING)
            and getattr(u, "current_link", "") in ("FANET_RELAY", "LEO_SATELLITE")
        ] or [
            u for u in cac_uav
            if u.trang_thai not in (AgentState.OFFLINE, AgentState.LOW_BATTERY, AgentState.CHARGING)
        ]
        if uav_ung_vien:
            idx_chon = (self.so_buoc_thoi_gian // 5) % len(uav_ung_vien)
            uav_phat = uav_ung_vien[idx_chon]
            asyncio.create_task(
                self.broker.gui_goi_tin_da_chang_ve_tram(
                    ma_nguon=uav_phat.ma_dinh_danh,
                    thong_diep={
                        "loai_thong_diep": "TELEMETRY_REPORT",
                        "kich_thuoc_kb": 12.0,
                        "noi_dung": {
                            "zone_id": getattr(uav_phat, "zone_id", None),
                            "pin_pct": round(uav_phat.phan_tram_pin, 1),
                            "trang_thai": uav_phat.trang_thai.value,
                        },
                    },
                )
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

    async def kich_hoat_su_co_khan_cap(self, loai_su_co: str, vi_do: float, kinh_do: float) -> dict:
        """
        Khởi tạo giao thức Contract Net Protocol (CNP) phân tán chuẩn FIPA:
        - Phát sóng CFP_FIRE_EVENT tới các UAV đang tuần tra/chờ lệnh.
        - Thu thập PROPOSE và REFUSE trong cửa sổ đấu giá.
        - Trao hợp đồng ACCEPT_PROPOSAL cho UAV có chi phí đa mục tiêu thấp nhất.
        """
        ma_phien = str(uuid.uuid4())
        thong_diep_cfp = {
            "loai_tin": "CFP_FIRE_EVENT",
            "ma_phien_dau_gia": ma_phien,
            "loai_su_co": loai_su_co,
            "vi_do": float(vi_do),
            "kinh_do": float(kinh_do),
            "thoi_diem_khoi_tao": time.time(),
            "nguoi_gui": "TRẠM_KIỂM_LÂM_SƠN_TRÀ"
        }

        uavs_kha_dung = []
        for tac_nhan in self.broker.danh_sach_tac_nhan.values():
            if tac_nhan.loai_tac_nhan != "MÁY_BAY":
                continue
            trang_thai = getattr(tac_nhan, "uav_state", None) or tac_nhan.trang_thai
            trang_thai_val = getattr(trang_thai, "value", str(trang_thai))
            trang_thai_name = getattr(trang_thai, "name", str(trang_thai))

            if (
                trang_thai_name in ("PATROLLING", "STANDBY", "IDLE", "RELAYING")
                or trang_thai_val in ("ĐANG_TUẦN_TRA", "NHÀN_RỖI", "ĐANG_TRUYỀN_TẢI", "STANDBY", "PATROLLING", "RELAYING")
            ):
                tac_vu_su_co = getattr(tac_nhan, "_tac_vu_su_co", None)
                if tac_vu_su_co is None or tac_vu_su_co.done():
                    uavs_kha_dung.append(tac_nhan)

        if not uavs_kha_dung:
            thong_bao = "Không có UAV nào đang ở trạng thái sẵn sàng để nhận CFP."
            thong_tin_uav = self._thong_tin_uav_gan_nhat(vi_do, kinh_do)
            if thong_tin_uav:
                thong_bao += (
                    f" UAV gần nhất [{thong_tin_uav['uav_gan_nhat']}] cách "
                    f"{thong_tin_uav['khoang_cach_gan_nhat_km']} km; cần bảo toàn "
                    f"{thong_tin_uav['nang_luong_du_tru_yeu_cau_joule']} J dự trữ."
                )
            await self.broker.gui_log_ra_web("WARN", f"⚠️ {thong_bao}")
            return {"thanh_cong": False, "thong_bao": thong_bao, **thong_tin_uav}

        await self.broker.gui_log_ra_web(
            "EVENT",
            f"🔥 Phát hiện sự cố [{loai_su_co}] tại ({vi_do:.4f}, {kinh_do:.4f}). "
            f"Phát quảng bá CFP_FIRE_EVENT tới {len(uavs_kha_dung)} UAV."
        )

        if not hasattr(self, "ground_station_server") or self.ground_station_server is None:
            self.ground_station_server = GroundStationServer(self.broker)

        recipient_ids = {uav.ma_dinh_danh for uav in uavs_kha_dung}
        for uav in uavs_kha_dung:
            if getattr(uav, "_dang_lang_nghe", False):
                await uav.hang_doi_thong_diep.put(thong_diep_cfp.copy())
            else:
                await uav.hang_doi_thong_diep.put(thong_diep_cfp.copy())
                if hasattr(uav, "xu_ly_thong_diep"):
                    await uav.xu_ly_thong_diep(thong_diep_cfp.copy())

        if self.ground_station_server.clients:
            await self.ground_station_server.broadcast_command(thong_diep_cfp, recipient_ids)

        ket_qua_dau_gia = await self.ground_station_server.mo_phien_dau_gia_chay_rung(
            ma_phien=ma_phien,
            thong_diep_cfp=thong_diep_cfp,
            thoi_gian_cho_s=0.6
        )

        if not ket_qua_dau_gia:
            thong_bao = "Không có UAV nào gửi PROPOSE khả dụng (tất cả không đủ pin dự trữ > 25% sau khứ hồi)."
            thong_tin_uav = self._thong_tin_uav_gan_nhat(vi_do, kinh_do)
            if thong_tin_uav:
                thong_bao += (
                    f" UAV gần nhất [{thong_tin_uav['uav_gan_nhat']}] cách "
                    f"{thong_tin_uav['khoang_cach_gan_nhat_km']} km; cần bảo toàn "
                    f"{thong_tin_uav['nang_luong_du_tru_yeu_cau_joule']} J dự trữ."
                )
            return {
                "thanh_cong": False,
                "ma_phien": ma_phien,
                "thong_bao": thong_bao,
                "auction_detail": getattr(self.broker, "phien_dau_gia_gan_nhat", None),
                **thong_tin_uav
            }

        winner_id = ket_qua_dau_gia["uav_id"]
        khoang_cach_km = round(ket_qua_dau_gia.get("khoang_cach_m", 0.0) / 1000.0, 2)
        chi_phi = ket_qua_dau_gia.get("chi_phi", 0.0)
        self._so_nhiem_vu_dieu_phoi += 1
        self._chi_so_lien_lac["nhiem_vu_da_dieu_phoi"] = self._so_nhiem_vu_dieu_phoi

        thong_bao_kq = (
            f"🎯 CNP thành công! Đã trao thầu cho [{winner_id}] (cách {khoang_cach_km} km, "
            f"chi phí {chi_phi}) tiếp cận hiện trường chữa cháy."
        )
        return {
            "thanh_cong": True,
            "ma_phien": ma_phien,
            "uav_dieu_dong": winner_id,
            "khoang_cach_tiep_can_km": khoang_cach_km,
            "chi_phi": chi_phi,
            "pin_du_kien_pct": ket_qua_dau_gia.get("pin_du_kien_pct", 0.0),
            "thong_bao": thong_bao_kq,
            "auction_detail": getattr(self.broker, "phien_dau_gia_gan_nhat", None)
        }

    def lay_du_lieu_khoi_tao_ban_do(self) -> dict:
        """
        Trả về trạng thái node, waypoint tuần tra, độ phủ 7 phân khu và chỉ số mạng khi trình duyệt mở trang.
        """
        return {
            "nodes": [a.xuat_ho_so_trang_thai() for a in self.broker.danh_sach_tac_nhan.values()],
            "uav_waypoints": self.ban_do_waypoints_uav,
            "patrol_zones": self.scenario.patrol_zones,
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
