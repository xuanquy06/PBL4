import asyncio
import time
from typing import Dict, List, Optional
from environment.physics_engine import PhysicsEngine
from environment.scenario_manager import ScenarioManager
from core.message_broker import MessageBroker
from core.ground_station_server import GroundStationServer
from core.base_agent import AgentState
from agents.ground_station import GroundStationAgent
from agents.satellite_agent import SatelliteAgent
from agents.uav_agent import UAVAgent

class SimulationLoop:
    """
    Bộ điều phối trung tâm (Simulation Engine & World Clock):
    - Quản lý khởi tạo kịch bản từ CSV / CelesTrak.
    - Điều khiển vật lý liên lạc SAGSINS, năng lượng UAV và quỹ đạo vệ tinh SGP4.
    - Cung cấp giao diện điều khiển tốc độ mô phỏng (Time Scale) và bơm sự kiện động.
    """
    def __init__(
        self,
        tick_interval_sec: float = 1.0,
        uav_ws_server_url: Optional[str] = None
    ):
        self.tick_interval = tick_interval_sec
        self.time_scale = 1.0          # Hệ số nhân tốc độ thời gian (1x, 2x, 5x...)
        self.dang_chay = False
        self.tam_dung = False
        self.so_buoc_thoi_gian = 0

        # Khởi tạo 3 trụ cột lõi của hệ thống
        self.scenario = ScenarioManager(data_dir="data")
        self.physics = PhysicsEngine()
        self.broker = MessageBroker(physics_engine=self.physics, scenario_manager=self.scenario)
        self.ground_station_server = GroundStationServer(self.broker)
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
            "do_kha_dung_pct": 100.0,
            "thoi_gian_mat_lien_lac_giay": 0.0,
            "mat_lien_lac_lien_tuc_toi_da_giay": 0.0,
            "nang_luong_tieu_thu_joule": 0.0,
            "nhiem_vu_da_dieu_phoi": 0
        }
        self._thoi_gian_tich_luy_kiem_tra_lien_lac = 0.0
        setattr(self.broker, "thong_ke_lien_tuc", self._chi_so_lien_lac)

    # =========================================================================
    # 1. KHỞI TẠO HỆ THỐNG ĐA TÁC NHÂN TỪ DỮ LIỆU CSV & CELESTRAK
    # =========================================================================
    def khoi_tao_kich_ban(self):
        """Đọc dữ liệu từ ScenarioManager và khởi tạo toàn bộ các thực thể mạng"""
        print("\n================ KHỞI TẠO KỊCH BẢN MẠNG SAGSINS ================")

        # Bước 1: Đồng bộ dữ liệu TLE vệ tinh LEO từ CelesTrak / CSV và nạp vào SGP4
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
        # Bước 2: Đọc cấu hình Trạm mặt đất, UAV và Waypoints từ CSV
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

        print(f"[SIMULATION] Đã khởi tạo thành công {len(self.broker.danh_sach_tac_nhan)} tác nhân!")
        print("================================================================\n")

    # =========================================================================
    # 2. VÒNG LẶP ĐỒNG HỒ VẬT LÝ & ĐỒNG BỘ TRẠNG THÁI MẠNG
    # =========================================================================
    async def bat_dau(self):
        """Khởi chạy vòng lặp lắng nghe của từng Agent và vòng lặp Vật lý trung tâm"""
        if self.dang_chay:
            return

        self.khoi_tao_kich_ban()
        await self._kiem_tra_lien_lac_lien_tuc(0.0)
        self.dang_chay = True

        # Đánh thức vòng lặp xử lý hàng đợi gói tin của tất cả các tác nhân
        for tac_nhan in self.broker.danh_sach_tac_nhan.values():
            task = asyncio.create_task(tac_nhan.lang_nghe_lien_tuc())
            self._cac_tac_vu_nen.append(task)

        # Đánh thức vòng lặp cập nhật vật lý, pin và trạng thái mạng.
        self._cac_tac_vu_nen.append(asyncio.create_task(self._vong_lap_vat_ly_chinh()))

    async def dung(self):
        self.dang_chay = False
        for agent in self.broker.danh_sach_tac_nhan.values():
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

            # 1. Cập nhật động học 3D (SGP4 cho Vệ tinh, Waypoints & Pin cho UAV)
            cac_tac_vu_vat_ly = [
                agent.cap_nhat_vat_ly(dt_mo_phong)
                for agent in self.broker.danh_sach_tac_nhan.values()
            ]
            await asyncio.gather(*cac_tac_vu_vat_ly)

            self._thoi_gian_tich_luy_kiem_tra_lien_lac += dt_mo_phong
            if self._thoi_gian_tich_luy_kiem_tra_lien_lac >= 5.0:
                await self._kiem_tra_lien_lac_lien_tuc(self._thoi_gian_tich_luy_kiem_tra_lien_lac)
                self._thoi_gian_tich_luy_kiem_tra_lien_lac = 0.0

            # Đồng bộ tọa độ, QoS, pin và mức sẵn sàng tuyến truyền.
            await self.broker.dong_bo_trang_thai_toan_mang()

    async def _kiem_tra_lien_lac_lien_tuc(self, khoang_thoi_gian_giay: float):
        """Đo định kỳ khả năng mỗi UAV tạo được tuyến tới ít nhất một trạm mặt đất."""
        cac_uav = [
            agent for agent in self.broker.danh_sach_tac_nhan.values()
            if agent.loai_tac_nhan == "MÁY_BAY"
        ]
        await self._lap_lich_ban_giao_vung(cac_uav)
        ket_noi = 0
        for uav in cac_uav:
            ma_uav = uav.ma_dinh_danh
            co_tuyen = (
                uav.trang_thai not in (AgentState.OFFLINE, AgentState.LOW_BATTERY)
                and self.broker.tim_tuyen_den_tram_mat_dat(ma_uav) is not None
            )
            self._tong_mau_lien_lac += 1
            if co_tuyen:
                ket_noi += 1
                self._mau_lien_lac_thanh_cong += 1
                self._mat_lien_lac_lien_tuc_theo_uav[ma_uav] = 0.0
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
        self._chi_so_lien_lac.update({
            "uav_ket_noi": ket_noi,
            "tong_uav": len(cac_uav),
            "do_kha_dung_pct": round(
                100.0 * self._mau_lien_lac_thanh_cong / self._tong_mau_lien_lac, 2
            ) if self._tong_mau_lien_lac else 100.0,
            "thoi_gian_mat_lien_lac_giay": round(self._tong_thoi_gian_mat_lien_lac, 2),
            "mat_lien_lac_lien_tuc_toi_da_giay": round(self._mat_lien_lac_lien_tuc_toi_da, 2),
            "nang_luong_tieu_thu_joule": tong_nang_luong,
            "nhiem_vu_da_dieu_phoi": self._so_nhiem_vu_dieu_phoi,
            "vung_tuan_tra_ban_giao": len(self._ban_giao_vung_uav)
        })
        self.broker.thong_ke_toan_mang["nang_luong_tieu_hao_joule"] = tong_nang_luong
        self.scenario.ghi_log_lien_lac_lien_tuc(self._chi_so_lien_lac)

    async def _lap_lich_ban_giao_vung(self, cac_uav: list):
        """Chuyển tạm tuyến tuần tra của UAV quay về sạc cho UAV khỏe pin."""
        by_id = {uav.ma_dinh_danh: uav for uav in cac_uav}
        for ma_chu_vung, ma_uav_nhan in list(self._ban_giao_vung_uav.items()):
            chu_vung = by_id.get(ma_chu_vung)
            uav_nhan = by_id.get(ma_uav_nhan)
            if (
                chu_vung
                and uav_nhan
                and chu_vung.trang_thai == AgentState.PATROLLING
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
            cac_ung_vien = [
                uav for uav in cac_uav
                if uav.ma_dinh_danh != chu_vung.ma_dinh_danh
                and getattr(uav, "zone_id", None) == getattr(chu_vung, "zone_id", None)
                and uav.trang_thai == AgentState.PATROLLING
                and uav.phan_tram_pin >= 45.0
                and not getattr(uav, "_tac_vu_su_co", None)
            ]
            if not cac_ung_vien:
                continue
            waypoints = chu_vung.waypoints
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

    # =========================================================================
    # 3. ĐIỀU KHIỂN KỊCH BẢN ĐỘNG (KÍCH HOẠT SỰ CỐ, THÊM UAV, NẠP PIN)
    # =========================================================================
    async def kich_hoat_su_co_khan_cap(self, loai_su_co: str, vi_do: float, kinh_do: float) -> dict:
        """
        Khởi tạo giao thức Contract Net Protocol (CNP) phân tán chuẩn MAS:
        - Xóa bỏ logic tính điểm tập trung (Heuristic) gán cứng UAV.
        - Tạo thông điệp CFP_FIRE_EVENT chứa tọa độ sự cố cháy rừng.
        - Broadcast tới tất cả UAV đang ở trạng thái PATROLLING hoặc STANDBY.
        - Thu thập và giải quyết phiên đấu giá qua bộ đếm thời gian của GroundStationServer.
        """
        import uuid
        ma_phien = str(uuid.uuid4())
        thong_diep_cfp = {
            "loai_tin": "CFP_FIRE_EVENT",
            "ma_phien_dau_gia": ma_phien,
            "loai_su_co": loai_su_co,
            "vi_do": float(vi_do),
            "kinh_do": float(kinh_do),
            "thoi_diem_khoi_tao": time.time(),
            "nguoi_gui": "GROUND_STATION"
        }

        # Tìm các UAV đang ở trạng thái PATROLLING hoặc STANDBY (không bị sập nguồn / pin yếu RTB)
        uavs_kha_dung = []
        for tac_nhan in self.broker.danh_sach_tac_nhan.values():
            if tac_nhan.loai_tac_nhan != "MÁY_BAY":
                continue
            trang_thai = getattr(tac_nhan, "uav_state", None) or tac_nhan.trang_thai
            trang_thai_val = getattr(trang_thai, "value", str(trang_thai))
            trang_thai_name = getattr(trang_thai, "name", str(trang_thai))

            # Chỉ xét UAV ở trạng thái tuần tra (PATROLLING) hoặc sẵn sàng/nhàn rỗi (STANDBY/IDLE)
            if (
                trang_thai_name in ("PATROLLING", "STANDBY", "IDLE")
                or trang_thai_val in ("ĐANG_TUẦN_TRA", "NHÀN_RỖI", "STANDBY", "PATROLLING")
            ):
                tac_vu_su_co = getattr(tac_nhan, "_tac_vu_su_co", None)
                if tac_vu_su_co is None or tac_vu_su_co.done():
                    uavs_kha_dung.append(tac_nhan)

        if not uavs_kha_dung:
            thong_bao = "Không có UAV nào đang ở trạng thái PATROLLING hoặc STANDBY để nhận CFP."
            await self.broker.gui_log_ra_web("WARN", f"⚠️ {thong_bao}")
            return {"thanh_cong": False, "thong_bao": thong_bao}

        await self.broker.gui_log_ra_web(
            "EVENT",
            f"🔥 Phát hiện sự cố [{loai_su_co}] tại ({vi_do:.4f}, {kinh_do:.4f}). "
            f"Broadcast CFP_FIRE_EVENT tới {len(uavs_kha_dung)} UAV đang tuần tra/chờ lệnh."
        )

        # Broadcast thông điệp CFP_FIRE_EVENT tới tất cả UAV đủ điều kiện
        recipient_ids = {uav.ma_dinh_danh for uav in uavs_kha_dung}
        for uav in uavs_kha_dung:
            await uav.hang_doi_thong_diep.put(thong_diep_cfp.copy())
            # Nếu agent không có task lắng nghe nền đang chạy, kích hoạt xử lý trực tiếp
            if not any(t for t in getattr(self, "_cac_tac_vu_nen", []) if not t.done()):
                if hasattr(uav, "xu_ly_thong_diep"):
                    asyncio.create_task(uav.xu_ly_thong_diep(thong_diep_cfp.copy()))

        # Đồng thời broadcast qua kênh WebSocket của Ground Station Server nếu có client kết nối
        if hasattr(self, "ground_station_server") and self.ground_station_server:
            await self.ground_station_server.broadcast_command(thong_diep_cfp, recipient_ids)

        # Đảm bảo ground_station_server tồn tại
        if not hasattr(self, "ground_station_server") or self.ground_station_server is None:
            self.ground_station_server = GroundStationServer(self.broker)

        # Kích hoạt bộ đếm thời gian 1.5s tại GroundStationServer để thu gom PROPOSE và chọn UAV tối ưu
        ket_qua_dau_gia = await self.ground_station_server.mo_phien_dau_gia_chay_rung(
            ma_phien=ma_phien,
            thong_diep_cfp=thong_diep_cfp,
            thoi_gian_cho_s=1.5
        )

        if not ket_qua_dau_gia:
            thong_bao = "Không có UAV nào gửi PROPOSE khả dụng (tất cả không đủ pin dự trữ > 25% sau khứ hồi)."
            return {
                "thanh_cong": False,
                "ma_phien": ma_phien,
                "thong_bao": thong_bao
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
            "thong_bao": thong_bao_kq
        }

    def lay_du_lieu_khoi_tao_ban_do(self) -> dict:
        """
        Trả về trạng thái node, waypoint tuần tra và chỉ số mạng khi trình duyệt mở trang.
        """
        return {
            "nodes": [a.xuat_ho_so_trang_thai() for a in self.broker.danh_sach_tac_nhan.values()],
            "uav_waypoints": self.ban_do_waypoints_uav,
            "patrol_zones": self.scenario.patrol_zones,
            "deployment_plan": self.scenario.ke_hoach_bo_tri_ban_dau,
            "network_metrics": self.broker.thong_ke_toan_mang,
            "connectivity_metrics": self._chi_so_lien_lac,
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