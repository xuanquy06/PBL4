import asyncio
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from environment.physics_engine import PhysicsEngine
from environment.scenario_manager import ScenarioManager, ZONE_BATTERY_CAPACITY_J
from core.message_broker import MessageBroker
from core.network_stack import detect_local_lan_ip
from core.ground_station_server import GroundStationServer
from core.base_agent import AgentState
from agents.ground_station import GroundStationAgent
from agents.satellite_agent import SatelliteAgent
from agents.uav_agent import LOW_BATTERY_THRESHOLD, UAVAgent, UAVState


class SimulationLoop:
    """
    Bộ điều phối đồng hồ vật lý & quản lý kịch bản trung tâm phía Server (SAGSINS Client-Server):
    - Khởi tạo Trạm Kiểm Lâm, 3 Vệ tinh LEO (SGP4), 7 phân khu Sơn Trà và 210 điểm nhiệt.
    - Chờ các tiến trình Client UAV kết nối qua TCP :9000 (JOIN) và truyền Telemetry qua UDP :9001.
    - Tự động gán cấu hình 35 slot UAV đội hình + đánh giá kênh A2G / LEO và điều phối sự cố.
    """

    def __init__(self, tick_interval_sec: float = 1.0):
        self.tick_interval = tick_interval_sec
        self.time_scale = 1.0
        self.dang_chay = False
        self.tam_dung = False
        self.so_buoc_thoi_gian = 0

        project_data_dir = Path(__file__).resolve().parents[1] / "data"
        self.scenario: Any = ScenarioManager(data_dir=str(project_data_dir))
        self.physics: Any = PhysicsEngine()
        self.broker: Any = MessageBroker(physics_engine=self.physics, scenario_manager=self.scenario)
        self.ground_station_server: Any = GroundStationServer(self.broker)
        setattr(self.broker, "ground_station_server", self.ground_station_server)
        setattr(self.broker, "simulation_loop", self)
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        self.broker.network_stack.on_external_tcp_cmd = self._handle_external_tcp_cmd
        self.broker.network_stack.on_external_udp_telemetry = self._handle_external_udp_telemetry
        self.broker.network_stack.on_external_disconnect = self._handle_external_disconnect

        self._cac_tac_vu_nen: List[asyncio.Task] = []
        self.ban_do_waypoints_uav: Dict[str, List[dict]] = {}
        self.fleet_blueprint: Dict[str, dict[str, Any]] = {}
        self._so_nhiem_vu_dieu_phoi = 0
        self._tong_mau_lien_lac = 0
        self._mau_lien_lac_thanh_cong = 0
        self._tong_thoi_gian_mat_lien_lac = 0.0
        self._mat_lien_lac_lien_tuc_theo_uav: Dict[str, float] = {}
        self._mat_lien_lac_lien_tuc_toi_da = 0.0
        self._chi_so_lien_lac: dict[str, Any] = {
            "uav_ket_noi": 0,
            "tong_uav": 0,
            "tong_uav_thiet_ke": 35,
            "uav_ground_link": 0,
            "uav_leo_satellite": 0,
            "uav_out_of_range": 0,
            "uav_offline": 0,
            "avg_latency_ms": 0.0,
            "leo_s2g_active": 3,
            "do_kha_dung_pct": 100.0,
            "thoi_gian_mat_lien_lac_giay": 0.0,
            "mat_lien_lac_lien_tuc_toi_da_giay": 0.0,
            "nang_luong_tieu_thu_joule": 0.0,
            "nhiem_vu_da_dieu_phoi": 0,
        }
        self._thoi_gian_tich_luy_kiem_tra_lien_lac = 0.0
        setattr(self.broker, "thong_ke_lien_tuc", self._chi_so_lien_lac)

    def _schedule_web_log(self, cap_do: str, noi_dung: str):
        if self._main_loop and self._main_loop.is_running():
            try:
                asyncio.run_coroutine_threadsafe(
                    self.broker.gui_log_ra_web(cap_do, noi_dung),
                    self._main_loop,
                )
            except RuntimeError:
                pass

    @staticmethod
    def _normalize_ext_uav_id(raw_id: Optional[str]) -> str:
        val = str(raw_id or "UAV_09").strip() or "UAV_09"
        return val if val.startswith("MÁY_BAY_") else f"MÁY_BAY_{val}"

    def _get_zone_center(self, zone_id: str) -> tuple[float, float]:
        features = (getattr(self.scenario, "patrol_zones", None) or {}).get("features", [])
        for feat in features:
            if feat.get("properties", {}).get("zone_id") == zone_id:
                pc = feat.get("properties", {}).get("patrol_center")
                if pc and len(pc) == 2:
                    return float(pc[1]), float(pc[0])
        return 16.125, 108.255

    def _get_zone_polygon(self, zone_id: str) -> list[list[float]]:
        """Trích xuất đa giác ranh giới (outer ring [[lon, lat], ...]) của phân khu để gửi cho Client."""
        features = (getattr(self.scenario, "patrol_zones", None) or {}).get("features", [])
        for feat in features:
            if feat.get("properties", {}).get("zone_id") == zone_id:
                geom = feat.get("geometry") or {}
                gtype = geom.get("type")
                coords = geom.get("coordinates") or []
                ring = []
                if gtype == "MultiPolygon" and coords and coords[0] and coords[0][0]:
                    ring = coords[0][0]
                elif gtype == "Polygon" and coords and coords[0]:
                    ring = coords[0]
                return [
                    [round(float(pt[0]), 6), round(float(pt[1]), 6)]
                    for pt in ring
                    if isinstance(pt, (list, tuple)) and len(pt) >= 2 and isinstance(pt[0], (int, float)) and isinstance(pt[1], (int, float))
                ]
        return []

    def _get_station_coords(self) -> tuple[float, float, float]:
        with self.broker.os_kernel.net_state_lock:
            for agent in self.broker.danh_sach_tac_nhan.values():
                if agent.loai_tac_nhan == "TRẠM_MẶT_ĐẤT":
                    return (
                        float(agent.toa_do.get("vi_do", 16.105)),
                        float(agent.toa_do.get("kinh_do", 108.250)),
                        float(agent.toa_do.get("do_cao", 25.0)),
                    )
        return 16.105, 108.250, 25.0

    def _handle_external_tcp_cmd(self, pkt: Any, payload: dict, addr: tuple[str, int]) -> dict:
        """Xử lý các bản tin điều khiển tin cậy TCP từ tiến trình Client ngoài."""
        cmd = str(payload.get("cmd", "")).upper()
        uav_id = self._normalize_ext_uav_id(payload.get("uav_id"))
        st_lat, st_lon, _ = self._get_station_coords()

        if cmd == "JOIN":
            pid = payload.get("pid")
            addr_str = f"{addr[0]}:{addr[1]}"
            slot = self.fleet_blueprint.get(uav_id)

            if slot is not None:
                zone_id_eff = str(payload.get("zone_id") or slot["zone_id"])
                home_zone_eff = str(slot["home_zone_id"])
                role_eff = str(slot["role"])
                c_lat, c_lon = self._get_zone_center(zone_id_eff)
                zone_poly = self._get_zone_polygon(zone_id_eff)
                init_lat = float(payload["lat"]) if payload.get("lat") is not None else float(slot["lat"])
                init_lon = float(payload["lon"]) if payload.get("lon") is not None else float(slot["lon"])
                init_alt = float(payload["alt"]) if payload.get("alt") is not None else float(slot["alt"])
                init_bat = float(payload["battery"]) if payload.get("battery") is not None else float(slot["battery"])
                cap_j = float(slot["battery_capacity_j"])
                pow_w = float(slot["power_w"])
            else:
                zone_id_eff = str(payload.get("zone_id") or "ZONE_03")
                home_zone_eff = zone_id_eff
                role_eff = "PATROL"
                c_lat, c_lon = self._get_zone_center(zone_id_eff)
                zone_poly = self._get_zone_polygon(zone_id_eff)
                init_lat = float(payload["lat"]) if payload.get("lat") is not None else c_lat
                init_lon = float(payload["lon"]) if payload.get("lon") is not None else c_lon
                init_alt = float(payload["alt"]) if payload.get("alt") is not None else 1050.0
                init_bat = float(payload["battery"]) if payload.get("battery") is not None else 90.0
                cap_j = float(ZONE_BATTERY_CAPACITY_J.get(zone_id_eff, 50000.0))
                pow_w = 80.0

            with self.broker.os_kernel.net_state_lock:
                old_agent = self.broker.danh_sach_tac_nhan.get(uav_id)
                if isinstance(old_agent, UAVAgent):
                    agent = old_agent
                    agent.zone_id = zone_id_eff
                    agent.home_zone_id = home_zone_eff
                    agent.role = role_eff
                    agent.lat = init_lat
                    agent.lon = init_lon
                    agent.toa_do["do_cao"] = init_alt
                    agent.pin_toi_da = cap_j
                    agent.pin_hien_tai = cap_j * min(100.0, max(0.0, init_bat)) / 100.0
                    agent.is_external_client = True
                    agent.client_pid = pid
                    agent.client_addr = addr_str
                    agent.last_external_heartbeat = time.time()
                    agent._da_goi_du_bi_thay_ca = False
                    if role_eff == "STANDBY" and agent._dang_o_tai_tram():
                        agent.chuyen_trang_thai(AgentState.IDLE)
                        agent.uav_state = UAVState.STANDBY
                    else:
                        agent.chuyen_trang_thai(AgentState.PATROLLING)
                        agent.uav_state = UAVState.PATROLLING
                else:
                    agent = UAVAgent(
                        ma_dinh_danh=uav_id,
                        bo_dinh_tuyen=self.broker,
                        vi_do=init_lat,
                        kinh_do=init_lon,
                        do_cao=init_alt,
                        pin_max_joule=cap_j,
                        pin_khoi_tao_pct=init_bat,
                        cong_suat_bay_w=pow_w,
                        zone_id=zone_id_eff,
                        home_zone_id=home_zone_eff,
                        role=role_eff,
                        is_external_client=True,
                        client_pid=pid,
                        client_addr=addr_str,
                    )

                rf_info = agent.evaluate_communication_link()
                agent._last_pushed_rf_link = rf_info["link"]
                cur_state = agent.uav_state.value
                ack_lat = float(agent.lat)
                ack_lon = float(agent.lon)
                ack_alt = float(agent.toa_do.get("do_cao", 1050.0))
                ack_bat = float(agent.phan_tram_pin)
                ack_ip = str(agent.ip_address)

            self._schedule_web_log(
                "INFO",
                f"[CLIENT_NET] Client [{uav_id}] (PID:{pid}, {addr_str} -> {ack_ip}) kết nối TCP :{self.broker.network_stack.tcp_port} "
                f"tại {zone_id_eff} ({role_eff}, Kênh={rf_info['link']})."
            )
            return {
                "ok": True,
                "cmd": "JOIN_ACK",
                "uav_id": uav_id,
                "assigned_ip": ack_ip,
                "udp_port": self.broker.network_stack.udp_port,
                "zone_id": zone_id_eff,
                "home_zone_id": home_zone_eff,
                "role": role_eff,
                "zone_center": [c_lat, c_lon],
                "zone_polygon": zone_poly,
                "station": [st_lat, st_lon],
                "lat": ack_lat,
                "lon": ack_lon,
                "alt": ack_alt,
                "battery": round(ack_bat, 2),
                "battery_capacity_j": cap_j,
                "power_w": pow_w,
                "state": cur_state,
                "paused": bool(self.tam_dung),
                "time_scale": float(self.time_scale),
                "rf_link": rf_info["link"],
                "rf_hops": rf_info["hops"],
                "rf_delay_ms": rf_info["delay_ms"],
                "rf_per": rf_info["per"],
                "rf_snr_db": rf_info["snr_db"],
                "distance_m": rf_info["distance_m"],
            }

        if cmd == "FIRE_ALERT":
            agent = self.broker.lay_tac_nhan(uav_id)
            if isinstance(agent, UAVAgent) and (
                agent.uav_state in (UAVState.OFFLINE, UAVState.STANDBY, UAVState.CHARGING, UAVState.CONTAINING, UAVState.RESPONDING)
                or agent._dang_o_tai_tram()
            ):
                return {
                    "ok": False,
                    "cmd": "FIRE_NACK",
                    "error": f"UAV đang ở trạng thái {agent.uav_state.value}, chỉ báo cháy mới khi đang tuần tra trong vùng rừng.",
                }

            zone_id = str(payload.get("zone_id") or (getattr(agent, "zone_id", None) if agent else None) or "ZONE_03")
            c_lat, c_lon = self._get_zone_center(zone_id)
            f_lat = float(payload["lat"]) if payload.get("lat") is not None else c_lat
            f_lon = float(payload["lon"]) if payload.get("lon") is not None else c_lon

            features = (getattr(self.scenario, "patrol_zones", None) or {}).get("features", [])
            if features and hasattr(self.scenario, "_diem_nam_trong_vung"):
                matched_zone = None
                for feat in features:
                    if self.scenario._diem_nam_trong_vung(f_lat, f_lon, feat):
                        matched_zone = feat.get("properties", {}).get("zone_id")
                        break
                if matched_zone is None:
                    return {
                        "ok": False,
                        "cmd": "FIRE_NACK",
                        "error": "Tọa độ báo cháy nằm ngoài 7 phân khu rừng Sơn Trà (tại Trạm hoặc mặt biển).",
                    }
                zone_id = str(matched_zone)

            already_locked = bool(self.broker.kiem_tra_muc_tieu_da_khoa(f_lat, f_lon, ban_kinh_khoa_m=600.0))
            near_active = any(
                self.broker._khoang_cach_mat_dat_m(f_lat, f_lon, float(f.get("lat", 0.0)), float(f.get("lon", 0.0))) <= 600.0
                for f in (getattr(self.scenario, "active_fires", None) or [])
            )
            if already_locked or near_active:
                return {
                    "ok": False,
                    "cmd": "FIRE_NACK",
                    "error": "Đã có đám cháy đang được xử lý gần vị trí này (<= 600 m).",
                }

            if isinstance(agent, UAVAgent):
                rf_info = self.broker.danh_gia_kenh_sagsins(agent, kich_thuoc_kb=250.0, ma_nguon=uav_id)
            else:
                rf_info = self.broker.danh_gia_kenh_sagsins(
                    {"node": uav_id, "vi_do": f_lat, "kinh_do": f_lon, "do_cao": 1050.0},
                    kich_thuoc_kb=250.0,
                    ma_nguon=uav_id,
                )

            if not rf_info["ok"]:
                self._schedule_web_log(
                    "WARN",
                    f"[RF_GATEWAY] Chặn báo cháy TCP từ [{uav_id}] do mất phủ sóng A2G/LEO (OUT_OF_RANGE)."
                )
                return {
                    "ok": False,
                    "cmd": "FIRE_NACK",
                    "error": "Ngoài vùng phủ sóng A2G và Vệ tinh LEO (OUT_OF_RANGE).",
                    "rf_link": "OUT_OF_RANGE",
                }

            pin_khu_hoi_pct = float(agent.phan_tram_pin) if isinstance(agent, UAVAgent) else 90.0
            handover_needed = False
            if isinstance(agent, UAVAgent):
                pin_khu_hoi_pct = float(agent._tinh_pin_du_kien_khu_hoi_pct(f_lat, f_lon))
                handover_needed = not (agent.phan_tram_pin > LOW_BATTERY_THRESHOLD and pin_khu_hoi_pct > LOW_BATTERY_THRESHOLD)
                with self.broker.os_kernel.net_state_lock:
                    agent.current_link = rf_info["link"]
                    agent.tuyen_ve_tram_cuc_bo = rf_info["hops"]
                    agent.do_tre_ve_tram_cuc_bo_ms = rf_info["delay_ms"]
                    if handover_needed:
                        agent._tam_khoanh_vung_chay = None
                        agent.trang_thai = AgentState.RETURNING
                        agent.uav_state = UAVState.RETURNING
                    else:
                        agent._tam_khoanh_vung_chay = {
                            "vi_do": f_lat,
                            "kinh_do": f_lon,
                            "ma_phien": f"FIRE_{uav_id}",
                        }
                        agent.trang_thai = AgentState.PATROLLING
                        agent.uav_state = UAVState.CONTAINING

            if self._main_loop and self._main_loop.is_running():
                async def _process_client_fire():
                    res = await self.kich_hoat_su_co_khan_cap("CHÁY_RỪNG", f_lat, f_lon)
                    ma_phien = res.get("ma_phien") or f"FIRE_{uav_id}"
                    if isinstance(agent, UAVAgent):
                        if handover_needed:
                            if self.ground_station_server and hasattr(self.ground_station_server, "ghi_nhan_phat_hien_dam_chay"):
                                await self.ground_station_server.ghi_nhan_phat_hien_dam_chay(
                                    f_lat, f_lon, uav_id, 0.0, zone_id=zone_id
                                )
                            await self.broker.gui_bao_cao_ve_tram(
                                ma_nguon=uav_id,
                                thong_diep={
                                    "loai_tin": "FIRE_DETECTION_REPORT",
                                    "ma_su_co": ma_phien,
                                    "ma_phien_su_co": ma_phien,
                                    "uav_phat_hien": uav_id,
                                    "loai_su_co": "CHÁY_RỪNG",
                                    "vi_do": f_lat,
                                    "kinh_do": f_lon,
                                    "thoi_diem_phat_hien": time.time(),
                                    "kich_thuoc_kb": 250.0,
                                },
                            )
                            await agent.dieu_phoi_ban_giao_nhiem_vu(f_lat, f_lon, pin_khu_hoi_pct)
                        else:
                            self.broker.khoa_muc_tieu_chay(f_lat, f_lon, uav_id)
                            if isinstance(agent._tam_khoanh_vung_chay, dict):
                                agent._tam_khoanh_vung_chay["ma_phien"] = ma_phien
                            if self.ground_station_server and hasattr(self.ground_station_server, "ghi_nhan_khoa_muc_tieu"):
                                await self.ground_station_server.ghi_nhan_khoa_muc_tieu(
                                    f_lat, f_lon, uav_id, pin_khu_hoi_pct,
                                    zone_id=zone_id, la_p2p=False, uav_phat_hien_goc=uav_id,
                                )
                            await self.broker.gui_bao_cao_ve_tram(
                                ma_nguon=uav_id,
                                thong_diep={
                                    "loai_tin": "FIRE_DETECTION_REPORT",
                                    "ma_su_co": ma_phien,
                                    "ma_phien_su_co": ma_phien,
                                    "uav_phat_hien": uav_id,
                                    "loai_su_co": "CHÁY_RỪNG",
                                    "vi_do": f_lat,
                                    "kinh_do": f_lon,
                                    "thoi_diem_phat_hien": time.time(),
                                    "kich_thuoc_kb": 250.0,
                                },
                            )
                            self.broker.cap_nhat_tuyen_e2e_dam_chay_lien_tuc()

                asyncio.run_coroutine_threadsafe(_process_client_fire(), self._main_loop)
            else:
                self.trigger_fire(f_lat, f_lon)

            self._schedule_web_log(
                "ALERT",
                f"[CLIENT_NET] Client [{uav_id}] ({addr[0]}:{addr[1]}) báo cháy tại ({f_lat:.4f}, {f_lon:.4f}) "
                f"qua kênh {rf_info['link']} ({rf_info['delay_ms']:.2f} ms"
                f"{', bàn giao nhiệm vụ qua Server do pin khứ hồi yếu' if handover_needed else ''})."
            )
            return {
                "ok": True,
                "cmd": "FIRE_ACK",
                "handover": handover_needed,
                "handover_p2p": handover_needed,
                "pin_khu_hoi_pct": round(pin_khu_hoi_pct, 1),
                "rf_link": rf_info["link"],
                "rf_hops": rf_info["hops"],
                "rf_hop_count": rf_info["hop_count"],
                "rf_delay_ms": rf_info["delay_ms"],
                "rf_snr_db": rf_info["snr_db"],
            }

        if cmd == "LEAVE":
            agent = self.broker.lay_tac_nhan(uav_id)
            fire_pos = agent._lay_tam_chay_dang_phu_trach() if isinstance(agent, UAVAgent) else None
            self.broker.mo_khoa_muc_tieu_chay(uav_id, ha_nhiet=False)
            self.broker.huy_dang_ky_tac_nhan(uav_id)
            if fire_pos is not None and isinstance(agent, UAVAgent) and self._main_loop and self._main_loop.is_running():
                asyncio.run_coroutine_threadsafe(agent.ban_giao_chay_khi_roi_nhiem_vu(fire_pos), self._main_loop)
            self._schedule_web_log(
                "INFO",
                f"[CLIENT_NET] Client [{uav_id}] ngắt kết nối lịch sự (LEAVE) -> Đã gỡ khỏi đội hình hoạt động."
            )
            return {"ok": True, "cmd": "LEAVE_ACK"}

        return {"ok": False, "error": f"Unknown cmd: {cmd}"}

    def _handle_external_udp_telemetry(self, pkt: Any, payload: dict, addr: tuple[str, int]) -> str:
        uav_id = self._normalize_ext_uav_id(payload.get("uav_id"))
        agent = self.broker.lay_tac_nhan(uav_id)
        if not isinstance(agent, UAVAgent):
            return "EXT_UDP_UNKNOWN"

        req_state = str(payload.get("state", "")).upper()
        if req_state == "OFFLINE":
            with self.broker.os_kernel.net_state_lock:
                fire_pos = agent._lay_tam_chay_dang_phu_trach()
                self.broker.mo_khoa_muc_tieu_chay(agent.ma_dinh_danh, ha_nhiet=False)
                agent.toa_do["van_toc_m_s"] = 0.0
                agent.pin_hien_tai = 0.0
                agent._tam_khoanh_vung_chay = None
                agent.toa_do_muc_tieu = None
                agent.chuyen_trang_thai(AgentState.OFFLINE)
                agent.uav_state = UAVState.OFFLINE
                agent.last_external_heartbeat = time.time()
            if self._main_loop and self._main_loop.is_running():
                if fire_pos is not None:
                    asyncio.run_coroutine_threadsafe(agent.ban_giao_chay_khi_roi_nhiem_vu(fire_pos), self._main_loop)
                asyncio.run_coroutine_threadsafe(agent._goi_uav_du_bi_thay_ca(), self._main_loop)
            return "EXT_UDP_OFFLINE"

        new_lat = float(payload["lat"]) if payload.get("lat") is not None else float(agent.lat)
        new_lon = float(payload["lon"]) if payload.get("lon") is not None else float(agent.lon)
        new_alt = float(payload["alt"]) if payload.get("alt") is not None else float(agent.toa_do.get("do_cao", 1050.0))

        need_standby_relief = False
        handover_fire_pos: Optional[tuple[float, float]] = None
        with self.broker.os_kernel.net_state_lock:
            orig_lat, orig_lon, orig_alt = agent.lat, agent.lon, float(agent.toa_do.get("do_cao", 1050.0))
            agent.lat = new_lat
            agent.lon = new_lon
            agent.toa_do["do_cao"] = new_alt
            rf_info = self.broker.danh_gia_kenh_sagsins(agent, kich_thuoc_kb=2.0, ma_nguon=agent.ma_dinh_danh)
            if not rf_info["ok"]:
                agent.lat, agent.lon, agent.toa_do["do_cao"] = orig_lat, orig_lon, orig_alt
                agent.current_link = "OUT_OF_RANGE"
                now_ts = time.time()
                if agent._last_pushed_rf_link != "OUT_OF_RANGE" or (now_ts - agent._last_oor_push_ts) >= 5.0:
                    agent._last_pushed_rf_link = "OUT_OF_RANGE"
                    agent._last_oor_push_ts = now_ts
                    self.broker.network_stack.send_command_to_external_client(
                        agent.ma_dinh_danh,
                        "RF_STATUS",
                        {
                            "link": "OUT_OF_RANGE",
                            "hops": [],
                            "hop_count": 0,
                            "delay_ms": 9999.0,
                            "per": 1.0,
                            "snr_db": -100.0,
                            "distance_m": rf_info["distance_m"],
                        },
                    )
                return "RF_OUT_OF_RANGE_DROP"

            if payload.get("speed") is not None:
                agent.toa_do["van_toc_m_s"] = float(payload["speed"])
            if payload.get("battery") is not None:
                bat_pct = min(100.0, max(0.0, float(payload["battery"])))
                agent.pin_hien_tai = agent.pin_toi_da * bat_pct / 100.0

            if req_state == "RETURNING":
                handover_fire_pos = agent._lay_tam_chay_dang_phu_trach()
                if handover_fire_pos is not None:
                    self.broker.mo_khoa_muc_tieu_chay(agent.ma_dinh_danh, ha_nhiet=False)
                agent._tam_khoanh_vung_chay = None
                agent.toa_do_muc_tieu = None
                agent.chuyen_trang_thai(AgentState.RETURNING)
                agent.uav_state = UAVState.RETURNING
                if not agent._da_goi_du_bi_thay_ca:
                    need_standby_relief = True
            elif req_state == "CHARGING":
                agent._tam_khoanh_vung_chay = None
                agent.chuyen_trang_thai(AgentState.CHARGING)
                agent.uav_state = UAVState.CHARGING
            elif req_state == "STANDBY":
                agent._tam_khoanh_vung_chay = None
                agent._da_goi_du_bi_thay_ca = False
                agent.chuyen_trang_thai(AgentState.IDLE)
                agent.uav_state = UAVState.STANDBY
            elif req_state == "TRANSITING":
                agent._tam_khoanh_vung_chay = None
                agent.chuyen_trang_thai(AgentState.PATROLLING)
                agent.uav_state = UAVState.TRANSITING
            elif req_state == "PATROLLING":
                agent._tam_khoanh_vung_chay = None
                agent.chuyen_trang_thai(AgentState.PATROLLING)
                agent.uav_state = UAVState.PATROLLING
            elif req_state == "RESPONDING":
                agent.chuyen_trang_thai(AgentState.RESPONDING)
                agent.uav_state = UAVState.RESPONDING
            elif req_state == "CONTAINING":
                f_lat = payload.get("fire_lat")
                f_lon = payload.get("fire_lon")
                if f_lat is not None and f_lon is not None:
                    cur_tam = getattr(agent, "_tam_khoanh_vung_chay", None)
                    ma_phien = (cur_tam or {}).get("ma_phien") or f"FIRE_{agent.ma_dinh_danh}"
                    agent._tam_khoanh_vung_chay = {
                        "vi_do": float(f_lat),
                        "kinh_do": float(f_lon),
                        "ma_phien": ma_phien,
                    }
                agent.chuyen_trang_thai(AgentState.PATROLLING)
                agent.uav_state = UAVState.CONTAINING

            agent.evaluate_communication_link()
            agent.last_external_heartbeat = time.time()
            agent._rf_udp_cnt += 1
            if agent._last_pushed_rf_link != rf_info["link"] or agent._rf_udp_cnt % 6 == 0:
                agent._last_pushed_rf_link = rf_info["link"]
                self.broker.network_stack.send_command_to_external_client(
                    agent.ma_dinh_danh,
                    "RF_STATUS",
                    {
                        "link": rf_info["link"],
                        "hops": rf_info["hops"],
                        "hop_count": rf_info["hop_count"],
                        "delay_ms": rf_info["delay_ms"],
                        "per": rf_info["per"],
                        "snr_db": rf_info["snr_db"],
                        "distance_m": rf_info["distance_m"],
                    },
                )
            reason = str(rf_info["reason"])

        if self._main_loop and self._main_loop.is_running():
            if handover_fire_pos is not None:
                asyncio.run_coroutine_threadsafe(agent.ban_giao_chay_khi_roi_nhiem_vu(handover_fire_pos), self._main_loop)
            if need_standby_relief:
                asyncio.run_coroutine_threadsafe(agent._goi_uav_du_bi_thay_ca(), self._main_loop)
        return reason

    def _handle_external_disconnect(self, uav_id: str, addr: tuple[str, int]):
        norm_id = self._normalize_ext_uav_id(uav_id)
        agent = self.broker.lay_tac_nhan(norm_id)
        fire_pos = agent._lay_tam_chay_dang_phu_trach() if isinstance(agent, UAVAgent) else None
        self.broker.mo_khoa_muc_tieu_chay(norm_id, ha_nhiet=False)
        if isinstance(agent, UAVAgent):
            with self.broker.os_kernel.net_state_lock:
                agent.is_external_client = False
                agent.client_pid = None
                agent.client_addr = None
                agent._tam_khoanh_vung_chay = None
                agent.toa_do_muc_tieu = None
                agent.toa_do["van_toc_m_s"] = 0.0
                agent.chuyen_trang_thai(AgentState.OFFLINE)
                agent.uav_state = UAVState.OFFLINE
            self._schedule_web_log(
                "WARN",
                f"[CLIENT_NET] Client [{norm_id}] ({addr[0]}:{addr[1]}) mất kết nối TCP đột ngột -> OFFLINE."
            )
            if self._main_loop and self._main_loop.is_running():
                if fire_pos is not None:
                    asyncio.run_coroutine_threadsafe(agent.ban_giao_chay_khi_roi_nhiem_vu(fire_pos), self._main_loop)
                asyncio.run_coroutine_threadsafe(agent._goi_uav_du_bi_thay_ca(), self._main_loop)

    def khoi_tao_kich_ban(self):
        """Khởi tạo Trạm Kiểm Lâm, 3 Vệ tinh LEO và bảng 35 slot cấu hình UAV chờ Client kết nối."""
        print("\n================ KHỞI TẠO SERVER SAGSINS CLIENT-SERVER ================")

        danh_sach_tle = self.scenario.dong_bo_ve_tinh_celestrak(so_luong_toi_da=3)
        self.physics.nap_danh_sach_tle(danh_sach_tle)

        danh_sach_nodes, self.ban_do_waypoints_uav = self.scenario.tai_hoac_sinh_kich_ban_mang()

        for node_cfg in danh_sach_nodes:
            if node_cfg["loai_tac_nhan"] == "TRẠM_MẶT_ĐẤT":
                GroundStationAgent(**node_cfg, bo_dinh_tuyen=self.broker)

        for sat_cfg in danh_sach_tle:
            ma_sat = sat_cfg["ma_ve_tinh"]
            SatelliteAgent(
                ma_dinh_danh=ma_sat,
                ten_quoc_te=sat_cfg.get("ten_quoc_te", ma_sat),
                cong_suat_dbm=sat_cfg["cong_suat_dbm"],
                tan_so_hz=sat_cfg["tan_so_hz"],
                bang_thong_hz=sat_cfg["bang_thong_hz"],
                bo_dinh_tuyen=self.broker,
            )

        self.fleet_blueprint = {}
        for node_cfg in danh_sach_nodes:
            if node_cfg["loai_tac_nhan"] != "MÁY_BAY":
                continue
            uid = str(node_cfg["ma_dinh_danh"])
            u_num = int(uid.rsplit("_", 1)[-1])
            zid = str(node_cfg.get("zone_id") or "ZONE_01")
            role = "STANDBY" if u_num >= 29 else "PATROL"
            c_lat, c_lon = self._get_zone_center(zid)
            self.fleet_blueprint[uid] = {
                "uav_id": uid,
                "short_id": uid.replace("MÁY_BAY_", ""),
                "zone_id": zid,
                "home_zone_id": zid,
                "role": role,
                "lat": float(node_cfg["vi_do"]),
                "lon": float(node_cfg["kinh_do"]),
                "alt": float(node_cfg["do_cao"]),
                "battery": float(node_cfg.get("pin_khoi_tao_pct", 100.0)),
                "battery_capacity_j": float(node_cfg.get("pin_max_joule", 50000.0)),
                "power_w": 80.0,
                "zone_center": [c_lat, c_lon],
            }

        print(
            f"[SIMULATION] Đã sẵn sàng 1 Trạm mặt đất + {len(danh_sach_tle)} Vệ tinh LEO; "
            f"0/{len(self.fleet_blueprint)} UAV Client đang chờ kết nối."
        )
        print("======================================================================\n")

    async def bat_dau(self):
        if self.dang_chay:
            return

        self._main_loop = asyncio.get_running_loop()
        self.broker.os_kernel.start()
        self.broker.network_stack.start()
        self.khoi_tao_kich_ban()
        await self._kiem_tra_lien_lac_lien_tuc(0.0)
        self.dang_chay = True

        lan_ip = detect_local_lan_ip()
        t_port = self.broker.network_stack.tcp_port
        u_port = self.broker.network_stack.udp_port
        print("[WEB-UI] Máy 1 Server Map: http://127.0.0.1:8000/ | Dashboard: http://127.0.0.1:8000/dashboard")
        print(f"[WEB-UI] Truy cập LAN: http://{lan_ip}:8000/ | http://{lan_ip}:8000/dashboard")
        print(f"[CLIENT-SERVER] TCP Server: 0.0.0.0:{t_port} | UDP Server: 0.0.0.0:{u_port} (Mode: {self.broker.network_stack.socket_mode})")
        print(f"[CLIENT-SERVER] Chạy Client Fleet Web UI ở Máy 2: python client/client_app.py --server-host {lan_ip} --tcp-port {t_port} --udp-port {u_port}")
        print(f"[CLIENT-SERVER] Hoặc chạy 1 UAV CLI lẻ: python client/uav_client.py --host {lan_ip} --tcp-port {t_port} --udp-port {u_port} --id UAV_09")

        with self.broker.os_kernel.net_state_lock:
            tac_nhan_init = list(self.broker.danh_sach_tac_nhan.values())
        for tac_nhan in tac_nhan_init:
            setattr(tac_nhan, "_dang_lang_nghe", True)
            self._cac_tac_vu_nen.append(asyncio.create_task(tac_nhan.lang_nghe_lien_tuc()))

        self._cac_tac_vu_nen.append(asyncio.create_task(self._vong_lap_vat_ly_chinh()))

    async def dung(self):
        self.dang_chay = False
        for agent in list(self.broker.danh_sach_tac_nhan.values()):
            setattr(agent, "_dang_lang_nghe", False)
        for task in self._cac_tac_vu_nen:
            task.cancel()
        if self._cac_tac_vu_nen:
            await asyncio.gather(*self._cac_tac_vu_nen, return_exceptions=True)
        self._cac_tac_vu_nen.clear()
        self.broker.network_stack.stop()
        self.broker.os_kernel.stop()

    async def dieu_khien_thoi_gian(
        self,
        time_scale: Optional[float] = None,
        tam_dung: Optional[bool] = None,
    ) -> dict:
        if time_scale is not None:
            self.time_scale = time_scale
        if tam_dung is not None:
            self.tam_dung = tam_dung

        self.broker.network_stack.broadcast_command_to_external_clients(
            "SET_CLOCK",
            {"paused": bool(self.tam_dung), "time_scale": float(self.time_scale)},
        )

        trang_thai = "TẠM DỪNG" if self.tam_dung else f"ĐANG CHẠY ({self.time_scale}x)"
        await self.broker.gui_log_ra_web("INFO", f"[CLIENT_NET] Điều chỉnh đồng hồ mô phỏng: {trang_thai}")
        return {"time_scale": self.time_scale, "paused": self.tam_dung}

    async def cap_nhat_pin_uav(self, ma_uav: str, phan_tram_pin: float) -> dict:
        agent = self.broker.lay_tac_nhan(ma_uav)
        if not isinstance(agent, UAVAgent):
            raise ValueError(f"Không tìm thấy UAV Client {ma_uav} đang kết nối")

        await agent.dat_pin_gia_lap(phan_tram_pin)
        if getattr(agent, "is_external_client", False):
            pushed = self.broker.network_stack.send_command_to_external_client(
                agent.ma_dinh_danh,
                "SET_BATTERY",
                {"battery": float(phan_tram_pin)},
            )
            if pushed:
                await self.broker.gui_log_ra_web(
                    "INFO",
                    f"[CLIENT_NET] Đã đẩy lệnh TCP SET_BATTERY={float(phan_tram_pin):.1f}% xuống Client [{agent.ma_dinh_danh}]."
                )
        agent.evaluate_communication_link()
        await self._kiem_tra_lien_lac_lien_tuc(0.0)

        thong_bao = f"Đã cập nhật năng lượng [{ma_uav}] -> {agent.phan_tram_pin}% (Trạng thái: {agent.uav_state.value})"
        await self.broker.gui_log_ra_web("WARN", f"[CLIENT_NET] {thong_bao}")
        await self.broker.dong_bo_trang_thai_toan_mang()
        return {"thanh_cong": True, "thong_bao": thong_bao}

    async def _vong_lap_vat_ly_chinh(self):
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

            try:
                with self.broker.os_kernel.net_state_lock:
                    agents_step = list(self.broker.danh_sach_tac_nhan.values())
                results = await asyncio.gather(
                    *(agent.cap_nhat_vat_ly(dt_mo_phong) for agent in agents_step),
                    return_exceptions=True,
                )
                for res in results:
                    if isinstance(res, Exception):
                        print(f"[SIMULATION WARN] Lỗi cập nhật tác nhân: {res}")

                self.update_heatmap(dt_mo_phong)
                self._push_swarm_env_to_clients()

                self._thoi_gian_tich_luy_kiem_tra_lien_lac += dt_mo_phong
                if self._thoi_gian_tich_luy_kiem_tra_lien_lac >= 2.0:
                    await self._kiem_tra_lien_lac_lien_tuc(self._thoi_gian_tich_luy_kiem_tra_lien_lac)
                    self._thoi_gian_tich_luy_kiem_tra_lien_lac = 0.0

                heatmap_payload = (
                    self.scenario.xuat_heatmap()
                    if hasattr(self.scenario, "xuat_heatmap")
                    else []
                )
                await self.broker.dong_bo_trang_thai_toan_mang(heatmap_nodes=heatmap_payload)
            except Exception as exc:
                print(f"[SIMULATION ERROR] Lỗi tại bước {self.so_buoc_thoi_gian}: {exc}")

    def _push_swarm_env_to_clients(self):
        """Gửi cảm nhận bầy đàn cục bộ (đồng đội lân cận + điểm nhiệt Heatmap) cho UAV PATROLLING."""
        heat_nodes = getattr(self.scenario, "heat_nodes", None) or []
        with self.broker.os_kernel.net_state_lock:
            uavs = [
                a for a in self.broker.danh_sach_tac_nhan.values()
                if isinstance(a, UAVAgent) and a.trang_thai != AgentState.OFFLINE
            ]
        air_states = (UAVState.PATROLLING, UAVState.TRANSITING, UAVState.RESPONDING, UAVState.CONTAINING)
        air_peers = [u for u in uavs if u.uav_state in air_states]
        if not air_peers:
            return

        by_zone_unlocked: dict[str, list[Any]] = {}
        by_zone_all: dict[str, list[Any]] = {}
        for h in heat_nodes:
            zid = str(h.zone_id)
            by_zone_all.setdefault(zid, []).append(h)
            if not getattr(h, "locked_by", None):
                by_zone_unlocked.setdefault(zid, []).append(h)
        zone_hotspots: dict[str, list[list[float]]] = {}
        for zid, all_nodes in by_zone_all.items():
            picked = by_zone_unlocked.get(zid) or all_nodes
            zone_hotspots[zid] = [
                [round(float(n.lat), 5), round(float(n.lon), 5), round(float(n.risk_score), 3)]
                for n in picked
            ]

        for uav in uavs:
            if not uav.is_external_client or uav.uav_state != UAVState.PATROLLING:
                continue
            peers_payload: list[list[Any]] = []
            for p in air_peers:
                if p.ma_dinh_danh == uav.ma_dinh_danh:
                    continue
                same_z = (p.zone_id == uav.zone_id)
                if same_z or self.broker._khoang_cach_mat_dat_m(uav.lat, uav.lon, p.lat, p.lon) <= 700.0:
                    peers_payload.append([
                        round(float(p.lat), 5),
                        round(float(p.lon), 5),
                        1 if same_z else 0,
                    ])
            hotspots = zone_hotspots.get(str(uav.zone_id), [])
            self.broker.network_stack.send_command_to_external_client(
                uav.ma_dinh_danh,
                "SWARM_ENV",
                {"peers": peers_payload, "hotspots": hotspots},
            )

    async def _kiem_tra_lien_lac_lien_tuc(self, khoang_thoi_gian_giay: float):
        """Tổng hợp thống kê 2 kênh A2G / LEO và điều phối dự bị STANDBY khi thiếu quân số vùng."""
        with self.broker.os_kernel.net_state_lock:
            agents_snapshot = list(self.broker.danh_sach_tac_nhan.values())

        cac_uav = [a for a in agents_snapshot if getattr(a, "loai_tac_nhan", None) == "MÁY_BAY"]
        cac_sat = [a for a in agents_snapshot if getattr(a, "loai_tac_nhan", None) == "VỆ_TINH_LEO"]
        leo_s2g = sum(1 for s in cac_sat if getattr(s, "current_link", "") == "S2G_LINK")

        await self._lap_lich_ban_giao_vung(cac_uav)

        ket_noi = 0
        dem_ground = 0
        dem_leo = 0
        dem_oor = 0
        dem_offline = 0
        delays: list[float] = []

        for uav in cac_uav:
            ma_uav = uav.ma_dinh_danh
            if uav.trang_thai == AgentState.OFFLINE or uav.uav_state == UAVState.OFFLINE:
                dem_offline += 1
                self._tong_mau_lien_lac += 1
                self._tong_thoi_gian_mat_lien_lac += khoang_thoi_gian_giay
                self._mat_lien_lac_lien_tuc_theo_uav[ma_uav] = self._mat_lien_lac_lien_tuc_theo_uav.get(ma_uav, 0.0) + khoang_thoi_gian_giay
                self._mat_lien_lac_lien_tuc_toi_da = max(self._mat_lien_lac_lien_tuc_toi_da, self._mat_lien_lac_lien_tuc_theo_uav[ma_uav])
                continue

            kenh = uav.evaluate_communication_link() if hasattr(uav, "evaluate_communication_link") else {"ok": False, "link": "OUT_OF_RANGE", "delay_ms": 0.0}
            self._tong_mau_lien_lac += 1
            if kenh.get("ok"):
                ket_noi += 1
                self._mau_lien_lac_thanh_cong += 1
                self._mat_lien_lac_lien_tuc_theo_uav[ma_uav] = 0.0
                delays.append(float(kenh.get("delay_ms", 0.0)))
                if kenh["link"] == "GROUND_LINK":
                    dem_ground += 1
                else:
                    dem_leo += 1
            else:
                dem_oor += 1
                self._tong_thoi_gian_mat_lien_lac += khoang_thoi_gian_giay

        tong_nang_luong = round(sum(a.thong_ke["nang_luong_tieu_hao_joule"] for a in agents_snapshot), 3)
        avg_delay = round(sum(delays) / len(delays), 2) if delays else 0.0
        self._chi_so_lien_lac.update({
            "uav_ket_noi": ket_noi,
            "tong_uav": len(cac_uav),
            "tong_uav_thiet_ke": max(35, len(self.fleet_blueprint)),
            "uav_ground_link": dem_ground,
            "uav_leo_satellite": dem_leo,
            "uav_out_of_range": dem_oor,
            "uav_offline": dem_offline,
            "avg_latency_ms": avg_delay,
            "leo_s2g_active": leo_s2g,
            "do_kha_dung_pct": round(100.0 * self._mau_lien_lac_thanh_cong / self._tong_mau_lien_lac, 2) if self._tong_mau_lien_lac else 100.0,
            "thoi_gian_mat_lien_lac_giay": round(self._tong_thoi_gian_mat_lien_lac, 2),
            "mat_lien_lac_lien_tuc_toi_da_giay": round(self._mat_lien_lac_lien_tuc_toi_da, 2),
            "nang_luong_tieu_thu_joule": tong_nang_luong,
            "nhiem_vu_da_dieu_phoi": self._so_nhiem_vu_dieu_phoi,
        })
        self.broker.thong_ke_toan_mang["nang_luong_tieu_hao_joule"] = tong_nang_luong
        self.scenario.ghi_log_lien_lac_lien_tuc(self._chi_so_lien_lac)

    async def _lap_lich_ban_giao_vung(self, cac_uav: list):
        """Điều động Client STANDBY (pin >= 95%) khi một phân khu chỉ còn dưới 4 UAV đang bay."""
        active_air_states = ("PATROLLING", "TRANSITING", "CONTAINING", "RESPONDING")
        standby_pool = [
            u for u in cac_uav
            if u.trang_thai != AgentState.OFFLINE
            and getattr(getattr(u, "uav_state", None), "value", None) == "STANDBY"
            and u._dang_o_tai_tram()
            and u.phan_tram_pin >= 95.0
        ]
        if not standby_pool:
            return

        for idx in range(1, 8):
            zone_id = f"ZONE_{idx:02d}"
            uavs_in_zone_all = [u for u in cac_uav if getattr(u, "zone_id", None) == zone_id]
            if not uavs_in_zone_all:
                continue
            uavs_active_in_zone = [
                u for u in uavs_in_zone_all
                if u.trang_thai != AgentState.OFFLINE
                and not u._dang_o_tai_tram()
                and getattr(getattr(u, "uav_state", None), "value", None) in active_air_states
            ]
            # Chỉ tự đánh thức STANDBY nếu phân khu này đã có UAV từng xuất kích nhưng nay hụt (RTB/OFFLINE)
            has_deficit = any(
                u.uav_state in (UAVState.RETURNING, UAVState.CHARGING, UAVState.OFFLINE)
                for u in uavs_in_zone_all
            )
            if not has_deficit:
                continue

            zone_standby = [u for u in standby_pool if (getattr(u, "home_zone_id", None) or getattr(u, "zone_id", None)) == zone_id]
            zone_standby.sort(key=lambda u: u.ma_dinh_danh)
            while len(uavs_active_in_zone) < 4 and zone_standby:
                u_reserve = zone_standby.pop(0)
                standby_pool.remove(u_reserve)
                u_reserve.zone_id = zone_id
                u_reserve.chuyen_trang_thai(AgentState.PATROLLING)
                u_reserve.uav_state = UAVState.TRANSITING
                self.broker.network_stack.send_command_to_external_client(
                    u_reserve.ma_dinh_danh,
                    "RESUME_PATROL",
                    {
                        "zone_id": zone_id,
                        "zone_center": list(self._get_zone_center(zone_id)),
                        "zone_polygon": self._get_zone_polygon(zone_id),
                    },
                )
                uavs_active_in_zone.append(u_reserve)
                await self.broker.gui_log_ra_web(
                    "INFO",
                    f"[SWARM_HANDOVER] Điều động Client dự bị [{u_reserve.ma_dinh_danh}] (STANDBY) cất cánh bù quân số cho {zone_id} ({len(uavs_active_in_zone)}/4)."
                )

    def update_heatmap(self, dt: float) -> list:
        scenario = getattr(self, "scenario", None)
        if scenario is not None and hasattr(scenario, "update_heatmap"):
            active_uavs = list(self.broker.danh_sach_tac_nhan.values())
            return scenario.update_heatmap(dt, active_uavs=active_uavs)
        return []

    def trigger_fire(self, lat: float, lon: float, radius_m: float = 800.0) -> list:
        scenario = getattr(self, "scenario", None)
        if scenario is not None and hasattr(scenario, "trigger_fire"):
            return scenario.trigger_fire(lat, lon, radius_m=radius_m)
        return []

    async def kich_hoat_su_co_khan_cap(
        self,
        loai_su_co: str,
        vi_do: float,
        kinh_do: float,
        ban_kinh_m: float = 800.0,
    ) -> dict:
        ma_phien = str(uuid.uuid4())
        fire_lat = float(vi_do)
        fire_lon = float(kinh_do)

        fire_zone_id = None
        if hasattr(self.scenario, "patrol_zones") and isinstance(self.scenario.patrol_zones, dict):
            for feat in self.scenario.patrol_zones.get("features", []):
                if self.scenario._diem_nam_trong_vung(fire_lat, fire_lon, feat):
                    fire_zone_id = feat.get("properties", {}).get("zone_id")
                    break

        affected_nodes = self.trigger_fire(fire_lat, fire_lon, radius_m=ban_kinh_m)
        if not fire_zone_id and affected_nodes:
            fire_zone_id = getattr(affected_nodes[0], "zone_id", None)

        so_diem_nong = len(affected_nodes)
        swarm_record = None
        if self.ground_station_server and hasattr(self.ground_station_server, "khoi_tao_dam_chay_unverified"):
            swarm_record = await self.ground_station_server.khoi_tao_dam_chay_unverified(
                ma_su_co=ma_phien,
                vi_do=fire_lat,
                kinh_do=fire_lon,
                zone_id=fire_zone_id,
                so_diem_nong=so_diem_nong,
            )

        await self.broker.dong_bo_trang_thai_toan_mang()
        swarm_state = (
            self.ground_station_server.xuat_trang_thai_phan_ung()
            if self.ground_station_server
            else None
        )
        return {
            "thanh_cong": True,
            "ma_phien": ma_phien,
            "loai_su_co": loai_su_co,
            "vi_do": fire_lat,
            "kinh_do": fire_lon,
            "zone_id": fire_zone_id,
            "trang_thai_chay": "UNVERIFIED",
            "so_diem_nhiet_kich_hoat": so_diem_nong,
            "affected_nodes": [n.to_dict() if hasattr(n, "to_dict") else dict(n) for n in affected_nodes],
            "swarm_response": swarm_state,
            "swarm_record": swarm_record,
            "thong_bao": f"[HEATMAP_ANOMALY] Đã khởi tạo đám cháy [{loai_su_co}] tại ({fire_lat:.4f}, {fire_lon:.4f}) thuộc {fire_zone_id or 'N/A'}.",
        }

    async def xoa_tat_ca_dam_chay(self) -> dict:
        so_dam_chay = int(self.scenario.clear_active_fires()) if hasattr(self.scenario, "clear_active_fires") else 0
        self.broker.muc_tieu_chay_da_khoa.clear()
        self.broker.tuyen_e2e_chay_hien_tai = None
        self.broker.danh_sach_tuyen_e2e_chay = []
        if isinstance(self.broker.tuyen_e2e_gan_nhat, dict) and self.broker.tuyen_e2e_gan_nhat.get("is_fire_route"):
            self.broker.tuyen_e2e_gan_nhat = None

        for agent in list(self.broker.danh_sach_tac_nhan.values()):
            if getattr(agent, "loai_tac_nhan", None) != "MÁY_BAY":
                continue
            had_containment = getattr(agent, "_tam_khoanh_vung_chay", None) is not None
            uav_state_val = getattr(getattr(agent, "uav_state", None), "value", None)
            if hasattr(agent, "_ket_thuc_khoanh_vung_chay"):
                agent._ket_thuc_khoanh_vung_chay()
            if hasattr(agent, "_su_co_da_nho_dong_doi"):
                agent._su_co_da_nho_dong_doi.clear()
            if hasattr(agent, "_da_phat_hien_dam_chay"):
                agent._da_phat_hien_dam_chay.clear()
            if had_containment or uav_state_val in ("CONTAINING", "RESPONDING"):
                if agent.trang_thai != AgentState.OFFLINE and uav_state_val not in ("RETURNING", "CHARGING", "STANDBY"):
                    agent.chuyen_trang_thai(AgentState.PATROLLING)
                    agent.uav_state = UAVState.PATROLLING

        if self.ground_station_server:
            self.ground_station_server.su_co_hien_tai = None
            self.ground_station_server.danh_sach_su_co.clear()

        self.broker.network_stack.broadcast_command_to_external_clients("RESUME_PATROL")
        await self.broker.phat_su_kien_ra_web("SWARM_RESPONSE_UPDATE", None)
        await self.broker.dong_bo_trang_thai_toan_mang()
        return {
            "thanh_cong": True,
            "so_dam_chay_da_xoa": so_dam_chay,
            "thong_bao": f"Đã xóa {so_dam_chay} đám cháy và giải phóng UAV khoanh vùng.",
        }

    def lay_du_lieu_khoi_tao_ban_do(self) -> dict:
        heatmap_nodes = self.scenario.xuat_heatmap() if hasattr(self.scenario, "xuat_heatmap") else []
        swarm_state = self.ground_station_server.xuat_trang_thai_phan_ung() if self.ground_station_server else None
        fire_routes = list(getattr(self.broker, "danh_sach_tuyen_e2e_chay", []) or [])
        with self.broker.os_kernel.net_state_lock:
            agents_snapshot = list(self.broker.danh_sach_tac_nhan.values())
        return {
            "nodes": [a.xuat_ho_so_trang_thai() for a in agents_snapshot],
            "fleet_blueprint": list(self.fleet_blueprint.values()),
            "uav_waypoints": self.ban_do_waypoints_uav,
            "patrol_zones": self.scenario.patrol_zones,
            "heatmap": heatmap_nodes,
            "heatmap_nodes": heatmap_nodes,
            "active_fires": list(getattr(self.scenario, "active_fires", []) or []),
            "fire_e2e_routes": fire_routes,
            "zone_coverage": self.broker.tinh_thong_ke_phu_song_vung(),
            "deployment_plan": self.scenario.ke_hoach_bo_tri_ban_dau,
            "network_metrics": self.broker.thong_ke_toan_mang,
            "connectivity_metrics": self._chi_so_lien_lac,
            "os_metrics": self.broker.os_kernel.snapshot(),
            "transport_metrics": self.broker.network_stack.snapshot(),
            "swarm_response": swarm_state,
            "last_auction": swarm_state,
            "last_e2e_route": getattr(self.broker, "tuyen_e2e_chay_hien_tai", None) or self.broker.tuyen_e2e_gan_nhat,
            "recent_logs": list(getattr(self.broker, "nhat_ky_gan_day", [])),
            "time_scale": self.time_scale,
            "paused": self.tam_dung,
        }
