import asyncio
import random
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from environment.physics_engine import PhysicsEngine
from environment.scenario_manager import ScenarioManager
from core.message_broker import MessageBroker
from core.network_stack import detect_local_lan_ip
from core.ground_station_server import GroundStationServer
from core.base_agent import AgentState
from agents.ground_station import GroundStationAgent
from agents.satellite_agent import SatelliteAgent
from agents.uav_agent import LOW_BATTERY_THRESHOLD, UAVAgent, UAVState


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

    def _evaluate_client_rf_gateway(
        self,
        agent: UAVAgent,
        protocol: str = "UDP",
        kich_thuoc_kb: float = 2.0,
        probe_pos: Optional[tuple[float, float, float]] = None,
    ) -> dict[str, Any]:
        """Kiểm định gói tin từ Client qua mặt phẳng vô tuyến SAGSINS (Virtual RF Gateway theo mô hình FlyNetSim)."""
        orig_lat = float(agent.toa_do.get("vi_do", 16.125))
        orig_lon = float(agent.toa_do.get("kinh_do", 108.255))
        orig_alt = float(agent.toa_do.get("do_cao", 1050.0))
        if probe_pos is not None:
            agent.toa_do["vi_do"] = float(probe_pos[0])
            agent.toa_do["kinh_do"] = float(probe_pos[1])
            agent.toa_do["do_cao"] = float(probe_pos[2])

        try:
            st_lat, st_lon, st_alt = self._get_station_coords()
            dist_station = self.physics.tinh_khoang_cach_3d(
                agent.toa_do,
                {"vi_do": st_lat, "kinh_do": st_lon, "do_cao": st_alt},
            )
            if dist_station <= 100.0 or agent._dang_o_tai_tram():
                return {
                    "ok": True,
                    "link": "GROUND_LINK",
                    "hops": [agent.ma_dinh_danh, "TRẠM_KIỂM_LÂM_SƠN_TRÀ"],
                    "hop_count": 1,
                    "delay_ms": 0.01,
                    "per": 0.0,
                    "reason": "EXT_UDP_A2G",
                }

            route = self.broker.tim_tuyen_dijkstra_ve_tram(
                agent.ma_dinh_danh,
                kich_thuoc_kb=kich_thuoc_kb,
            )
            if not route or len(route.get("nodes", [])) < 2:
                return {
                    "ok": False,
                    "link": "OUT_OF_RANGE",
                    "hops": [],
                    "hop_count": 0,
                    "delay_ms": 9999.0,
                    "per": 1.0,
                    "reason": "RF_OUT_OF_RANGE_DROP",
                }

            nodes = list(route["nodes"])
            channels = route.get("channels", [])
            delay_ms = round(float(route.get("delay_ms", 0.0)), 2)
            p_ok = 1.0
            for ch in channels:
                p_ok *= max(0.0, 1.0 - float(ch.get("per", 0.0)))
            path_per = round(max(0.0, min(1.0, 1.0 - p_ok)), 4)

            if len(nodes) == 2:
                link, ok_tag = "GROUND_LINK", "EXT_UDP_A2G"
            elif any("VỆ_TINH" in str(n) for n in nodes[1:]):
                link, ok_tag = "LEO_SATELLITE", "EXT_UDP_LEO"
            else:
                link, ok_tag = "FANET_RELAY", "EXT_UDP_FANET"

            if str(protocol).upper() == "UDP" and path_per > 0.0 and random.random() < path_per:
                self.broker.thong_ke_toan_mang["tong_goi_tin_rot_per"] = (
                    self.broker.thong_ke_toan_mang.get("tong_goi_tin_rot_per", 0) + 1
                )
                return {
                    "ok": False,
                    "link": link,
                    "hops": nodes,
                    "hop_count": len(nodes) - 1,
                    "delay_ms": delay_ms,
                    "per": path_per,
                    "reason": "RF_PER_DROP",
                }

            return {
                "ok": True,
                "link": link,
                "hops": nodes,
                "hop_count": len(nodes) - 1,
                "delay_ms": delay_ms,
                "per": path_per,
                "reason": ok_tag,
            }
        finally:
            if probe_pos is not None:
                agent.toa_do["vi_do"] = orig_lat
                agent.toa_do["kinh_do"] = orig_lon
                agent.toa_do["do_cao"] = orig_alt

    def _handle_external_tcp_cmd(self, pkt, payload: dict, addr: tuple[str, int]) -> dict:
        """Xử lý các bản tin điều khiển tin cậy từ tiến trình `uav_client.py` ngoài (trên Thread riêng của Client)."""
        cmd = str(payload.get("cmd", "")).upper()
        uav_id = self._normalize_ext_uav_id(payload.get("uav_id"))
        st_lat, st_lon, _ = self._get_station_coords()

        if cmd == "JOIN":
            pid = payload.get("pid")
            addr_str = f"{addr[0]}:{addr[1]}"

            with self.broker.os_kernel.net_state_lock:
                agent = self.broker.danh_sach_tac_nhan.get(uav_id)
                if agent is not None and isinstance(agent, UAVAgent):
                    is_takeover = True
                    agent._is_fleet_uav = getattr(agent, "_is_fleet_uav", "EXT" not in uav_id.upper())
                    zone_id_eff = agent.zone_id or getattr(agent, "home_zone_id", None) or "ZONE_03"
                    c_lat, c_lon = self._get_zone_center(zone_id_eff)
                    if agent.uav_state == UAVState.OFFLINE and agent.pin_hien_tai > 0.0:
                        if agent._dang_o_tai_tram():
                            agent.chuyen_trang_thai(AgentState.IDLE)
                            agent.uav_state = UAVState.STANDBY
                        else:
                            agent.chuyen_trang_thai(AgentState.PATROLLING)
                            agent.uav_state = UAVState.PATROLLING
                    agent.is_external_client = True
                    agent.client_pid = pid
                    agent.client_addr = addr_str
                    agent.last_external_heartbeat = time.time()
                else:
                    is_takeover = False
                    zone_id_eff = str(payload.get("zone_id") or "ZONE_03")
                    c_lat, c_lon = self._get_zone_center(zone_id_eff)
                    raw_lat = payload.get("lat")
                    raw_lon = payload.get("lon")
                    raw_alt = payload.get("alt")
                    raw_bat = payload.get("battery")
                    init_lat = float(raw_lat) if raw_lat is not None else c_lat
                    init_lon = float(raw_lon) if raw_lon is not None else c_lon
                    init_alt = float(raw_alt) if raw_alt is not None else 1050.0
                    init_bat = float(raw_bat) if raw_bat is not None else 90.0
                    agent = UAVAgent(
                        ma_dinh_danh=uav_id,
                        bo_dinh_tuyen=self.broker,
                        vi_do=init_lat,
                        kinh_do=init_lon,
                        do_cao=init_alt,
                        pin_khoi_tao_pct=init_bat,
                        zone_id=zone_id_eff,
                        is_external_client=True,
                        client_pid=pid,
                        client_addr=addr_str,
                    )
                    agent._is_fleet_uav = False
                    agent.chuyen_trang_thai(AgentState.PATROLLING)
                    agent.uav_state = UAVState.PATROLLING

                agent.evaluate_communication_link()
                rf_info = self._evaluate_client_rf_gateway(agent, protocol="TCP")
                agent.current_link = rf_info["link"]
                agent._last_pushed_rf_link = rf_info["link"]
                cur_state = getattr(getattr(agent, "uav_state", None), "value", "PATROLLING")
                if getattr(agent, "_toa_do_ve_vung", None) is not None and cur_state == "PATROLLING":
                    cur_state = "TRANSITING"
                ack_lat = float(agent.lat)
                ack_lon = float(agent.lon)
                ack_alt = float(agent.toa_do.get("do_cao", 1050.0))
                ack_bat = float(agent.phan_tram_pin)
                ack_cap_j = float(agent.pin_toi_da)
                ack_pow_w = float(getattr(agent, "cong_suat_bay_w", 80.0))
                ack_ip = str(agent.ip_address)
                ack_home_zone = getattr(agent, "home_zone_id", None) or zone_id_eff

            if self._main_loop and self._main_loop.is_running() and not getattr(agent, "_dang_lang_nghe", False):
                def _spawn_listener(ag=agent):
                    setattr(ag, "_dang_lang_nghe", True)
                    self._cac_tac_vu_nen.append(asyncio.create_task(ag.lang_nghe_lien_tuc()))
                self._main_loop.call_soon_threadsafe(_spawn_listener)

            mode_desc = "tiếp quản UAV đội hình" if is_takeover else "bổ sung UAV ngoài"
            self._schedule_web_log(
                "INFO",
                f"[CLIENT_NET] 🛰️ Client [{uav_id}] (PID:{pid}, {addr_str} -> {ack_ip}) "
                f"{mode_desc} qua TCP :{self.broker.network_stack.tcp_port} tại {zone_id_eff} (RF={rf_info['link']})."
            )
            return {
                "ok": True,
                "cmd": "JOIN_ACK",
                "uav_id": uav_id,
                "is_takeover": is_takeover,
                "assigned_ip": ack_ip,
                "udp_port": self.broker.network_stack.udp_port,
                "zone_id": zone_id_eff,
                "home_zone_id": ack_home_zone,
                "zone_center": [c_lat, c_lon],
                "station": [st_lat, st_lon],
                "lat": ack_lat,
                "lon": ack_lon,
                "alt": ack_alt,
                "battery": round(ack_bat, 2),
                "battery_capacity_j": ack_cap_j,
                "power_w": ack_pow_w,
                "state": cur_state,
                "paused": bool(self.tam_dung),
                "time_scale": float(self.time_scale),
                "rf_link": rf_info["link"],
                "rf_hops": rf_info["hops"],
                "rf_delay_ms": rf_info["delay_ms"],
                "rf_per": rf_info["per"],
            }

        if cmd == "FIRE_ALERT":
            agent = self.broker.lay_tac_nhan(uav_id)
            zone_id = str(payload.get("zone_id") or (getattr(agent, "zone_id", None) if agent else None) or "ZONE_03")
            c_lat, c_lon = self._get_zone_center(zone_id)
            raw_f_lat = payload.get("lat")
            raw_f_lon = payload.get("lon")
            f_lat = float(raw_f_lat) if raw_f_lat is not None else c_lat
            f_lon = float(raw_f_lon) if raw_f_lon is not None else c_lon

            if isinstance(agent, UAVAgent):
                rf_info = self._evaluate_client_rf_gateway(agent, protocol="TCP", kich_thuoc_kb=250.0)
            else:
                rf_info = {
                    "ok": True,
                    "link": "GROUND_LINK",
                    "hops": [uav_id, "TRẠM_KIỂM_LÂM_SƠN_TRÀ"],
                    "hop_count": 1,
                    "delay_ms": 0.01,
                    "per": 0.0,
                }

            if not rf_info["ok"]:
                self._schedule_web_log(
                    "WARN",
                    f"[RF_GATEWAY] 📵 Chặn báo cháy TCP từ [{uav_id}] do ngoài vùng phủ sóng A2G/FANET/LEO (OUT_OF_RANGE)."
                )
                return {
                    "ok": False,
                    "cmd": "FIRE_NACK",
                    "error": "Mất sóng vô tuyến (OUT_OF_RANGE), không tìm thấy tuyến A2G/FANET/LEO về Trạm.",
                    "rf_link": "OUT_OF_RANGE",
                }

            if isinstance(agent, UAVAgent):
                with self.broker.os_kernel.net_state_lock:
                    agent._tam_khoanh_vung_chay = {
                        "vi_do": f_lat,
                        "kinh_do": f_lon,
                        "ma_phien": f"FIRE_{uav_id}",
                    }
                    agent.trang_thai = AgentState.PATROLLING
                    agent.uav_state = UAVState.CONTAINING
                    agent.current_link = rf_info["link"]
                    agent.tuyen_ve_tram_cuc_bo = rf_info["hops"]
                    agent.do_tre_ve_tram_cuc_bo_ms = rf_info["delay_ms"]

            if self._main_loop and self._main_loop.is_running():
                async def _process_client_fire():
                    res = await self.kich_hoat_su_co_khan_cap("CHÁY_RỪNG", f_lat, f_lon)
                    ma_phien = res.get("ma_phien") or f"FIRE_{uav_id}"
                    if isinstance(agent, UAVAgent):
                        if hasattr(self.broker, "khoa_muc_tieu_chay"):
                            self.broker.khoa_muc_tieu_chay(f_lat, f_lon, uav_id)
                        if hasattr(agent, "_tam_khoanh_vung_chay") and isinstance(agent._tam_khoanh_vung_chay, dict):
                            agent._tam_khoanh_vung_chay["ma_phien"] = ma_phien
                        if self.ground_station_server and hasattr(self.ground_station_server, "ghi_nhan_khoa_muc_tieu"):
                            await self.ground_station_server.ghi_nhan_khoa_muc_tieu(
                                f_lat,
                                f_lon,
                                uav_id,
                                float(agent.phan_tram_pin),
                                zone_id=zone_id,
                                la_p2p=False,
                                uav_phat_hien_goc=uav_id,
                            )
                        await self.broker.gui_goi_tin_da_chang_ve_tram(
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
                f"[CLIENT_NET] 🔥 Client [{uav_id}] ({addr[0]}:{addr[1]}) báo cháy tại ({f_lat:.4f}, {f_lon:.4f}) "
                f"qua {rf_info['link']} ({rf_info['hop_count']} hops, {rf_info['delay_ms']:.2f} ms)."
            )
            return {
                "ok": True,
                "cmd": "FIRE_ACK",
                "rf_link": rf_info["link"],
                "rf_hops": rf_info["hops"],
                "rf_hop_count": rf_info["hop_count"],
                "rf_delay_ms": rf_info["delay_ms"],
            }

        if cmd == "LEAVE":
            agent = self.broker.lay_tac_nhan(uav_id)
            if isinstance(agent, UAVAgent):
                is_fleet = bool(getattr(agent, "_is_fleet_uav", "EXT" not in uav_id.upper()))
                with self.broker.os_kernel.net_state_lock:
                    agent.is_external_client = False
                    agent.client_pid = None
                    agent.client_addr = None
                    if is_fleet:
                        agent.zone_id = getattr(agent, "home_zone_id", None) or agent.zone_id
                        if agent.pin_hien_tai <= 0.0:
                            agent.toa_do["van_toc_m_s"] = 0.0
                            agent.chuyen_trang_thai(AgentState.OFFLINE)
                            agent.uav_state = UAVState.OFFLINE
                        elif agent._dang_o_tai_tram() or self.physics.tinh_khoang_cach_3d(
                            agent.toa_do,
                            {"vi_do": st_lat, "kinh_do": st_lon, "do_cao": agent.toa_do.get("do_cao", 25.0)},
                        ) <= 100.0:
                            if agent.phan_tram_pin >= 100.0:
                                agent.chuyen_trang_thai(AgentState.IDLE)
                                agent.uav_state = UAVState.STANDBY
                            else:
                                agent.chuyen_trang_thai(AgentState.CHARGING)
                                agent.uav_state = UAVState.CHARGING
                        elif agent.uav_state == UAVState.STANDBY or agent.trang_thai == AgentState.OFFLINE:
                            agent.chuyen_trang_thai(AgentState.PATROLLING)
                            agent.uav_state = UAVState.PATROLLING
                            agent.toa_do["van_toc_m_s"] = 40.0
                        agent.evaluate_communication_link()
                    else:
                        agent.toa_do["van_toc_m_s"] = 0.0
                        agent.chuyen_trang_thai(AgentState.OFFLINE)
                        agent.uav_state = UAVState.OFFLINE
                if is_fleet:
                    self._schedule_web_log(
                        "INFO",
                        f"[CLIENT_NET] 👋 Client [{uav_id}] rời mạng lịch sự (LEAVE) ➔ Bàn giao lại quyền bay tự động cho Server."
                    )
                else:
                    self._schedule_web_log(
                        "WARN",
                        f"[CLIENT_NET] 👋 Client [{uav_id}] rời mạng lịch sự (LEAVE) ➔ OFFLINE."
                    )
            return {"ok": True, "cmd": "LEAVE_ACK"}

        return {"ok": False, "error": f"Unknown cmd: {cmd}"}

    def _handle_external_udp_telemetry(self, pkt, payload: dict, addr: tuple[str, int]) -> str:
        uav_id = self._normalize_ext_uav_id(payload.get("uav_id"))
        agent = self.broker.lay_tac_nhan(uav_id)
        if not isinstance(agent, UAVAgent):
            return "EXT_UDP_UNKNOWN"

        req_state = str(payload.get("state", "")).upper()
        if req_state == "OFFLINE":
            with self.broker.os_kernel.net_state_lock:
                agent.toa_do["van_toc_m_s"] = 0.0
                agent.pin_hien_tai = 0.0
                agent._tam_khoanh_vung_chay = None
                agent.chuyen_trang_thai(AgentState.OFFLINE)
                agent.uav_state = UAVState.OFFLINE
                agent.last_external_heartbeat = time.time()
            return "EXT_UDP_OFFLINE"

        new_lat = float(payload["lat"]) if payload.get("lat") is not None else float(agent.lat)
        new_lon = float(payload["lon"]) if payload.get("lon") is not None else float(agent.lon)
        new_alt = float(payload["alt"]) if payload.get("alt") is not None else float(agent.toa_do.get("do_cao", 1050.0))

        with self.broker.os_kernel.net_state_lock:
            rf_info = self._evaluate_client_rf_gateway(
                agent,
                protocol="UDP",
                kich_thuoc_kb=2.0,
                probe_pos=(new_lat, new_lon, new_alt),
            )
            if not rf_info["ok"]:
                if rf_info["reason"] == "RF_OUT_OF_RANGE_DROP":
                    agent.current_link = "OUT_OF_RANGE"
                    now_ts = time.time()
                    prev_link = getattr(agent, "_last_pushed_rf_link", None)
                    last_oor = float(getattr(agent, "_last_oor_push_ts", 0.0))
                    if prev_link != "OUT_OF_RANGE" or (now_ts - last_oor) >= 5.0:
                        agent._last_pushed_rf_link = "OUT_OF_RANGE"
                        agent._last_oor_push_ts = now_ts
                        self.broker.network_stack.send_command_to_external_client(
                            agent.ma_dinh_danh,
                            "RF_STATUS",
                            {"link": "OUT_OF_RANGE", "hops": [], "hop_count": 0, "delay_ms": 9999.0, "per": 1.0},
                        )
                return str(rf_info["reason"])

            agent.lat = new_lat
            agent.lon = new_lon
            agent.toa_do["do_cao"] = new_alt
            if payload.get("speed") is not None:
                agent.toa_do["van_toc_m_s"] = float(payload["speed"])
            if payload.get("battery") is not None:
                bat_pct = min(100.0, max(0.0, float(payload["battery"])))
                agent.pin_hien_tai = agent.pin_toi_da * bat_pct / 100.0

            if req_state == "RETURNING":
                agent._tam_khoanh_vung_chay = None
                agent.chuyen_trang_thai(AgentState.RETURNING)
                agent.uav_state = UAVState.RETURNING
            elif req_state == "CHARGING":
                agent._tam_khoanh_vung_chay = None
                agent.chuyen_trang_thai(AgentState.CHARGING)
                agent.uav_state = UAVState.CHARGING
            elif req_state == "STANDBY":
                agent._tam_khoanh_vung_chay = None
                agent.chuyen_trang_thai(AgentState.IDLE)
                agent.uav_state = UAVState.STANDBY
            elif req_state in ("PATROLLING", "TRANSITING"):
                agent._tam_khoanh_vung_chay = None
                agent.chuyen_trang_thai(AgentState.PATROLLING)
                agent.uav_state = UAVState.PATROLLING
            elif req_state == "RESPONDING":
                agent._tam_khoanh_vung_chay = None
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

            agent.current_link = rf_info["link"]
            agent.in_comm_range = (rf_info["link"] == "GROUND_LINK")
            agent.direct_los = agent.in_comm_range
            agent.relay_required = not agent.in_comm_range
            agent.relay_target_id = rf_info["hops"][1] if len(rf_info["hops"]) >= 2 and rf_info["link"] != "GROUND_LINK" else None
            agent.tuyen_ve_tram_cuc_bo = rf_info["hops"]
            agent.do_tre_ve_tram_cuc_bo_ms = rf_info["delay_ms"]
            agent.last_external_heartbeat = time.time()

            pkt_cnt = int(getattr(agent, "_rf_udp_cnt", 0)) + 1
            agent._rf_udp_cnt = pkt_cnt
            prev_link = getattr(agent, "_last_pushed_rf_link", None)
            if prev_link != rf_info["link"] or pkt_cnt % 6 == 0:
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
                    },
                )
            return str(rf_info["reason"])

    def _handle_external_disconnect(self, uav_id: str, addr: tuple[str, int]):
        norm_id = self._normalize_ext_uav_id(uav_id)
        agent = self.broker.lay_tac_nhan(norm_id)
        if isinstance(agent, UAVAgent):
            is_fleet = bool(getattr(agent, "_is_fleet_uav", "EXT" not in norm_id.upper()))
            with self.broker.os_kernel.net_state_lock:
                agent.is_external_client = False
                agent.client_pid = None
                agent.client_addr = None
                agent._tam_khoanh_vung_chay = None
                agent.toa_do["van_toc_m_s"] = 0.0
                agent.chuyen_trang_thai(AgentState.OFFLINE)
                agent.uav_state = UAVState.OFFLINE
            self._schedule_web_log(
                "WARN",
                f"[CLIENT_NET] 🔌 Client [{norm_id}] ({addr[0]}:{addr[1]}) mất kết nối TCP đột ngột ➔ OFFLINE."
            )
            if is_fleet and self._main_loop and self._main_loop.is_running():
                asyncio.run_coroutine_threadsafe(agent._goi_uav_du_bi_thay_ca(), self._main_loop)

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

            if loai == "TRẠM_MẶT_ĐẤT":
                GroundStationAgent(**node_cfg, bo_dinh_tuyen=self.broker)
            elif loai == "MÁY_BAY":
                uav_ag = UAVAgent(
                    **node_cfg,
                    bo_dinh_tuyen=self.broker
                )
                uav_ag._is_fleet_uav = True

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
        """Khởi chạy lõi HĐH/Socket thật, vòng lặp lắng nghe của từng Agent và vòng lặp Vật lý trung tâm"""
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
        print("[WEB-UI] Bản đồ: http://127.0.0.1:8000/ | Dashboard: http://127.0.0.1:8000/dashboard")
        print(f"[WEB-UI] Truy cập qua LAN: http://{lan_ip}:8000/ | http://{lan_ip}:8000/dashboard")
        print(f"[CLIENT-SERVER] TCP Server: 0.0.0.0:{t_port} | UDP Server: 0.0.0.0:{u_port} (Mode: {self.broker.network_stack.socket_mode})")
        if t_port > 0:
            print(
                f"[CLIENT-SERVER] Tiếp quản UAV đội hình: python uav_client.py --host {lan_ip} "
                f"--tcp-port {t_port} --udp-port {u_port} --id UAV_09 "
                f"(hoặc --id UAV_25 để thử nghiệm FANET/LEO từ ZONE_07)"
            )

        with self.broker.os_kernel.net_state_lock:
            tac_nhan_init = list(self.broker.danh_sach_tac_nhan.values())
        for tac_nhan in tac_nhan_init:
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
        self.broker.network_stack.stop()
        self.broker.os_kernel.stop()

    async def dieu_khien_thoi_gian(
        self,
        time_scale: Optional[float] = None,
        tam_dung: Optional[bool] = None
    ) -> dict:
        if time_scale is not None:
            self.time_scale = time_scale
        if tam_dung is not None:
            self.tam_dung = tam_dung

        self.broker.network_stack.broadcast_command_to_external_clients(
            "SET_CLOCK",
            {"paused": bool(self.tam_dung), "time_scale": float(self.time_scale)},
        )

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

            try:
                with self.broker.os_kernel.net_state_lock:
                    agents_step = list(self.broker.danh_sach_tac_nhan.values())
                cac_tac_vu_vat_ly = [
                    agent.cap_nhat_vat_ly(dt_mo_phong)
                    for agent in agents_step
                ]
                results = await asyncio.gather(*cac_tac_vu_vat_ly, return_exceptions=True)
                for res in results:
                    if isinstance(res, Exception):
                        print(f"[SIMULATION WARN] Lỗi cập nhật tác nhân: {res}")

                # Cập nhật Heatmap độ bất định theo thời gian thực
                self.update_heatmap(dt_mo_phong)

                self._thoi_gian_tich_luy_kiem_tra_lien_lac += dt_mo_phong
                if self._thoi_gian_tich_luy_kiem_tra_lien_lac >= 5.0:
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

        # Luân phiên gửi telemetry qua broker cho các UAV đang bay trên không (bỏ qua UAV STANDBY/CHARGING tại trạm).
        uav_ung_vien = [
            u for u in cac_uav
            if u.trang_thai not in (AgentState.OFFLINE, AgentState.LOW_BATTERY, AgentState.CHARGING, AgentState.IDLE)
            and not u._dang_o_tai_tram()
            and getattr(getattr(u, "uav_state", None), "value", getattr(u, "uav_state", None)) not in (
                "STANDBY", "CHARGING", "OFFLINE"
            )
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
        """Cách C: Khóa cứng 5 UAV/phân khu — chỉ điều động UAV STANDBY thuộc đúng tổ `home_zone_id` của vùng đó."""
        active_air_states = ("PATROLLING", "RELAYING", "CONTAINING", "RESPONDING")
        standby_pool = [
            u for u in cac_uav
            if u.trang_thai != AgentState.OFFLINE
            and getattr(getattr(u, "uav_state", None), "value", getattr(u, "uav_state", None)) == "STANDBY"
            and u._dang_o_tai_tram()
            and u.phan_tram_pin >= 99.0
        ]

        for idx in range(1, 8):
            zone_id = f"ZONE_{idx:02d}"
            uavs_active_in_zone = [
                u for u in cac_uav
                if getattr(u, "zone_id", None) == zone_id
                and u.trang_thai != AgentState.OFFLINE
                and not u._dang_o_tai_tram()
                and getattr(getattr(u, "uav_state", None), "value", getattr(u, "uav_state", None)) in active_air_states
            ]
            zone_standby = [
                u for u in standby_pool
                if (getattr(u, "home_zone_id", None) or getattr(u, "zone_id", None)) == zone_id
            ]
            zone_standby.sort(key=lambda u: u.ma_dinh_danh)
            while len(uavs_active_in_zone) < 4 and zone_standby:
                u_reserve = zone_standby.pop(0)
                standby_pool.remove(u_reserve)
                u_reserve.zone_id = zone_id
                u_reserve.toa_do["do_cao"] = 1000.0
                u_reserve.toa_do["van_toc_m_s"] = 45.0
                u_reserve.toa_do_tram_sac = None
                u_reserve.toa_do_muc_tieu = None
                if hasattr(u_reserve, "dat_muc_tieu_bay_ve_vung"):
                    u_reserve.dat_muc_tieu_bay_ve_vung()
                u_reserve._yeu_cau_ban_giao_da_phat = False
                u_reserve._da_goi_du_bi_thay_ca = False
                u_reserve._waiting_for_relief_id = None
                u_reserve._da_ban_giao_tai_cho = False
                u_reserve.chuyen_trang_thai(AgentState.PATROLLING)
                if getattr(u_reserve, "is_external_client", False):
                    self.broker.network_stack.send_command_to_external_client(
                        u_reserve.ma_dinh_danh,
                        "RESUME_PATROL",
                        {"zone_id": zone_id, "zone_center": list(self._get_zone_center(zone_id))},
                    )
                uavs_active_in_zone.append(u_reserve)
                await self.broker.gui_log_ra_web(
                    "INFO",
                    f"[P2P_HANDOVER] 🔄 Điều động dự bị [{u_reserve.ma_dinh_danh}] (STANDBY 100%) "
                    f"cất cánh bù quân số cho {zone_id} ({len(uavs_active_in_zone)}/4)."
                )

    async def xu_ly_uav_gap_su_co_nhiem_vu(self, ma_phien: str, uav_id: str, ly_do: str):
        """Giải phóng khóa mục tiêu khi UAV gặp sự cố giữa nhiệm vụ để đồng đội cùng phân khu tiếp ứng."""
        if hasattr(self.broker, "mo_khoa_muc_tieu_chay"):
            self.broker.mo_khoa_muc_tieu_chay(uav_id, ha_nhiet=False)

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
                "ma_phien_su_co": ma_phien,
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
        Cập nhật các điểm nhiệt (HeatNode): làm mát tức thì <= 600m quanh UAV và tăng staleness 0.01/s ngoài tầm quét.
        """
        scenario = getattr(self, "scenario", None)
        if scenario is not None and hasattr(scenario, "update_heatmap"):
            active_uavs = list(self.broker.danh_sach_tac_nhan.values())
            return scenario.update_heatmap(dt, active_uavs=active_uavs)
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
        swarm_record = None
        if hasattr(self, "ground_station_server") and self.ground_station_server is not None:
            if hasattr(self.ground_station_server, "khoi_tao_dam_chay_unverified"):
                swarm_record = await self.ground_station_server.khoi_tao_dam_chay_unverified(
                    ma_su_co=ma_phien,
                    vi_do=fire_lat,
                    kinh_do=fire_lon,
                    zone_id=fire_zone_id,
                    so_diem_nong=so_diem_nong,
                )

        thong_bao_kq = (
            f"[HEATMAP_ANOMALY] Đã khởi tạo đám cháy [{loai_su_co}] ở trạng thái UNVERIFIED tại ({fire_lat:.4f}, {fire_lon:.4f}) "
            f"{f'thuộc {fire_zone_id} ' if fire_zone_id else ''}"
            f"— Đẩy {so_diem_nong} nút Heatmap trong bán kính {ban_kinh_m:.0f}m lên risk_score = 1.0."
        )

        if hasattr(self, "broker") and self.broker is not None:
            if hasattr(self.broker, "dong_bo_trang_thai_toan_mang"):
                await self.broker.dong_bo_trang_thai_toan_mang()

        ground_station_server = getattr(self, "ground_station_server", None)
        swarm_state = (
            ground_station_server.xuat_trang_thai_phan_ung()
            if ground_station_server is not None and hasattr(ground_station_server, "xuat_trang_thai_phan_ung")
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
            "affected_nodes": [
                n.to_dict() if hasattr(n, "to_dict") else dict(n)
                for n in affected_nodes
            ],
            "swarm_response": swarm_state,
            "swarm_record": swarm_record,
            "auction_triggered": False,
            "thong_bao": thong_bao_kq,
        }

    async def xoa_tat_ca_dam_chay(self) -> dict:
        """Xóa toàn bộ đám cháy active_fires theo yêu cầu người dùng và giải phóng các UAV đang khoanh vùng."""
        so_dam_chay = 0
        if hasattr(self.scenario, "clear_active_fires"):
            so_dam_chay = int(self.scenario.clear_active_fires())
        elif hasattr(self.scenario, "active_fires"):
            so_dam_chay = len(self.scenario.active_fires)
            self.scenario.active_fires.clear()

        self.broker.muc_tieu_chay_da_khoa.clear()
        self.broker.tuyen_e2e_chay_hien_tai = None
        self.broker.danh_sach_tuyen_e2e_chay = []
        if isinstance(self.broker.tuyen_e2e_gan_nhat, dict) and self.broker.tuyen_e2e_gan_nhat.get("is_fire_route"):
            self.broker.tuyen_e2e_gan_nhat = None

        for agent in self.broker.danh_sach_tac_nhan.values():
            if getattr(agent, "loai_tac_nhan", None) != "MÁY_BAY":
                continue
            had_containment = getattr(agent, "_tam_khoanh_vung_chay", None) is not None
            uav_state_val = getattr(getattr(agent, "uav_state", None), "value", getattr(agent, "uav_state", None))
            if hasattr(agent, "_ket_thuc_khoanh_vung_chay"):
                agent._ket_thuc_khoanh_vung_chay()
            if hasattr(agent, "_su_co_da_nho_dong_doi"):
                agent._su_co_da_nho_dong_doi.clear()
            if had_containment or uav_state_val in ("CONTAINING", "RESPONDING"):
                if agent.trang_thai.value not in ("NGẮT_KẾT_NỐI", "PIN_YẾU_RTB", "ĐANG_QUAY_VỀ_SẠC", "ĐANG_SẠC"):
                    agent.chuyen_trang_thai(type(agent.trang_thai).PATROLLING)
                    if hasattr(agent, "uav_state"):
                        agent.uav_state = type(agent.uav_state).PATROLLING

        if hasattr(self, "ground_station_server") and self.ground_station_server:
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
        """
        Trả về trạng thái node, waypoint tuần tra, bản đồ nhiệt HeatNode, độ phủ 7 phân khu và chỉ số mạng.
        """
        heatmap_nodes = (
            self.scenario.xuat_heatmap()
            if hasattr(self.scenario, "xuat_heatmap")
            else []
        )
        swarm_state = (
            self.ground_station_server.xuat_trang_thai_phan_ung()
            if hasattr(self, "ground_station_server") and hasattr(self.ground_station_server, "xuat_trang_thai_phan_ung")
            else None
        )
        fire_routes = list(getattr(self.broker, "danh_sach_tuyen_e2e_chay", []) or [])
        with self.broker.os_kernel.net_state_lock:
            agents_snapshot = list(self.broker.danh_sach_tac_nhan.values())
        return {
            "nodes": [a.xuat_ho_so_trang_thai() for a in agents_snapshot],
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
