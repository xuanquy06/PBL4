"""UAV patrol, energy-aware scheduling, and CNP mission behavior."""
import asyncio
import json
import math
import os
import time
import uuid
from enum import Enum
from typing import Dict, List, Optional
from urllib.parse import quote
import websockets
from websockets.exceptions import WebSocketException
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
        ws_server_url: Optional[str] = None,
        **kwargs
    ):
        kwargs.setdefault("loai_tac_nhan", "MÁY_BAY")
        kwargs.setdefault("pin_max_joule", 50000.0)
        kwargs.setdefault("cong_suat_dbm", 35.0)
        kwargs.setdefault("tan_so_hz", 2.4e9)
        kwargs.setdefault("bang_thong_hz", 20e6)
        super().__init__(bo_dinh_tuyen=bo_dinh_tuyen, **kwargs)
        self.waypoints = waypoints or []
        self.waypoint_index = 0
        self.cong_suat_bay_w = max(1.0, float(cong_suat_bay_w))
        self.zone_id = zone_id
        self.nguong_pin_yeu_pct = LOW_BATTERY_THRESHOLD
        self.danh_sach_bao_gia: Dict[str, list[dict]] = {}
        self.so_nhiem_vu_da_nhan = 0
        self.toa_do_muc_tieu: Optional[dict] = None
        self.toa_do_tram_sac: Optional[dict] = None
        self._vung_tuan_tra_phu: Dict[str, list[dict]] = {}
        self._den_muc_tieu = asyncio.Event()
        self._tac_vu_su_co: Optional[asyncio.Task] = None
        self._yeu_cau_ban_giao_da_phat = False
        self.uav_state = UAVState.STANDBY
        self.current_link = "LEO_SATELLITE"
        self.direct_los = False
        self.relay_required = True
        self._relay_uav_id: Optional[str] = None
        self.relay_target_id: Optional[str] = None
        self._relay_requesters: set[str] = set()
        self.ws_server_url = (
            ws_server_url
            or os.getenv("SAGSINS_UAV_WS_URL")
        )
        self._websocket = None
        self._satellite_websocket = None
        self._websocket_receiver_task: Optional[asyncio.Task] = None
        self._satellite_uplink_task: Optional[asyncio.Task] = None
        self._pending_satellite_payload: Optional[dict] = None
        self._websocket_send_lock = asyncio.Lock()
        self._websocket_connect_lock = asyncio.Lock()
        self._websocket_retry_after = {"uav": 0.0, "satellite": 0.0}
        self._websocket_error_reported: set[str] = set()
        self.chuyen_trang_thai(AgentState.PATROLLING)

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
        ho_so["current_link"] = self.current_link
        ho_so["direct_los"] = self.direct_los
        ho_so["relay_required"] = self.relay_required
        ho_so["relay_target_id"] = self.relay_target_id
        if self.toa_do_muc_tieu:
            ho_so["target_lat"] = round(float(self.toa_do_muc_tieu["vi_do"]), 5)
            ho_so["target_lng"] = round(float(self.toa_do_muc_tieu["kinh_do"]), 5)
        return ho_so

    async def cap_nhat_vat_ly(self, dt_giay: float):
        await self.update(dt_giay)

    async def update(self, dt_giay: float):
        if dt_giay <= 0 or self.trang_thai == AgentState.OFFLINE:
            return
        await self._cap_nhat_duong_truyen()
        await self._gui_telemetry_websocket()
        if self.trang_thai == AgentState.CHARGING or (
            self.uav_state == UAVState.STANDBY and self._dang_o_tai_tram()
        ):
            self.pin_hien_tai = min(self.pin_toi_da, self.pin_hien_tai + 300.0 * dt_giay)
            if self.pin_hien_tai >= self.pin_toi_da:
                self._yeu_cau_ban_giao_da_phat = False
                self.chuyen_trang_thai(AgentState.PATROLLING)
                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "INFO", f"[{self.ma_dinh_danh}] Sạc đầy pin, tiếp tục tuần tra."
                )
            return
        if self.phan_tram_pin < LOW_BATTERY_THRESHOLD:
            if self.uav_state != UAVState.RETURNING:
                self.chuyen_trang_thai(AgentState.LOW_BATTERY_RTB)
            if not self._yeu_cau_ban_giao_da_phat:
                await self._phat_yeu_cau_ban_giao_vung()
        if self.trang_thai in (AgentState.LOW_BATTERY_RTB, AgentState.PATROLLING) and (
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
                self._den_muc_tieu.set()
            elif self.waypoints:
                self.waypoint_index = (self.waypoint_index + 1) % len(self.waypoints)
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
                self._den_muc_tieu.set()
            elif self.waypoints:
                self.waypoint_index = (self.waypoint_index + 1) % len(self.waypoints)

    async def _cap_nhat_duong_truyen(self):
        cac_tram = [
            agent for agent in self.bo_dinh_tuyen.danh_sach_tac_nhan.values()
            if agent.loai_tac_nhan == "TRẠM_MẶT_ĐẤT"
        ]
        if not cac_tram:
            self.direct_los = False
            self.relay_required = True
            self.current_link = "LEO_SATELLITE"
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
        return any(
            agent.loai_tac_nhan == "TRẠM_MẶT_ĐẤT"
            and self._khoang_cach_m(
                self.toa_do["vi_do"],
                self.toa_do["kinh_do"],
                agent.toa_do["vi_do"],
                agent.toa_do["kinh_do"]
            ) <= 1.0
            for agent in self.bo_dinh_tuyen.danh_sach_tac_nhan.values()
        )

    def evaluate_communication_link(self, tram_lat: Optional[float] = None, tram_lon: Optional[float] = None) -> str:
        """
        Đánh giá và lựa chọn đường truyền tối ưu dựa trên thuật toán Dijkstra
        (gọi trực tiếp MessageBroker.tim_tuyen_den_tram_mat_dat).
        Gỡ bỏ hoàn toàn các điều kiện rẽ nhánh bằng con số tĩnh (như 7km cho Ground Link, 5km cho Relay).
        Dựa vào chuỗi node trả về từ hàng đợi ưu tiên của MessageBroker, tự động quyết định:
        - GROUND_LINK: Nếu chặng kế tiếp là Trạm mặt đất trực tiếp.
        - FANET_RELAY: Nếu chặng kế tiếp là một UAV khác làm trung chuyển.
        - LEO_SATELLITE: Nếu chặng kế tiếp là vệ tinh LEO hoặc không tìm thấy tuyến mặt đất khả dụng.
        """
        tuyen_dijkstra = self.bo_dinh_tuyen.tim_tuyen_den_tram_mat_dat(self.ma_dinh_danh)
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

        # Fallback khi Dijkstra không tìm được tuyến khả dụng: Chuyển sang vệ tinh LEO
        self._giai_phong_uav_chuyen_tiep()
        self.direct_los = False
        self.relay_required = True
        self.current_link = "LEO_SATELLITE"
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

    def _telemetry_payload(self) -> dict:
        return {
            **self.xuat_ho_so_trang_thai(),
            "direct_los": self.direct_los,
            "relay_required": self.relay_required,
            "relay_target_id": self.relay_target_id,
        }

    async def _gui_telemetry_websocket(self):
        if not self.ws_server_url:
            return
        payload = self._telemetry_payload()
        if self.direct_los:
            if await self._ensure_websocket_connection():
                await self._send_websocket_json(self._websocket, payload)
            return
        if self.current_link == "FANET_RELAY" and self.relay_target_id:
            relay = self.bo_dinh_tuyen.lay_tac_nhan(self.relay_target_id)
            if isinstance(relay, UAVAgent) and await relay._ensure_websocket_connection():
                relay_packet = {
                    "type": "RELAY",
                    "source_id": self.ma_dinh_danh,
                    "relay_target_id": relay.ma_dinh_danh,
                    "payload": payload,
                }
                if await relay._send_websocket_json(relay._websocket, relay_packet):
                    return
            self.current_link = "LEO_SATELLITE"
            self.relay_target_id = None
            payload = self._telemetry_payload()
        self._pending_satellite_payload = payload
        if not self._satellite_uplink_task or self._satellite_uplink_task.done():
            self._satellite_uplink_task = asyncio.create_task(
                self._gui_qua_ve_tinh_sau_do_tre()
            )

    async def _ensure_websocket_connection(self, satellite: bool = False) -> bool:
        async with self._websocket_connect_lock:
            return await self._open_websocket_connection(satellite)

    async def _open_websocket_connection(self, satellite: bool = False) -> bool:
        if not self.ws_server_url:
            return False
        attribute = "_satellite_websocket" if satellite else "_websocket"
        channel = "satellite" if satellite else "uav"
        existing = getattr(self, attribute)
        if existing:
            is_closed = getattr(existing, "closed", None)
            is_open = getattr(getattr(existing, "state", None), "name", None) == "OPEN"
            if is_closed is False or is_open:
                return True
            setattr(self, attribute, None)
        now = time.monotonic()
        if now < self._websocket_retry_after[channel]:
            return False
        server_base = self.ws_server_url.rstrip("/")
        path = "satellite" if satellite else "uav"
        url = f"{server_base.rsplit('/', 1)[0]}/{path}/{quote(self.ma_dinh_danh, safe='')}"
        try:
            websocket = await asyncio.wait_for(
                websockets.connect(url, open_timeout=0.5),
                timeout=0.75
            )
        except (OSError, asyncio.TimeoutError, WebSocketException) as error:
            self._websocket_retry_after[channel] = now + 2.0
            if channel not in self._websocket_error_reported:
                self._websocket_error_reported.add(channel)
                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "WARN",
                    f"[{self.ma_dinh_danh}] Không kết nối được WebSocket "
                    f"{'vệ tinh' if satellite else 'trạm'}: {error}"
                )
            return False

        setattr(self, attribute, websocket)
        self._websocket_error_reported.discard(channel)
        if not satellite:
            self._websocket_receiver_task = asyncio.create_task(
                self._nhan_lenh_websocket(websocket)
            )
        return True

    async def _send_websocket_json(self, websocket, payload: dict) -> bool:
        if not websocket:
            return False
        is_closed = getattr(websocket, "closed", None)
        is_open = getattr(getattr(websocket, "state", None), "name", None) == "OPEN"
        if is_closed is True or (is_closed is None and not is_open):
            return False
        try:
            async with self._websocket_send_lock:
                await websocket.send(json.dumps(payload, ensure_ascii=False))
            return True
        except (OSError, WebSocketException) as error:
            if websocket is self._websocket:
                self._websocket = None
            elif websocket is self._satellite_websocket:
                self._satellite_websocket = None
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "WARN", f"[{self.ma_dinh_danh}] Lỗi gửi WebSocket: {error}"
            )
            return False

    async def _nhan_lenh_websocket(self, websocket):
        try:
            async for raw_message in websocket:
                try:
                    packet = json.loads(raw_message)
                except (TypeError, json.JSONDecodeError) as error:
                    await self.bo_dinh_tuyen.gui_log_ra_web(
                        "WARN", f"[{self.ma_dinh_danh}] Bỏ qua lệnh WebSocket sai JSON: {error}"
                    )
                    continue
                if packet.get("type") != "COMMAND" or not isinstance(packet.get("data"), dict):
                    continue
                try:
                    self.hang_doi_thong_diep.put_nowait(packet["data"])
                except asyncio.QueueFull:
                    await self.bo_dinh_tuyen.gui_log_ra_web(
                        "WARN", f"[{self.ma_dinh_danh}] Hàng đợi đầy, bỏ qua lệnh từ GroundStationServer."
                    )
        except (OSError, WebSocketException) as error:
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "WARN", f"[{self.ma_dinh_danh}] Kết nối WebSocket đã gián đoạn: {error}"
            )
        finally:
            if self._websocket is websocket:
                self._websocket = None

    async def _gui_qua_ve_tinh_sau_do_tre(self):
        while self._pending_satellite_payload:
            payload = self._pending_satellite_payload
            self._pending_satellite_payload = None
            await asyncio.sleep(2.0)
            if not await self._ensure_websocket_connection(satellite=True):
                continue
            await self._send_websocket_json(
                self._satellite_websocket,
                {
                    "type": "SATELLITE_UPLINK",
                    "id": self.ma_dinh_danh,
                    "payload": payload
                }
            )

    async def dong_ket_noi_websocket(self):
        for websocket in (self._websocket, self._satellite_websocket):
            if websocket:
                await websocket.close()
        background_tasks = [
            task for task in (
                self._websocket_receiver_task,
                self._satellite_uplink_task
            )
            if task and not task.done()
        ]
        for task in background_tasks:
            task.cancel()
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)
        self._websocket = None
        self._satellite_websocket = None

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

        if loai_tin == "CFP_FIRE_EVENT":
            # Xử lý kêu gọi thầu sự cố cháy rừng qua CNP
            trang_thai_uav = getattr(self, "uav_state", None)
            if (
                trang_thai_uav not in (UAVState.PATROLLING, UAVState.STANDBY)
                and self.trang_thai not in (AgentState.PATROLLING, AgentState.IDLE)
            ):
                return
            if self._tac_vu_su_co and not self._tac_vu_su_co.done():
                return

            tam_chan = {
                "vi_do": float(thong_diep["vi_do"]),
                "kinh_do": float(thong_diep["kinh_do"]),
                "do_cao": float(self.toa_do.get("do_cao", 1000.0))
            }
            # 1. Khoảng cách tuyến tính đến điểm cháy
            kc_m = self.bo_dinh_tuyen.physics.tinh_khoang_cach_3d(self.toa_do, tam_chan)
            van_toc = max(1.0, float(self.toa_do.get("van_toc_m_s") or 45.0))

            # 2. Mức hao hụt pin dự kiến sau chuyến bay khứ hồi (2 chiều: tới điểm cháy và quay về)
            thoi_gian_bay_khu_hoi_s = (2.0 * kc_m) / van_toc
            nang_luong_tieu_hao_j = thoi_gian_bay_khu_hoi_s * self.cong_suat_bay_w
            pin_con_lai_j = self.pin_hien_tai - nang_luong_tieu_hao_j
            pin_du_kien_pct = (pin_con_lai_j / self.pin_toi_da) * 100.0

            # 3. Điều kiện: Nếu pin sau khi bay khứ hồi vẫn > 25%
            if pin_du_kien_pct > LOW_BATTERY_THRESHOLD:
                # Cost Function dựa trên khoảng cách tuyến tính và hao hụt pin dự kiến
                chi_phi = round(kc_m + (100.0 - self.phan_tram_pin) * 40.0 + self.so_nhiem_vu_da_nhan * 3000.0, 2)
                propose_msg = {
                    "loai_tin": "PROPOSE",
                    "type": "PROPOSE",
                    "ma_phien_dau_gia": thong_diep.get("ma_phien_dau_gia"),
                    "uav_id": self.ma_dinh_danh,
                    "nguoi_gui": self.ma_dinh_danh,
                    "chi_phi": chi_phi,
                    "khoang_cach_m": round(kc_m, 2),
                    "pin_hien_tai_pct": round(self.phan_tram_pin, 2),
                    "pin_du_kien_pct": round(pin_du_kien_pct, 2),
                    "toa_do": self.toa_do,
                    "thoi_diem_gui": time.time()
                }

                # Gửi gói PROPOSE về Trạm mặt đất
                gcs_server = getattr(self.bo_dinh_tuyen, "ground_station_server", None)
                if gcs_server:
                    gcs_server.nhan_propose(propose_msg)

                for agent in self.bo_dinh_tuyen.danh_sach_tac_nhan.values():
                    if agent.loai_tac_nhan == "TRẠM_MẶT_ĐẤT":
                        await self.bo_dinh_tuyen.gui_don_huong(self.ma_dinh_danh, agent.ma_dinh_danh, propose_msg)

                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "BID",
                    f"[{self.ma_dinh_danh}] Gửi PROPOSE cho phiên {thong_diep.get('ma_phien_dau_gia', '')[:8]}: "
                    f"Chi phí={chi_phi}, cự ly={kc_m/1000.0:.2f} km, pin khứ hồi dự kiến={pin_du_kien_pct:.1f}%."
                )

        elif loai_tin == "ACCEPT_PROPOSAL":
            if thong_diep.get("uav_id") != self.ma_dinh_danh:
                return
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "WIN",
                f"🏆 [{self.ma_dinh_danh}] Nhận lệnh ACCEPT_PROPOSAL phiên {thong_diep.get('ma_phien_dau_gia', '')[:8]}! "
                f"Chuyển trạng thái sang RESPONDING để dập lửa."
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
            # Không trúng thầu phiên CNP, tiếp tục nhiệm vụ tuần tra
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
            if thong_diep.get("zone_id") == self.zone_id:
                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "INFO",
                    f"[{self.ma_dinh_danh}] Nhận yêu cầu tiếp quản tuần tra "
                    f"từ {thong_diep.get('nguoi_gui', 'UAV khác')} trong {self.zone_id}."
                )

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
                else:
                    await self.bo_dinh_tuyen.gui_log_ra_web(
                        "SUCCESS",
                        f"[{self.ma_dinh_danh}] Nhận ACK từ {nguoi_gui}; hoàn tất truyền dữ liệu."
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

    async def _thuc_hien_nhiem_vu(self, su_co: dict):
        try:
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "EVENT", f"[{self.ma_dinh_danh}] Bay tới vùng nhiệm vụ để bắt đầu phối hợp."
            )
            khoang_cach = self.bo_dinh_tuyen.physics.tinh_khoang_cach_3d(
                self.toa_do, self.toa_do_muc_tieu
            )
            van_toc = max(1.0, float(self.toa_do_muc_tieu.get("van_toc_m_s", 45.0)))
            timeout = min(1800.0, max(60.0, khoang_cach / van_toc * 2.0 + 30.0))
            deadline = time.monotonic() + timeout
            while not self._den_muc_tieu.is_set():
                if self.trang_thai in (AgentState.OFFLINE, AgentState.LOW_BATTERY):
                    await self.bo_dinh_tuyen.gui_log_ra_web(
                        "WARN", f"[{self.ma_dinh_danh}] Hủy nhiệm vụ do pin dưới ngưỡng an toàn."
                    )
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise asyncio.TimeoutError
                try:
                    await asyncio.wait_for(self._den_muc_tieu.wait(), timeout=min(1.0, remaining))
                except asyncio.TimeoutError:
                    continue
            if self.trang_thai in (AgentState.OFFLINE, AgentState.LOW_BATTERY):
                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "WARN", f"[{self.ma_dinh_danh}] Hủy nhiệm vụ do pin dưới ngưỡng an toàn."
                )
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
        vi_do = float(su_co["vi_do"])
        kinh_do = float(su_co["kinh_do"])
        do_cao = float(self.toa_do["do_cao"])
        delta_lat = 0.018
        delta_lon = delta_lat / max(0.2, abs(math.cos(math.radians(vi_do))))
        self.waypoints = [
            {"vi_do": vi_do - delta_lat, "kinh_do": kinh_do - delta_lon, "do_cao": do_cao, "van_toc_m_s": 45.0},
            {"vi_do": vi_do - delta_lat, "kinh_do": kinh_do + delta_lon, "do_cao": do_cao, "van_toc_m_s": 45.0},
            {"vi_do": vi_do + delta_lat, "kinh_do": kinh_do + delta_lon, "do_cao": do_cao, "van_toc_m_s": 45.0},
            {"vi_do": vi_do + delta_lat, "kinh_do": kinh_do - delta_lon, "do_cao": do_cao, "van_toc_m_s": 45.0}
        ]
        self.waypoint_index = 0

    async def _chon_tram_sac(self):
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
        if self.trang_thai != AgentState.LOW_BATTERY_RTB:
            nguong_quay_ve = max(
                self.pin_toi_da * 0.4,
                energy_to_base * 1.3 + 2500.0,
                energy_to_base
                + self.pin_toi_da * (self.nguong_pin_yeu_pct / 100.0)
                + 1000.0
            )
            if self.pin_hien_tai > nguong_quay_ve:
                return
        self.toa_do_tram_sac = {
            **tram.toa_do,
            "van_toc_m_s": max(20.0, float(self.toa_do.get("van_toc_m_s") or 45.0))
        }
        if self.trang_thai != AgentState.LOW_BATTERY_RTB:
            self.chuyen_trang_thai(AgentState.RETURNING)
        await self.bo_dinh_tuyen.gui_log_ra_web(
            "WARN",
            f"[{self.ma_dinh_danh}] Pin {self.phan_tram_pin}%: quay về "
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
            "waypoints": self.waypoints
        }, set(cac_uav_cung_vung))

    async def _phat_lenh_quang_ba(self, thong_diep: dict, recipient_ids: set[str]):
        server = getattr(self.bo_dinh_tuyen, "ground_station_server", None)
        da_gui_qua_ws = (
            await server.broadcast_command(thong_diep, recipient_ids)
            if server
            else set()
        )
        recipients_not_reached = recipient_ids - da_gui_qua_ws
        if recipients_not_reached:
            await self.bo_dinh_tuyen.phat_song_quang_ba(
                self.ma_dinh_danh,
                thong_diep,
                danh_sach_nguoi_nhan=recipients_not_reached
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
        self.waypoint_index = max(0, min(len(self.waypoints) - 1, self.waypoint_index - so_truoc))

    async def _dau_gia_va_truyen(self, su_co: dict):
        """
        Định tuyến gói tin sự cố về Trạm mặt đất bằng thuật toán Dijkstra
        thông qua MessageBroker.tim_tuyen_den_tram_mat_dat.
        (Đã gỡ bỏ việc dùng sai CNP để tìm relay theo đúng lý thuyết MAS).
        """
        kich_thuoc_kb = float(su_co.get("kich_thuoc_kb", 250.0))
        tuyen_dijkstra = self.bo_dinh_tuyen.tim_tuyen_den_tram_mat_dat(
            self.ma_dinh_danh, kich_thuoc_kb
        )

        if not tuyen_dijkstra or len(tuyen_dijkstra.get("nodes", [])) < 2:
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "WARN", f"[{self.ma_dinh_danh}] Thuật toán Dijkstra không tìm thấy tuyến khả dụng tới trạm mặt đất."
            )
            return

        tuyen_nodes = tuyen_dijkstra["nodes"]
        ma_ke_tiep = tuyen_nodes[1]
        chi_phi = tuyen_dijkstra.get("cost", 0.0)

        await self.bo_dinh_tuyen.gui_log_ra_web(
            "INFO",
            f"[{self.ma_dinh_danh}] Dijkstra xác định tuyến tối ưu: {' ➔ '.join(tuyen_nodes)} "
            f"(Chi phí: {chi_phi}). Bắt đầu truyền dữ liệu sự cố..."
        )

        goi_tin = {
            "loai_tin": "GIAO_NHIEM_VU_DU_LIEU",
            "ma_phien_dau_gia": su_co.get("ma_phien_dau_gia", str(uuid.uuid4())),
            "loai_su_co": su_co.get("loai_su_co", "CHÁY_RỪNG"),
            "toa_do_su_co": {"vi_do": su_co["vi_do"], "kinh_do": su_co["kinh_do"]},
            "thoi_diem_khoi_tao": time.time(),
            "lo_trinh_di_qua": [self.ma_dinh_danh],
            "tuyen_du_kien": tuyen_nodes,
            "nang_luong_du_kien_joule": tuyen_dijkstra.get("energy_j", 0.0),
            "kich_thuoc_kb": kich_thuoc_kb
        }

        da_gui = await self.bo_dinh_tuyen.gui_don_huong(
            self.ma_dinh_danh, ma_ke_tiep, goi_tin
        )
        if da_gui:
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "RELAY", f"[{self.ma_dinh_danh}] Đã chuyển tiếp gói tin tới chặng kế tiếp [{ma_ke_tiep}]."
            )
        else:
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "WARN", f"[{self.ma_dinh_danh}] Không thể gửi gói tin tới chặng kế tiếp [{ma_ke_tiep}]."
            )

    async def _chuyen_tiep_du_lieu(self, thong_diep: dict):
        tuyen = list(thong_diep.get("tuyen_du_kien", []))
        lo_trinh = list(thong_diep.get("lo_trinh_di_qua", []))
        if self.ma_dinh_danh not in lo_trinh:
            lo_trinh.append(self.ma_dinh_danh)
        if self.ma_dinh_danh not in tuyen or tuyen.index(self.ma_dinh_danh) + 1 >= len(tuyen):
            tuyen_moi = self.bo_dinh_tuyen.tim_tuyen_den_tram_mat_dat(
                self.ma_dinh_danh,
                float(thong_diep.get("kich_thuoc_kb", 250.0)),
                loai_tru=set(lo_trinh[:-1])
            )
            tuyen = tuyen_moi["nodes"] if tuyen_moi else []
        for _ in range(2):
            if self.ma_dinh_danh not in tuyen or tuyen.index(self.ma_dinh_danh) + 1 >= len(tuyen):
                break
            ma_ke_tiep = tuyen[tuyen.index(self.ma_dinh_danh) + 1]
            da_gui = await self.bo_dinh_tuyen.gui_don_huong(
                self.ma_dinh_danh,
                ma_ke_tiep,
                {
                    **thong_diep,
                    "loai_tin": "GIAO_NHIEM_VU_DU_LIEU",
                    "lo_trinh_di_qua": lo_trinh,
                    "tuyen_du_kien": tuyen
                }
            )
            if da_gui:
                self.thong_ke["goi_tin_chuyen_tiep"] += 1
                return
            tuyen_moi = self.bo_dinh_tuyen.tim_tuyen_den_tram_mat_dat(
                self.ma_dinh_danh,
                float(thong_diep.get("kich_thuoc_kb", 250.0)),
                loai_tru=set(lo_trinh[:-1])
            )
            tuyen = tuyen_moi["nodes"] if tuyen_moi else []
        await self.bo_dinh_tuyen.gui_log_ra_web(
            "WARN", f"[{self.ma_dinh_danh}] Không còn tuyến relay khả dụng tới trạm mặt đất."
        )
