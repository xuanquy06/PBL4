"""WebSocket server endpoint state for connected SAGSINS UAV clients."""
import asyncio
from typing import Optional

from fastapi import WebSocket, WebSocketDisconnect


class GroundStationServer:
    def __init__(self, broker):
        self.broker = broker
        self.clients: dict[str, WebSocket] = {}
        self.satellite_clients: dict[str, WebSocket] = {}
        self.latest_telemetry: dict[str, dict] = {}
        self.danh_sach_propose: dict[str, list[dict]] = {}
        self._lock = asyncio.Lock()

    def nhan_propose(self, propose_data: dict):
        """Tiếp nhận gói PROPOSE từ UAV nộp thầu cho một phiên sự cố cháy rừng."""
        ma_phien = propose_data.get("ma_phien_dau_gia")
        if ma_phien:
            if ma_phien not in self.danh_sach_propose:
                self.danh_sach_propose[ma_phien] = []
            self.danh_sach_propose[ma_phien].append(propose_data)

    async def mo_phien_dau_gia_chay_rung(
        self,
        ma_phien: str,
        thong_diep_cfp: dict,
        thoi_gian_cho_s: float = 1.5
    ) -> Optional[dict]:
        """
        Bộ đếm thời gian gom tất cả các gói PROPOSE từ các UAV trong thời gian quy định (1.5s).
        Sau 1.5 giây, so sánh điểm chi phí, chọn ra UAV có điểm thấp nhất (tối ưu nhất),
        và gửi lệnh ACCEPT_PROPOSAL đích danh cho UAV đó để nó chuyển trạng thái sang RESPONDING.
        """
        if ma_phien not in self.danh_sach_propose:
            self.danh_sach_propose[ma_phien] = []
        await self.broker.gui_log_ra_web(
            "BID",
            f"📢 [TRẠM MẶT ĐẤT] Khởi tạo CNP phiên {ma_phien[:8]} dập lửa tại "
            f"({thong_diep_cfp['vi_do']:.4f}, {thong_diep_cfp['kinh_do']:.4f}). "
            f"Đang gom báo giá trong {thoi_gian_cho_s}s..."
        )

        # Bộ đếm thời gian 1.5 giây để gom tất cả các gói PROPOSE
        await asyncio.sleep(thoi_gian_cho_s)

        danh_sach = self.danh_sach_propose.pop(ma_phien, [])
        if not danh_sach:
            await self.broker.gui_log_ra_web(
                "WARN",
                f"⚠️ [TRẠM MẶT ĐẤT] Phiên {ma_phien[:8]} kết thúc: Không có UAV nào nộp PROPOSE "
                f"(hoặc không đủ pin khứ hồi > 25%)."
            )
            return None

        # So sánh điểm chi phí, chọn ra UAV có điểm thấp nhất (tối ưu nhất)
        danh_sach_sorted = sorted(danh_sach, key=lambda p: float(p.get("chi_phi", float("inf"))))
        winner = danh_sach_sorted[0]
        winner_id = winner["uav_id"]

        await self.broker.gui_log_ra_web(
            "WIN",
            f"🏆 [TRẠM MẶT ĐẤT] Phiên {ma_phien[:8]}: Trao hợp đồng cho UAV [{winner_id}] "
            f"(Chi phí: {winner['chi_phi']}, cự ly: {winner.get('khoang_cach_m', 0)/1000.0:.2f} km, "
            f"pin dự kiến: {winner.get('pin_du_kien_pct', 0):.1f}%)."
        )

        # Gửi lệnh ACCEPT_PROPOSAL đích danh cho UAV chiến thắng để chuyển sang RESPONDING
        accept_msg = {
            "loai_tin": "ACCEPT_PROPOSAL",
            "ma_phien_dau_gia": ma_phien,
            "uav_id": winner_id,
            "vi_do": thong_diep_cfp["vi_do"],
            "kinh_do": thong_diep_cfp["kinh_do"],
            "loai_su_co": thong_diep_cfp.get("loai_su_co", "CHÁY_RỪNG"),
            "nguoi_gui": "GROUND_STATION"
        }

        winner_agent = self.broker.lay_tac_nhan(winner_id)
        if winner_agent:
            await winner_agent.hang_doi_thong_diep.put(accept_msg.copy())
            if hasattr(winner_agent, "xu_ly_thong_diep"):
                await winner_agent.xu_ly_thong_diep(accept_msg.copy())
        await self.broadcast_command(accept_msg, {winner_id})

        # Gửi REJECT_PROPOSAL cho các ứng viên còn lại
        for loser in danh_sach_sorted[1:]:
            loser_id = loser["uav_id"]
            reject_msg = {
                "loai_tin": "REJECT_PROPOSAL",
                "ma_phien_dau_gia": ma_phien,
                "uav_id": loser_id,
                "nguoi_gui": "GROUND_STATION"
            }
            loser_agent = self.broker.lay_tac_nhan(loser_id)
            if loser_agent:
                await loser_agent.hang_doi_thong_diep.put(reject_msg.copy())
                if hasattr(loser_agent, "xu_ly_thong_diep"):
                    await loser_agent.xu_ly_thong_diep(reject_msg.copy())
            await self.broadcast_command(reject_msg, {loser_id})

        return winner

    async def _accept_uav(self, uav_id: str, websocket: WebSocket) -> bool:
        agent = self.broker.lay_tac_nhan(uav_id)
        if not agent or agent.loai_tac_nhan != "MÁY_BAY":
            await websocket.close(code=1008, reason="UAV không hợp lệ")
            return False
        await websocket.accept()
        return True

    async def connect(self, uav_id: str, websocket: WebSocket) -> bool:
        if not await self._accept_uav(uav_id, websocket):
            return False
        async with self._lock:
            previous = self.clients.get(uav_id)
            self.clients[uav_id] = websocket
        if previous and previous is not websocket:
            await previous.close(code=1001, reason="UAV kết nối lại")
        return True

    async def connect_satellite(self, uav_id: str, websocket: WebSocket) -> bool:
        if not await self._accept_uav(uav_id, websocket):
            return False
        async with self._lock:
            previous = self.satellite_clients.get(uav_id)
            self.satellite_clients[uav_id] = websocket
        if previous and previous is not websocket:
            await previous.close(code=1001, reason="UAV kết nối vệ tinh lại")
        return True

    async def disconnect(self, uav_id: str, websocket: WebSocket):
        async with self._lock:
            if self.clients.get(uav_id) is websocket:
                del self.clients[uav_id]

    async def disconnect_satellite(self, uav_id: str, websocket: WebSocket):
        async with self._lock:
            if self.satellite_clients.get(uav_id) is websocket:
                del self.satellite_clients[uav_id]

    @staticmethod
    def _is_valid_telemetry(payload: object, uav_id: str) -> bool:
        return (
            isinstance(payload, dict)
            and payload.get("id") == uav_id
            and all(
                key in payload
                for key in ("state", "battery", "lat", "lng", "current_link")
            )
        )

    async def receive_telemetry(self, uav_id: str, websocket: WebSocket):
        try:
            while True:
                payload = await websocket.receive_json()
                if not isinstance(payload, dict):
                    await websocket.close(code=1008, reason="Telemetry không hợp lệ")
                    return
                if payload.get("type") in ("PROPOSE", "PROPOSE_BAO_GIA") or payload.get("loai_tin") == "PROPOSE":
                    self.nhan_propose(payload)
                elif payload.get("type") == "RELAY":
                    source_id = payload.get("source_id")
                    relay_id = payload.get("relay_target_id")
                    telemetry = payload.get("payload")
                    if (
                        relay_id != uav_id
                        or not isinstance(source_id, str)
                        or not self._is_valid_telemetry(telemetry, source_id)
                    ):
                        await websocket.close(code=1008, reason="Gói relay không hợp lệ")
                        return
                    relay_agent = self.broker.lay_tac_nhan(uav_id)
                    source_agent = self.broker.lay_tac_nhan(source_id)
                    if (
                        not relay_agent
                        or not getattr(relay_agent, "direct_los", False)
                        or not source_agent
                        or getattr(source_agent, "direct_los", True)
                        or getattr(source_agent, "relay_target_id", None) != uav_id
                    ):
                        continue
                    self.latest_telemetry[source_id] = {
                        **telemetry,
                        "direct_los": False,
                        "relay_required": True,
                        "relay_target_id": uav_id
                    }
                elif payload.get("id") == uav_id:
                    if not self._is_valid_telemetry(payload, uav_id):
                        await websocket.close(code=1008, reason="Telemetry thiếu trường bắt buộc")
                        return
                    agent = self.broker.lay_tac_nhan(uav_id)
                    if not agent or not getattr(agent, "direct_los", False):
                        await websocket.close(code=1008, reason="UAV không có LoS trực tiếp")
                        return
                    self.latest_telemetry[uav_id] = payload
                else:
                    await websocket.close(code=1008, reason="Telemetry không hợp lệ")
                    return
        except WebSocketDisconnect:
            return

    async def receive_satellite_telemetry(self, uav_id: str, websocket: WebSocket):
        try:
            while True:
                payload = await websocket.receive_json()
                if (
                    not isinstance(payload, dict)
                    or payload.get("id") != uav_id
                    or payload.get("type") != "SATELLITE_UPLINK"
                    or not self._is_valid_telemetry(payload.get("payload"), uav_id)
                ):
                    await websocket.close(code=1008, reason="Gói vệ tinh không hợp lệ")
                    return
                self.latest_telemetry[uav_id] = {
                    **payload["payload"],
                    "direct_los": False,
                    "relay_required": True,
                    "current_link": "LEO_SATELLITE",
                    "relay_target_id": None
                }
        except WebSocketDisconnect:
            return

    async def broadcast_command(
        self,
        command: dict,
        recipient_ids: Optional[set[str]] = None
    ) -> set[str]:
        async with self._lock:
            clients = list(self.clients.items())

        sent_to = set()
        for uav_id, websocket in clients:
            if recipient_ids is not None and uav_id not in recipient_ids:
                continue
            agent = self.broker.lay_tac_nhan(uav_id)
            if (
                not agent
                or agent.loai_tac_nhan != "MÁY_BAY"
                or agent.trang_thai.value in ("NGẮT_KẾT_NỐI", "PIN_YẾU_RTB")
            ):
                continue
            try:
                await websocket.send_json({
                    "type": "COMMAND",
                    "data": {
                        **command,
                        "nguoi_gui": command.get("nguoi_gui", "GROUND_STATION")
                    }
                })
            except (WebSocketDisconnect, RuntimeError):
                await self.disconnect(uav_id, websocket)
                continue
            sent_to.add(uav_id)
        return sent_to
