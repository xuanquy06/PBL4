"""WebSocket server endpoint state and CNP Auction Coordinator for SAGSINS."""
import asyncio
import time
from typing import Any, Optional, TypeGuard

from fastapi import WebSocket, WebSocketDisconnect


class GroundStationServer:
    def __init__(self, broker: Any):
        self.broker: Any = broker
        self.clients: dict[str, Any] = {}
        self.satellite_clients: dict[str, Any] = {}
        self.latest_telemetry: dict[str, dict[str, Any]] = {}
        self.danh_sach_propose: dict[str, list[dict[str, Any]]] = {}
        self.danh_sach_refuse: dict[str, list[dict[str, Any]]] = {}
        self._lock = asyncio.Lock()

    def nhan_propose(self, propose_data: dict):
        """Tiếp nhận gói PROPOSE từ UAV nộp thầu (khử trùng lặp theo uav_id trong cùng phiên)."""
        ma_phien = propose_data.get("ma_phien_dau_gia")
        uav_id = propose_data.get("uav_id") or propose_data.get("nguoi_gui")
        if not ma_phien or not uav_id:
            return
        danh_sach = self.danh_sach_propose.setdefault(ma_phien, [])
        for idx, item in enumerate(danh_sach):
            if (item.get("uav_id") or item.get("nguoi_gui")) == uav_id:
                danh_sach[idx] = propose_data
                return
        danh_sach.append(propose_data)

    def nhan_refuse(self, refuse_data: dict):
        """Tiếp nhận gói REFUSE từ UAV từ chối thầu do không đủ năng lượng khứ hồi."""
        ma_phien = refuse_data.get("ma_phien_dau_gia")
        uav_id = refuse_data.get("uav_id") or refuse_data.get("nguoi_gui")
        if not ma_phien or not uav_id:
            return
        danh_sach = self.danh_sach_refuse.setdefault(ma_phien, [])
        for idx, item in enumerate(danh_sach):
            if (item.get("uav_id") or item.get("nguoi_gui")) == uav_id:
                danh_sach[idx] = refuse_data
                return
        danh_sach.append(refuse_data)

    async def _chuyen_lenh_cho_tac_nhan(self, agent, thong_diep: dict):
        """Đảm bảo mỗi lệnh CNP chỉ được xử lý đúng 1 lần duy nhất bởi tác nhân."""
        if not agent:
            return
        if getattr(agent, "_dang_lang_nghe", False):
            await agent.hang_doi_thong_diep.put(thong_diep.copy())
        else:
            await agent.hang_doi_thong_diep.put(thong_diep.copy())
            if hasattr(agent, "xu_ly_thong_diep"):
                await agent.xu_ly_thong_diep(thong_diep.copy())

    async def mo_phien_dau_gia_chay_rung(
        self,
        ma_phien: str,
        thong_diep_cfp: dict,
        thoi_gian_cho_s: float = 0.8
    ) -> Optional[dict]:
        """
        Thu gom các gói PROPOSE và REFUSE từ các UAV trong cửa sổ thời gian đấu giá.
        So sánh điểm hàm chi phí đa mục tiêu, chọn UAV tối ưu nhất và phát ACCEPT_PROPOSAL.
        """
        self.danh_sach_propose.setdefault(ma_phien, [])
        self.danh_sach_refuse.setdefault(ma_phien, [])
        await self.broker.gui_log_ra_web(
            "BID",
            f"📢 [TRẠM MẶT ĐẤT] Khởi tạo phiên CNP #{ma_phien[:8]} tại "
            f"({thong_diep_cfp['vi_do']:.4f}, {thong_diep_cfp['kinh_do']:.4f})..."
        )

        await asyncio.sleep(thoi_gian_cho_s)

        danh_sach = self.danh_sach_propose.pop(ma_phien, [])
        danh_sach_tu_choi = self.danh_sach_refuse.pop(ma_phien, [])

        if not danh_sach:
            ket_qua_rong = {
                "ma_phien": ma_phien,
                "thanh_cong": False,
                "vi_do": thong_diep_cfp["vi_do"],
                "kinh_do": thong_diep_cfp["kinh_do"],
                "thoi_gian": time.strftime("%H:%M:%S"),
                "winner_id": None,
                "proposals": [],
                "refusals": danh_sach_tu_choi
            }
            if hasattr(self.broker, "phat_su_kien_ra_web"):
                await self.broker.phat_su_kien_ra_web("CNP_AUCTION_RESULT", ket_qua_rong)
            await self.broker.gui_log_ra_web(
                "WARN",
                f"⚠️ [TRẠM MẶT ĐẤT] Phiên #{ma_phien[:8]} kết thúc: Không có UAV nào đủ điều kiện "
                f"năng lượng khứ hồi (> 25%)."
            )
            return None

        danh_sach_sorted = sorted(danh_sach, key=lambda p: float(p.get("chi_phi", float("inf"))))
        winner = danh_sach_sorted[0]
        winner_id = winner["uav_id"]

        await self.broker.gui_log_ra_web(
            "WIN",
            f"🏆 [TRẠM MẶT ĐẤT] Phiên #{ma_phien[:8]}: Trao thầu cho [{winner_id}] "
            f"(Chi phí: {winner['chi_phi']}, cự ly: {winner.get('khoang_cach_m', 0)/1000.0:.2f} km, "
            f"pin sau khứ hồi: {winner.get('pin_du_kien_pct', 0):.1f}%)."
        )

        # Phát sự kiện kết quả phiên đấu giá CNP xuống giao diện Web
        ket_qua_phien = {
            "ma_phien": ma_phien,
            "thanh_cong": True,
            "vi_do": thong_diep_cfp["vi_do"],
            "kinh_do": thong_diep_cfp["kinh_do"],
            "thoi_gian": time.strftime("%H:%M:%S"),
            "winner_id": winner_id,
            "winner": winner,
            "proposals": danh_sach_sorted,
            "refusals": danh_sach_tu_choi
        }
        if hasattr(self.broker, "phat_su_kien_ra_web"):
            await self.broker.phat_su_kien_ra_web("CNP_AUCTION_RESULT", ket_qua_phien)

        accept_msg = {
            "loai_tin": "ACCEPT_PROPOSAL",
            "ma_phien_dau_gia": ma_phien,
            "uav_id": winner_id,
            "vi_do": thong_diep_cfp["vi_do"],
            "kinh_do": thong_diep_cfp["kinh_do"],
            "loai_su_co": thong_diep_cfp.get("loai_su_co", "CHÁY_RỪNG"),
            "nguoi_gui": "TRẠM_KIỂM_LÂM_SƠN_TRÀ"
        }

        winner_agent = self.broker.lay_tac_nhan(winner_id)
        await self._chuyen_lenh_cho_tac_nhan(winner_agent, accept_msg)
        if winner_id in self.clients:
            await self.broadcast_command(accept_msg, {winner_id})

        for loser in danh_sach_sorted[1:]:
            loser_id = loser["uav_id"]
            reject_msg = {
                "loai_tin": "REJECT_PROPOSAL",
                "ma_phien_dau_gia": ma_phien,
                "uav_id": loser_id,
                "nguoi_gui": "TRẠM_KIỂM_LÂM_SƠN_TRÀ"
            }
            loser_agent = self.broker.lay_tac_nhan(loser_id)
            await self._chuyen_lenh_cho_tac_nhan(loser_agent, reject_msg)
            if loser_id in self.clients:
                await self.broadcast_command(reject_msg, {loser_id})

        return winner

    async def _accept_uav(self, uav_id: str, websocket: Any) -> bool:
        agent = self.broker.lay_tac_nhan(uav_id)
        if not agent or agent.loai_tac_nhan != "MÁY_BAY":
            await websocket.close(code=1008, reason="UAV không hợp lệ")
            return False
        await websocket.accept()
        return True

    async def connect(self, uav_id: str, websocket: Any) -> bool:
        if not await self._accept_uav(uav_id, websocket):
            return False
        async with self._lock:
            previous = self.clients.get(uav_id)
            self.clients[uav_id] = websocket
        if previous and previous is not websocket:
            await previous.close(code=1001, reason="UAV kết nối lại")
        return True

    async def connect_satellite(self, uav_id: str, websocket: Any) -> bool:
        if not await self._accept_uav(uav_id, websocket):
            return False
        async with self._lock:
            previous = self.satellite_clients.get(uav_id)
            self.satellite_clients[uav_id] = websocket
        if previous and previous is not websocket:
            await previous.close(code=1001, reason="UAV kết nối vệ tinh lại")
        return True

    async def disconnect(self, uav_id: str, websocket: Any):
        async with self._lock:
            if self.clients.get(uav_id) is websocket:
                del self.clients[uav_id]

    async def disconnect_satellite(self, uav_id: str, websocket: Any):
        async with self._lock:
            if self.satellite_clients.get(uav_id) is websocket:
                del self.satellite_clients[uav_id]

    @staticmethod
    def _is_valid_telemetry(payload: object, uav_id: str) -> TypeGuard[dict[str, Any]]:
        return (
            isinstance(payload, dict)
            and payload.get("id") == uav_id
            and all(
                key in payload
                for key in ("state", "battery", "lat", "lng", "current_link")
            )
        )

    async def receive_telemetry(self, uav_id: str, websocket: Any):
        try:
            while True:
                payload = await websocket.receive_json()
                if not isinstance(payload, dict):
                    await websocket.close(code=1008, reason="Telemetry không hợp lệ")
                    return
                if payload.get("type") in ("PROPOSE", "PROPOSE_BAO_GIA") or payload.get("loai_tin") == "PROPOSE":
                    self.nhan_propose(payload)
                elif payload.get("type") in ("REFUSE", "REFUSE_BAO_GIA") or payload.get("loai_tin") == "REFUSE":
                    self.nhan_refuse(payload)
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

    async def receive_satellite_telemetry(self, uav_id: str, websocket: Any):
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
