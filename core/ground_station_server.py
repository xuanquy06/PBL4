"""CNP auction coordinator for the simulated ground station."""
import asyncio
import time
from typing import Any, Optional


class GroundStationServer:
    def __init__(self, broker: Any):
        self.broker: Any = broker
        self.danh_sach_propose: dict[str, list[dict[str, Any]]] = {}
        self.danh_sach_refuse: dict[str, list[dict[str, Any]]] = {}

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

    def nhan_mission_abort(self, abort_data: dict):
        """Tiếp nhận báo cáo huỷ nhiệm vụ khi UAV gặp sự cố giữa đường bay."""
        ma_phien = abort_data.get("ma_phien_dau_gia")
        uav_id = abort_data.get("uav_id")
        sim_loop = getattr(self.broker, "simulation_loop", None)
        if sim_loop and hasattr(sim_loop, "xu_ly_uav_gap_su_co_nhiem_vu"):
            asyncio.create_task(
                sim_loop.xu_ly_uav_gap_su_co_nhiem_vu(ma_phien, uav_id, abort_data.get("ly_do", ""))
            )

    async def _chuyen_lenh_cho_tac_nhan(self, agent, thong_diep: dict):
        """Đảm bảo mỗi lệnh CNP chỉ được xử lý đúng 1 lần duy nhất bởi tác nhân."""
        if not agent:
            return
        if getattr(agent, "_dang_lang_nghe", False):
            await agent.hang_doi_thong_diep.put(thong_diep.copy())
            return
        await agent.xu_ly_thong_diep(thong_diep.copy())

    async def mo_phien_dau_gia_chay_rung(
        self,
        ma_phien: str,
        thong_diep_cfp: dict,
        thoi_gian_cho_s: float = 0.8,
        exclude_uavs: Optional[set[str]] = None
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
        if exclude_uavs:
            danh_sach = [p for p in danh_sach if (p.get("uav_id") or p.get("nguoi_gui")) not in exclude_uavs]

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

        fire_zone = thong_diep_cfp.get("zone_id")
        def ranking_key(p):
            p_zone = p.get("zone_id")
            # Cùng phân khu cháy xếp ưu tiên tuyệt đối (hạng 0), phân khu khác hạng 1
            zone_rank = 0 if (fire_zone and p_zone == fire_zone) else 1
            return (zone_rank, float(p.get("chi_phi", float("inf"))))

        danh_sach_sorted = sorted(danh_sach, key=ranking_key)
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

        return winner
