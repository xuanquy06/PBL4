import time
from core.base_agent import BaseAgent, AgentState


class GroundStationAgent(BaseAgent):
    """
    Tác nhân Trạm Kiểm Lâm Mặt Đất (Ground Control Station - GCS):
    - Đóng vai trò Server trung tâm thu thập Telemetry UDP (:9001) và báo cháy TCP (:9000) từ các UAV Client.
    - Đồng bộ dữ liệu sự cố lên GroundStationServer và WebSocket bản đồ / Dashboard.
    """

    def __init__(self, bo_dinh_tuyen, **kwargs):
        kwargs.setdefault("loai_tac_nhan", "TRẠM_MẶT_ĐẤT")
        kwargs.setdefault("pin_max_joule", 100000000.0)
        super().__init__(bo_dinh_tuyen=bo_dinh_tuyen, **kwargs)
        self.chuyen_trang_thai(AgentState.IDLE)
        self.lich_su_goi_tin_thanh_cong = []
        self.bao_cao_phat_hien_chay: dict[str, dict] = {}
        self.telemetry_uav_moi_nhat = {}

    async def cap_nhat_vat_ly(self, dt_giay: float):
        """Trạm mặt đất đứng cố định, không tiêu hao pin vận hành."""
        return

    async def xu_ly_thong_diep(self, thong_diep: dict):
        loai_tin = thong_diep.get("loai_tin")
        nguoi_gui = thong_diep.get("nguoi_gui")
        thong_so_kenh = thong_diep.get("thong_so_kenh", {})

        if loai_tin == "FIRE_DETECTION_REPORT":
            ma_su_co = thong_diep.get("ma_su_co")
            uav_phat_hien = thong_diep.get("uav_phat_hien")
            lo_trinh = thong_diep.get("lo_trinh_di_qua", [])
            try:
                vi_do = float(thong_diep["vi_do"])
                kinh_do = float(thong_diep["kinh_do"])
            except (KeyError, TypeError, ValueError):
                return

            if not isinstance(ma_su_co, str) or not isinstance(uav_phat_hien, str) or not isinstance(lo_trinh, list):
                return

            bao_cao = {
                "ma_su_co": ma_su_co,
                "uav_phat_hien": uav_phat_hien,
                "nguoi_gui_truc_tiep": nguoi_gui,
                "loai_su_co": thong_diep.get("loai_su_co", "CHÁY_RỪNG"),
                "vi_do": vi_do,
                "kinh_do": kinh_do,
                "thoi_diem_phat_hien": thong_diep.get("thoi_diem_phat_hien"),
                "lo_trinh": list(lo_trinh),
                "thoi_gian_nhan": time.time(),
            }
            self.bao_cao_phat_hien_chay[ma_su_co] = bao_cao
            self.lich_su_goi_tin_thanh_cong.append({
                "ma_phien": ma_su_co,
                "loai_su_co": bao_cao["loai_su_co"],
                "lo_trinh": list(lo_trinh),
                "so_chang_hop": max(1, len(lo_trinh) - 1),
                "tong_do_tre_ms": round(float(thong_so_kenh.get("do_tre_ms", 0.5)), 2),
                "thoi_gian": time.strftime("%H:%M:%S"),
            })
            await self.bo_dinh_tuyen.phat_su_kien_ra_web("FIRE_DETECTION_RECEIVED", bao_cao)
            return

        if loai_tin == "TELEMETRY_REPORT":
            telemetry = thong_diep.get("noi_dung")
            if isinstance(nguoi_gui, str) and isinstance(telemetry, dict):
                self.telemetry_uav_moi_nhat[nguoi_gui] = {
                    **telemetry,
                    "uav_id": nguoi_gui,
                    "lo_trinh": list(thong_diep.get("lo_trinh_di_qua", [])),
                    "thoi_gian_nhan": time.time(),
                }
