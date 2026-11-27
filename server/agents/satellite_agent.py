from typing import Optional, Tuple
from core.base_agent import BaseAgent, AgentState


class SatelliteAgent(BaseAgent):
    """
    Tác nhân Vệ tinh Quỹ đạo Thấp (LEO Satellite Agent):
    - Di chuyển liên tục theo mô hình cơ học quỹ đạo SGP4 từ dữ liệu TLE thực tế.
    - Đánh giá liên kết Vũ trụ - Mặt đất (S2G_LINK) xuống Trạm Kiểm lâm Sơn Trà để làm trạm chuyển tiếp
      kênh dự phòng cho các UAV bay ngoài vùng phủ A2G (> 3.5 km).
    - Tự động nạp năng lượng từ pin mặt trời (Solar Array) khi bay trên quỹ đạo.
    """

    def __init__(self, bo_dinh_tuyen, ten_quoc_te: str = "", **kwargs):
        kwargs.setdefault("loai_tac_nhan", "VỆ_TINH_LEO")
        kwargs.setdefault("pin_max_joule", 500000.0)
        kwargs.setdefault("vi_do", 0.0)
        kwargs.setdefault("kinh_do", 0.0)
        kwargs.setdefault("do_cao", 550000.0)

        super().__init__(bo_dinh_tuyen=bo_dinh_tuyen, **kwargs)
        self.ten_quoc_te = ten_quoc_te or self.ma_dinh_danh
        self.thoi_gian_mo_phong_tich_luy = 0.0
        self.current_link = "S2G_LINK"
        self.relay_target_id: Optional[str] = None
        self.s2g_metrics: dict = {}
        self.chuyen_trang_thai(AgentState.IDLE)

        self._dong_bo_toa_do_sgp4(0.0)
        self._cap_nhat_lien_ket_s2g()

    def _dong_bo_toa_do_sgp4(self, bu_thoi_gian: float):
        """Gọi PhysicsEngine để lấy tọa độ WGS84 và vận tốc quỹ đạo thực tế."""
        try:
            vi_tri = self.bo_dinh_tuyen.physics.lay_vi_tri_ve_tinh(
                self.ma_dinh_danh, bu_thoi_gian_giay=bu_thoi_gian
            )
            self.toa_do["vi_do"] = vi_tri["vi_do"]
            self.toa_do["kinh_do"] = vi_tri["kinh_do"]
            self.toa_do["do_cao"] = vi_tri["do_cao"]
            self.toa_do["van_toc_m_s"] = vi_tri["van_toc_m_s"]
        except Exception as e:
            print(f"[SGP4 WARN - {self.ma_dinh_danh}] {e}")

    def _cap_nhat_lien_ket_s2g(self) -> Tuple[Optional[str], Optional[dict]]:
        """Đánh giá trực tiếp liên kết S2G xuống Trạm mặt đất gần nhất trong vùng phủ <= 2500 km."""
        cac_tram = [
            ag for ag in self.bo_dinh_tuyen.danh_sach_tac_nhan.values()
            if getattr(ag, "loai_tac_nhan", None) == "TRẠM_MẶT_ĐẤT"
        ]
        if not cac_tram:
            self.relay_target_id = None
            self.current_link = "OUT_OF_RANGE"
            self.tuyen_ve_tram_cuc_bo = []
            self.do_tre_ve_tram_cuc_bo_ms = 0.0
            self.dung_luong_ve_tram_cuc_bo_mbps = 0.0
            self.s2g_metrics = {}
            return None, None

        best_tram = None
        best_ch = None
        for tram in cac_tram:
            ch = self.bo_dinh_tuyen.physics.danh_gia_chat_luong_kenh_truyen(
                pos1=self.toa_do,
                pos2=tram.toa_do,
                tan_so_hz=self.radio_config["tan_so_hz"],
                cong_suat_phat_dbm=self.radio_config["cong_suat_phat_dbm"],
                bang_thong_hz=self.radio_config["bang_thong_hz"],
            )
            if ch.get("kha_dung") and (best_ch is None or ch["snr_db"] > best_ch["snr_db"]):
                best_tram = tram
                best_ch = ch

        if best_tram is not None and best_ch is not None:
            self.relay_target_id = best_tram.ma_dinh_danh
            self.current_link = "S2G_LINK"
            self.tuyen_ve_tram_cuc_bo = [self.ma_dinh_danh, best_tram.ma_dinh_danh]
            self.do_tre_ve_tram_cuc_bo_ms = float(best_ch["do_tre_ms"])
            self.dung_luong_ve_tram_cuc_bo_mbps = float(best_ch["dung_luong_mbps"])
            self.s2g_metrics = best_ch
            return best_tram.ma_dinh_danh, best_ch

        self.relay_target_id = None
        self.current_link = "OUT_OF_RANGE"
        self.tuyen_ve_tram_cuc_bo = []
        self.do_tre_ve_tram_cuc_bo_ms = 0.0
        self.dung_luong_ve_tram_cuc_bo_mbps = 0.0
        self.s2g_metrics = {}
        return None, None

    def xuat_ho_so_trang_thai(self) -> dict:
        ho_so = super().xuat_ho_so_trang_thai()
        ho_so["ten_quoc_te"] = self.ten_quoc_te
        ho_so["current_link"] = self.current_link
        ho_so["relay_target_id"] = self.relay_target_id
        if self.s2g_metrics:
            ho_so["snr_db"] = self.s2g_metrics.get("snr_db", 0.0)
            ho_so["delay_ms"] = self.s2g_metrics.get("do_tre_ms", 0.0)
        return ho_so

    async def cap_nhat_vat_ly(self, dt_giay: float):
        """Cập nhật tọa độ quỹ đạo SGP4, pin mặt trời và trạng thái liên kết S2G."""
        if self.trang_thai == AgentState.OFFLINE:
            return

        self.thoi_gian_mo_phong_tich_luy += dt_giay * 2.0
        self._dong_bo_toa_do_sgp4(self.thoi_gian_mo_phong_tich_luy)
        self.pin_hien_tai = min(self.pin_toi_da, self.pin_hien_tai + (7.0 * dt_giay))
        self._cap_nhat_lien_ket_s2g()

    async def xu_ly_thong_diep(self, thong_diep: dict):
        loai_tin = thong_diep.get("loai_tin")
        if loai_tin in ("FIRE_DETECTION_REPORT", "GIAO_NHIEM_VU_DU_LIEU"):
            self.thong_ke["goi_tin_chuyen_tiep"] += 1
            tram_id, _ = self._cap_nhat_lien_ket_s2g()
            if tram_id:
                tram = self.bo_dinh_tuyen.lay_tac_nhan(tram_id)
                if tram is not None:
                    await tram.hang_doi_thong_diep.put({
                        **thong_diep,
                        "nguoi_gui": self.ma_dinh_danh,
                    })
