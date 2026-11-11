from typing import Optional, Tuple
from core.base_agent import BaseAgent, AgentState


class SatelliteAgent(BaseAgent):
    """
    Tác nhân Vệ tinh Quỹ đạo Thấp (LEO Satellite Agent):
    - Di chuyển liên tục theo mô hình cơ học quỹ đạo SGP4 từ dữ liệu TLE thực tế.
    - Đánh giá đồng thời liên kết Không-Vũ trụ (A2S), Vũ trụ-Mặt đất (S2G) và Liên vệ tinh (ISL).
    - Tự động nạp năng lượng từ pin mặt trời (Solar Panel Harvesting) khi bay trên quỹ đạo.
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
        self._thoi_gian_beacon_tich_luy = 0.0
        self.current_link = "S2G_LINK"
        self.relay_target_id: Optional[str] = None
        self.chuyen_trang_thai(AgentState.IDLE)

        # Đồng bộ ngay tọa độ SGP4 ban đầu khi vừa khởi tạo
        self._dong_bo_toa_do_sgp4(0.0)

    def _dong_bo_toa_do_sgp4(self, bu_thoi_gian: float):
        """Gọi PhysicsEngine để lấy tọa độ WGS84 và vận tốc quỹ đạo thực tế"""
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

    def xuat_ho_so_trang_thai(self) -> dict:
        ho_so = super().xuat_ho_so_trang_thai()
        ho_so["ten_quoc_te"] = self.ten_quoc_te
        ho_so["current_link"] = self.current_link
        ho_so["relay_target_id"] = self.relay_target_id
        return ho_so

    async def cap_nhat_vat_ly(self, dt_giay: float):
        """
        Cập nhật vị trí quỹ đạo theo bước thời gian mô phỏng `dt_giay`,
        đánh giá liên kết S2G / ISL và phát sóng HELLO_BEACON định kỳ.
        """
        if self.trang_thai == AgentState.OFFLINE:
            return

        # Tốc độ quỹ đạo vừa phải để duy trì thời gian phủ sóng trên vùng trời Đà Nẵng
        self.thoi_gian_mo_phong_tich_luy += dt_giay * 2.0
        self._dong_bo_toa_do_sgp4(self.thoi_gian_mo_phong_tich_luy)

        # Thu hoạch năng lượng mặt trời (Solar Array): Nạp ròng +7 Joule/s
        self.pin_hien_tai = min(self.pin_toi_da, self.pin_hien_tai + (7.0 * dt_giay))
        self.don_dep_lang_gieng_het_han()

        # Cập nhật liên kết S2G hoặc ISL xuống trạm mặt đất
        dich_ke_tiep, thong_so, loai_ket_noi = self._tim_duong_xuong_tram_mat_dat()
        self.relay_target_id = dich_ke_tiep
        if dich_ke_tiep and thong_so:
            self.current_link = "S2G_LINK" if loai_ket_noi == "S2G" else "ISL_LINK"
        else:
            self.current_link = "OUT_OF_LOS"
            self.tuyen_ve_tram_cuc_bo = []
            self.chi_phi_ve_tram_cuc_bo = float("inf")
            self.nang_luong_ve_tram_cuc_bo = 0.0
            self.do_tre_ve_tram_cuc_bo_ms = 0.0
            self.dung_luong_ve_tram_cuc_bo_mbps = 0.0

        self._thoi_gian_beacon_tich_luy += dt_giay
        if self._thoi_gian_beacon_tich_luy >= 5.0 and dich_ke_tiep:
            self._thoi_gian_beacon_tich_luy = 0.0
            await self.bo_dinh_tuyen.phat_song_quang_ba(
                self.ma_dinh_danh,
                {
                    "loai_tin": "HELLO_BEACON",
                    "cost_to_sink": self.chi_phi_ve_tram_cuc_bo,
                    "path_to_sink": self.tuyen_ve_tram_cuc_bo,
                    "energy_to_sink_j": self.nang_luong_ve_tram_cuc_bo,
                    "delay_to_sink_ms": self.do_tre_ve_tram_cuc_bo_ms,
                    "capacity_to_sink_mbps": self.dung_luong_ve_tram_cuc_bo_mbps,
                    "kich_thuoc_kb": 1.0
                }
            )

    def _tim_duong_xuong_tram_mat_dat(self) -> Tuple[Optional[str], Optional[dict], str]:
        """
        Quét các Trạm Mặt đất để tìm liên kết S2G tốt nhất (SNR cao nhất).
        Nếu khuất góc ngẩng với mặt đất, tự động tìm Vệ tinh LEO lân cận qua ISL.
        """
        route = self.bo_dinh_tuyen.tim_tuyen_den_tram_mat_dat(
            self.ma_dinh_danh
        )
        if not route or len(route["nodes"]) < 2:
            return None, None, "ISL"

        next_hop = route["nodes"][1]
        neighbor = self.bo_dinh_tuyen.lay_tac_nhan(next_hop)
        if not neighbor:
            return None, None, "ISL"
        channel = self.bo_dinh_tuyen.physics.danh_gia_chat_luong_kenh_truyen(
            pos1=self.toa_do,
            pos2=neighbor.toa_do,
            tan_so_hz=self.radio_config["tan_so_hz"],
            cong_suat_phat_dbm=self.radio_config["cong_suat_phat_dbm"],
            bang_thong_hz=self.radio_config["bang_thong_hz"]
        )
        return (
            next_hop,
            channel,
            "S2G" if neighbor.loai_tac_nhan == "TRẠM_MẶT_ĐẤT" else "ISL"
        )

    async def xu_ly_thong_diep(self, thong_diep: dict):
        loai_tin = thong_diep.get("loai_tin")
        nguoi_gui = thong_diep.get("nguoi_gui")

        if loai_tin == "HELLO_BEACON":
            return

        if loai_tin == "CFP_GOI_THAU":
            if self.trang_thai in (AgentState.LOW_BATTERY, AgentState.OFFLINE, AgentState.BIDDING):
                return
            self.chuyen_trang_thai(AgentState.BIDDING)
            try:
                bao_gia = self.bo_dinh_tuyen.tao_bao_gia(
                    self.ma_dinh_danh,
                    nguoi_gui,
                    thong_diep.get("thong_so_kenh", {}),
                    float(thong_diep.get("kich_thuoc_kb", 250.0))
                )
                if not bao_gia:
                    return
                await self.bo_dinh_tuyen.gui_don_huong(
                    self.ma_dinh_danh,
                    nguoi_gui,
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
                        "kich_thuoc_kb": 2.0
                    }
                )
                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "BID", f"[{self.ma_dinh_danh}] Báo giá tuyến {bao_gia['tuyen']} · chi phí {bao_gia['chi_phi']}."
                )
            finally:
                if self.trang_thai == AgentState.BIDDING:
                    self.chuyen_trang_thai(AgentState.IDLE)

        elif loai_tin in ("GIAO_NHIEM_VU_DU_LIEU", "PROPOSE", "REFUSE"):
            self.chuyen_trang_thai(AgentState.RELAYING)
            try:
                await self._chuyen_tiep_du_lieu(thong_diep)
            finally:
                if self.trang_thai != AgentState.OFFLINE:
                    self.chuyen_trang_thai(AgentState.IDLE)

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
                        f"[{self.ma_dinh_danh}] ACK đã tới UAV khởi phát nhiệm vụ."
                    )

    async def _chuyen_tiep_du_lieu(self, thong_diep: dict):
        tuyen = await self.bo_dinh_tuyen.gui_chang_tiep_ve_tram(
            self.ma_dinh_danh,
            thong_diep
        )
        if not tuyen:
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "WARN", f"[{self.ma_dinh_danh}] Không còn tuyến S2G/ISL khả dụng tới trạm mặt đất."
            )
            return

        self.thong_ke["goi_tin_chuyen_tiep"] += 1
        await self.bo_dinh_tuyen.gui_log_ra_web(
            "RELAY",
            f"🛰️ [{self.ma_dinh_danh}] Chuyển tiếp không gian "
            f"({self.current_link}) tới {tuyen['nodes'][1]}."
        )
