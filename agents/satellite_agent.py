import time
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
        # Vệ tinh LEO có nguồn năng lượng lớn (Pin mặt trời + Ắc quy Lithium-Ion)
        kwargs.setdefault("loai_tac_nhan", "VỆ_TINH_LEO")
        kwargs.setdefault("pin_max_joule", 500000.0)
        kwargs.setdefault("vi_do", 0.0)
        kwargs.setdefault("kinh_do", 0.0)
        kwargs.setdefault("do_cao", 550000.0)

        super().__init__(bo_dinh_tuyen=bo_dinh_tuyen, **kwargs)
        self.ten_quoc_te = ten_quoc_te or self.ma_dinh_danh
        self.thoi_gian_mo_phong_tich_luy = 0.0
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

    async def cap_nhat_vat_ly(self, dt_giay: float):
        """
        Cập nhật vị trí quỹ đạo theo bước thời gian mô phỏng `dt_giay`.
        Hệ số nhân thời gian (Time Scale = 10x) giúp quan sát vệ tinh chuyển động trên bản đồ.
        """
        if self.trang_thai == AgentState.OFFLINE:
            return

        # Tăng tốc thời gian quỹ đạo x10 lần để thấy vệ tinh bay qua khu vực trong lúc mô phỏng.
        self.thoi_gian_mo_phong_tich_luy += dt_giay * 10.0
        self._dong_bo_toa_do_sgp4(self.thoi_gian_mo_phong_tich_luy)

        # Mô hình thu hoạch năng lượng mặt trời (Solar Array): Nạp bù +15 Joule/s, tiêu thụ nền -8 Joule/s
        self.pin_hien_tai = min(self.pin_toi_da, self.pin_hien_tai + (7.0 * dt_giay))

    # =========================================================================
    # THUẬT TOÁN TÌM TRẠM MẶT ĐẤT TỐI ƯU HOẶC VỆ TINH CHUYỂN TIẾP (S2G / ISL)
    # =========================================================================
    def _tim_duong_xuong_tram_mat_dat(self) -> Tuple[Optional[str], Optional[dict], str]:
        """
        Quét toàn bộ các Trạm Mặt đất để tìm liên kết S2G tốt nhất (SNR cao nhất).
        Nếu không có Trạm Mặt đất nào trong tầm nhìn thẳng (LoS), tự động tìm Vệ tinh lân cận qua ISL.
        Trả về: (Mã_Node_Đích_Kế_Tiếp, Thông_Số_Kênh, Loại_Liên_Kết: 'S2G' hoặc 'ISL')
        """
        tram_tot_nhat = None
        kenh_s2g_tot_nhat = None
        snr_max = -999.0

        # 1. Ưu tiên tìm đường truyền thẳng xuống Trạm Mặt đất (Space-to-Ground - S2G)
        for ma_node, tac_nhan in self.bo_dinh_tuyen.danh_sach_tac_nhan.items():
            if tac_nhan.loai_tac_nhan == "TRẠM_MẶT_ĐẤT":
                thong_so = self.bo_dinh_tuyen.physics.danh_gia_chat_luong_kenh_truyen(
                    pos1=self.toa_do,
                    pos2=tac_nhan.toa_do,
                    tan_so_hz=self.radio_config["tan_so_hz"],
                    cong_suat_phat_dbm=self.radio_config["cong_suat_phat_dbm"],
                    bang_thong_hz=self.radio_config["bang_thong_hz"]
                )
                if thong_so["kha_dung"] and thong_so["snr_db"] > snr_max:
                    snr_max = thong_so["snr_db"]
                    tram_tot_nhat = ma_node
                    kenh_s2g_tot_nhat = thong_so

        if tram_tot_nhat and kenh_s2g_tot_nhat:
            return tram_tot_nhat, kenh_s2g_tot_nhat, "S2G"

        # 2. Nếu khuất góc ngẩng với mặt đất, tìm Vệ tinh LEO khác qua Liên kết Liên vệ tinh (ISL)
        ve_tinh_ke_tiep = None
        kenh_isl_tot_nhat = None
        snr_isl_max = -999.0

        for ma_node, tac_nhan in self.bo_dinh_tuyen.danh_sach_tac_nhan.items():
            if tac_nhan.loai_tac_nhan == "VỆ_TINH_LEO" and ma_node != self.ma_dinh_danh:
                thong_so_isl = self.bo_dinh_tuyen.physics.danh_gia_chat_luong_kenh_truyen(
                    pos1=self.toa_do,
                    pos2=tac_nhan.toa_do,
                    tan_so_hz=self.radio_config["tan_so_hz"],
                    cong_suat_phat_dbm=self.radio_config["cong_suat_phat_dbm"],
                    bang_thong_hz=self.radio_config["bang_thong_hz"]
                )
                if thong_so_isl["kha_dung"] and thong_so_isl["snr_db"] > snr_isl_max:
                    snr_isl_max = thong_so_isl["snr_db"]
                    ve_tinh_ke_tiep = ma_node
                    kenh_isl_tot_nhat = thong_so_isl

        return ve_tinh_ke_tiep, kenh_isl_tot_nhat, "ISL"

    # =========================================================================
    # XỬ LÝ GIAO THỨC ĐẤU GIÁ CONTRACT NET PROTOCOL & ĐỊNH TUYẾN KHÔNG GIAN
    # =========================================================================
    async def xu_ly_thong_diep(self, thong_diep: dict):
        loai_tin = thong_diep.get("loai_tin")
        nguoi_gui = thong_diep.get("nguoi_gui")
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

        elif loai_tin == "GIAO_NHIEM_VU_DU_LIEU":
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
                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "RELAY", f"[{self.ma_dinh_danh}] Chuyển tiếp dữ liệu tới {ma_ke_tiep}."
                )
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