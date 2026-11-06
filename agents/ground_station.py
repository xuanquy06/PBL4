import time
from core.base_agent import BaseAgent, AgentState


class GroundStationAgent(BaseAgent):
    """
    Tác nhân Trạm Điều khiển Mặt đất (Ground Control Station - GCS):
    - Đóng vai trò Điểm thu thập dữ liệu trung tâm (Data Sink / Gateway) với cost_to_sink = 0.
    - Phát sóng HELLO_BEACON định kỳ để các UAV và Vệ tinh LEO trong vùng phủ sóng
      cập nhật bảng láng giềng cục bộ (Local Neighbor Belief).
    - Tiếp nhận báo giá CNP và tính toán các chỉ số QoS toàn trình (E2E Delay, Hop Count, Route Path).
    """
    def __init__(self, bo_dinh_tuyen, **kwargs):
        kwargs.setdefault("loai_tac_nhan", "TRẠM_MẶT_ĐẤT")
        kwargs.setdefault("pin_max_joule", 100000000.0)
        super().__init__(bo_dinh_tuyen=bo_dinh_tuyen, **kwargs)
        self.chuyen_trang_thai(AgentState.IDLE)
        self.lich_su_goi_tin_thanh_cong = []
        self._thoi_gian_beacon_tich_luy = 0.0

    async def cap_nhat_vat_ly(self, dt_giay: float):
        """
        Trạm mặt đất có tọa độ cố định, định kỳ phát HELLO_BEACON quảng bá trạng thái Sink.
        """
        self.don_dep_lang_gieng_het_han()
        self._thoi_gian_beacon_tich_luy += dt_giay
        if self._thoi_gian_beacon_tich_luy >= 4.0:
            self._thoi_gian_beacon_tich_luy = 0.0
            await self.bo_dinh_tuyen.phat_song_quang_ba(
                self.ma_dinh_danh,
                {
                    "loai_tin": "HELLO_BEACON",
                    "cost_to_sink": 0.0,
                    "path_to_sink": [self.ma_dinh_danh],
                    "kich_thuoc_kb": 1.0
                }
            )

    async def xu_ly_thong_diep(self, thong_diep: dict):
        loai_tin = thong_diep.get("loai_tin")
        nguoi_gui = thong_diep.get("nguoi_gui")
        thong_so_kenh = thong_diep.get("thong_so_kenh", {})

        if loai_tin == "HELLO_BEACON":
            return

        # 1. Trạm mặt đất báo giá nếu UAV gọi CFP_GOI_THAU trực tiếp trong tầm A2G
        if loai_tin == "CFP_GOI_THAU":
            snr_db = thong_so_kenh.get("snr_db", -100.0)
            if snr_db >= 5.0 and thong_so_kenh.get("per", 1.0) < 0.25:
                bao_gia = self.bo_dinh_tuyen.tao_bao_gia(
                    self.ma_dinh_danh,
                    nguoi_gui,
                    thong_so_kenh,
                    float(thong_diep.get("kich_thuoc_kb", 250.0))
                )
                if not bao_gia:
                    return
                await self.bo_dinh_tuyen.gui_don_huong(
                    ma_nguoi_gui=self.ma_dinh_danh,
                    ma_nguoi_nhan=nguoi_gui,
                    thong_diep={
                        "loai_tin": "PROPOSE_BAO_GIA",
                        "ma_phien_dau_gia": thong_diep.get("ma_phien_dau_gia"),
                        "chi_phi": bao_gia["chi_phi"],
                        "loai_ung_vien": "TRẠM_MẶT_ĐẤT",
                        "tuyen": bao_gia["tuyen"],
                        "nang_luong_du_kien_joule": bao_gia["nang_luong_du_kien_joule"],
                        "do_tre_du_kien_ms": bao_gia["do_tre_du_kien_ms"],
                        "dung_luong_du_kien_mbps": bao_gia["dung_luong_du_kien_mbps"],
                        "pin_pct": 100.0,
                        "kich_thuoc_kb": 2.0
                    }
                )

        elif loai_tin == "PROPOSE":
            gcs_server = getattr(self.bo_dinh_tuyen, "ground_station_server", None)
            if gcs_server:
                gcs_server.nhan_propose(thong_diep)

        elif loai_tin in ("REFUSE", "REFUSE_BAO_GIA"):
            gcs_server = getattr(self.bo_dinh_tuyen, "ground_station_server", None)
            if gcs_server:
                gcs_server.nhan_refuse(thong_diep)

        # 2. NHẬN DỮ LIỆU KHẨN CẤP (TỪ UAV TRUYỀN THẲNG HOẶC QUA FANET/VỆ TINH LEO CHUYỂN TIẾP)
        elif loai_tin in ("GIAO_NHIEM_VU_DU_LIEU", "CHUYEN_TIEP_VE_TRAM"):
            self.chuyen_trang_thai(AgentState.RELAYING)

            thoi_diem_goc = thong_diep.get("thoi_diem_khoi_tao", time.time())
            tong_do_tre_e2e_ms = round((time.time() - thoi_diem_goc) * 1000.0, 2)

            lo_trinh = list(thong_diep.get("lo_trinh_di_qua", [nguoi_gui]))
            if self.ma_dinh_danh not in lo_trinh:
                lo_trinh.append(self.ma_dinh_danh)
            chuoi_lo_trinh = " ➔ ".join(lo_trinh)
            so_chang_hop = len(lo_trinh) - 1

            loai_su_co = thong_diep.get("loai_su_co", "CHÁY_RỪNG")
            toa_do_su_co = thong_diep.get("toa_do_su_co", {})

            ban_ghi_ket_qua = {
                "ma_phien": thong_diep.get("ma_phien_dau_gia", "N/A"),
                "loai_su_co": loai_su_co,
                "toa_do_su_co": toa_do_su_co,
                "lo_trinh": lo_trinh,
                "lo_trinh_hop": lo_trinh,
                "so_chang_hop": so_chang_hop,
                "e2e_delay_ms": tong_do_tre_e2e_ms,
                "tong_do_tre_ms": tong_do_tre_e2e_ms,
                "snr_chang_cuoi_db": thong_so_kenh.get("snr_db", 0.0),
                "dung_luong_chang_cuoi_mbps": thong_so_kenh.get("dung_luong_mbps", 0.0),
                "nang_luong_du_kien_joule": thong_diep.get("nang_luong_du_kien_joule", 0.0),
                "thoi_gian": time.strftime("%H:%M:%S"),
                "trang_thai_phu_song": "KET_NOI"
            }
            self.lich_su_goi_tin_thanh_cong.append(ban_ghi_ket_qua)

            nhat_ky = (
                f"🎯 [{self.ma_dinh_danh}] ĐÃ THU THẬP GÓI TIN ({loai_su_co}) | "
                f"Lộ trình ({so_chang_hop} hops): {chuoi_lo_trinh} | "
                f"Tổng trễ E2E: {tong_do_tre_e2e_ms} ms | SNR cuối: {thong_so_kenh.get('snr_db')} dB"
            )
            print(nhat_ky)
            await self.bo_dinh_tuyen.gui_log_ra_web("SUCCESS", nhat_ky)

            await self.bo_dinh_tuyen.phat_su_kien_ra_web("E2E_ROUTE_COMPLETED", ban_ghi_ket_qua)
            await self.bo_dinh_tuyen.phat_su_kien_ra_web("MISSION_COMPLETED", ban_ghi_ket_qua)

            tuyen_phan_hoi = list(reversed(lo_trinh))
            if len(tuyen_phan_hoi) > 1:
                await self.bo_dinh_tuyen.gui_don_huong(
                    ma_nguoi_gui=self.ma_dinh_danh,
                    ma_nguoi_nhan=tuyen_phan_hoi[1],
                    thong_diep={
                        "loai_tin": "XAC_NHAN_ACK",
                        "ma_phien_dau_gia": thong_diep.get("ma_phien_dau_gia"),
                        "e2e_delay_ms": tong_do_tre_e2e_ms,
                        "tuyen_phan_hoi": tuyen_phan_hoi,
                        "kich_thuoc_kb": 1.0
                    }
                )
            self.chuyen_trang_thai(AgentState.IDLE)
