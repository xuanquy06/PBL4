import time
from core.base_agent import BaseAgent, AgentState

class GroundStationAgent(BaseAgent):
    """
    Tác nhân Trạm Điều khiển Mặt đất (Ground Control Station - GCS):
    - Đóng vai trò Điểm thu thập dữ liệu trung tâm (Data Sink / Gateway).
    - Tham gia đấu giá trực tiếp nếu UAV nằm trong vùng phủ sóng Air-to-Ground (A2G).
    - Tính toán các chỉ số QoS toàn trình (End-to-End Delay, Hop Count, Route Path).
    """
    def __init__(self, bo_dinh_tuyen, **kwargs):
        kwargs.setdefault("loai_tac_nhan", "TRẠM_MẶT_ĐẤT")
        kwargs.setdefault("pin_max_joule", 100000000.0)
        super().__init__(bo_dinh_tuyen=bo_dinh_tuyen, **kwargs)
        self.chuyen_trang_thai(AgentState.IDLE)
        self.lich_su_goi_tin_thanh_cong = []

    async def cap_nhat_vat_ly(self, dt_giay: float):
        """
        Trạm mặt đất có tọa độ cố định trên mặt đất và dùng nguồn điện lưới
        nên không bị dịch chuyển tọa độ hay suy giảm pin theo thời gian.
        """
        pass

    async def xu_ly_thong_diep(self, thong_diep: dict):
        loai_tin = thong_diep.get("loai_tin")
        nguoi_gui = thong_diep.get("nguoi_gui")
        thong_so_kenh = thong_diep.get("thong_so_kenh", {})

        # Trạm mặt đất báo giá như đích cuối nếu liên kết từ UAV khả dụng.
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

        # 2. NHẬN DỮ LIỆU KHẨN CẤP (TỪ UAV TRUYỀN THẲNG HOẶC TỪ VỆ TINH/UAV CHUYỂN TIẾP VỀ)
        elif loai_tin in ("GIAO_NHIEM_VU_DU_LIEU", "CHUYEN_TIEP_VE_TRAM"):
            self.chuyen_trang_thai(AgentState.RELAYING)

            # Tính toán độ trễ Đầu-Cuối (End-to-End Delay)
            thoi_diem_goc = thong_diep.get("thoi_diem_khoi_tao", time.time())
            tong_do_tre_e2e_ms = round((time.time() - thoi_diem_goc) * 1000.0, 2)

            # Truy vết lộ trình gói tin đã đi qua (Routing Path)
            lo_trinh = thong_diep.get("lo_trinh_di_qua", [nguoi_gui])
            if self.ma_dinh_danh not in lo_trinh:
                lo_trinh.append(self.ma_dinh_danh)
            chuoi_lo_trinh = " ➔ ".join(lo_trinh)
            so_chang_hop = len(lo_trinh) - 1

            loai_su_co = thong_diep.get("loai_su_co", "KHẨN_CẤP")
            toa_do_su_co = thong_diep.get("toa_do_su_co", {})

            ban_ghi_ket_qua = {
                "ma_phien": thong_diep.get("ma_phien_dau_gia", "N/A"),
                "loai_su_co": loai_su_co,
                "toa_do_su_co": toa_do_su_co,
                "lo_trinh": lo_trinh,
                "so_chang_hop": so_chang_hop,
                "e2e_delay_ms": tong_do_tre_e2e_ms,
                "snr_chang_cuoi_db": thong_so_kenh.get("snr_db", 0.0),
                "dung_luong_chang_cuoi_mbps": thong_so_kenh.get("dung_luong_mbps", 0.0),
                "nang_luong_du_kien_joule": thong_diep.get("nang_luong_du_kien_joule", 0.0),
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

            # Đẩy sự kiện hoàn tất đường truyền để hiển thị tuyến dữ liệu trên bản đồ.
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