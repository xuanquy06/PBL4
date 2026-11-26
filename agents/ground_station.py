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
        self.bao_cao_phat_hien_chay: dict[str, dict] = {}
        self.telemetry_uav_moi_nhat = {}
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
                    "energy_to_sink_j": 0.0,
                    "delay_to_sink_ms": 0.0,
                    "capacity_to_sink_mbps": float("inf"),
                    "kich_thuoc_kb": 1.0
                }
            )

    async def xu_ly_thong_diep(self, thong_diep: dict):
        loai_tin = thong_diep.get("loai_tin")
        nguoi_gui = thong_diep.get("nguoi_gui")
        thong_so_kenh = thong_diep.get("thong_so_kenh", {})

        if loai_tin == "HELLO_BEACON":
            return

        if loai_tin == "FIRE_DETECTION_REPORT":
            ma_su_co = thong_diep.get("ma_su_co")
            uav_phat_hien = thong_diep.get("uav_phat_hien")
            lo_trinh = thong_diep.get("lo_trinh_di_qua", [])
            try:
                vi_do = float(thong_diep["vi_do"])
                kinh_do = float(thong_diep["kinh_do"])
            except (KeyError, TypeError, ValueError):
                vi_do = kinh_do = None

            if (
                not isinstance(ma_su_co, str)
                or not ma_su_co
                or not isinstance(uav_phat_hien, str)
                or not uav_phat_hien
                or vi_do is None
                or kinh_do is None
                or not isinstance(lo_trinh, list)
                or not all(isinstance(node_id, str) for node_id in lo_trinh)
            ):
                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "WARN", f"[FIRE DETECTION] Bỏ qua báo cáo không hợp lệ từ {nguoi_gui}."
                )
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
                "thoi_gian_nhan": time.time()
            }
            self.bao_cao_phat_hien_chay[ma_su_co] = bao_cao
            await self.bo_dinh_tuyen.phat_su_kien_ra_web(
                "FIRE_DETECTION_RECEIVED",
                bao_cao
            )
            await self.bo_dinh_tuyen.gui_log_ra_web(
                "EVENT",
                f"[FIRE DETECTION] Nhận báo cáo sự cố từ UAV [{uav_phat_hien}] "
                f"qua {max(0, len(lo_trinh) - 1)} chặng."
            )
            return

        if loai_tin == "TELEMETRY_REPORT":
            telemetry = thong_diep.get("noi_dung")
            if (
                not isinstance(nguoi_gui, str)
                or not nguoi_gui.strip()
                or not isinstance(telemetry, dict)
            ):
                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "WARN", f"[TELEMETRY] Bỏ qua dữ liệu không hợp lệ từ {nguoi_gui}."
                )
                return

            lo_trinh = thong_diep.get("lo_trinh_di_qua", [])
            if not isinstance(lo_trinh, list) or not all(
                isinstance(node_id, str) for node_id in lo_trinh
            ):
                await self.bo_dinh_tuyen.gui_log_ra_web(
                    "WARN", f"[TELEMETRY] Lộ trình không hợp lệ từ {nguoi_gui}."
                )
                return

            bao_cao = {
                **telemetry,
                "uav_id": nguoi_gui,
                "lo_trinh": list(lo_trinh),
                "thoi_gian_nhan": time.time()
            }
            self.telemetry_uav_moi_nhat[nguoi_gui] = bao_cao
            await self.bo_dinh_tuyen.phat_su_kien_ra_web(
                "UAV_TELEMETRY_RECEIVED",
                bao_cao
            )
            return

        # NHẬN DỮ LIỆU KHẨN CẤP (TỪ UAV TRUYỀN THẲNG HOẶC QUA FANET/VỆ TINH LEO CHUYỂN TIẾP)
        if loai_tin in ("GIAO_NHIEM_VU_DU_LIEU", "CHUYEN_TIEP_VE_TRAM"):
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
                "ma_phien": thong_diep.get("ma_phien_su_co", "N/A"),
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

            gcs_server = getattr(self.bo_dinh_tuyen, "ground_station_server", None)
            if gcs_server and hasattr(gcs_server, "ghi_nhan_bao_cao_dijkstra") and toa_do_su_co:
                await gcs_server.ghi_nhan_bao_cao_dijkstra(
                    float(toa_do_su_co.get("vi_do", 0.0)),
                    float(toa_do_su_co.get("kinh_do", 0.0)),
                    lo_trinh[0],
                    lo_trinh,
                    do_tre_ms=tong_do_tre_e2e_ms,
                )

            await self.bo_dinh_tuyen.phat_su_kien_ra_web("E2E_ROUTE_COMPLETED", ban_ghi_ket_qua)
            await self.bo_dinh_tuyen.phat_su_kien_ra_web("MISSION_COMPLETED", ban_ghi_ket_qua)

            tuyen_phan_hoi = list(reversed(lo_trinh))
            if len(tuyen_phan_hoi) > 1:
                await self.bo_dinh_tuyen.gui_don_huong(
                    ma_nguoi_gui=self.ma_dinh_danh,
                    ma_nguoi_nhan=tuyen_phan_hoi[1],
                    thong_diep={
                        "loai_tin": "XAC_NHAN_ACK",
                        "ma_phien_su_co": thong_diep.get("ma_phien_su_co"),
                        "e2e_delay_ms": tong_do_tre_e2e_ms,
                        "tuyen_phan_hoi": tuyen_phan_hoi,
                        "kich_thuoc_kb": 1.0
                    }
                )
            self.chuyen_trang_thai(AgentState.IDLE)
