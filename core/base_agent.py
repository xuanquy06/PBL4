import asyncio
import time
from enum import Enum
from abc import ABC, abstractmethod

class AgentState( str, Enum ):
    """Máy trạng thái hữu hạn (FSM) chuẩn hóa cho các tác nhân trong mạng SAGSINS"""
    IDLE = "NHÀN_RỖI"                 # Sẵn sàng chờ lệnh (Trạm mặt đất / Vệ tinh)
    PATROLLING = "ĐANG_TUẦN_TRA"      # Đang bay theo quỹ đạo quét sự cố (UAV)
    BIDDING = "ĐANG_ĐẤU_GIÁ"          # Đang thương lượng giao thức Contract Net Protocol
    RELAYING = "ĐANG_TRUYỀN_TẢI"      # Đang đóng vai trò trạm trung chuyển dữ liệu
    RETURNING = "ĐANG_QUAY_VỀ_SẠC"    # UAV quay về trạm để tránh cạn pin
    CHARGING = "ĐANG_SẠC"             # UAV nạp pin tại trạm mặt đất
    LOW_BATTERY_RTB = "PIN_YẾU_RTB"   # Pin dưới ngưỡng an toàn, quay về trạm
    LOW_BATTERY = "PIN_YẾU_RTB"       # Bí danh tương thích với trạng thái cũ
    OFFLINE = "NGẮT_KẾT_NỐI"          # Sập nguồn hoặc hỏng hóc phần cứng

class BaseAgent(ABC):
    """
    Lớp trừu tượng cơ sở (Abstract Base Class) định nghĩa kiến trúc chuẩn cho mọi Node mạng.
    Tuân thủ nguyên lý Đóng/Mở (Open/Closed Principle) và Đa hình trong SOLID.
    """
    def __init__(
        self,
        ma_dinh_danh: str,
        loai_tac_nhan: str,
        bo_dinh_tuyen,
        vi_do: float = 0.0,
        kinh_do: float = 0.0,
        do_cao: float = 0.0,
        pin_max_joule: float = 50000.0,
        pin_khoi_tao_pct: float = 100.0,
        cong_suat_dbm: float = 33.0,
        tan_so_hz: float = 2.4e9,
        bang_thong_hz: float = 20e6,
        **kwargs  # BẮT BUỘC PHẢI CÓ DÒNG NÀY để hấp thụ dữ liệu thừa từ CSV
    ):
        self.ma_dinh_danh = ma_dinh_danh
        self.loai_tac_nhan = loai_tac_nhan
        self.bo_dinh_tuyen = bo_dinh_tuyen

        self.toa_do = {
            "node": self.ma_dinh_danh,
            "vi_do": float(vi_do),
            "kinh_do": float(kinh_do),
            "do_cao": float(do_cao),
            "van_toc_m_s": 0.0
        }

        self.radio_config = {
            "cong_suat_phat_dbm": float(cong_suat_dbm),
            "tan_so_hz": float(tan_so_hz),
            "bang_thong_hz": float(bang_thong_hz)
        }

        self.pin_toi_da = float(pin_max_joule)
        self.pin_hien_tai = self.pin_toi_da * min(100.0, max(0.0, float(pin_khoi_tao_pct))) / 100.0
        self.nguong_pin_yeu_pct = 20.0

        self.trang_thai = AgentState.IDLE
        self.hang_doi_thong_diep = asyncio.Queue(maxsize=100)
        self.dang_hoat_dong = True

        self.thong_ke = {
            "goi_tin_da_gui": 0,
            "goi_tin_da_nhan": 0,
            "goi_tin_chuyen_tiep": 0,
            "nang_luong_tieu_hao_joule": 0.0,
            "thoi_diem_cap_nhat_cuoi": time.time()
        }

        self.bo_dinh_tuyen.dang_ky_tac_nhan(self)

    @property
    def phan_tram_pin(self) -> float:
        """Tính tỷ lệ phần trăm năng lượng còn lại (0.0 -> 100.0%)"""
        if self.pin_toi_da <= 0:
            return 100.0
        return round(max(0.0, (self.pin_hien_tai / self.pin_toi_da) * 100.0), 2)

    @property
    def tai_hang_doi_pct(self) -> float:
        """Đánh giá mức độ tắc nghẽn bộ đệm (Buffer Congestion Level %) của Node"""
        if self.hang_doi_thong_diep.maxsize <= 0:
            return 0.0
        return round((self.hang_doi_thong_diep.qsize() / self.hang_doi_thong_diep.maxsize) * 100.0, 2)

    def tieu_hao_nang_luong(self, luong_joule: float) -> bool:
        """
        Khấu trừ năng lượng khi hoạt động động cơ hoặc phát sóng RF.
        Tự động chuyển trạng thái FSM nếu chạm ngưỡng Pin yếu hoặc Sập nguồn.
        """
        if self.trang_thai == AgentState.OFFLINE:
            return False

        self.pin_hien_tai = max(0.0, self.pin_hien_tai - luong_joule)
        self.thong_ke["nang_luong_tieu_hao_joule"] = round(
            self.thong_ke["nang_luong_tieu_hao_joule"] + luong_joule, 2
        )

        if self.pin_hien_tai <= 0.0:
            self.chuyen_trang_thai(AgentState.OFFLINE)
            return False
        elif (
            self.phan_tram_pin < self.nguong_pin_yeu_pct
            and self.trang_thai not in (
                AgentState.LOW_BATTERY_RTB,
                AgentState.RETURNING,
                AgentState.CHARGING
            )
        ):
            self.chuyen_trang_thai(AgentState.LOW_BATTERY_RTB)

        return True

    def tieu_hao_nang_luong_truyen_tin(self, kich_thuoc_goi_kb: float = 10.0, la_phat_song: bool = True) -> float:
        """
        Mô hình hóa năng lượng tiêu thụ cho giao tiếp vô tuyến:
        - Khi phát sóng (Tx): Phụ thuộc vào công suất phát P_tx (Watts) và thời gian truyền gói tin.
        - Khi thu sóng (Rx): Tiêu tốn năng lượng xử lý mạch thu cố định nhỏ hơn.
        """
        # Đổi dBm sang Watts: P(W) = 10^((P_dbm - 30) / 10)
        p_tx_watts = 10.0 ** ((self.radio_config["cong_suat_phat_dbm"] - 30.0) / 10.0)
        
        if la_phat_song:
            # Giả định thời gian phát trung bình tỷ lệ với kích thước gói tin + mạch khuếch đại công suất
            nang_luong_rf = p_tx_watts * 0.05 * (kich_thuoc_goi_kb / 10.0)
            self.thong_ke["goi_tin_da_gui"] += 1
        else:
            # Mạch thu (LNA + DSP) tiêu tốn khoảng 1.5W trong thời gian ngắn
            nang_luong_rf = 0.02 * (kich_thuoc_goi_kb / 10.0)
            self.thong_ke["goi_tin_da_nhan"] += 1

        self.tieu_hao_nang_luong(nang_luong_rf)
        return round(nang_luong_rf, 4)

    def chuyen_trang_thai(self, trang_thai_moi: AgentState):
        """Cập nhật trạng thái trong FSM của Tác nhân"""
        if self.trang_thai != trang_thai_moi:
            self.trang_thai = trang_thai_moi

    def xuat_ho_so_trang_thai(self) -> dict:
        """
        Đóng gói thông số telemetry của node thành dữ liệu JSON cho bản đồ và bảng giám sát.
        """
        vi_do = round(self.toa_do["vi_do"], 5)
        kinh_do = round(self.toa_do["kinh_do"], 5)
        van_toc = round(self.toa_do["van_toc_m_s"], 1)
        return {
            "id": self.ma_dinh_danh,
            "loai": self.loai_tac_nhan,
            "state": self.trang_thai.name,
            "battery": self.phan_tram_pin,
            "lat": vi_do,
            "lng": kinh_do,
            "speed": van_toc,
            "trang_thai": self.trang_thai.value,
            "vi_do": vi_do,
            "kinh_do": kinh_do,
            "do_cao": round(self.toa_do["do_cao"], 1),
            "van_toc_m_s": van_toc,
            "pin_pct": self.phan_tram_pin,
            "pin_joule": round(self.pin_hien_tai, 1),
            "tai_hang_doi_pct": self.tai_hang_doi_pct,
            "cong_suat_dbm": self.radio_config["cong_suat_phat_dbm"],
            "thong_ke": self.thong_ke
        }

    async def lang_nghe_lien_tuc(self):
        """Vòng lặp Bất đồng bộ (Async Event Loop) xử lý hàng đợi gói tin đến của Tác nhân"""
        while self.dang_hoat_dong:
            try:
                thong_diep = await self.hang_doi_thong_diep.get()
                if self.trang_thai == AgentState.OFFLINE:
                    continue

                # Khấu trừ năng lượng mạch thu (Rx) khi nhận gói tin
                kich_thuoc_kb = thong_diep.get("kich_thuoc_kb", 5.0)
                self.tieu_hao_nang_luong_truyen_tin(kich_thuoc_kb, la_phat_song=False)

                # Gọi hàm xử lý chuyên biệt của từng loại Tác nhân (Đa hình)
                await self.xu_ly_thong_diep(thong_diep)
                self.hang_doi_thong_diep.task_done()
            except Exception as e:
                print(f"[LỖI TÁC NHÂN {self.ma_dinh_danh}] {e}")

    @abstractmethod
    async def cap_nhat_vat_ly(self, dt_giay: float):
        """
        Phương thức trừu tượng: Cập nhật vị trí di chuyển 3D và tiêu hao năng lượng cơ học
        theo từng bước nhảy thời gian `dt_giay` của đồng hồ mô phỏng.
        """
        pass

    @abstractmethod
    async def xu_ly_thong_diep(self, thong_diep: dict):
        """
        Phương thức trừu tượng: Xử lý gói tin giao thức mạng và đấu giá MAS.
        """
        pass