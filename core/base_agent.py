import asyncio
import time
from enum import Enum
from abc import ABC, abstractmethod
from typing import Dict, Optional
from core.network_stack import assign_node_ip
from core.os_kernel import BoundedPriorityQueue


class AgentState(str, Enum):
    """Máy trạng thái hữu hạn (FSM) chuẩn hóa cho các tác nhân trong mạng SAGSINS"""
    IDLE = "NHÀN_RỖI"                 # Sẵn sàng chờ lệnh (Trạm mặt đất / Vệ tinh)
    PATROLLING = "ĐANG_TUẦN_TRA"      # Đang bay theo quỹ đạo quét sự cố (UAV)
    BIDDING = "ĐANG_ĐẤU_GIÁ"          # Đang thương lượng giao thức Contract Net Protocol
    RELAYING = "ĐANG_TRUYỀN_TẢI"      # Đang đóng vai trò trạm trung chuyển dữ liệu
    RETURNING = "ĐANG_QUAY_VỀ_SẠC"    # UAV quay về trạm để tránh cạn pin
    WAITING_DOCK = "CHỜ_CỔNG_SẠC"     # UAV bị chặn ở hàng đợi Semaphore chờ cổng sạc rảnh
    CHARGING = "ĐANG_SẠC"             # UAV nạp pin tại trạm mặt đất
    LOW_BATTERY_RTB = "PIN_YẾU_RTB"   # Pin dưới ngưỡng an toàn, quay về trạm
    LOW_BATTERY = "PIN_YẾU_RTB"       # Bí danh tương thích với trạng thái cũ
    OFFLINE = "NGẮT_KẾT_NỐI"          # Sập nguồn hoặc hỏng hóc phần cứng


class BaseAgent(ABC):
    """
    Lớp trừu tượng cơ sở (Abstract Base Class) định nghĩa kiến trúc chuẩn cho mọi Node mạng.
    Mỗi tác nhân duy trì bảng láng giềng cục bộ (Local Neighbor Belief) được cập nhật
    qua các bản tin vô tuyến nhận được, đảm bảo tính tự trị độc lập của hệ thống MAS.
    """
    NEIGHBOR_TIMEOUT_SEC = 15.0

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
        **kwargs
    ):
        self.ma_dinh_danh = ma_dinh_danh
        self.loai_tac_nhan = loai_tac_nhan
        self.bo_dinh_tuyen = bo_dinh_tuyen
        self.ip_address = assign_node_ip(ma_dinh_danh, loai_tac_nhan, kwargs.get("zone_id"))

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
        self._hang_doi_thong_diep: Optional[asyncio.Queue] = None
        self.dang_hoat_dong = True

        # Tri thức cục bộ (Local Belief): Bảng láng giềng 1-hop và tuyến đường hiện tại về Sink
        self.bang_lang_gieng: Dict[str, dict] = {}
        self.tuyen_ve_tram_cuc_bo: list[str] = (
            [self.ma_dinh_danh] if self.loai_tac_nhan == "TRẠM_MẶT_ĐẤT" else []
        )
        self.chi_phi_ve_tram_cuc_bo: float = (
            0.0 if self.loai_tac_nhan == "TRẠM_MẶT_ĐẤT" else float("inf")
        )
        self.nang_luong_ve_tram_cuc_bo = 0.0
        self.do_tre_ve_tram_cuc_bo_ms = 0.0
        self.dung_luong_ve_tram_cuc_bo_mbps = float("inf")

        self.thong_ke = {
            "goi_tin_da_gui": 0,
            "goi_tin_da_nhan": 0,
            "goi_tin_chuyen_tiep": 0,
            "nang_luong_tieu_hao_joule": 0.0,
            "thoi_diem_cap_nhat_cuoi": time.time()
        }

        self.bo_dinh_tuyen.dang_ky_tac_nhan(self)

    @property
    def hang_doi_thong_diep(self) -> BoundedPriorityQueue:
        """Khởi tạo hàng đợi lập lịch ưu tiên (BoundedPriorityQueue) lười (lazy)."""
        if self._hang_doi_thong_diep is None:
            os_kernel = getattr(self.bo_dinh_tuyen, "os_kernel", None)
            self._hang_doi_thong_diep = BoundedPriorityQueue(maxsize=100, os_kernel=os_kernel)
        return self._hang_doi_thong_diep

    @hang_doi_thong_diep.setter
    def hang_doi_thong_diep(self, value):
        self._hang_doi_thong_diep = value

    @property
    def phan_tram_pin(self) -> float:
        """Tính tỷ lệ phần trăm năng lượng còn lại (0.0 -> 100.0%)"""
        if self.pin_toi_da <= 0:
            return 100.0
        return round(max(0.0, (self.pin_hien_tai / self.pin_toi_da) * 100.0), 2)

    @property
    def tai_hang_doi_pct(self) -> float:
        """Đánh giá mức độ tắc nghẽn bộ đệm (Buffer Congestion Level %) của Node"""
        if self._hang_doi_thong_diep is None or self._hang_doi_thong_diep.maxsize <= 0:
            return 0.0
        return round((self._hang_doi_thong_diep.qsize() / self._hang_doi_thong_diep.maxsize) * 100.0, 2)

    def cap_nhat_lang_gieng_tu_goi_tin(self, thong_diep: dict):
        """
        Cập nhật Bảng láng giềng cục bộ (Local Neighbor Belief) từ siêu dữ liệu lớp vật lý
        và thông tin trạng thái đính kèm trong gói tin vô tuyến nhận được.
        """
        nguoi_gui = thong_diep.get("nguoi_gui")
        if not nguoi_gui or nguoi_gui == self.ma_dinh_danh:
            return

        thong_so_kenh = thong_diep.get("thong_so_kenh", {})
        hien_co = self.bang_lang_gieng.get(nguoi_gui, {})

        self.bang_lang_gieng[nguoi_gui] = {
            "ma_dinh_danh": nguoi_gui,
            "loai_tac_nhan": thong_diep.get("loai_nguoi_gui", hien_co.get("loai_tac_nhan", "UNKNOWN")),
            "toa_do": thong_diep.get("toa_do_nguoi_gui", hien_co.get("toa_do", {})),
            "pin_pct": float(thong_diep.get("pin_pct", hien_co.get("pin_pct", 100.0))),
            "tai_hang_doi_pct": float(thong_diep.get("tai_hang_doi_pct", hien_co.get("tai_hang_doi_pct", 0.0))),
            "trang_thai": thong_diep.get("trang_thai_nguoi_gui", hien_co.get("trang_thai", "NHÀN_RỖI")),
            "zone_id": thong_diep.get("zone_id", hien_co.get("zone_id")),
            "cost_to_sink": float(thong_diep.get("cost_to_sink", hien_co.get("cost_to_sink", float("inf")))),
            "path_to_sink": list(thong_diep.get("path_to_sink", hien_co.get("path_to_sink", []))),
            "energy_to_sink_j": float(
                thong_diep.get("energy_to_sink_j", hien_co.get("energy_to_sink_j", 0.0))
            ),
            "delay_to_sink_ms": float(
                thong_diep.get("delay_to_sink_ms", hien_co.get("delay_to_sink_ms", 0.0))
            ),
            "capacity_to_sink_mbps": float(
                thong_diep.get(
                    "capacity_to_sink_mbps",
                    hien_co.get("capacity_to_sink_mbps", float("inf"))
                )
            ),
            "snr_db": float(thong_so_kenh.get("snr_db", hien_co.get("snr_db", 20.0))),
            "do_tre_ms": float(thong_so_kenh.get("do_tre_ms", hien_co.get("do_tre_ms", 5.0))),
            "dung_luong_mbps": float(thong_so_kenh.get("dung_luong_mbps", hien_co.get("dung_luong_mbps", 20.0))),
            "per": float(thong_so_kenh.get("per", hien_co.get("per", 0.001))),
            "thoi_diem_cap_nhat": time.time()
        }

    def don_dep_lang_gieng_het_han(self, thoi_diem_hien_tai: Optional[float] = None):
        """Loại bỏ các láng giềng đã mất liên lạc quá thời gian timeout."""
        now = thoi_diem_hien_tai or time.time()
        het_han = [
            ma_node for ma_node, info in self.bang_lang_gieng.items()
            if now - info.get("thoi_diem_cap_nhat", 0.0) > self.NEIGHBOR_TIMEOUT_SEC
        ]
        for ma_node in het_han:
            del self.bang_lang_gieng[ma_node]

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
        - Khi phát sóng (Tx): Phụ thuộc vào công suất phát P_tx (Watts) và kích thước gói tin.
        - Khi thu sóng (Rx): Tiêu tốn năng lượng xử lý mạch thu LNA + DSP.
        """
        p_tx_watts = 10.0 ** ((self.radio_config["cong_suat_phat_dbm"] - 30.0) / 10.0)

        if la_phat_song:
            nang_luong_rf = p_tx_watts * 0.05 * (kich_thuoc_goi_kb / 10.0)
            self.thong_ke["goi_tin_da_gui"] += 1
        else:
            nang_luong_rf = 0.02 * (kich_thuoc_goi_kb / 10.0)
            self.thong_ke["goi_tin_da_nhan"] += 1

        self.tieu_hao_nang_luong(nang_luong_rf)
        return round(nang_luong_rf, 4)

    def chuyen_trang_thai(self, trang_thai_moi: AgentState):
        """Cập nhật trạng thái trong FSM của Tác nhân"""
        if self.trang_thai != trang_thai_moi:
            self.trang_thai = trang_thai_moi

    def xuat_bang_dinh_tuyen(self) -> list[dict]:
        """Xuất bảng định tuyến tầng Mạng (L3 Routing Table) của node hiện tại."""
        routes: list[dict] = []
        now = time.time()
        tac_nhan_map = getattr(self.bo_dinh_tuyen, "danh_sach_tac_nhan", {}) or {}

        if len(self.tuyen_ve_tram_cuc_bo) >= 2:
            dest_id = self.tuyen_ve_tram_cuc_bo[-1]
            next_id = self.tuyen_ve_tram_cuc_bo[1]
            dest_ag = tac_nhan_map.get(dest_id)
            next_ag = tac_nhan_map.get(next_id)
            next_kind = getattr(next_ag, "loai_tac_nhan", "")
            iface = (
                "A2G_RADIO" if next_kind == "TRẠM_MẶT_ĐẤT"
                else ("LEO_KU_BAND" if "VỆ_TINH" in next_kind else "FANET_WIFI")
            )
            routes.append({
                "dest": dest_id,
                "dest_ip": getattr(dest_ag, "ip_address", "10.0.0.1"),
                "next_hop": next_id,
                "next_hop_ip": getattr(next_ag, "ip_address", "10.0.0.1"),
                "metric": round(float(self.chi_phi_ve_tram_cuc_bo), 2)
                if self.chi_phi_ve_tram_cuc_bo != float("inf") else 9999.0,
                "interface": iface,
                "hops": len(self.tuyen_ve_tram_cuc_bo) - 1,
                "ttl_remaining": 16,
            })

        for nid, info in list(self.bang_lang_gieng.items()):
            ag = tac_nhan_map.get(nid)
            n_kind = str(info.get("loai_tac_nhan", getattr(ag, "loai_tac_nhan", "")))
            iface = (
                "A2G_RADIO" if n_kind == "TRẠM_MẶT_ĐẤT"
                else ("LEO_KU_BAND" if "VỆ_TINH" in n_kind else "FANET_WIFI")
            )
            ttl_rem = max(0.0, round(self.NEIGHBOR_TIMEOUT_SEC - (now - float(info.get("thoi_diem_cap_nhat", now))), 1))
            routes.append({
                "dest": nid,
                "dest_ip": getattr(ag, "ip_address", "10.0.0.0"),
                "next_hop": nid,
                "next_hop_ip": getattr(ag, "ip_address", "10.0.0.0"),
                "metric": round(float(info.get("do_tre_ms", 0.0)), 2),
                "interface": iface,
                "hops": 1,
                "ttl_remaining": ttl_rem,
            })
        return routes

    def xuat_ho_so_trang_thai(self) -> dict:
        """
        Đóng gói thông số telemetry của node thành dữ liệu JSON cho bản đồ và bảng giám sát.
        """
        vi_do = round(self.toa_do["vi_do"], 5)
        kinh_do = round(self.toa_do["kinh_do"], 5)
        van_toc = round(self.toa_do["van_toc_m_s"], 1)
        return {
            "id": self.ma_dinh_danh,
            "ip": self.ip_address,
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
            "queue_size": self.hang_doi_thong_diep.qsize(),
            "tai_hang_doi_pct": self.tai_hang_doi_pct,
            "cong_suat_dbm": self.radio_config["cong_suat_phat_dbm"],
            "so_lang_gieng": len(self.bang_lang_gieng),
            "route_to_sink": list(self.tuyen_ve_tram_cuc_bo),
            "thong_ke": self.thong_ke
        }

    async def lang_nghe_lien_tuc(self):
        """Vòng lặp Bất đồng bộ (Async Event Loop) xử lý hàng đợi gói tin đến của Tác nhân"""
        while self.dang_hoat_dong:
            thong_diep = await self.hang_doi_thong_diep.get()
            try:
                if self.trang_thai == AgentState.OFFLINE:
                    continue

                # Khấu trừ năng lượng mạch thu (Rx) khi nhận gói tin
                kich_thuoc_kb = thong_diep.get("kich_thuoc_kb", 5.0)
                self.tieu_hao_nang_luong_truyen_tin(kich_thuoc_kb, la_phat_song=False)

                # Cập nhật tri thức cục bộ về láng giềng từ gói tin vừa nhận
                self.cap_nhat_lang_gieng_tu_goi_tin(thong_diep)

                # Gọi hàm xử lý chuyên biệt của từng loại Tác nhân (Đa hình)
                await self.xu_ly_thong_diep(thong_diep)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[LỖI TÁC NHÂN {self.ma_dinh_danh}] {e}")
            finally:
                self.hang_doi_thong_diep.task_done()

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
