import asyncio
import heapq
import json
import random
import time
from typing import Dict, List, TYPE_CHECKING, Optional
from fastapi import WebSocket

# Chỉ import BaseAgent cho Pylance kiểm tra kiểu & gợi ý code (Autocomplete)
if TYPE_CHECKING:
    from core.base_agent import BaseAgent

class MessageBroker:
    """
    Môi trường truyền dẫn vô tuyến trung tâm (Wireless Channel & Event Broker):
    - Kiểm tra điều kiện vật lý (LoS, SNR, PER, Delay) trước khi chuyển phát gói tin.
    - Đồng bộ trạng thái mạng, liên kết và số liệu QoS ra giao diện Web qua WebSocket.
    """
    def __init__(self, physics_engine, scenario_manager):
        self.physics = physics_engine
        self.scenario = scenario_manager
        # Khai báo rõ kiểu BaseAgent thay vì object để Pylance nhận diện đầy đủ thuộc tính
        self.danh_sach_tac_nhan: Dict[str, "BaseAgent"] = {}
        self.danh_sach_client_ws: List[WebSocket] = []

        # Thống kê toàn mạng phục vụ vẽ biểu đồ Real-time trên giao diện
        self.thong_ke_toan_mang = {
            "tong_goi_tin_phat": 0,
            "tong_goi_tin_thanh_cong": 0,
            "tong_goi_tin_rot_per": 0,
            "tong_goi_tin_mat_los": 0,
            "do_tre_trung_binh_ms": 0.0,
            "nang_luong_tieu_hao_joule": 0.0
        }

    def dang_ky_tac_nhan(self, tac_nhan):
        """Đăng ký một Node mạng (UAV, Vệ tinh, Trạm mặt đất) vào không gian mô phỏng"""
        self.danh_sach_tac_nhan[tac_nhan.ma_dinh_danh] = tac_nhan
        print(f"[WIRELESS MEDIUM] Đã đăng ký Node: {tac_nhan.ma_dinh_danh} ({tac_nhan.loai_tac_nhan})")

    def lay_tac_nhan(self, ma_dinh_danh: str) -> Optional["BaseAgent"]:
        return self.danh_sach_tac_nhan.get(ma_dinh_danh)

    def cap_nhat_nang_luong_tong(self):
        self.thong_ke_toan_mang["nang_luong_tieu_hao_joule"] = round(
            sum(agent.thong_ke["nang_luong_tieu_hao_joule"] for agent in self.danh_sach_tac_nhan.values()),
            3
        )

    def tim_tuyen_den_tram_mat_dat(
        self,
        ma_bat_dau: str,
        kich_thuoc_kb: float = 250.0,
        loai_tru: Optional[set[str]] = None
    ) -> Optional[dict]:
        """Tìm tuyến khả dụng có chi phí thấp nhất từ một node tới trạm mặt đất."""
        bat_dau = self.danh_sach_tac_nhan.get(ma_bat_dau)
        if not bat_dau:
            return None
        cac_node_loai_tru = loai_tru or set()
        if bat_dau.loai_tac_nhan == "TRẠM_MẶT_ĐẤT":
            return {"nodes": [ma_bat_dau], "channels": [], "cost": 0.0, "energy_j": 0.0, "delay_ms": 0.0}

        hang_doi = [(0.0, ma_bat_dau, (ma_bat_dau,), ())]
        chi_phi_tot_nhat = {ma_bat_dau: 0.0}
        while hang_doi:
            chi_phi, ma_hien_tai, nodes, channels = heapq.heappop(hang_doi)
            if chi_phi > chi_phi_tot_nhat.get(ma_hien_tai, float("inf")):
                continue
            hien_tai = self.danh_sach_tac_nhan[ma_hien_tai]
            if hien_tai.loai_tac_nhan == "TRẠM_MẶT_ĐẤT":
                return {
                    "nodes": list(nodes),
                    "channels": list(channels),
                    "cost": round(chi_phi, 3),
                    "energy_j": round(sum(ch["energy_j"] for ch in channels), 3),
                    "delay_ms": round(sum(ch["do_tre_ms"] for ch in channels), 3)
                }

            for ma_ke_tiep, ke_tiep in self.danh_sach_tac_nhan.items():
                if (
                    ma_ke_tiep in nodes
                    or ma_ke_tiep in cac_node_loai_tru
                    or ke_tiep.trang_thai.value == "NGẮT_KẾT_NỐI"
                    or (
                        ke_tiep.loai_tac_nhan == "MÁY_BAY"
                        and ke_tiep.trang_thai.value in ("ĐANG_ĐẤU_GIÁ", "ĐANG_TRUYỀN_TẢI")
                    )
                ):
                    continue
                if ke_tiep.loai_tac_nhan in ("MÁY_BAY", "VỆ_TINH_LEO") and ke_tiep.phan_tram_pin < ke_tiep.nguong_pin_yeu_pct:
                    continue
                if (
                    hien_tai.loai_tac_nhan == "MÁY_BAY"
                    and hien_tai.trang_thai.value == "ĐANG_SẠC"
                    and ke_tiep.loai_tac_nhan == "TRẠM_MẶT_ĐẤT"
                    and self.physics.tinh_khoang_cach_3d(hien_tai.toa_do, ke_tiep.toa_do) <= 1.0
                ):
                    kenh = {
                        "kha_dung": True,
                        "do_tre_ms": 0.0,
                        "dung_luong_mbps": max(
                            0.5,
                            hien_tai.radio_config["bang_thong_hz"] / 1_000_000.0
                        ),
                        "per": 0.0,
                        "snr_db": 100.0
                    }
                else:
                    kenh = self.physics.danh_gia_chat_luong_kenh_truyen(
                        pos1=hien_tai.toa_do,
                        pos2=ke_tiep.toa_do,
                        tan_so_hz=hien_tai.radio_config["tan_so_hz"],
                        cong_suat_phat_dbm=hien_tai.radio_config["cong_suat_phat_dbm"],
                        bang_thong_hz=hien_tai.radio_config["bang_thong_hz"]
                    )
                if not kenh["kha_dung"] or kenh["per"] >= 0.25 or kenh["dung_luong_mbps"] <= 0:
                    continue

                energy_j = self._uoc_tinh_nang_luong_truyen(hien_tai, kich_thuoc_kb)
                kenh_co_nang_luong = {**kenh, "energy_j": energy_j}
                weight = (
                    kenh["do_tre_ms"]
                    + 40.0 / max(0.5, kenh["dung_luong_mbps"])
                    + energy_j
                    + ke_tiep.tai_hang_doi_pct * 0.05
                )
                next_cost = chi_phi + weight
                if next_cost < chi_phi_tot_nhat.get(ma_ke_tiep, float("inf")):
                    chi_phi_tot_nhat[ma_ke_tiep] = next_cost
                    heapq.heappush(
                        hang_doi,
                        (next_cost, ma_ke_tiep, (*nodes, ma_ke_tiep), (*channels, kenh_co_nang_luong))
                    )
        return None

    @staticmethod
    def _uoc_tinh_nang_luong_truyen(agent, kich_thuoc_kb: float) -> float:
        cong_suat_w = 10.0 ** ((agent.radio_config["cong_suat_phat_dbm"] - 30.0) / 10.0)
        return round(cong_suat_w * 0.05 * (kich_thuoc_kb / 10.0), 4)

    def tao_bao_gia(self, ma_ung_vien: str, ma_nguon: str, thong_so_chang_dau: dict, kich_thuoc_kb: float) -> Optional[dict]:
        """Định giá đường truyền từ nguồn, qua ứng viên, đến một trạm mặt đất."""
        ung_vien = self.danh_sach_tac_nhan.get(ma_ung_vien)
        if not ung_vien:
            return None
        if ung_vien.loai_tac_nhan == "TRẠM_MẶT_ĐẤT":
            tuyen = {"nodes": [ma_ung_vien], "channels": [], "cost": 0.0, "energy_j": 0.0, "delay_ms": 0.0}
        else:
            tuyen = self.tim_tuyen_den_tram_mat_dat(
                ma_ung_vien, kich_thuoc_kb, loai_tru={ma_nguon}
            )
            if not tuyen:
                return None

        bw_dau = max(0.5, thong_so_chang_dau.get("dung_luong_mbps", 0.0))
        bw_tuyen = [max(0.5, kenh["dung_luong_mbps"]) for kenh in tuyen["channels"]]
        bw_nut_that_co_chai = min([bw_dau, *bw_tuyen])
        tong_tre = thong_so_chang_dau.get("do_tre_ms", 9999.0) + tuyen["delay_ms"]
        energy = self._uoc_tinh_nang_luong_truyen(
            self.danh_sach_tac_nhan[ma_nguon], kich_thuoc_kb
        ) + tuyen["energy_j"]
        energy += 0.02 * (kich_thuoc_kb / 10.0) * (len(tuyen["channels"]) + 1)
        chi_phi = tong_tre + 120.0 / bw_nut_that_co_chai + energy
        if ung_vien.loai_tac_nhan in ("MÁY_BAY", "VỆ_TINH_LEO"):
            chi_phi += (100.0 - ung_vien.phan_tram_pin) * 0.15
        chi_phi += ung_vien.tai_hang_doi_pct * 0.2
        return {
            "chi_phi": round(chi_phi, 3),
            "tuyen": [*tuyen["nodes"]],
            "nang_luong_du_kien_joule": round(energy, 3),
            "do_tre_du_kien_ms": round(tong_tre, 3),
            "dung_luong_du_kien_mbps": round(bw_nut_that_co_chai, 3)
        }

    # =========================================================================
    # 1. CƠ CHẾ TRUYỀN GÓI TIN QUA KÊNH VẬT LÝ (UNICAST & BROADCAST)
    # =========================================================================
    async def gui_don_huong(self, ma_nguoi_gui: str, ma_nguoi_nhan: str, thong_diep: dict) -> bool:
        """
        Truyền gói tin điểm-nối-điểm (Unicast) có kiểm tra suy hao vật lý, 
        độ trễ lan truyền sóng và xác suất rớt gói PER.
        """
        nguoi_gui = self.danh_sach_tac_nhan.get(ma_nguoi_gui)
        nguoi_nhan = self.danh_sach_tac_nhan.get(ma_nguoi_nhan)

        if not nguoi_gui or not nguoi_nhan:
            return False

        # Khấu trừ năng lượng phát sóng (Tx) của người gửi
        kich_thuoc_kb = float(thong_diep.get("kich_thuoc_kb", 10.0))
        nang_luong_tx = nguoi_gui.tieu_hao_nang_luong_truyen_tin(
            kich_thuoc_kb, la_phat_song=True
        )
        self.cap_nhat_nang_luong_tong()
        self.thong_ke_toan_mang["tong_goi_tin_phat"] += 1

        # Tính toán thông số Lớp Vật lý (PHY Layer) tại thời điểm phát sóng
        thong_so_kenh = self.physics.danh_gia_chat_luong_kenh_truyen(
            pos1=nguoi_gui.toa_do,
            pos2=nguoi_nhan.toa_do,
            tan_so_hz=nguoi_gui.radio_config["tan_so_hz"],
            cong_suat_phat_dbm=nguoi_gui.radio_config["cong_suat_phat_dbm"],
            bang_thong_hz=nguoi_gui.radio_config["bang_thong_hz"]
        )
        thong_so_kenh["nang_luong_tx_joule"] = nang_luong_tx
        thong_so_kenh["nang_luong_toan_mang_joule"] = self.thong_ke_toan_mang["nang_luong_tieu_hao_joule"]

        loai_tin = thong_diep.get("loai_tin", "UNKNOWN")

        # Kiểm tra 1: Mất tầm nhìn thẳng (Khuất bóng Trái Đất hoặc quá cự ly A2A)
        if not thong_so_kenh["kha_dung"]:
            self.thong_ke_toan_mang["tong_goi_tin_mat_los"] += 1
            self.scenario.ghi_log_thuc_nghiem_qos(
                ma_nguoi_gui, ma_nguoi_nhan, loai_tin, thong_so_kenh, "MAT_KET_NOI_LOS"
            )
            return False

        # Kiểm tra 2: Xét xác suất lỗi gói tin (Packet Error Rate - PER) do nhiễu nhiệt
        if random.random() < thong_so_kenh["per"]:
            self.thong_ke_toan_mang["tong_goi_tin_rot_per"] += 1
            self.scenario.ghi_log_thuc_nghiem_qos(
                ma_nguoi_gui, ma_nguoi_nhan, loai_tin, thong_so_kenh, "ROT_GOI_DO_NHIEU_PER"
            )
            await self.phat_su_kien_ra_web("LOG", {
                "cap_do": "WARN",
                "noi_dung": f"[RỚT GÓI - PER] {ma_nguoi_gui} -> {ma_nguoi_nhan} ({loai_tin}) | SNR: {thong_so_kenh['snr_db']} dB"
            })
            return False

        # Tính tổng độ trễ = Độ trễ lan truyền sóng (Propagation) + Độ trễ truyền dữ liệu (Serialization)
        dung_luong_mbps = max(0.1, thong_so_kenh["dung_luong_mbps"])
        do_tre_truyen_tai_ms = (kich_thuoc_kb * 8.0 / 1000.0) / dung_luong_mbps * 1000.0
        tong_do_tre_ms = round(thong_so_kenh["do_tre_ms"] + do_tre_truyen_tai_ms, 3)
        thong_so_kenh["do_tre_ms"] = tong_do_tre_ms

        # Gửi dữ liệu liên kết vô tuyến để hiển thị tuyến trên bản đồ.
        await self.phat_su_kien_ra_web("RF_LINK", {
            "tu_node": ma_nguoi_gui,
            "den_node": ma_nguoi_nhan,
            "loai_tin": loai_tin,
            "toa_do_dau": nguoi_gui.toa_do,
            "toa_do_cuoi": nguoi_nhan.toa_do,
            "snr_db": thong_so_kenh["snr_db"],
            "do_tre_ms": tong_do_tre_ms,
            "dung_luong_mbps": thong_so_kenh["dung_luong_mbps"]
        })

        # Mô phỏng thời gian sóng bay trong không gian (Scale nhẹ để hiệu ứng thị giác mượt mà)
        await asyncio.sleep(min(0.5, max(0.02, tong_do_tre_ms / 100.0)))

        # Đính kèm Metadata lớp Vật lý vào gói tin để người nhận sử dụng tính toán
        goi_tin_hoan_chinh = {
            **thong_diep,
            "nguoi_gui": ma_nguoi_gui,
            "nguoi_nhan": ma_nguoi_nhan,
            "thoi_diem_gui": thong_diep.get("thoi_diem_gui", time.time()),
            "thong_so_kenh": thong_so_kenh
        }

        await nguoi_nhan.hang_doi_thong_diep.put(goi_tin_hoan_chinh)

        # Cập nhật thống kê toàn mạng & ghi vào file CSV thực nghiệm
        self.thong_ke_toan_mang["tong_goi_tin_thanh_cong"] += 1
        n_ok = self.thong_ke_toan_mang["tong_goi_tin_thanh_cong"]
        cur_avg = self.thong_ke_toan_mang["do_tre_trung_binh_ms"]
        self.thong_ke_toan_mang["do_tre_trung_binh_ms"] = round(((cur_avg * (n_ok - 1)) + tong_do_tre_ms) / n_ok, 3)
        self.thong_ke_toan_mang["nang_luong_tieu_hao_joule"] = round(
            sum(agent.thong_ke["nang_luong_tieu_hao_joule"] for agent in self.danh_sach_tac_nhan.values()),
            3
        )

        self.scenario.ghi_log_thuc_nghiem_qos(
            ma_nguoi_gui, ma_nguoi_nhan, loai_tin, thong_so_kenh, "THANH_CONG"
        )
        return True

    async def phat_song_quang_ba(
        self,
        ma_nguoi_gui: str,
        thong_diep: dict,
        danh_sach_nguoi_nhan: Optional[set[str]] = None
    ):
        """
        Phát sóng quảng bá (Broadcast) đến tất cả các Node lân cận nằm trong tầm nhìn thẳng (LoS).
        Các tác vụ gửi tới từng đích được thực thi song song bằng `asyncio.gather`.
        Khi cung cấp danh sách người nhận, giới hạn broadcast trong nhóm tác nhân đó.
        """
        cac_tac_vu = []
        for ma_dich in self.danh_sach_tac_nhan:
            if (
                ma_dich != ma_nguoi_gui
                and (
                    danh_sach_nguoi_nhan is None
                    or ma_dich in danh_sach_nguoi_nhan
                )
            ):
                cac_tac_vu.append(
                    self.gui_don_huong(ma_nguoi_gui, ma_dich, thong_diep.copy())
                )
        if cac_tac_vu:
            await asyncio.gather(*cac_tac_vu)

    # =========================================================================
    # 2. QUẢN LÝ KẾT NỐI WEBSOCKET & ĐỒNG BỘ GIAO DIỆN
    # =========================================================================
    async def phat_su_kien_ra_web(self, loai_su_kien: str, du_lieu):
        """Đẩy gói tin JSON thời gian thực xuống tất cả trình duyệt đang mở"""
        if not self.danh_sach_client_ws:
            return

        payload = json.dumps({
            "type": loai_su_kien,
            "timestamp": time.strftime("%H:%M:%S"),
            "data": du_lieu
        }, ensure_ascii=False)

        client_loi = []
        for ws in self.danh_sach_client_ws:
            try:
                await ws.send_text(payload)
            except Exception:
                client_loi.append(ws)

        for ws in client_loi:
            if ws in self.danh_sach_client_ws:
                self.danh_sach_client_ws.remove(ws)

    async def gui_log_ra_web(self, loai_log: str, noi_dung: str):
        """Hàm tiện ích tương thích nhanh để in nhật ký sự kiện lên bảng điều khiển Web"""
        await self.phat_su_kien_ra_web("LOG", {
            "cap_do": loai_log,
            "noi_dung": noi_dung
        })

    async def dong_bo_trang_thai_toan_mang(self):
        """
        Thu thập tọa độ, pin và trạng thái của toàn bộ tác tử cùng thống kê QoS.
        """
        danh_sach_nodes = [
            agent.xuat_ho_so_trang_thai()
            for agent in self.danh_sach_tac_nhan.values()
        ]
        await self.phat_su_kien_ra_web("WORLD_STATE", {
            "nodes": danh_sach_nodes,
            "network_metrics": self.thong_ke_toan_mang,
            "connectivity_metrics": getattr(self, "thong_ke_lien_tuc", {})
        })