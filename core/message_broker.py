import asyncio
import heapq
import json
import random
import time
from typing import Any, Dict, List, TYPE_CHECKING, Optional
from fastapi import WebSocket

if TYPE_CHECKING:
    from core.base_agent import BaseAgent

RouteChannel = dict[str, Any]
RouteState = tuple[float, str, tuple[str, ...], tuple[RouteChannel, ...]]


class MessageBroker:
    """
    Môi trường truyền dẫn vô tuyến trung tâm (Wireless Channel & Event Broker):
    - Kiểm tra điều kiện vật lý (LoS, SNR, PER, Delay) trước khi chuyển phát gói tin.
    - Cập nhật tri thức láng giềng cục bộ cho nút nhận khi gói tin truyền qua kênh vật lý.
    - Đồng bộ trạng thái mạng, liên kết, độ phủ 7 phân khu và số liệu QoS ra giao diện Web.
    """
    def __init__(self, physics_engine: Any, scenario_manager: Any):
        self.physics: Any = physics_engine
        self.scenario: Any = scenario_manager
        self.danh_sach_tac_nhan: Dict[str, Any] = {}
        self.danh_sach_client_ws: List[WebSocket] = []
        self.phien_dau_gia_gan_nhat: Optional[dict[str, Any]] = None
        self.tuyen_e2e_gan_nhat: Optional[dict[str, Any]] = None
        self.nhat_ky_gan_day: List[dict[str, Any]] = []
        self.ground_station_server: Any = None
        self.thong_ke_lien_tuc: dict[str, Any] = {}

        self.thong_ke_toan_mang: dict[str, Any] = {
            "tong_goi_tin_phat": 0,
            "tong_goi_tin_thanh_cong": 0,
            "tong_goi_tin_rot_per": 0,
            "tong_goi_tin_mat_los": 0,
            "do_tre_trung_binh_ms": 0.0,
            "nang_luong_tieu_hao_joule": 0.0
        }

    def dang_ky_tac_nhan(self, tac_nhan: Any):
        """Đăng ký một Node mạng (UAV, Vệ tinh, Trạm mặt đất) vào không gian mô phỏng"""
        self.danh_sach_tac_nhan[tac_nhan.ma_dinh_danh] = tac_nhan
        print(f"[WIRELESS MEDIUM] Đã đăng ký Node: {tac_nhan.ma_dinh_danh} ({tac_nhan.loai_tac_nhan})")

    def lay_tac_nhan(self, ma_dinh_danh: str) -> Optional[Any]:
        return self.danh_sach_tac_nhan.get(ma_dinh_danh)

    def cap_nhat_nang_luong_tong(self):
        self.thong_ke_toan_mang["nang_luong_tieu_hao_joule"] = round(
            sum(
                getattr(agent, "thong_ke", {}).get("nang_luong_tieu_hao_joule", 0.0)
                for agent in self.danh_sach_tac_nhan.values()
            ),
            3
        )

    def tim_tuyen_den_tram_mat_dat(
        self,
        ma_bat_dau: str,
        kich_thuoc_kb: float = 250.0,
        loai_tru: Optional[set[str]] = None
    ) -> Optional[dict[str, Any]]:
        """
        Tìm tuyến khả dụng có chi phí QoS + Năng lượng thấp nhất từ một node tới trạm mặt đất.
        Đã khắc phục lỗi tự khóa trạng thái RELAYING: cho phép nhiều UAV cùng định tuyến
        qua một UAV trung chuyển và cân bằng tải dựa trên số luồng relay + mức pin + bộ đệm.
        """
        bat_dau = self.danh_sach_tac_nhan.get(ma_bat_dau)
        if not bat_dau:
            return None
        cac_node_loai_tru = loai_tru or set()
        if bat_dau.loai_tac_nhan == "TRẠM_MẶT_ĐẤT":
            return {"nodes": [ma_bat_dau], "channels": [], "cost": 0.0, "energy_j": 0.0, "delay_ms": 0.0}

        hang_doi: List[RouteState] = [(0.0, ma_bat_dau, (ma_bat_dau,), ())]
        chi_phi_tot_nhat = {ma_bat_dau: 0.0}
        while hang_doi:
            chi_phi, ma_hien_tai, nodes, channels = heapq.heappop(hang_doi)
            if chi_phi > chi_phi_tot_nhat.get(ma_hien_tai, float("inf")):
                continue
            hien_tai = self.danh_sach_tac_nhan[ma_hien_tai]
            if hien_tai.loai_tac_nhan == "TRẠM_MẶT_ĐẤT":
                ket_qua = {
                    "nodes": list(nodes),
                    "channels": list(channels),
                    "cost": round(chi_phi, 3),
                    "energy_j": round(sum(ch["energy_j"] for ch in channels), 3),
                    "delay_ms": round(sum(ch["do_tre_ms"] for ch in channels), 3)
                }
                if hasattr(bat_dau, "tuyen_ve_tram_cuc_bo"):
                    bat_dau.tuyen_ve_tram_cuc_bo = list(nodes)
                    bat_dau.chi_phi_ve_tram_cuc_bo = round(chi_phi, 3)
                return ket_qua

            for ma_ke_tiep, ke_tiep in self.danh_sach_tac_nhan.items():
                kenh: RouteChannel
                if (
                    ma_ke_tiep in nodes
                    or ma_ke_tiep in cac_node_loai_tru
                    or ke_tiep.trang_thai.value == "NGẮT_KẾT_NỐI"
                    or (
                        ke_tiep.loai_tac_nhan == "MÁY_BAY"
                        and ke_tiep.trang_thai.value == "ĐANG_ĐẤU_GIÁ"
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
                kenh_co_nang_luong: RouteChannel = {**kenh, "energy_j": energy_j}

                # Trọng số cân bằng tải khi đi qua UAV Relay hoặc Vệ tinh LEO Backhaul
                so_luong_relay = len(getattr(ke_tiep, "_relay_requesters", ()))
                relay_load_penalty = so_luong_relay * 2.0 if ke_tiep.loai_tac_nhan == "MÁY_BAY" else 0.0
                battery_penalty = (
                    (100.0 - ke_tiep.phan_tram_pin) * 0.08
                    if ke_tiep.loai_tac_nhan == "MÁY_BAY" else 0.0
                )
                space_backhaul_cost = 18.0 if ke_tiep.loai_tac_nhan == "VỆ_TINH_LEO" else 0.0

                weight = (
                    kenh["do_tre_ms"]
                    + 40.0 / max(0.5, kenh["dung_luong_mbps"])
                    + energy_j
                    + ke_tiep.tai_hang_doi_pct * 0.05
                    + relay_load_penalty
                    + battery_penalty
                    + space_backhaul_cost
                )
                next_cost = chi_phi + weight
                if next_cost < chi_phi_tot_nhat.get(ma_ke_tiep, float("inf")):
                    chi_phi_tot_nhat[ma_ke_tiep] = next_cost
                    heapq.heappush(
                        hang_doi,
                        (next_cost, ma_ke_tiep, (*nodes, ma_ke_tiep), (*channels, kenh_co_nang_luong))
                    )

        if hasattr(bat_dau, "tuyen_ve_tram_cuc_bo"):
            bat_dau.tuyen_ve_tram_cuc_bo = []
            bat_dau.chi_phi_ve_tram_cuc_bo = float("inf")
        return None

    @staticmethod
    def _uoc_tinh_nang_luong_truyen(agent, kich_thuoc_kb: float) -> float:
        cong_suat_w = 10.0 ** ((agent.radio_config["cong_suat_phat_dbm"] - 30.0) / 10.0)
        return round(cong_suat_w * 0.05 * (kich_thuoc_kb / 10.0), 4)

    def tao_bao_gia(self, ma_ung_vien: str, ma_nguon: str, thong_so_chang_dau: dict[str, Any], kich_thuoc_kb: float) -> Optional[dict[str, Any]]:
        """Định giá đường truyền từ nguồn, qua ứng viên, đến một trạm mặt đất."""
        ung_vien = self.danh_sach_tac_nhan.get(ma_ung_vien)
        if not ung_vien:
            return None
        tuyen: dict[str, Any]
        if ung_vien.loai_tac_nhan == "TRẠM_MẶT_ĐẤT":
            tuyen = {"nodes": [ma_ung_vien], "channels": [], "cost": 0.0, "energy_j": 0.0, "delay_ms": 0.0}
        else:
            tuyen_tim = self.tim_tuyen_den_tram_mat_dat(
                ma_ung_vien, kich_thuoc_kb, loai_tru={ma_nguon}
            )
            if not tuyen_tim:
                return None
            tuyen = tuyen_tim

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

        loai_tin = thong_diep.get("loai_tin", "UNKNOWN")
        la_beacon = (loai_tin == "HELLO_BEACON")
        kich_thuoc_kb = float(thong_diep.get("kich_thuoc_kb", 10.0))

        # Tính toán thông số Lớp Vật lý (PHY Layer) tại thời điểm phát sóng
        thong_so_kenh = self.physics.danh_gia_chat_luong_kenh_truyen(
            pos1=nguoi_gui.toa_do,
            pos2=nguoi_nhan.toa_do,
            tan_so_hz=nguoi_gui.radio_config["tan_so_hz"],
            cong_suat_phat_dbm=nguoi_gui.radio_config["cong_suat_phat_dbm"],
            bang_thong_hz=nguoi_gui.radio_config["bang_thong_hz"]
        )

        # Với bản tin quảng bá láng giềng 1-hop (HELLO_BEACON), các node ngoài vùng phủ LoS tự nhiên không thu được sóng
        if la_beacon and not thong_so_kenh["kha_dung"]:
            return False

        # Khấu trừ năng lượng phát sóng (Tx) của người gửi (nếu chưa khấu trừ ở mức quảng bá)
        if not thong_diep.get("_da_khau_tru_tx") and hasattr(nguoi_gui, "tieu_hao_nang_luong_truyen_tin"):
            nang_luong_tx = nguoi_gui.tieu_hao_nang_luong_truyen_tin(
                kich_thuoc_kb, la_phat_song=True
            )
        else:
            nang_luong_tx = float(thong_diep.get("_nang_luong_tx", 0.0))
        self.cap_nhat_nang_luong_tong()

        if not la_beacon:
            self.thong_ke_toan_mang["tong_goi_tin_phat"] += 1

        thong_so_kenh["nang_luong_tx_joule"] = nang_luong_tx
        thong_so_kenh["do_kha_dung_pct"] = self.thong_ke_lien_tuc.get("do_kha_dung_pct", 100.0)
        thong_so_kenh["thoi_gian_mat_lien_lac_giay"] = self.thong_ke_lien_tuc.get("thoi_gian_mat_lien_lac_giay", 0.0)
        thong_so_kenh["nang_luong_toan_mang_joule"] = self.thong_ke_toan_mang["nang_luong_tieu_hao_joule"]

        # Kiểm tra 1: Mất tầm nhìn thẳng (Khuất núi Sơn Trà / Khuất bóng Trái Đất / Quá cự ly)
        if not thong_so_kenh["kha_dung"]:
            self.thong_ke_toan_mang["tong_goi_tin_mat_los"] += 1
            if hasattr(self.scenario, "ghi_log_thuc_nghiem_qos"):
                self.scenario.ghi_log_thuc_nghiem_qos(
                    ma_nguoi_gui, ma_nguoi_nhan, loai_tin, thong_so_kenh, "MAT_KET_NOI_LOS"
                )
            return False

        # Kiểm tra 2: Xét xác suất lỗi gói tin (Packet Error Rate - PER) do nhiễu nhiệt
        if random.random() < thong_so_kenh["per"]:
            if not la_beacon:
                self.thong_ke_toan_mang["tong_goi_tin_rot_per"] += 1
                if hasattr(self.scenario, "ghi_log_thuc_nghiem_qos"):
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

        # Gửi dữ liệu liên kết vô tuyến để hiển thị tia sóng trên bản đồ
        if not la_beacon:
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

        await asyncio.sleep(min(0.25, max(0.01, tong_do_tre_ms / 150.0)))

        # Đính kèm Metadata lớp Vật lý và thông tin Beacon người gửi để người nhận cập nhật Bảng láng giềng cục bộ
        goi_tin_hoan_chinh = {
            k: v for k, v in thong_diep.items()
            if not k.startswith("_")
        }
        goi_tin_hoan_chinh.update({
            "nguoi_gui": ma_nguoi_gui,
            "nguoi_nhan": ma_nguoi_nhan,
            "loai_nguoi_gui": nguoi_gui.loai_tac_nhan,
            "toa_do_nguoi_gui": dict(nguoi_gui.toa_do),
            "pin_pct": getattr(nguoi_gui, "phan_tram_pin", 100.0),
            "tai_hang_doi_pct": getattr(nguoi_gui, "tai_hang_doi_pct", 0.0),
            "trang_thai_nguoi_gui": getattr(nguoi_gui.trang_thai, "value", str(nguoi_gui.trang_thai)),
            "zone_id": getattr(nguoi_gui, "zone_id", None),
            "cost_to_sink": getattr(nguoi_gui, "chi_phi_ve_tram_cuc_bo", float("inf")),
            "path_to_sink": list(getattr(nguoi_gui, "tuyen_ve_tram_cuc_bo", [])),
            "thoi_diem_gui": thong_diep.get("thoi_diem_gui", time.time()),
            "thong_so_kenh": thong_so_kenh
        })

        if hasattr(nguoi_nhan, "cap_nhat_lang_gieng_tu_goi_tin"):
            nguoi_nhan.cap_nhat_lang_gieng_tu_goi_tin(goi_tin_hoan_chinh)

        await nguoi_nhan.hang_doi_thong_diep.put(goi_tin_hoan_chinh)

        # Cập nhật thống kê toàn mạng & ghi vào file CSV thực nghiệm cho các bản tin dữ liệu/điều khiển
        if not la_beacon:
            self.thong_ke_toan_mang["tong_goi_tin_thanh_cong"] += 1
            n_ok = self.thong_ke_toan_mang["tong_goi_tin_thanh_cong"]
            cur_avg = self.thong_ke_toan_mang["do_tre_trung_binh_ms"]
            self.thong_ke_toan_mang["do_tre_trung_binh_ms"] = round(((cur_avg * (n_ok - 1)) + tong_do_tre_ms) / n_ok, 3)
            self.cap_nhat_nang_luong_tong()

            if hasattr(self.scenario, "ghi_log_thuc_nghiem_qos"):
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
        Chỉ khấu trừ năng lượng phát sóng 1 lần cho mỗi bản tin quảng bá vô hướng.
        """
        nguoi_gui = self.danh_sach_tac_nhan.get(ma_nguoi_gui)
        kich_thuoc_kb = float(thong_diep.get("kich_thuoc_kb", 10.0))
        nang_luong_tx = 0.0
        if nguoi_gui and hasattr(nguoi_gui, "tieu_hao_nang_luong_truyen_tin"):
            nang_luong_tx = nguoi_gui.tieu_hao_nang_luong_truyen_tin(kich_thuoc_kb, la_phat_song=True)

        goi_quang_ba = {
            **thong_diep,
            "_da_khau_tru_tx": True,
            "_nang_luong_tx": nang_luong_tx
        }
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
                    self.gui_don_huong(ma_nguoi_gui, ma_dich, goi_quang_ba.copy())
                )
        if cac_tac_vu:
            await asyncio.gather(*cac_tac_vu)

    async def gui_goi_tin_da_chang_ve_tram(self, ma_nguon: str, thong_diep: dict) -> bool:
        """
        Định tuyến và truyền gói tin đa chặng (Multi-hop E2E) từ UAV nguồn về Trạm mặt đất:
        - Tìm lộ trình tối ưu qua Dijkstra (A2G trực tiếp, FANET Relay hoặc Vệ tinh LEO).
        - Phát từng chặng qua `gui_don_huong` để kích hoạt hiệu ứng tia `RF_LINK` và thống kê QoS.
        - Phát sự kiện `E2E_ROUTE_COMPLETED` khi gói tin tới Trạm mặt đất.
        """
        kich_thuoc_kb = float(thong_diep.get("kich_thuoc_kb", 12.0))
        loai_tin = thong_diep.get("loai_tin") or thong_diep.get("loai_thong_diep") or "TELEMETRY_REPORT"
        tuyen = self.tim_tuyen_den_tram_mat_dat(ma_nguon, kich_thuoc_kb=kich_thuoc_kb)
        if not tuyen or len(tuyen.get("nodes", [])) < 2:
            return False

        nodes = list(tuyen["nodes"])
        channels = list(tuyen.get("channels", []))
        for idx in range(len(nodes) - 1):
            hop_msg = {
                **thong_diep,
                "loai_tin": loai_tin,
                "kich_thuoc_kb": kich_thuoc_kb,
                "lo_trinh_di_qua": nodes[: idx + 2],
            }
            ok = await self.gui_don_huong(nodes[idx], nodes[idx + 1], hop_msg)
            if not ok:
                return False

        last_ch = channels[-1] if channels else {}
        tong_tre_ms = round(float(tuyen.get("delay_ms", 0.0)), 2)
        ket_qua_e2e = {
            "ma_phien": thong_diep.get("ma_phien_dau_gia", "TELEM"),
            "loai_su_co": loai_tin,
            "lo_trinh": nodes,
            "lo_trinh_hop": nodes,
            "so_chang_hop": len(nodes) - 1,
            "e2e_delay_ms": tong_tre_ms,
            "tong_do_tre_ms": tong_tre_ms,
            "snr_chang_cuoi_db": last_ch.get("snr_db", 0.0),
            "dung_luong_chang_cuoi_mbps": last_ch.get("dung_luong_mbps", 0.0),
            "nang_luong_du_kien_joule": round(float(tuyen.get("energy_j", 0.0)), 3),
            "thoi_gian": time.strftime("%H:%M:%S"),
            "trang_thai_phu_song": "KET_NOI",
        }
        await self.phat_su_kien_ra_web("E2E_ROUTE_COMPLETED", ket_qua_e2e)
        return True

    # =========================================================================
    # 2. QUẢN LÝ KẾT NỐI WEBSOCKET & ĐỒNG BỘ GIAO DIỆN
    # =========================================================================
    async def phat_su_kien_ra_web(self, loai_su_kien: str, du_lieu):
        """Đẩy gói tin JSON thời gian thực xuống tất cả trình duyệt đang mở"""
        thoi_diem = time.strftime("%H:%M:%S")
        if loai_su_kien == "CNP_AUCTION_RESULT":
            self.phien_dau_gia_gan_nhat = du_lieu
        elif loai_su_kien == "E2E_ROUTE_COMPLETED":
            self.tuyen_e2e_gan_nhat = du_lieu
        elif loai_su_kien == "LOG" and isinstance(du_lieu, dict):
            self.nhat_ky_gan_day.append({
                "timestamp": thoi_diem,
                "cap_do": du_lieu.get("cap_do", "INFO"),
                "noi_dung": du_lieu.get("noi_dung", "")
            })
            if len(self.nhat_ky_gan_day) > 60:
                self.nhat_ky_gan_day = self.nhat_ky_gan_day[-60:]

        if not self.danh_sach_client_ws:
            return

        payload = json.dumps({
            "type": loai_su_kien,
            "timestamp": thoi_diem,
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
        """Hàm tiện ích in nhật ký sự kiện lên bảng điều khiển Web"""
        await self.phat_su_kien_ra_web("LOG", {
            "cap_do": loai_log,
            "noi_dung": noi_dung
        })

    def tinh_thong_ke_phu_song_vung(self) -> Dict[str, dict]:
        """Tính trạng thái độ phủ sóng thực tế của 7 phân khu rừng Sơn Trà."""
        vung_stats: Dict[str, dict] = {}
        for idx in range(1, 8):
            zone_id = f"ZONE_{idx:02d}"
            vung_stats[zone_id] = {
                "zone_id": zone_id,
                "tong_uav": 0,
                "uav_hoat_dong": 0,
                "uav_dang_sac_hoac_rtb": 0,
                "uav_nhan_ban_giao": 0,
                "trang_thai": "DU_PHU_SONG"
            }

        for agent in self.danh_sach_tac_nhan.values():
            if agent.loai_tac_nhan != "MÁY_BAY":
                continue
            z_id = getattr(agent, "zone_id", None)
            if not z_id or z_id not in vung_stats:
                continue
            entry = vung_stats[z_id]
            entry["tong_uav"] += 1
            if agent.trang_thai.value in ("NGẮT_KẾT_NỐI", "PIN_YẾU_RTB", "ĐANG_QUAY_VỀ_SẠC", "ĐANG_SẠC"):
                entry["uav_dang_sac_hoac_rtb"] += 1
            else:
                entry["uav_hoat_dong"] += 1
            vung_phu = getattr(agent, "_vung_tuan_tra_phu", None)
            if vung_phu:
                entry["uav_nhan_ban_giao"] += len(vung_phu)

        for z_id, entry in vung_stats.items():
            if entry["uav_hoat_dong"] == 0:
                entry["trang_thai"] = "MAT_PHU_SONG"
            elif entry["uav_hoat_dong"] < entry["tong_uav"]:
                entry["trang_thai"] = "BAN_GIAO_TAM_THOI"
            else:
                entry["trang_thai"] = "DU_PHU_SONG"
        return vung_stats

    async def dong_bo_trang_thai_toan_mang(self):
        """
        Thu thập tọa độ, pin, tuyến truyền, độ phủ 7 phân khu và thống kê QoS đẩy ra giao diện.
        """
        danh_sach_nodes = [
            agent.xuat_ho_so_trang_thai()
            for agent in self.danh_sach_tac_nhan.values()
        ]
        await self.phat_su_kien_ra_web("WORLD_STATE", {
            "nodes": danh_sach_nodes,
            "zone_coverage": self.tinh_thong_ke_phu_song_vung(),
            "network_metrics": self.thong_ke_toan_mang,
            "connectivity_metrics": getattr(self, "thong_ke_lien_tuc", {}),
            "last_auction": self.phien_dau_gia_gan_nhat,
            "last_e2e_route": self.tuyen_e2e_gan_nhat
        })
