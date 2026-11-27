"""
Bộ điều phối Giám sát Tự Phát hiện & Phản ứng Cục bộ theo Heatmap (Autonomous Swarm Response).
Mô hình Client-Server 3 tầng SAGSINS thuần (A2G trực tiếp <= 3.5 km hoặc Vệ tinh LEO <= 2500 km).
"""
import math
import time
from typing import Any, Optional


class GroundStationServer:
    """
    Trung tâm Giám sát & Tiếp nhận Báo cáo Sự cố:
    - Khi tạo đám cháy: Đánh dấu trạng thái "UNVERIFIED" (Trạm chưa biết / Đang âm ỉ).
    - Khi UAV Client bay vào bán kính <= 600m: Chuyển trạng thái sang "DETECTED".
    - Khi UAV Client đủ pin khứ hồi (> 25%) hoặc được Server điều phối bàn giao:
      Chuyển trạng thái sang "CONTAINING" (Đang khoanh vùng 130m) và ghi nhận kênh truyền A2G/LEO.
    """

    def __init__(self, broker: Any):
        self.broker: Any = broker
        self.danh_sach_su_co: dict[str, dict[str, Any]] = {}
        self.nhat_ky_phan_ung_bay_dan: list[dict[str, Any]] = []
        self.su_co_hien_tai: Optional[dict[str, Any]] = None
        self.so_su_co_tu_phat_hien: int = 0

    @staticmethod
    def _khoang_cach_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        r_earth = 6371000.0
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp = p2 - p1
        dl = math.radians(lon2 - lon1)
        a = math.sin(dp / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
        return r_earth * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

    def _tinh_diem_rui_ro_nhiet_tb(self, zone_id: Optional[str] = None) -> float:
        scenario = getattr(self.broker, "scenario", None)
        if scenario and hasattr(scenario, "tinh_diem_rui_ro_nhiet_tb"):
            return scenario.tinh_diem_rui_ro_nhiet_tb(zone_id=zone_id)
        heat_nodes = getattr(scenario, "heat_nodes", None) or []
        if zone_id:
             filtered = [n for n in heat_nodes if getattr(n, "zone_id", None) == zone_id]
             if filtered:
                 heat_nodes = filtered
        if not heat_nodes:
            return 0.05
        return round(sum(float(getattr(n, "risk_score", 0.05)) for n in heat_nodes) / len(heat_nodes), 4)

    def _cap_nhat_trang_thai_heat_nodes(self, vi_do: float, kinh_do: float, fire_status: str, ban_kinh_m: float = 850.0):
        scenario = getattr(self.broker, "scenario", None)
        heat_nodes = getattr(scenario, "heat_nodes", None) or []
        for node in heat_nodes:
            if self._khoang_cach_m(vi_do, kinh_do, node.lat, node.lon) <= ban_kinh_m:
                node.fire_status = fire_status

    def tim_su_co_gan_nhat(self, vi_do: float, kinh_do: float, ban_kinh_m: float = 950.0) -> Optional[dict[str, Any]]:
        tot_nhat = None
        kc_min = float("inf")
        for record in self.danh_sach_su_co.values():
            kc = self._khoang_cach_m(vi_do, kinh_do, record["vi_do"], record["kinh_do"])
            if kc <= ban_kinh_m and kc < kc_min:
                kc_min = kc
                tot_nhat = record
        return tot_nhat

    def xuat_trang_thai_phan_ung(self) -> dict[str, Any]:
        """Xuất dữ liệu đồng bộ cho cả trang Bản đồ và trang Dashboard."""
        avg_global = self._tinh_diem_rui_ro_nhiet_tb(None)
        current = None
        if self.su_co_hien_tai:
            z_id = self.su_co_hien_tai.get("zone_id")
            self.su_co_hien_tai["diem_rui_ro_nhiet_tb"] = self._tinh_diem_rui_ro_nhiet_tb(z_id)
            current = dict(self.su_co_hien_tai)

        history = [dict(item) for item in reversed(self.nhat_ky_phan_ung_bay_dan[-25:])]
        return {
            "current": current,
            "history": history,
            "total_detected": self.so_su_co_tu_phat_hien,
            "avg_heat_risk": avg_global,
        }

    async def _dong_bo_su_kien_ra_web(self):
        payload = self.xuat_trang_thai_phan_ung()
        if hasattr(self.broker, "phat_su_kien_ra_web"):
            await self.broker.phat_su_kien_ra_web("SWARM_RESPONSE_UPDATE", payload)

    async def khoi_tao_dam_chay_unverified(
        self,
        ma_su_co: str,
        vi_do: float,
        kinh_do: float,
        zone_id: Optional[str],
        so_diem_nong: int
    ) -> dict[str, Any]:
        """
        1. Khi tạo đám cháy: Đánh dấu đám cháy ở trạng thái "UNVERIFIED" (Trạm chưa biết)
        và tăng risk_score các nút Heatmap xung quanh lên 1.0.
        """
        z_id = zone_id or "ZONE_01"
        avg_risk = self._tinh_diem_rui_ro_nhiet_tb(z_id)
        self._cap_nhat_trang_thai_heat_nodes(vi_do, kinh_do, "UNVERIFIED")

        record: dict[str, Any] = {
            "ma_su_co": ma_su_co,
            "thoi_gian": time.strftime("%H:%M:%S"),
            "zone_id": z_id,
            "vi_do": round(float(vi_do), 4),
            "kinh_do": round(float(kinh_do), 4),
            "so_diem_nong": int(so_diem_nong),
            "trang_thai_chay": "UNVERIFIED",
            "trang_thai_hien_thi": "Đang âm ỉ (UNVERIFIED)",
            "uav_phat_hien_dau_tien": None,
            "uav_nhan_nhiem_vu": None,
            "loai_phan_ung": "PENDING",
            "co_che_phan_ung": "Đang hút theo trường nhiệt",
            "pin_khu_hoi_du_kien_pct": None,
            "diem_rui_ro_nhiet_tb": avg_risk,
            "tuyen_bao_cao_e2e": [],
            "_da_dem_kpi": False,
        }
        self.danh_sach_su_co[ma_su_co] = record
        self.nhat_ky_phan_ung_bay_dan.append(record)
        self.su_co_hien_tai = record

        await self.broker.gui_log_ra_web(
            "ALERT",
            f"[HEATMAP_ANOMALY] Khởi phát đám cháy tại ({vi_do:.4f}, {kinh_do:.4f}) thuộc {z_id} | "
            f"Trạng thái: UNVERIFIED (Trạm chưa biết) | Đẩy {so_diem_nong} nút Heatmap xung quanh lên risk_score=1.0."
        )
        await self.broker.gui_log_ra_web(
            "INFO",
            f"[GRADIENT_PULL] Các UAV tuần tra trong {z_id} bị hút về phía vùng risk_score=1.0 "
            f"quanh tọa độ ({vi_do:.4f}, {kinh_do:.4f}) (Rủi ro nhiệt TB vùng: {avg_risk:.2f})."
        )
        await self._dong_bo_su_kien_ra_web()
        return record

    async def ghi_nhan_phat_hien_dam_chay(
        self,
        vi_do: float,
        kinh_do: float,
        uav_phat_hien: str,
        khoang_cach_m: float,
        zone_id: Optional[str] = None
    ) -> dict[str, Any]:
        """
        2. Khi 1 UAV bay vào cách đám cháy <= 600m, chuyển trạng thái đám cháy sang "DETECTED".
        """
        record = self.tim_su_co_gan_nhat(vi_do, kinh_do)
        if not record:
            ma_moi = f"FIRE_{int(time.time() * 1000)}"
            record: dict[str, Any] = {
                "ma_su_co": ma_moi,
                "thoi_gian": time.strftime("%H:%M:%S"),
                "zone_id": zone_id or "ZONE_01",
                "vi_do": round(float(vi_do), 4),
                "kinh_do": round(float(kinh_do), 4),
                "so_diem_nong": 1,
                "trang_thai_chay": "DETECTED",
                "trang_thai_hien_thi": "Đã phát hiện (DETECTED)",
                "uav_phat_hien_dau_tien": uav_phat_hien,
                "uav_nhan_nhiem_vu": None,
                "loai_phan_ung": "DETECTED",
                "co_che_phan_ung": "Đang kiểm tra pin khứ hồi",
                "pin_khu_hoi_du_kien_pct": None,
                "diem_rui_ro_nhiet_tb": self._tinh_diem_rui_ro_nhiet_tb(zone_id),
                "tuyen_bao_cao_e2e": [],
                "_da_dem_kpi": False,
            }
            self.danh_sach_su_co[ma_moi] = record
            self.nhat_ky_phan_ung_bay_dan.append(record)

        if not record.get("uav_phat_hien_dau_tien"):
            record["uav_phat_hien_dau_tien"] = uav_phat_hien
        if record.get("trang_thai_chay") == "UNVERIFIED":
            record["trang_thai_chay"] = "DETECTED"
            record["trang_thai_hien_thi"] = "Đã phát hiện (DETECTED)"

        self._cap_nhat_trang_thai_heat_nodes(vi_do, kinh_do, "DETECTED")
        self.su_co_hien_tai = record

        await self.broker.gui_log_ra_web(
            "WARN",
            f"[FIRE_DETECTED] [{uav_phat_hien}] bay vào bán kính quét <= 600m (cách {khoang_cach_m:.0f}m) "
            f"tại ({vi_do:.4f}, {kinh_do:.4f}) ➔ Chuyển trạng thái đám cháy sang DETECTED."
        )
        await self._dong_bo_su_kien_ra_web()
        return record

    async def ghi_nhan_khoa_muc_tieu(
        self,
        vi_do: float,
        kinh_do: float,
        uav_id: str,
        pin_khu_hoi_pct: float,
        zone_id: Optional[str] = None,
        la_p2p: bool = False,
        uav_phat_hien_goc: Optional[str] = None
    ) -> dict[str, Any]:
        """
        3a. Khi UAV đủ pin khứ hồi (> 25%) tự khóa mục tiêu (Direct Lock) hoặc nhận bàn giao P2P,
        lao vào khoanh vùng 130m.
        """
        record = self.tim_su_co_gan_nhat(vi_do, kinh_do)
        if not record:
            record = await self.ghi_nhan_phat_hien_dam_chay(
                vi_do, kinh_do, uav_phat_hien_goc or uav_id, 0.0, zone_id=zone_id
            )

        if uav_phat_hien_goc and not record.get("uav_phat_hien_dau_tien"):
            record["uav_phat_hien_dau_tien"] = uav_phat_hien_goc
        elif not record.get("uav_phat_hien_dau_tien"):
            record["uav_phat_hien_dau_tien"] = uav_id

        record["uav_nhan_nhiem_vu"] = uav_id
        record["trang_thai_chay"] = "CONTAINING"
        record["trang_thai_hien_thi"] = "Đang khoanh vùng (CONTAINING)"
        record["pin_khu_hoi_du_kien_pct"] = round(float(pin_khu_hoi_pct), 1)
        record["diem_rui_ro_nhiet_tb"] = self._tinh_diem_rui_ro_nhiet_tb(zone_id or record.get("zone_id"))

        if la_p2p or record.get("loai_phan_ung") == "SWARM_HANDOVER":
            record["loai_phan_ung"] = "SWARM_HANDOVER"
            record["co_che_phan_ung"] = "Bàn giao qua Server (Task Handover)"
        else:
            record["loai_phan_ung"] = "DIRECT_LOCK"
            record["co_che_phan_ung"] = "Tự khóa mục tiêu (Direct Lock)"

        if not record.get("_da_dem_kpi"):
            record["_da_dem_kpi"] = True
            self.so_su_co_tu_phat_hien += 1
            sim_loop = getattr(self.broker, "simulation_loop", None)
            if sim_loop is not None:
                sim_loop._so_nhiem_vu_dieu_phoi = self.so_su_co_tu_phat_hien
                sim_loop._chi_so_lien_lac["nhiem_vu_da_dieu_phoi"] = self.so_su_co_tu_phat_hien

        self._cap_nhat_trang_thai_heat_nodes(vi_do, kinh_do, "CONTAINING")
        self.su_co_hien_tai = record

        await self.broker.gui_log_ra_web(
            "WIN",
            f"[TARGET_LOCKED] [{uav_id}] (Pin khứ hồi dự kiến: {pin_khu_hoi_pct:.1f}% > 25%) "
            f"khóa mục tiêu đám cháy ({vi_do:.4f}, {kinh_do:.4f}) "
            f"[{record['co_che_phan_ung']}] và bay vào khoanh vùng 130m."
        )
        await self._dong_bo_su_kien_ra_web()
        return record

    async def ghi_nhan_ban_giao_nhiem_vu(
        self,
        vi_do: float,
        kinh_do: float,
        uav_phat_hien: str,
        uav_tiep_ung: str,
        pin_phat_hien_pct: float,
        pin_tiep_ung_pct: float,
        zone_id: Optional[str] = None
    ) -> dict[str, Any]:
        """
        3b. Khi UAV phát hiện có pin khứ hồi <= 25%, Server điều phối bàn giao trực tiếp
        cho UAV Client có pin cao nhất khả dụng trong cùng phân khu xử lý thay.
        """
        record = self.tim_su_co_gan_nhat(vi_do, kinh_do)
        if not record:
            record = await self.ghi_nhan_phat_hien_dam_chay(
                vi_do, kinh_do, uav_phat_hien, 0.0, zone_id=zone_id
            )

        record["uav_phat_hien_dau_tien"] = uav_phat_hien
        record["uav_nhan_nhiem_vu"] = uav_tiep_ung
        record["loai_phan_ung"] = "SWARM_HANDOVER"
        record["co_che_phan_ung"] = "Bàn giao qua Server (Task Handover)"
        record["pin_khu_hoi_du_kien_pct"] = round(float(pin_tiep_ung_pct), 1)
        record["diem_rui_ro_nhiet_tb"] = self._tinh_diem_rui_ro_nhiet_tb(zone_id or record.get("zone_id"))
        self.su_co_hien_tai = record

        await self.broker.gui_log_ra_web(
            "WARN",
            f"[SWARM_HANDOVER] [{uav_phat_hien}] pin khứ hồi yếu ({pin_phat_hien_pct:.1f}% <= 25%) "
            f"➔ Server điều phối [{uav_tiep_ung}] (pin vùng {zone_id or record.get('zone_id')}: "
            f"khứ hồi {pin_tiep_ung_pct:.1f}%) xử lý đám cháy ({vi_do:.4f}, {kinh_do:.4f})."
        )
        await self._dong_bo_su_kien_ra_web()
        return record

    async def ghi_nhan_bao_cao_truyen_tin(
        self,
        vi_do: float,
        kinh_do: float,
        uav_id: str,
        lo_trinh_hop: list[str],
        do_tre_ms: float = 0.0,
        link_type: str = "GROUND_LINK",
    ):
        """Ghi nhận tuyến báo cáo 2 kênh SAGSINS (A2G / LEO) từ UAV khoanh vùng về Trạm Kiểm Lâm."""
        record = self.tim_su_co_gan_nhat(vi_do, kinh_do)
        if record:
            record["tuyen_bao_cao_e2e"] = list(lo_trinh_hop)
            record["kenh_truyen"] = link_type
            self.su_co_hien_tai = record

        chuoi_tuyen = " -> ".join(
            n.replace("MÁY_BAY_", "").replace("TRẠM_KIỂM_LÂM_", "")
            for n in lo_trinh_hop
        )
        kenh_label = "A2G trực tiếp" if link_type == "GROUND_LINK" else "Vệ tinh LEO (A2S+S2G)"
        await self.broker.gui_log_ra_web(
            "ROUTE",
            f"[SAGSINS_REPORT] [{uav_id}] gửi báo cáo cháy ({vi_do:.4f}, {kinh_do:.4f}) "
            f"qua kênh {kenh_label}: {chuoi_tuyen} | Trễ: {do_tre_ms:.1f} ms."
        )
        await self._dong_bo_su_kien_ra_web()
