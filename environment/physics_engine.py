"""Orbital and line-of-sight calculations using Skyfield."""
import math
from datetime import datetime, timezone, timedelta
from typing import cast
import numpy as np
from skyfield.api import load, EarthSatellite, wgs84

# Các hằng số Vật lý & Viễn thông chuẩn quốc tế
SPEED_OF_LIGHT = 299792458.0          # Vận tốc ánh sáng trong chân không (m/s)
EARTH_RADIUS_M = 6371000.0            # Bán kính trung bình Trái Đất (m)
BOLTZMANN_CONST = 1.380649e-23        # Hằng số Boltzmann (J/K)
SYSTEM_TEMP_K = 290.0                 # Nhiệt độ nhiễu hệ thống tiêu chuẩn (K)

class PhysicsEngine:
    """
    Động cơ Vật lý mô phỏng Quỹ đạo SGP4, Hình học Không gian 3D (ECEF),
    và Mô hình Kênh truyền Vô tuyến (FSPL, SNR, Shannon Capacity, PER).
    """
    def __init__(self):
        self.ts = load.timescale()
        self.satellites_tle = {}

    def dang_ky_tle_ve_tinh(self, sat_id: str, line1: str, line2: str):
        """Đăng ký đối tượng vệ tinh SGP4 vào hệ thống"""
        self.satellites_tle[sat_id] = EarthSatellite(line1, line2, sat_id, self.ts)

    def nap_danh_sach_tle(self, danh_sach_tle: list[dict]):
        """Nhận trực tiếp danh sách TLE từ ScenarioManager nạp vào bộ giải SGP4"""
        for item in danh_sach_tle:
            self.dang_ky_tle_ve_tinh(item["ma_ve_tinh"], item["tle_line1"], item["tle_line2"])

    def lay_vi_tri_ve_tinh(self, sat_id: str, bu_thoi_gian_giay: float = 0.0) -> dict:
        """
        Tính toán tọa độ (Vĩ độ, Kinh độ, Độ cao) và vận tốc 3D của vệ tinh bằng SGP4.
        Cho phép truyền `bu_thoi_gian_giay` để tăng tốc thời gian mô phỏng (Time Warp).
        """
        if sat_id not in self.satellites_tle:
            raise ValueError(f"Chưa có dữ liệu TLE cho {sat_id}")

        dt_now = datetime.now(timezone.utc) + timedelta(seconds=bu_thoi_gian_giay)
        t_skyfield = self.ts.from_datetime(dt_now)
        
        sat = self.satellites_tle[sat_id]
        geocentric = sat.at(t_skyfield)
        subpoint = wgs84.subpoint(geocentric)
        
        velocity_km_s = geocentric.velocity.km_per_s
        speed_m_s = float(np.linalg.norm(velocity_km_s) * 1000.0)

        return {
            "vi_do": round(cast(float, subpoint.latitude.degrees), 5),
            "kinh_do": round((cast(float, subpoint.longitude.degrees)), 5),
            "do_cao": round(cast(float, subpoint.elevation.m), 2),
            "van_toc_m_s": round(speed_m_s, 2)
        }

    @staticmethod
    def geodetic_to_ecef(lat: float, lon: float, alt: float) -> np.ndarray:
        """
        Chuyển đổi tọa độ Địa lý WGS84 (Lat, Lon, Alt) sang tọa độ Đề-các Địa tâm ECEF (X, Y, Z).
        Đơn vị đầu ra: mét.
        """
        lat_rad = math.radians(lat)
        lon_rad = math.radians(lon)
        
        a = 6378137.0               # Bán trục lớn Elipsoid WGS84 (m)
        f = 1.0 / 298.257223563     # Độ dẹt
        e2 = 2.0 * f - f * f        # Bình phương tâm sai thứ nhất

        N = a / math.sqrt(1.0 - e2 * (math.sin(lat_rad) ** 2))

        x = (N + alt) * math.cos(lat_rad) * math.cos(lon_rad)
        y = (N + alt) * math.cos(lat_rad) * math.sin(lon_rad)
        z = (N * (1.0 - e2) + alt) * math.sin(lat_rad)

        return np.array([x, y, z], dtype=float)

    def tinh_khoang_cach_3d(self, pos1: dict, pos2: dict) -> float:
        """Tính khoảng cách Euclid 3D chính xác (mét) giữa 2 node bất kỳ"""
        p1 = self.geodetic_to_ecef(pos1["vi_do"], pos1["kinh_do"], pos1["do_cao"])
        p2 = self.geodetic_to_ecef(pos2["vi_do"], pos2["kinh_do"], pos2["do_cao"])
        return float(np.linalg.norm(p2 - p1))

    def kiem_tra_tam_nhin_thang(self, pos1: dict, pos2: dict, goc_ngang_toi_thieu_deg: float = 5.0) -> tuple[bool, float]:
        """
        Kiểm tra điều kiện Tầm nhìn thẳng (Line-of-Sight - LoS) và tính Góc ngẩng (Elevation Angle).
        Trả về: (True/False kết nối khả dụng, Góc ngẩng tính bằng độ).
        """
        p1 = self.geodetic_to_ecef(pos1["vi_do"], pos1["kinh_do"], pos1["do_cao"])
        p2 = self.geodetic_to_ecef(pos2["vi_do"], pos2["kinh_do"], pos2["do_cao"])

        d_vec = p2 - p1
        dist_sq = float(np.dot(d_vec, d_vec))
        if dist_sq == 0.0:
            return True, 90.0

        # 1. Nếu là liên kết giữa 2 Vệ tinh (ISL - Inter-Satellite Link): Kiểm tra che khuất bởi Trái Đất
        if pos1["do_cao"] > 80000.0 and pos2["do_cao"] > 80000.0:
            t = -float(np.dot(p1, d_vec)) / dist_sq
            t_clamped = max(0.0, min(1.0, t))
            diem_gan_tam_nhat = p1 + t_clamped * d_vec
            khoang_cach_toi_tam = float(np.linalg.norm(diem_gan_tam_nhat))
            # Tia laser/vô tuyến ISL không được cắt qua tầng khí quyển dày (< 50km từ mặt đất)
            co_los = khoang_cach_toi_tam >= (EARTH_RADIUS_M + 50000.0)
            return co_los, 0.0

        # 2. Liên kết giữa Tầng thấp (UAV / Mặt đất) và Tầng cao (Vệ tinh hoặc UAV khác)
        low_pos, high_pos = (pos1, pos2) if pos1["do_cao"] <= pos2["do_cao"] else (pos2, pos1)
        p_low = self.geodetic_to_ecef(low_pos["vi_do"], low_pos["kinh_do"], low_pos["do_cao"])
        p_high = self.geodetic_to_ecef(high_pos["vi_do"], high_pos["kinh_do"], high_pos["do_cao"])

        vec_to_high = p_high - p_low
        dist = float(np.linalg.norm(vec_to_high))
        
        # Vector pháp tuyến hướng thẳng lên bầu trời (Zenith) tại vị trí node thấp
        up_unit = p_low / float(np.linalg.norm(p_low))
        target_unit = vec_to_high / dist

        sin_el = max(-1.0, min(1.0, float(np.dot(target_unit, up_unit))))
        elevation_deg = math.degrees(math.asin(sin_el))

        # Liên kết Không - Vũ trụ (A2S / S2G): Góc ngẩng phải >= goc_ngang_toi_thieu_deg (mặc định 5 độ)
        if high_pos["do_cao"] > 80000.0:
            return (elevation_deg >= goc_ngang_toi_thieu_deg), elevation_deg

        # Liên kết Nội tầng Không - Không (A2A giữa các UAV) hoặc Không - Đất (A2G):
        # Giới hạn cự ly truyền dẫn vô tuyến chiến thuật là 25 km
        return (dist <= 25000.0), elevation_deg

    def danh_gia_chat_luong_kenh_truyen(
        self,
        pos1: dict,
        pos2: dict,
        tan_so_hz: float = 2.4e9,        # Tần số sóng mang (2.4 GHz cho A2A, 12 GHz Ku-band cho Vệ tinh)
        cong_suat_phat_dbm: float = 33.0,# Công suất phát: 33 dBm (~2W cho UAV), 43 dBm (~20W cho Vệ tinh)
        bang_thong_hz: float = 20e6      # Băng thông kênh: 20 MHz
    ) -> dict:
        """
        Đánh giá toàn diện thông số Lớp Vật lý (PHY Layer) của liên kết vô tuyến:
        - Khoảng cách 3D, Độ trễ lan truyền (Propagation Delay), Suy hao FSPL, SNR, Dung lượng Shannon và PER.
        """
        co_los, goc_ngang = self.kiem_tra_tam_nhin_thang(pos1, pos2)
        khoang_cach_m = self.tinh_khoang_cach_3d(pos1, pos2)

        if not co_los or khoang_cach_m <= 0:
            return {
                "kha_dung": False,
                "khoang_cach_km": round(khoang_cach_m / 1000.0, 2),
                "goc_ngang_do": round(goc_ngang, 2),
                "do_tre_ms": 9999.0,
                "fspl_db": 999.0,
                "snr_db": -100.0,
                "dung_luong_mbps": 0.0,
                "per": 1.0
            }

        # 1. Độ trễ truyền sóng điện từ (ms)
        do_tre_ms = (khoang_cach_m / SPEED_OF_LIGHT) * 1000.0

        # 2. Suy hao không gian tự do (Free-Space Path Loss - FSPL)
        fspl_db = (
            20.0 * math.log10(khoang_cach_m)
            + 20.0 * math.log10(tan_so_hz)
            + 20.0 * math.log10(4.0 * math.pi / SPEED_OF_LIGHT)
        )

        # Suy hao khí quyển khi truyền xuyên tầng điện ly/đối lưu (Vệ tinh <-> UAV/Mặt đất)
        suy_hao_khi_quyen_db = 0.0
        if max(pos1["do_cao"], pos2["do_cao"]) > 80000.0 and min(pos1["do_cao"], pos2["do_cao"]) < 20000.0:
            sin_el = max(0.087, math.sin(math.radians(max(5.0, goc_ngang))))
            suy_hao_khi_quyen_db = 1.8 / sin_el

        tong_suy_hao_db = fspl_db + suy_hao_khi_quyen_db

        # 3. Quỹ đường truyền (Link Budget): Độ lợi ăng-ten (Tx Gain + Rx Gain)
        # Nếu có Vệ tinh tham gia (dùng ăng-ten chảo/mảng pha độ lợi cao): Tổng Gain = 38 dBi, ngược lại A2A = 16 dBi
        tong_antenna_gain_dbi = 38.0 if max(pos1["do_cao"], pos2["do_cao"]) > 80000.0 else 16.0
        cong_suat_thu_dbm = cong_suat_phat_dbm + tong_antenna_gain_dbi - tong_suy_hao_db

        # 4. Công suất nhiễu nhiệt nền (Thermal Noise: N = k * T * B) + Hệ số nhiễu bộ thu (Noise Figure = 4 dB)
        noise_watts = BOLTZMANN_CONST * SYSTEM_TEMP_K * bang_thong_hz
        noise_dbm = 10.0 * math.log10(noise_watts * 1000.0) + 4.0

        # 5. Tỷ số Tín hiệu trên Nhiễu (SNR)
        snr_db = cong_suat_thu_dbm - noise_dbm
        snr_linear = 10.0 ** (snr_db / 10.0)

        # 6. Dung lượng kênh truyền theo định lý Shannon-Hartley (Mbps)
        dung_luong_bps = bang_thong_hz * math.log2(1.0 + max(0.0, snr_linear))
        dung_luong_mbps = dung_luong_bps / 1e6

        # 7. Tỷ lệ lỗi gói tin (Packet Error Rate - PER) mô hình hóa theo hàm mũ của SNR
        if snr_db < -2.0:
            per = 1.0
        elif snr_db > 25.0:
            per = 0.0001
        else:
            per = math.exp(-0.32 * (snr_db + 2.0))

        return {
            "kha_dung": True,
            "khoang_cach_km": round(khoang_cach_m / 1000.0, 2),
            "goc_ngang_do": round(goc_ngang, 2),
            "do_tre_ms": round(do_tre_ms, 3),
            "fspl_db": round(tong_suy_hao_db, 2),
            "snr_db": round(snr_db, 2),
            "dung_luong_mbps": round(dung_luong_mbps, 2),
            "per": round(min(1.0, max(0.0, per)), 4)
        }