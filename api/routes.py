import os
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import FileResponse
from api.schemas import (
    KichHoatSuCoRequest,
    DieuKhienThoiGianRequest,
    CapNhatPinNodeRequest
)
from core.base_agent import AgentState

def tao_bo_dinh_tuyen_api(sim_loop) -> APIRouter:
    """
    Khởi tạo APIRouter gắn kết trực tiếp với thực thể SimulationLoop.
    Đảm bảo kiến trúc Dependency Injection sạch sẽ.
    """
    router = APIRouter()

    # =========================================================================
    # 1. CỔNG WEBSOCKET ĐỒNG BỘ DỮ LIỆU THỜI GIAN THỰC (TELEMETRY & RF LINKS)
    # =========================================================================
    @router.websocket("/ws/mophong")
    async def ket_noi_websocket(websocket: WebSocket):
        await websocket.accept()
        sim_loop.broker.danh_sach_client_ws.append(websocket)
        try:
            # Ngay khi trình duyệt kết nối, gửi lập tức trạng thái ban đầu
            await sim_loop.broker.dong_bo_trang_thai_toan_mang()
            while True:
                # Duy trì kết nối lắng nghe lệnh ping từ trình duyệt
                await websocket.receive_text()
        except WebSocketDisconnect:
            if websocket in sim_loop.broker.danh_sach_client_ws:
                sim_loop.broker.danh_sach_client_ws.remove(websocket)

    @router.websocket("/ws/uav/{uav_id}")
    async def ket_noi_uav(uav_id: str, websocket: WebSocket):
        server = sim_loop.ground_station_server
        if not await server.connect(uav_id, websocket):
            return
        try:
            await server.receive_telemetry(uav_id, websocket)
        finally:
            await server.disconnect(uav_id, websocket)

    @router.websocket("/ws/satellite/{uav_id}")
    async def ket_noi_ve_tinh(uav_id: str, websocket: WebSocket):
        server = sim_loop.ground_station_server
        if not await server.connect_satellite(uav_id, websocket):
            return
        try:
            await server.receive_satellite_telemetry(uav_id, websocket)
        finally:
            await server.disconnect_satellite(uav_id, websocket)

    # =========================================================================
    # 2. CÁC ENDPOINT RESTFUL API ĐIỀU KHIỂN KỊCH BẢN MÔ PHỎNG
    # =========================================================================
    @router.get("/api/init-state")
    async def lay_trang_thai_khoi_tao():
        """Trả trạng thái tác tử, tuyến tuần tra và chỉ số mạng."""
        return sim_loop.lay_du_lieu_khoi_tao_ban_do()

    @router.post("/api/su-co")
    async def kich_hoat_su_co(req: KichHoatSuCoRequest):
        """Mô phỏng phát hiện điểm cháy và điều động UAV qua CNP."""
        ket_qua = await sim_loop.kich_hoat_su_co_khan_cap(
            loai_su_co=req.loai_su_co,
            vi_do=req.vi_do,
            kinh_do=req.kinh_do
        )
        if not ket_qua["thanh_cong"]:
            raise HTTPException(status_code=400, detail=ket_qua["thong_bao"])
        return ket_qua

    @router.post("/api/dieu-khien-thoi-gian")
    async def dieu_khien_thoi_gian(req: DieuKhienThoiGianRequest):
        """Thay đổi tốc độ thời gian mô phỏng (Time Scale) hoặc Tạm dừng/Tiếp tục"""
        if req.time_scale is not None:
            sim_loop.time_scale = req.time_scale
        if req.tam_dung is not None:
            sim_loop.tam_dung = req.tam_dung

        trang_thai_str = "TẠM DỪNG" if sim_loop.tam_dung else f"ĐANG CHẠY ({sim_loop.time_scale}x)"
        await sim_loop.broker.gui_log_ra_web(
            "INFO", f"⏱️ Điều chỉnh đồng hồ mô phỏng: {trang_thai_str}"
        )
        return {
            "time_scale": sim_loop.time_scale,
            "paused": sim_loop.tam_dung
        }

    @router.post("/api/cap-nhat-pin")
    async def cap_nhat_pin_tac_nhan(req: CapNhatPinNodeRequest):
        """
        Giả lập tình huống UAV bị suy giảm pin đột ngột (< 20%) hoặc sập nguồn (0%)
        để kiểm chứng thuật toán tự động né tránh Node yếu pin khi đấu giá.
        """
        tac_nhan = sim_loop.broker.lay_tac_nhan(req.ma_dinh_danh)
        if not tac_nhan:
            raise HTTPException(status_code=404, detail=f"Không tìm thấy Node {req.ma_dinh_danh}")

        tac_nhan.pin_hien_tai = (req.phan_tram_pin_moi / 100.0) * tac_nhan.pin_toi_da
        if tac_nhan.pin_hien_tai <= 0:
            tac_nhan.chuyen_trang_thai(AgentState.OFFLINE)
        elif req.phan_tram_pin_moi < tac_nhan.nguong_pin_yeu_pct:
            tac_nhan.chuyen_trang_thai(AgentState.LOW_BATTERY)
        else:
            tac_nhan.chuyen_trang_thai(
                AgentState.PATROLLING if tac_nhan.loai_tac_nhan == "MÁY_BAY" else AgentState.IDLE
            )

        thong_bao = (
            f"🔋 Đã cập nhật năng lượng [{req.ma_dinh_danh}] ➔ {tac_nhan.phan_tram_pin}% "
            f"(Trạng thái FSM: {tac_nhan.trang_thai.value})"
        )
        await sim_loop.broker.gui_log_ra_web("WARN", thong_bao)
        await sim_loop.broker.dong_bo_trang_thai_toan_mang()
        return {"thanh_cong": True, "thong_bao": thong_bao}

    @router.get("/api/xuat-csv")
    async def tai_file_ket_qua_csv():
        """Cho phép tải trực tiếp file CSV chứa nhật ký thực nghiệm QoS về máy"""
        csv_path = sim_loop.scenario.metrics_log_path
        if not os.path.exists(csv_path):
            raise HTTPException(status_code=404, detail="Chưa có dữ liệu thực nghiệm CSV.")
        return FileResponse(
            path=csv_path,
            media_type="text/csv",
            filename="sagsins_qos_metrics_log.csv"
        )

    return router