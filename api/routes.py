import os
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import FileResponse
from api.schemas import (
    KichHoatSuCoRequest,
    DieuKhienThoiGianRequest,
    CapNhatPinNodeRequest
)


def tao_bo_dinh_tuyen_api(sim_loop) -> APIRouter:
    """
    Khởi tạo APIRouter gắn kết trực tiếp với thực thể SimulationLoop.
    """
    router = APIRouter()

    @router.websocket("/ws/mophong")
    async def ket_noi_websocket(websocket: WebSocket):
        await websocket.accept()
        sim_loop.broker.danh_sach_client_ws.append(websocket)
        try:
            await sim_loop.broker.dong_bo_trang_thai_toan_mang()
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            if websocket in sim_loop.broker.danh_sach_client_ws:
                sim_loop.broker.danh_sach_client_ws.remove(websocket)

    @router.get("/api/init-state")
    async def lay_trang_thai_khoi_tao():
        """Trả trạng thái tác tử, tuyến tuần tra, độ phủ 7 vùng và chỉ số mạng."""
        return sim_loop.lay_du_lieu_khoi_tao_ban_do()

    @router.post("/api/su-co")
    async def kich_hoat_su_co(req: KichHoatSuCoRequest):
        """Mô phỏng phát hiện điểm cháy và điều động UAV qua FIPA-CNP."""
        ket_qua = await sim_loop.kich_hoat_su_co_khan_cap(
            loai_su_co=req.loai_su_co,
            vi_do=req.vi_do,
            kinh_do=req.kinh_do
        )
        if not ket_qua["thanh_cong"]:
            raise HTTPException(status_code=400, detail=ket_qua["thong_bao"])
        return ket_qua

    @router.post("/api/xoa-chay")
    async def xoa_dam_chay():
        """Xóa toàn bộ đám cháy active_fires và giải phóng UAV khoanh vùng."""
        return await sim_loop.xoa_tat_ca_dam_chay()

    @router.post("/api/dieu-khien-thoi-gian")
    async def dieu_khien_thoi_gian(req: DieuKhienThoiGianRequest):
        """Thay đổi tốc độ thời gian mô phỏng hoặc tạm dừng/tiếp tục."""
        return await sim_loop.dieu_khien_thoi_gian(
            time_scale=req.time_scale,
            tam_dung=req.tam_dung
        )

    @router.post("/api/cap-nhat-pin")
    async def cap_nhat_pin_tac_nhan(req: CapNhatPinNodeRequest):
        """Giả lập pin UAV thông qua hành vi của chính tác tử."""
        try:
            return await sim_loop.cap_nhat_pin_uav(
                req.ma_dinh_danh,
                req.phan_tram_pin_moi
            )
        except ValueError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error

    @router.get("/api/routing-table/{node_id}")
    async def lay_bang_dinh_tuyen_node(node_id: str):
        """Trả bảng định tuyến tầng Mạng (L3 Routing Table) và IP của node."""
        agent = sim_loop.broker.lay_tac_nhan(node_id)
        if not agent:
            raise HTTPException(status_code=404, detail=f"Không tìm thấy node {node_id}")
        routes = agent.xuat_bang_dinh_tuyen() if hasattr(agent, "xuat_bang_dinh_tuyen") else []
        return {
            "id": agent.ma_dinh_danh,
            "ip": getattr(agent, "ip_address", "10.0.0.0"),
            "state": getattr(getattr(agent, "uav_state", agent.trang_thai), "value", str(agent.trang_thai)),
            "routes": routes,
        }

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
