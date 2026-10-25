import os
import inspect
from io import TextIOWrapper
import sys
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

if sys.platform == "win32":
    for stream in (sys.stdout, sys.stderr):
        if isinstance(stream, TextIOWrapper):
            stream.reconfigure(encoding="utf-8", errors="replace")

from environment.simulation_loop import SimulationLoop
from api.routes import tao_bo_dinh_tuyen_api

# Tự động tạo các thư mục hệ thống nếu chưa tồn tại
os.makedirs("static", exist_ok=True)
os.makedirs("templates", exist_ok=True)
os.makedirs("data", exist_ok=True)

app = FastAPI(
    title="SAGSINS - Hệ đa tác nhân giám sát cháy rừng",
    description="Mô phỏng MAS điều phối UAV và mạng SAGSINS phục vụ phát hiện, giám sát cháy rừng.",
    version="2.0.0"
)

# Gắn thư mục tĩnh chứa tệp JavaScript/CSS và Thư mục chứa giao diện HTML
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Khởi tạo Động cơ Đồng hồ Mô phỏng (Simulation Loop với bước nhảy 1 giây)
sim_loop = SimulationLoop(
    tick_interval_sec=1.0,
    uav_ws_server_url=(
        os.getenv("SAGSINS_UAV_WS_URL")
        or f"ws://127.0.0.1:{os.getenv('PORT', '8016')}/ws/uav"
    )
)

# Gắn toàn bộ REST API và WebSocket Router vào ứng dụng chính
app.include_router(tao_bo_dinh_tuyen_api(sim_loop))

@app.on_event("startup")
async def khi_khoi_dong_server():
    """Tự động nạp kịch bản CSV/CelesTrak và khởi chạy vòng lặp vật lý ngầm"""
    await sim_loop.bat_dau()

@app.on_event("shutdown")
async def khi_tat_server():
    """Dừng an toàn các vòng lặp bất đồng bộ khi tắt server"""
    await sim_loop.dung()

@app.get("/", response_class=HTMLResponse)
async def trang_chu(request: Request):
    """Render giao diện giám sát cháy rừng."""
    if "request" in inspect.signature(templates.TemplateResponse).parameters:
        return templates.TemplateResponse(request=request, name="index.html", context={})
    return templates.TemplateResponse("index.html", {"request": request})
