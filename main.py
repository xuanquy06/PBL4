import os
from contextlib import asynccontextmanager
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


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Khởi chạy và dừng mô phỏng cùng vòng đời ứng dụng."""
    await sim_loop.bat_dau()
    try:
        yield
    finally:
        await sim_loop.dung()


os.makedirs("static", exist_ok=True)
os.makedirs("templates", exist_ok=True)
os.makedirs("data", exist_ok=True)

app = FastAPI(
    title="SAGSINS - Hệ đa tác nhân giám sát cháy rừng",
    description="Mô phỏng MAS điều phối UAV, Vệ tinh LEO và Trạm mặt đất trong mạng SAGSINS.",
    version="2.1.0",
    lifespan=lifespan
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

sim_loop = SimulationLoop(
    tick_interval_sec=1.0,
    uav_ws_server_url=os.getenv("SAGSINS_UAV_WS_URL")
)

app.include_router(tao_bo_dinh_tuyen_api(sim_loop))


@app.get("/", response_class=HTMLResponse)
async def trang_chu(request: Request):
    """Render giao diện giám sát và điều phối mạng SAGSINS."""
    content = templates.get_template("index.html").render(request=request)
    return HTMLResponse(content=content)
