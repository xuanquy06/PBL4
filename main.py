from contextlib import asynccontextmanager
from io import TextIOWrapper
from pathlib import Path
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

PROJECT_ROOT = Path(__file__).resolve().parent


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Khởi chạy và dừng mô phỏng cùng vòng đời ứng dụng."""
    await sim_loop.bat_dau()
    try:
        yield
    finally:
        await sim_loop.dung()


app = FastAPI(
    title="Hệ thống MAS_SAGSINS",
    description="Mô phỏng Hệ thống MAS_SAGSINS điều phối UAV tự chủ, Vệ tinh LEO và Trạm mặt đất.",
    version="2.1.0",
    lifespan=lifespan
)

app.mount("/static", StaticFiles(directory=PROJECT_ROOT / "static"), name="static")
templates = Jinja2Templates(directory=PROJECT_ROOT / "templates")
templates.env.auto_reload = True

sim_loop = SimulationLoop(tick_interval_sec=1.0)

app.include_router(tao_bo_dinh_tuyen_api(sim_loop))

NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


@app.get("/", response_class=HTMLResponse)
async def trang_chu(request: Request):
    """Render trang bản đồ Hệ thống MAS_SAGSINS."""
    content = templates.get_template("index.html").render(request=request)
    return HTMLResponse(content=content, headers=NO_CACHE_HEADERS)


@app.get("/dashboard", response_class=HTMLResponse)
async def trang_dashboard(request: Request):
    """Render trang Dashboard thống kê và điều phối Hệ thống MAS_SAGSINS."""
    content = templates.get_template("dashboard.html").render(request=request)
    return HTMLResponse(content=content, headers=NO_CACHE_HEADERS)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
