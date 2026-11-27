"""
Lõi Hệ điều hành (OS Kernel Manager) cho hệ thống SAGSINS PBL4:
1. Mutex (threading.RLock `net_state_lock`) đồng bộ vùng nhớ dùng chung giữa luồng Socket TCP/UDP và luồng chính.
2. Lập lịch ưu tiên 3 cấp QoS (P0 / P1 / P2) có giới hạn bộ đệm chống tràn RAM (`BoundedPriorityQueue`).
3. Giám sát tài nguyên tiến trình thực tế của Hệ điều hành (PID, Threads, CPU %, RAM MB).
"""
from __future__ import annotations

import asyncio
import ctypes
from ctypes import wintypes
import itertools
import os
import sys
import threading
import time
from typing import Any, Optional


CRITICAL_MSG_TYPES = frozenset({
    "FIRE_ALERT",
    "FIRE_DETECTION_REPORT",
    "GIAO_NHIEM_VU_DU_LIEU",
    "YEU_CAU_HO_TRO_CHAY_P2P",
    "FIRE_CONTAINMENT_TELEMETRY",
    "XAC_NHAN_ACK",
})

NORMAL_MSG_TYPES = frozenset({
    "TELEMETRY_REPORT",
    "YEU_CAU_BAN_GIAO_VUNG",
    "THONG_BAO_KHOI_PHUC_VUNG",
    "EMERGENCY_RETURN",
    "PHAT_HIEN_SU_CO",
})


def phan_loai_do_uu_tien(loai_tin: Optional[str]) -> int:
    """
    Ánh xạ loại bản tin sang độ ưu tiên lập lịch HĐH:
    - 0 (P0 - CRITICAL): Cảnh báo cháy, dữ liệu khoanh vùng, bàn giao cháy P2P, ACK.
    - 1 (P1 - NORMAL): Telemetry định kỳ, bàn giao vùng pin yếu, RTB.
    - 2 (P2 - LOW): Gói HELLO_BEACON quảng bá láng giềng.
    """
    kind = str(loai_tin or "UNKNOWN").upper()
    if kind in CRITICAL_MSG_TYPES:
        return 0
    if kind in NORMAL_MSG_TYPES:
        return 1
    return 2


class _PROCESS_MEMORY_COUNTERS(ctypes.Structure):
    _fields_ = [
        ("cb", wintypes.DWORD),
        ("PageFaultCount", wintypes.DWORD),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
    ]


class BoundedPriorityQueue:
    """
    Hàng đợi lập lịch ưu tiên QoS giới hạn bộ đệm (Bounded Priority Queue - Producer/Consumer):
    - Rút gói tin theo thứ tự ưu tiên P0 -> P1 -> P2, cùng mức ưu tiên theo FIFO (`seq`).
    - Tự động hủy gói mới và đếm Buffer Overflow nếu đầy để bảo vệ bộ nhớ.
    """
    _global_seq = itertools.count()

    def __init__(self, maxsize: int = 100, os_kernel: Optional["OSKernelManager"] = None):
        self.maxsize = max(0, int(maxsize))
        self._os_kernel = os_kernel
        self._queue: Optional[asyncio.PriorityQueue] = None

    def _get_queue(self) -> asyncio.PriorityQueue:
        if self._queue is None:
            self._queue = asyncio.PriorityQueue(maxsize=self.maxsize)
        return self._queue

    def qsize(self) -> int:
        if self._queue is None:
            return 0
        return self._queue.qsize()

    def empty(self) -> bool:
        if self._queue is None:
            return True
        return self._queue.empty()

    def full(self) -> bool:
        if self._queue is None or self.maxsize <= 0:
            return False
        return self._queue.full()

    def _build_entry(self, item: Any) -> tuple[int, int, float, Any]:
        loai_tin = item.get("loai_tin", "UNKNOWN") if isinstance(item, dict) else "UNKNOWN"
        prio = phan_loai_do_uu_tien(loai_tin)
        seq = next(self._global_seq)
        return (prio, seq, time.monotonic(), item)

    async def put(self, item: Any):
        self.put_nowait(item)

    def put_nowait(self, item: Any):
        q = self._get_queue()
        if self.maxsize > 0 and q.full():
            if self._os_kernel is not None:
                self._os_kernel.record_buffer_drop()
            return
        try:
            q.put_nowait(self._build_entry(item))
        except asyncio.QueueFull:
            if self._os_kernel is not None:
                self._os_kernel.record_buffer_drop()

    def _unwrap_entry(self, entry: tuple[int, int, float, Any]) -> Any:
        prio, _, enqueue_ts, item = entry
        wait_ms = max(0.0, (time.monotonic() - enqueue_ts) * 1000.0)
        if self._os_kernel is not None:
            self._os_kernel.record_scheduler_dispatch(prio, wait_ms)
        return item

    async def get(self) -> Any:
        entry = await self._get_queue().get()
        return self._unwrap_entry(entry)

    def get_nowait(self) -> Any:
        entry = self._get_queue().get_nowait()
        return self._unwrap_entry(entry)

    def task_done(self):
        self._get_queue().task_done()


class OSKernelManager:
    """
    Trình quản lý tài nguyên Hệ điều hành gắn với tầng Mạng SAGSINS:
    - Mutex RLock (`net_state_lock`) bảo vệ danh sách tác nhân, Client ngoài và bộ đệm Packet Sniffer
      khi các luồng Socket TCP/UDP truy cập đồng thời với vòng lặp chính.
    - Thống kê bộ lập lịch QoS 3 cấp (P0/P1/P2) và tràn bộ đệm (`buffer_drops`).
    - Giám sát tài nguyên tiến trình thực tế (PID, Threads, CPU %, RAM MB).
    """

    def __init__(self):
        self.net_state_lock = threading.RLock()
        self._stats_lock = threading.RLock()

        self.mutex_acquisitions = 0
        self.buffer_drops = 0
        self.dispatched_by_prio = {0: 0, 1: 0, 2: 0}
        self.wait_ms_sum_by_prio = {0: 0.0, 1: 0.0, 2: 0.0}

        self._last_cpu_time = time.process_time()
        self._last_wall_time = time.monotonic()
        self._last_cpu_pct = 0.0

    def start(self):
        self._last_cpu_time = time.process_time()
        self._last_wall_time = time.monotonic()

    def stop(self):
        pass

    def record_mutex_acquire(self):
        with self._stats_lock:
            self.mutex_acquisitions += 1

    def record_buffer_drop(self):
        with self._stats_lock:
            self.buffer_drops += 1

    def record_scheduler_dispatch(self, priority: int, wait_ms: float):
        prio = priority if priority in (0, 1, 2) else 2
        with self._stats_lock:
            self.dispatched_by_prio[prio] += 1
            self.wait_ms_sum_by_prio[prio] += max(0.0, float(wait_ms))

    @staticmethod
    def get_memory_rss_mb() -> float:
        """Đọc RAM thực tế (Working Set / RSS) của tiến trình từ OS API."""
        if sys.platform == "win32":
            try:
                kernel32 = ctypes.windll.kernel32
                psapi = ctypes.windll.psapi
                handle = kernel32.GetCurrentProcess()
                pmc = _PROCESS_MEMORY_COUNTERS()
                pmc.cb = ctypes.sizeof(_PROCESS_MEMORY_COUNTERS)
                if psapi.GetProcessMemoryInfo(handle, ctypes.byref(pmc), pmc.cb):
                    return round(pmc.WorkingSetSize / (1024.0 * 1024.0), 2)
            except Exception:
                pass
        else:
            try:
                import resource
                usage_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                if sys.platform == "darwin":
                    return round(usage_kb / (1024.0 * 1024.0), 2)
                return round(usage_kb / 1024.0, 2)
            except Exception:
                pass
        return 0.0

    def _sample_cpu_percent(self) -> tuple[float, float]:
        now_cpu = time.process_time()
        now_wall = time.monotonic()
        d_wall = now_wall - self._last_wall_time
        if d_wall >= 0.2:
            d_cpu = max(0.0, now_cpu - self._last_cpu_time)
            cpu_count = max(1, os.cpu_count() or 1)
            raw_pct = (d_cpu / d_wall) * 100.0 / cpu_count
            self._last_cpu_pct = round(min(100.0, max(0.0, raw_pct)), 2)
            self._last_cpu_time = now_cpu
            self._last_wall_time = now_wall
        return self._last_cpu_pct, round(now_cpu, 2)

    def snapshot(self) -> dict[str, Any]:
        cpu_pct, cpu_sec = self._sample_cpu_percent()
        threads_list = []
        for t in threading.enumerate():
            threads_list.append({
                "name": t.name,
                "tid": getattr(t, "native_id", None) or t.ident,
                "daemon": t.daemon,
                "alive": t.is_alive(),
            })

        with self._stats_lock:
            p0_c = self.dispatched_by_prio[0]
            p1_c = self.dispatched_by_prio[1]
            p2_c = self.dispatched_by_prio[2]
            scheduler_snap = {
                "p0_count": p0_c,
                "p1_count": p1_c,
                "p2_count": p2_c,
                "p0_avg_wait_ms": round(self.wait_ms_sum_by_prio[0] / p0_c, 2) if p0_c else 0.0,
                "p1_avg_wait_ms": round(self.wait_ms_sum_by_prio[1] / p1_c, 2) if p1_c else 0.0,
                "p2_avg_wait_ms": round(self.wait_ms_sum_by_prio[2] / p2_c, 2) if p2_c else 0.0,
                "buffer_drops": self.buffer_drops,
            }
            mutex_snap = {
                "acquisitions": self.mutex_acquisitions,
            }

        return {
            "pid": os.getpid(),
            "memory_rss_mb": self.get_memory_rss_mb(),
            "cpu_percent": cpu_pct,
            "cpu_time_sec": cpu_sec,
            "active_threads_count": len(threads_list),
            "threads": threads_list[:16],
            "mutex": mutex_snap,
            "scheduler": scheduler_snap,
        }
