"""本地模拟岸站：持续生成 AIS 船位，提供 SSE 实时流与 HTTP 历史分页接口。

序号 seq 从 1 开始全局连续递增，源记录只增不改。
- GET /stream            SSE 实时流（仅推送连接之后产生的新船位，不补发旧记录）
- GET /history?cursor=&limit=  按 seq 升序分页，可配置随机失败用于演练容错
- GET /station/status    岸站自身运行状态
- POST /station/stream-stop / stream-resume   模拟实时流中断 / 恢复
- POST /station/history-fail-rate             动态调整历史翻页失败率
- POST /station/reset                         清空并重新生成（演示用）
"""

from __future__ import annotations

import asyncio
import math
import os
import random
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse, StreamingResponse
from pydantic import BaseModel

# ----------------------------- 配置与数据模型 -----------------------------

HISTORY_PAGE_LIMIT = int(os.getenv("HISTORY_PAGE_LIMIT", "100"))
GEN_INTERVAL = float(os.getenv("GEN_INTERVAL", "0.25"))  # 每 0.25s 生成一条船位
HISTORY_FAIL_RATE = float(os.getenv("HISTORY_FAIL_RATE", "0.0"))
INITIAL_COUNT = int(os.getenv("INITIAL_COUNT", "60"))
# 8 条模拟船，围绕一个港区做圆周运动，轨迹具备靠离泊语义
VESSEL_COUNT = int(os.getenv("VESSEL_COUNT", "8"))
PORT_LAT = 31.2300
PORT_LON = 121.4700


class Position(BaseModel):
    seq: int
    mmsi: str
    utc_ts: float          # UTC Unix 时间戳（秒，毫秒精度）
    latitude: float
    longitude: float


class HistoryPage(BaseModel):
    next_cursor: int | None
    limit: int
    count: int
    has_more: bool
    records: list[Position]


class StationStatus(BaseModel):
    total_positions: int
    stream_active: bool
    stream_generation_paused: bool
    history_fail_rate: float
    generation_interval: float
    last_seq: int | None


class FailRateIn(BaseModel):
    rate: float


# ----------------------------- 岸站核心状态 -----------------------------

class Station:
    def __init__(self) -> None:
        self._records: list[Position] = []
        self._seq = 0
        self._subscribers: set[asyncio.Queue[Position | None]] = set()
        self._gen_task: asyncio.Task | None = None
        # stream_active=False 时 SSE 连接会被主动断开（模拟断流）
        self.stream_active = True
        # generation_paused=True 时暂停生成新船位（模拟岸站设备故障，断流的一种）
        self.generation_paused = False
        self.history_fail_rate = HISTORY_FAIL_RATE
        self._lock = asyncio.Lock()

    def _make_position(self, seq: int) -> Position:
        vessel = (seq - 1) % VESSEL_COUNT
        # 每条船有不同的轨道半径、相位与速度，形成不断靠近/远离港区的轨迹
        radius = 0.02 + vessel * 0.004
        phase = vessel * (2 * math.pi / VESSEL_COUNT)
        angle = seq * 0.05 + phase
        lat = PORT_LAT + radius * math.sin(angle)
        lon = PORT_LON + radius * math.cos(angle)
        return Position(
            seq=seq,
            mmsi=f"4130000{vessel + 1:02d}",
            utc_ts=round(time.time(), 3),
            latitude=round(lat, 6),
            longitude=round(lon, 6),
        )

    async def preload(self, n: int) -> None:
        async with self._lock:
            for _ in range(n):
                self._seq += 1
                self._records.append(self._make_position(self._seq))
                await asyncio.sleep(0)

    async def generate_one(self) -> Position:
        async with self._lock:
            self._seq += 1
            pos = self._make_position(self._seq)
            self._records.append(pos)
        # 广播给所有在线 SSE 订阅者（背压时丢弃慢订阅者之外不阻塞生成）
        dead: list[asyncio.Queue] = []
        for q in list(self._subscribers):
            try:
                q.put_nowait(pos)
            except asyncio.QueueFull:
                dead.append(q)
        for q in dead:
            self._subscribers.discard(q)
        return pos

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subscribers.discard(q)

    def history_page(self, cursor: int, limit: int) -> list[Position]:
        # cursor 语义：返回 seq > cursor 的记录，按 seq 升序
        start = cursor
        end = min(start + limit, len(self._records))
        return self._records[start:end]

    @property
    def last_seq(self) -> int | None:
        return self._records[-1].seq if self._records else None

    def status(self) -> StationStatus:
        return StationStatus(
            total_positions=len(self._records),
            stream_active=self.stream_active,
            stream_generation_paused=self.generation_paused,
            history_fail_rate=self.history_fail_rate,
            generation_interval=GEN_INTERVAL,
            last_seq=self.last_seq,
        )

    async def reset(self) -> None:
        async with self._lock:
            self._records.clear()
            self._seq = 0
            self.stream_active = True
            self.generation_paused = False
            self.history_fail_rate = HISTORY_FAIL_RATE
            for _ in range(INITIAL_COUNT):
                self._seq += 1
                self._records.append(self._make_position(self._seq))
                await asyncio.sleep(0)


station = Station()


async def _generator_loop() -> None:
    while True:
        try:
            if not station.generation_paused:
                await station.generate_one()
        except asyncio.CancelledError:
            raise
        except Exception:  # 生成异常不杀任务
            await asyncio.sleep(1)
        await asyncio.sleep(GEN_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await station.preload(INITIAL_COUNT)
    station._gen_task = asyncio.create_task(_generator_loop())
    yield
    if station._gen_task:
        station._gen_task.cancel()


app = FastAPI(
    title="模拟岸站 AIS 服务",
    description="持续生成船位；SSE 只推新记录，历史接口分页可注入失败。",
    version="1.0.0",
    lifespan=lifespan,
)


# ----------------------------- HTTP 接口 -----------------------------

@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/stream")
async def stream():
    """SSE 实时流：仅推送连接建立之后产生的船位，不补发历史。"""
    if not station.stream_active:
        raise HTTPException(status_code=503, detail="stream is down (模拟断流)")

    q = station.subscribe()

    async def event_gen():
        try:
            # 连接建立即发一个 retry 提示与注释心跳
            yield "retry: 2000\n\n"
            while True:
                if not station.stream_active:
                    # 模拟断流：服务端主动结束响应
                    yield "event: error\ndata: stream interrupted\n\n"
                    return
                try:
                    pos = await asyncio.wait_for(q.get(), timeout=10)
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
                    continue
                if pos is None:
                    return
                yield f"event: position\ndata: {pos.model_dump_json()}\n\n"
        finally:
            station.unsubscribe(q)

    # media_type 必须是 text/event-stream
    return StreamingResponse(event_gen(), media_type="text/event-stream")


@app.get("/history", response_model=HistoryPage)
async def history(
    cursor: int = Query(0, ge=0, description="返回 seq > cursor 的记录"),
    limit: int = Query(HISTORY_PAGE_LIMIT, ge=1, le=1000),
) -> HistoryPage:
    # 注入随机翻页失败（同一失败率下每页独立判定）
    if station.history_fail_rate > 0 and random.random() < station.history_fail_rate:
        raise HTTPException(status_code=500, detail="history page failed (模拟历史接口故障)")
    page = station.history_page(cursor, limit)
    has_more = cursor + limit < len(station._records)
    next_cursor = page[-1].seq if page and has_more else None
    return HistoryPage(
        next_cursor=next_cursor,
        limit=limit,
        count=len(page),
        has_more=has_more,
        records=page,
    )


@app.get("/station/status", response_model=StationStatus)
async def station_status() -> StationStatus:
    return station.status()


@app.post("/station/stream-stop")
async def stream_stop() -> dict[str, str]:
    station.stream_active = False
    station.generation_paused = True
    # 唤醒所有订阅者使其尽快结束
    for q in list(station._subscribers):
        q.put_nowait(None)
    return {"status": "stream stopped"}


@app.post("/station/stream-resume")
async def stream_resume() -> dict[str, str]:
    station.generation_paused = False
    station.stream_active = True
    return {"status": "stream resumed"}


@app.post("/station/history-fail-rate")
async def set_history_fail_rate(body: FailRateIn) -> dict[str, float]:
    if not 0.0 <= body.rate <= 1.0:
        raise HTTPException(status_code=422, detail="rate 必须在 [0,1] 之间")
    station.history_fail_rate = body.rate
    return {"history_fail_rate": station.history_fail_rate}


@app.post("/station/reset")
async def reset_station() -> StationStatus:
    await station.reset()
    return station.status()
