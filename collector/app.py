"""采集归档服务 API。

- GET  /health                       健康检查
- GET  /status                       已接收数、连续进度、缺口与运行指标
- GET  /errors                       错误查询（默认只看未恢复）
- GET  /vessels                      已接收船舶清单
- GET  /trajectories/{mmsi}         按 MMSI 读取连续轨迹（不返回缺口后的记录）
- GET  /port-events/{mmsi}          港区进出事件分页（含同次查询一致的处理进度）
- GET  /port-events/progress         港区事件处理进度
- POST /admin/reset                  清空归档（含事件）并重置进度（演示用）
- POST /admin/inject-gap             注入缺口并回退连续/事件进度（演示用）
- POST /admin/rewind-backfill        回退补采游标重新补齐（演示缺口补齐）
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from .db import Database
from .events import (
    EVENT_BATCH_SIZE,
    PORT_LAT_MAX,
    PORT_LAT_MIN,
    PORT_LON_MAX,
    PORT_LON_MIN,
    EventEngine,
)
from .ingest import STATION_URL, IngestEngine

DB_PATH = os.getenv("DB_PATH", "/data/collector.db")


# ----------------------------- 响应模型 -----------------------------

class PositionOut(BaseModel):
    seq: int
    mmsi: str
    utc_ts: float
    latitude: float
    longitude: float
    source: str
    received_at: float


class StatusOut(BaseModel):
    received_total: int
    contiguous_seq: int
    complete: bool
    vessel_count: int
    open_error_count: int
    next_missing_seqs: list[int]
    sse_connected: bool
    sse_reconnects: int
    sse_disconnects: int
    sse_received: int
    history_received: int
    history_requests: int
    history_failures: int
    last_event_at: float | None
    last_history_at: float | None
    last_backfill_seq: int | None
    max_seen_seq: int
    history_cursor: int
    station_url: str
    # 港区事件处理进度
    event_processed_seq: int
    event_total: int
    event_caught_up: bool


class ErrorOut(BaseModel):
    id: int
    kind: str
    message: str
    occurred_at: float
    resolved_at: float | None


class VesselOut(BaseModel):
    mmsi: str
    received: int
    min_seq: int
    max_seq: int


class TrajectoryOut(BaseModel):
    mmsi: str
    contiguous_seq: int
    returned: int
    received_total_for_mmsi: int
    truncated_by_gap: bool
    limit: int
    positions: list[PositionOut]


class GapIn(BaseModel):
    # 删除这些序号的船位，模拟岸站缺报；删除后连续水位回退到缺口之前
    seqs: list[int]


class PortEventOut(BaseModel):
    mmsi: str
    direction: str  # enter=进港（外转内），exit=出港（内转外）
    before_seq: int
    after_seq: int
    before_utc_ts: float
    after_utc_ts: float


class EventProgressOut(BaseModel):
    contiguous_seq: int          # 连续水位：事件处理的数据可见上界
    event_processed_seq: int     # 事件引擎已处理到的序号
    event_total: int             # 当前持久化的事件总数
    caught_up: bool              # 处理进度是否已追平连续水位
    port_rect: dict[str, float]
    batch_size: int
    processed_last_at: float | None


class PortEventsOut(BaseModel):
    mmsi: str
    direction: str | None
    limit: int
    after_seq: int              # 本次查询使用的分页游标
    next_after_seq: int | None  # 下一页游标（无更多数据时为 None）
    has_more: bool
    returned: int
    # 同次查询的事件与处理进度一致：取自同一连接、同一请求时刻
    progress: EventProgressOut
    events: list[PortEventOut]


# ----------------------------- 应用装配 -----------------------------

db = Database(DB_PATH)
engine = IngestEngine(db)
event_engine = EventEngine(db)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    await engine.start()
    await event_engine.start()
    yield
    await event_engine.stop()
    await engine.stop()
    await db.close()


app = FastAPI(
    title="AIS 船位采集归档与港区进出事件服务",
    description=(
        "连接岸站 SSE 实时流并从历史接口分页补采。"
        "序号全局连续，重叠去重；连续进度只在无缺口时推进，"
        "轨迹与港区事件查询均不越过连续进度；"
        "港区进出事件按同船相邻船位的港内/港外状态切换判定，"
        "后台补建历史并随新增连续船位增量处理，重启自动接续。"
    ),
    version="2.0.0",
    lifespan=lifespan,
)


def _row_to_position(row: Any) -> PositionOut:
    return PositionOut(
        seq=row["seq"],
        mmsi=row["mmsi"],
        utc_ts=row["utc_ts"],
        latitude=row["latitude"],
        longitude=row["longitude"],
        source=row["source"],
        received_at=row["received_at"],
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/status", response_model=StatusOut, summary="采集状态与连续进度")
async def status() -> StatusOut:
    watermark = await db.get_watermark()
    total = await db.count_positions()
    event_seq = await db.get_event_seq()
    return StatusOut(
        received_total=total,
        contiguous_seq=watermark,
        complete=total > 0 and watermark >= total,
        vessel_count=await db.count_mmsi(),
        open_error_count=await db.count_open_errors(),
        next_missing_seqs=await db.first_missing_after(
            watermark, engine.max_seen_seq
        ),
        sse_connected=engine.sse_connected,
        sse_reconnects=engine.sse_reconnects,
        sse_disconnects=engine.sse_disconnects,
        sse_received=engine.sse_received,
        history_received=engine.history_received,
        history_requests=engine.history_requests,
        history_failures=engine.history_failures,
        last_event_at=engine.last_event_at,
        last_history_at=engine.last_history_at,
        last_backfill_seq=engine.last_backfill_seq,
        max_seen_seq=engine.max_seen_seq,
        history_cursor=int(await db.get_meta("history_cursor", "0")),
        station_url=STATION_URL,
        event_processed_seq=event_seq,
        event_total=await db.count_port_events(),
        event_caught_up=event_seq >= watermark,
    )


@app.get("/errors", response_model=list[ErrorOut], summary="错误查询")
async def errors(
    limit: int = Query(50, ge=1, le=500),
    include_resolved: bool = Query(False, description="是否包含已恢复错误"),
) -> list[ErrorOut]:
    rows = await db.list_errors(limit, include_resolved)
    return [
        ErrorOut(
            id=r["id"],
            kind=r["kind"],
            message=r["message"],
            occurred_at=r["occurred_at"],
            resolved_at=r["resolved_at"],
        )
        for r in rows
    ]


@app.get("/vessels", response_model=list[VesselOut], summary="已接收船舶")
async def vessels() -> list[VesselOut]:
    rows = await db.list_vessels()
    return [
        VesselOut(
            mmsi=r["mmsi"],
            received=r["cnt"],
            min_seq=r["min_seq"],
            max_seq=r["max_seq"],
        )
        for r in rows
    ]


@app.get(
    "/trajectories/{mmsi}",
    response_model=TrajectoryOut,
    summary="按 MMSI 读取连续轨迹",
)
async def trajectory(
    mmsi: str,
    limit: int = Query(500, ge=1, le=5000, description="返回最近 N 条（按 seq 倒序截断）"),
) -> TrajectoryOut:
    # 连续水位是可见性上界：缺口之后的记录一律不返回
    watermark = await db.get_watermark()
    rows, total, visible = await db.trajectory(mmsi, watermark, limit)
    if total == 0:
        raise HTTPException(status_code=404, detail=f"未接收到 MMSI={mmsi} 的船位")
    return TrajectoryOut(
        mmsi=mmsi,
        contiguous_seq=watermark,
        returned=len(rows),
        received_total_for_mmsi=total,
        truncated_by_gap=total > visible,
        limit=limit,
        positions=[_row_to_position(r) for r in reversed(rows)],
    )


# ----------------------------- 港区进出事件 -----------------------------

def _port_rect() -> dict[str, float]:
    return {
        "lat_min": PORT_LAT_MIN,
        "lat_max": PORT_LAT_MAX,
        "lon_min": PORT_LON_MIN,
        "lon_max": PORT_LON_MAX,
    }


async def _event_progress() -> EventProgressOut:
    # 与事件查询在同一请求、同一 WAL 连接内读取，保证同次查询一致
    watermark = await db.get_watermark()
    event_seq = await db.get_event_seq()
    return EventProgressOut(
        contiguous_seq=watermark,
        event_processed_seq=event_seq,
        event_total=await db.count_port_events(),
        caught_up=event_seq >= watermark,
        port_rect=_port_rect(),
        batch_size=EVENT_BATCH_SIZE,
        processed_last_at=event_engine.processed_last_at,
    )


async def _event_progress_snapshot(
    watermark: int, event_seq: int
) -> EventProgressOut:
    """按请求开始时的进度快照构造进度对象，保证与本次返回的事件自洽。"""
    return EventProgressOut(
        contiguous_seq=watermark,
        event_processed_seq=event_seq,
        event_total=await db.count_port_events(),
        caught_up=event_seq >= watermark,
        port_rect=_port_rect(),
        batch_size=EVENT_BATCH_SIZE,
        processed_last_at=event_engine.processed_last_at,
    )


@app.get(
    "/port-events/progress",
    response_model=EventProgressOut,
    summary="港区事件处理进度",
)
async def port_event_progress() -> EventProgressOut:
    return await _event_progress()


@app.get(
    "/port-events/{mmsi}",
    response_model=PortEventsOut,
    summary="按 MMSI 分页查询港区进出事件",
)
async def port_events(
    mmsi: str,
    after_seq: int = Query(
        0, ge=0, description="分页游标：只返回 after_seq 大于该值的事件"
    ),
    limit: int = Query(100, ge=1, le=500, description="每页事件数"),
    direction: str | None = Query(
        None, description="方向过滤：enter=进港，exit=出港；缺省返回全部"
    ),
) -> PortEventsOut:
    if direction is not None and direction not in ("enter", "exit"):
        raise HTTPException(status_code=422, detail="direction 只能是 enter 或 exit")
    # 请求开始即固定连续水位与处理进度快照；本次事件与进度都基于该快照，
    # 即使请求处理期间发生回退/推进，同次查询仍严格一致。
    watermark = await db.get_watermark()
    event_seq = await db.get_event_seq()
    # 先确认该 MMSI 已在连续区间内出现（保持与轨迹接口一致的 404 语义）
    cur = await db.conn.execute(
        "SELECT 1 FROM positions WHERE mmsi = ? AND seq <= ? LIMIT 1",
        (mmsi, watermark),
    )
    if await cur.fetchone() is None:
        raise HTTPException(
            status_code=404,
            detail=f"连续进度内未接收到 MMSI={mmsi} 的船位",
        )

    # 事件可见上界锁定为请求开始时的处理进度快照
    rows, has_more = await db.list_port_events(
        mmsi, after_seq, limit, direction, visible_bound=event_seq
    )
    events = [
        PortEventOut(
            mmsi=r["mmsi"],
            direction=r["direction"],
            before_seq=r["before_seq"],
            after_seq=r["after_seq"],
            before_utc_ts=r["before_utc_ts"],
            after_utc_ts=r["after_utc_ts"],
        )
        for r in rows
    ]
    next_after_seq = rows[-1]["after_seq"] if has_more and rows else None
    progress = await _event_progress_snapshot(watermark, event_seq)
    return PortEventsOut(
        mmsi=mmsi,
        direction=direction,
        limit=limit,
        after_seq=after_seq,
        next_after_seq=next_after_seq,
        has_more=has_more,
        returned=len(events),
        progress=progress,
        events=events,
    )


@app.post("/admin/reset", summary="清空归档并重置（演示用）")
async def admin_reset() -> dict[str, Any]:
    # 与事件循环串行：清空期间事件引擎不会写入，清空后 epoch 作废旧计算
    async with event_engine.lock:
        await db.reset_all()
        await event_engine.reset_locked()
    # 重置内存计数，引擎循环会以 cursor=0 重新补采
    engine.sse_reconnects = 0
    engine.sse_disconnects = 0
    engine.sse_received = 0
    engine.history_received = 0
    engine.history_requests = 0
    engine.history_failures = 0
    engine.last_event_at = None
    engine.last_history_at = None
    engine.last_backfill_seq = None
    engine.max_seen_seq = 0
    engine._backfill_open_error = None
    engine._sse_open_error = None
    event_engine.processed_last_at = None
    event_engine.last_batch_events = 0
    event_engine.last_batch_size = 0
    return {"status": "reset done"}


@app.post(
    "/admin/inject-gap",
    summary="故障注入：删除指定序号并回退连续水位与事件进度（演示缺口语义）",
)
async def admin_inject_gap(body: GapIn) -> dict[str, Any]:
    seqs = sorted({int(s) for s in body.seqs if int(s) >= 1})
    if not seqs:
        raise HTTPException(status_code=422, detail="seqs 不能为空")
    # 与事件循环串行：先停住事件处理，再删船位、回退水位，
    # 最后原子清理越过新水位的事件并推进 epoch 作废旧计算
    async with event_engine.lock:
        deleted = await db.delete_positions(seqs)
        new_watermark = await db.recompute_watermark()
        rollback = await event_engine.rollback_to_watermark_locked(new_watermark)
    return {
        "deleted": deleted,
        "requested": seqs,
        "contiguous_seq": new_watermark,
        "event_processed_seq": await db.get_event_seq(),
        "event_rollback": rollback,
    }


@app.post(
    "/admin/rewind-backfill",
    summary="把历史补采游标回退到连续水位，重新补采缺口区间（演示补齐）",
)
async def admin_rewind_backfill() -> dict[str, Any]:
    watermark = await db.get_watermark()
    await db.set_meta("history_cursor", str(watermark))
    engine.last_backfill_seq = None
    return {"history_cursor": watermark, "contiguous_seq": watermark}
