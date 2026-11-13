"""采集归档服务 API。

- GET  /health                       健康检查
- GET  /status                       已接收数、连续进度、缺口与运行指标
- GET  /errors                       错误查询（默认只看未恢复）
- GET  /vessels                      已接收船舶清单
- GET  /trajectories/{mmsi}         按 MMSI 读取连续轨迹（不返回缺口后的记录）
- POST /admin/reset                  清空归档并重置进度（演示用）
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from .db import Database
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


# ----------------------------- 应用装配 -----------------------------

db = Database(DB_PATH)
engine = IngestEngine(db)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    await engine.start()
    yield
    await engine.stop()
    await db.close()


app = FastAPI(
    title="AIS 船位采集归档服务",
    description=(
        "连接岸站 SSE 实时流并从历史接口分页补采。"
        "序号全局连续，重叠去重；连续进度只在无缺口时推进，"
        "轨迹查询不返回缺口之后的记录；重启自动接续补齐。"
    ),
    version="1.0.0",
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


@app.post("/admin/reset", summary="清空归档并重置（演示用）")
async def admin_reset() -> dict[str, str]:
    await db.reset_all()
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
    return {"status": "reset done"}


@app.post(
    "/admin/inject-gap",
    summary="故障注入：删除指定序号并回退连续水位（演示缺口语义）",
)
async def admin_inject_gap(body: GapIn) -> dict[str, Any]:
    seqs = sorted({int(s) for s in body.seqs if int(s) >= 1})
    if not seqs:
        raise HTTPException(status_code=422, detail="seqs 不能为空")
    deleted = await db.delete_positions(seqs)
    new_watermark = await db.recompute_watermark()
    return {
        "deleted": deleted,
        "requested": seqs,
        "contiguous_seq": new_watermark,
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
