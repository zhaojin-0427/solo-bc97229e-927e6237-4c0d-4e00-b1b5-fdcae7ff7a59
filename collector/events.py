"""港区进出事件引擎。

语义（对应需求）：
- 港区为部署配置的固定经纬度矩形，边界算内部，不跨 180° 经线。
- 按 seq 比较同一 MMSI 的相邻船位：外→内记进港（enter），内→外记出港
  （exit）；每艘船处理到的第一个船位只建立衔接状态、不记事件。
- 后台先补建历史，随后只处理新增的连续船位；只使用连续进度
  contiguous_seq 以内的数据，并对外返回处理进度 event_processed_seq。
- 事件、同船衔接状态、处理进度共同持久化（port_events /
  vessel_port_state / meta），失败或重启自动接续，不漏记、不重复。
- 历史扫描分批小事务提交，不占用长写事务，期间采集与查询持续可用。
- 缺口演练回退连续进度后：越过新进度的事件立即删除不可见，旧的在途
  计算受 epoch 护栏约束不能写回；补齐后从回退点自动重建正确事件。
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from .db import Database

# 港区矩形（部署配置，固定不变）：边界算内部
PORT_LAT_MIN = float(os.getenv("PORT_LAT_MIN", "31.208"))
PORT_LAT_MAX = float(os.getenv("PORT_LAT_MAX", "31.252"))
PORT_LON_MIN = float(os.getenv("PORT_LON_MIN", "121.448"))
PORT_LON_MAX = float(os.getenv("PORT_LON_MAX", "121.492"))

# 单批处理的船位数：小批量提交，历史扫描不形成长写事务
EVENT_BATCH_SIZE = int(os.getenv("EVENT_BATCH_SIZE", "200"))
# 没有新连续船位时的轮询间隔
EVENT_IDLE_INTERVAL = float(os.getenv("EVENT_IDLE_INTERVAL", "0.3"))


def validate_port_rect() -> None:
    if not (-90 <= PORT_LAT_MIN <= PORT_LAT_MAX <= 90):
        raise ValueError(
            f"港区纬度配置非法: PORT_LAT_MIN={PORT_LAT_MIN}, "
            f"PORT_LAT_MAX={PORT_LAT_MAX}"
        )
    if not (-180 <= PORT_LON_MIN <= PORT_LON_MAX <= 180):
        raise ValueError(
            f"港区经度配置非法（矩形不跨 180°，需满足 "
            f"PORT_LON_MIN <= PORT_LON_MAX）: "
            f"PORT_LON_MIN={PORT_LON_MIN}, PORT_LON_MAX={PORT_LON_MAX}"
        )


def inside_port(latitude: float, longitude: float) -> bool:
    """船位是否位于港区矩形内（闭区间，边界算内部）。"""
    return (
        PORT_LAT_MIN <= latitude <= PORT_LAT_MAX
        and PORT_LON_MIN <= longitude <= PORT_LON_MAX
    )


class EventEngine:
    """后台事件处理循环，与采集循环并发独立运行。"""

    def __init__(self, db: Database) -> None:
        validate_port_rect()
        self.db = db
        # 与管理接口共享的串行锁：回退/清空期间暂停事件处理，
        # 保证回退与事件提交不会交错。
        self.lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        # 仅用于状态展示；权威值以数据库 meta 表为准
        self.processed_last_at: float | None = None
        self.last_batch_events = 0
        self.last_batch_size = 0

    # ----------------------------- 生命周期 -----------------------------

    async def start(self) -> None:
        # 不变量：事件处理进度永远不超过连续水位。
        # 启动时若发现历史脏状态（进度越过水位），按当前水位回退事件视图，
        # 之后水位补齐时该区间会被重新处理，避免漏记。
        async with self.lock:
            watermark = await self.db.get_watermark()
            if await self.db.get_event_seq() > watermark:
                await self.rollback_to_watermark_locked(watermark)
        self._stop.clear()
        self._task = asyncio.create_task(self._run_loop(), name="event-loop")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    # ----------------------------- 主循环 -----------------------------

    async def _run_loop(self) -> None:
        while not self._stop.is_set():
            try:
                # 连续水位是事件处理的唯一可见性上界；缺口之后的数据不参与
                watermark = await self.db.get_watermark()
                processed = await self.db.get_event_seq()
                # 防御：任何情况下处理进度都不得越过连续水位
                if processed > watermark:
                    async with self.lock:
                        await self.rollback_to_watermark_locked(watermark)
                    continue
                if processed >= watermark:
                    try:
                        await asyncio.wait_for(
                            self._stop.wait(), timeout=EVENT_IDLE_INTERVAL
                        )
                    except asyncio.TimeoutError:
                        pass
                    continue
                await self.process_batch()
            except asyncio.CancelledError:
                raise
            except Exception:
                # 单批失败不杀循环：进度未提交，下轮从已持久化进度重试
                # （事件 after_seq 唯一 + 状态 upsert 保证幂等不漏不重）
                await asyncio.sleep(EVENT_IDLE_INTERVAL)

    async def process_batch(self) -> int:
        """处理一批连续区间内的新船位，返回本批处理条数。"""
        async with self.lock:
            return await self._process_batch_locked()

    async def _process_batch_locked(self) -> int:
        # 进入临界区后取 epoch 与进度快照；提交时若 epoch 已变化
        # （发生过回退/清空），本批结果一律作废，防止旧计算写回。
        epoch_at_start = await self.db.get_event_epoch()
        watermark = await self.db.get_watermark()
        processed = await self.db.get_event_seq()
        target = min(processed + EVENT_BATCH_SIZE, watermark)
        if target <= processed:
            return 0

        rows = await self.db.positions_in_range(processed, target)
        # 连续水位保证 (processed, target] 无缺口；若行数不符说明水位刚被
        # 管理接口回退，直接放弃本批，下轮按新进度处理。
        if len(rows) != target - processed:
            return 0

        events: list[dict[str, Any]] = []
        state_upserts: dict[str, tuple[int, int]] = {}
        # 同批内同一 MMSI 可能出现多次，缓存衔接状态避免逐条查库
        state_cache: dict[str, tuple[int, int] | None] = {}

        for row in rows:
            mmsi = row["mmsi"]
            seq = row["seq"]
            inside = 1 if inside_port(row["latitude"], row["longitude"]) else 0

            if mmsi not in state_cache:
                prev = await self.db.get_vessel_state(mmsi)
                state_cache[mmsi] = (
                    (prev["last_seq"], prev["inside"]) if prev is not None else None
                )
            prev_state = state_cache[mmsi]

            if prev_state is not None:
                prev_seq, prev_inside = prev_state
                # 外转内记进港、内转外记出港；状态相同不产生事件
                if prev_inside != inside:
                    # 取前点 UTC 时刻：同船相邻点，优先用本批已见行
                    before_ts = await self._utc_of(mmsi, prev_seq, rows)
                    events.append(
                        {
                            "mmsi": mmsi,
                            "direction": "enter" if inside else "exit",
                            "before_seq": prev_seq,
                            "after_seq": seq,
                            "before_utc_ts": before_ts,
                            "after_utc_ts": row["utc_ts"],
                        }
                    )

            state_cache[mmsi] = (seq, inside)
            state_upserts[mmsi] = (seq, inside)

        # 提交前复核 epoch：回退/清空发生后旧计算不得写回
        if await self.db.get_event_epoch() != epoch_at_start:
            return 0

        await self.db.apply_event_batch(target, events, state_upserts)
        self.processed_last_at = time.time()
        self.last_batch_events = len(events)
        self.last_batch_size = len(rows)
        return len(rows)

    async def _utc_of(
        self, mmsi: str, seq: int, batch_rows: list[Any]
    ) -> float:
        for row in batch_rows:
            if row["seq"] == seq and row["mmsi"] == mmsi:
                return row["utc_ts"]
        cur = await self.db.conn.execute(
            "SELECT utc_ts FROM positions WHERE seq = ? AND mmsi = ?",
            (seq, mmsi),
        )
        row = await cur.fetchone()
        return row["utc_ts"] if row else 0.0

    # ----------------------------- 回退/清空护栏 -----------------------------

    async def rollback_to_watermark(self, watermark: int) -> dict[str, int]:
        """连续水位回退后调用：原子清理越界事件并作废在途计算。"""
        async with self.lock:
            return await self.rollback_to_watermark_locked(watermark)

    async def rollback_to_watermark_locked(self, watermark: int) -> dict[str, int]:
        """同临界区内调用（调用方已持有 self.lock）。"""
        new_epoch = await self.db.get_event_epoch() + 1
        deleted_events, rebuilt_states = await self.db.rollback_events_to(
            watermark, new_epoch, inside_port
        )
        return {
            "epoch": new_epoch,
            "deleted_events": deleted_events,
            "rebuilt_vessel_states": rebuilt_states,
        }

    async def reset(self) -> None:
        """归档清空后调用：推进 epoch，作废一切在途事件计算。"""
        async with self.lock:
            await self.reset_locked()

    async def reset_locked(self) -> None:
        """同临界区内调用（调用方已持有 self.lock）。"""
        new_epoch = await self.db.get_event_epoch() + 1
        await self.db.set_meta("event_epoch", str(new_epoch))
        self.processed_last_at = None
        self.last_batch_events = 0
        self.last_batch_size = 0
