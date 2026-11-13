"""采集引擎：并发运行 SSE 实时接收与历史分页补采两个循环。

容错原则（对应需求）：
1. 启动即同时开 SSE 与历史补采；两者重叠记录靠 seq 主键去重，只入库一次。
2. SSE 不补发旧记录，断线重连期间错过的序号由历史补采循环补齐。
3. 历史翻页失败只记错误并重试，绝不影响实时接收。
4. 每次写入后尝试推进连续水位；只有 1..w 全部入库，水位才等于 w。
5. 所有进度（船位、水位、历史翻页游标、错误）都在 SQLite 中，重启自动接续。
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from typing import Any

import httpx

from .db import (
    ERROR_KIND_HISTORY,
    ERROR_KIND_PARSE,
    ERROR_KIND_SSE,
    Database,
)

STATION_URL = os.getenv("STATION_URL", "http://station:8000")
HISTORY_PAGE_SIZE = int(os.getenv("HISTORY_PAGE_SIZE", "50"))
HISTORY_RETRY_BASE = float(os.getenv("HISTORY_RETRY_BASE", "1.0"))
HISTORY_RETRY_MAX = float(os.getenv("HISTORY_RETRY_MAX", "30"))
SSE_RECONNECT_BASE = float(os.getenv("SSE_RECONNECT_BASE", "0.5"))
SSE_RECONNECT_MAX = float(os.getenv("SSE_RECONNECT_MAX", "30"))
SSE_READ_TIMEOUT = float(os.getenv("SSE_READ_TIMEOUT", "60"))
STATION_CATCHUP_IDLE = float(os.getenv("STATION_CATCHUP_IDLE", "0.5"))

META_HISTORY_CURSOR = "history_cursor"


def _validate_position(raw: dict[str, Any]) -> dict[str, Any]:
    required = {"seq", "mmsi", "utc_ts", "latitude", "longitude"}
    missing = required - raw.keys()
    if missing:
        raise ValueError(f"船位缺少字段: {sorted(missing)}")
    seq = raw["seq"]
    if not isinstance(seq, int) or seq < 1:
        raise ValueError(f"非法 seq: {seq!r}")
    lat = float(raw["latitude"])
    lon = float(raw["longitude"])
    if not -90 <= lat <= 90 or not -180 <= lon <= 180:
        raise ValueError(f"经纬度越界: lat={lat}, lon={lon}")
    return {
        "seq": seq,
        "mmsi": str(raw["mmsi"]),
        "utc_ts": float(raw["utc_ts"]),
        "latitude": lat,
        "longitude": lon,
    }


class IngestEngine:
    def __init__(self, db: Database) -> None:
        self.db = db
        self._tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()
        # 仅用于状态展示的内存计数（权威计数以数据库为准）
        self.sse_connected = False
        self.sse_reconnects = 0
        self.sse_disconnects = 0
        self.sse_received = 0
        self.history_received = 0
        self.history_requests = 0
        self.history_failures = 0
        self.last_event_at: float | None = None
        self.last_history_at: float | None = None
        self.last_backfill_seq: int | None = None
        # 任一来源见过的最大序号，用于界定真实缺口的上界
        self.max_seen_seq = 0
        self._backfill_open_error: int | None = None
        self._sse_open_error: int | None = None

    # ----------------------------- 生命周期 -----------------------------

    async def start(self) -> None:
        # 重启恢复：以库内最大序号作为缺口上界的初值
        cur = await self.db.conn.execute("SELECT COALESCE(MAX(seq), 0) m FROM positions")
        row = await cur.fetchone()
        self.max_seen_seq = row["m"]
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(self._sse_loop(), name="sse-loop"),
            asyncio.create_task(self._history_loop(), name="history-loop"),
        ]

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    # ----------------------------- 公共写入路径 -----------------------------

    async def _ingest(self, raw: dict[str, Any], source: str) -> bool:
        try:
            pos = _validate_position(raw)
        except (ValueError, TypeError, KeyError) as exc:
            await self.db.add_error(ERROR_KIND_PARSE, f"{source} 记录解析失败: {exc}")
            return False
        inserted = await self.db.upsert_position(pos, source)
        # SSE 侧代际检测：已入库的最大序号远大于当前序号（回退超过一个补采页），
        # 且当前序号很小，判定岸站重置；回退历史游标重拉新一代际（去重仍安全）
        if (
            source == "sse"
            and not inserted
            and self.max_seen_seq - pos["seq"] > HISTORY_PAGE_SIZE
        ):
            cursor_now = int(await self.db.get_meta(META_HISTORY_CURSOR, "0"))
            if cursor_now > pos["seq"]:
                await self.db.set_meta(META_HISTORY_CURSOR, "0")
                eid = await self.db.add_error(
                    ERROR_KIND_SSE,
                    f"SSE 收到回退序号 seq={pos['seq']}（已见最大 "
                    f"{self.max_seen_seq}，历史游标 {cursor_now}），"
                    "判定岸站重置，已回退补采游标",
                )
                await self.db.resolve_error(eid)
        if pos["seq"] > self.max_seen_seq:
            self.max_seen_seq = pos["seq"]
        if inserted:
            if source == "sse":
                self.sse_received += 1
            else:
                self.history_received += 1
            self.last_event_at = time.time()
        # 无论新老记录都尝试推进水位（新记录可能恰好补上缺口）
        await self.db.advance_watermark()
        return inserted

    # ----------------------------- SSE 实时循环 -----------------------------

    async def _sse_loop(self) -> None:
        backoff = SSE_RECONNECT_BASE
        while not self._stop.is_set():
            self.sse_connected = False
            # 每次循环代表一次连接尝试（含首次连接），先计数再握手
            self.sse_reconnects += 1
            try:
                timeout = httpx.Timeout(connect=10, read=SSE_READ_TIMEOUT,
                                        write=10, pool=10)
                async with httpx.AsyncClient(timeout=timeout) as client:
                    async with client.stream(
                        "GET", f"{STATION_URL}/stream"
                    ) as resp:
                        if resp.status_code != 200:
                            body = await resp.aread()
                            raise httpx.HTTPStatusError(
                                f"SSE 握手失败 HTTP {resp.status_code}: "
                                f"{body[:200]!r}",
                                request=resp.request,
                                response=resp,
                            )
                        # 连接成功：退避与未恢复错误归零
                        backoff = SSE_RECONNECT_BASE
                        self.sse_connected = True
                        # 首次成功连接也算一次 reconnect（=1 表示已连上）
                        if self._sse_open_error is not None:
                            await self.db.resolve_error(self._sse_open_error)
                            self._sse_open_error = None
                        async for raw_line in resp.aiter_lines():
                            if self._stop.is_set():
                                break
                            if not raw_line or not raw_line.startswith("data:"):
                                # 注释心跳、event: 行、空行均忽略
                                continue
                            payload = raw_line[len("data:"):].strip()
                            if payload == "stream interrupted":
                                raise ConnectionError("岸站主动中断实时流")
                            try:
                                obj = json.loads(payload)
                            except json.JSONDecodeError as exc:
                                await self.db.add_error(
                                    ERROR_KIND_PARSE,
                                    f"SSE 数据非合法 JSON: {exc}",
                                )
                                continue
                            await self._ingest(obj, "sse")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # 断流/超时/连接拒绝都走这里：记录一次未恢复错误，退避重连
                self.sse_connected = False
                self.sse_disconnects += 1
                if self._sse_open_error is None:
                    self._sse_open_error = await self.db.add_error(
                        ERROR_KIND_SSE, f"实时流中断: {type(exc).__name__}: {exc}"
                    )
            if self._stop.is_set():
                break
            # 指数退避 + 抖动，避免重连风暴
            delay = min(backoff, SSE_RECONNECT_MAX) * (
                0.5 + random.random() * 0.5
            )
            backoff = min(backoff * 2, SSE_RECONNECT_MAX)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    # ----------------------------- 历史补采循环 -----------------------------

    async def _history_loop(self) -> None:
        backoff = HISTORY_RETRY_BASE
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(15)
        ) as client:
            while not self._stop.is_set():
                cursor = int(await self.db.get_meta(META_HISTORY_CURSOR, "0"))
                try:
                    self.history_requests += 1
                    resp = await client.get(
                        f"{STATION_URL}/history",
                        params={"cursor": cursor, "limit": HISTORY_PAGE_SIZE},
                    )
                    resp.raise_for_status()
                    page = resp.json()
                except (httpx.HTTPError, json.JSONDecodeError, KeyError) as exc:
                    # 翻页失败：持久化错误并退避重试，实时循环不受任何影响
                    self.history_failures += 1
                    if self._backfill_open_error is None:
                        self._backfill_open_error = await self.db.add_error(
                            ERROR_KIND_HISTORY,
                            f"历史翻页失败 cursor={cursor}: "
                            f"{type(exc).__name__}: {exc}",
                        )
                    delay = min(backoff, HISTORY_RETRY_MAX) * (
                        0.5 + random.random() * 0.5
                    )
                    backoff = min(backoff * 2, HISTORY_RETRY_MAX)
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=delay)
                    except asyncio.TimeoutError:
                        pass
                    continue

                # 成功拿到一页：恢复退避与错误状态
                backoff = HISTORY_RETRY_BASE
                if self._backfill_open_error is not None:
                    await self.db.resolve_error(self._backfill_open_error)
                    self._backfill_open_error = None

                records = page.get("records", [])

                # 岸站代际检测（空页路径）：游标很高却拿不到任何记录，
                # 探测第 1 页判断岸站是否已重置（新代际 seq 从 1 重新开始）
                if not records and cursor > 0:
                    try:
                        probe = await client.get(
                            f"{STATION_URL}/history",
                            params={"cursor": 0, "limit": 1},
                        )
                        probe.raise_for_status()
                        probe_page = probe.json()
                        probe_records = probe_page.get("records", [])
                        if probe_records and probe_records[0]["seq"] <= cursor:
                            await self.db.set_meta(META_HISTORY_CURSOR, "0")
                            # 这是可自愈的代际切换告警，直接标记已解决
                            eid = await self.db.add_error(
                                ERROR_KIND_HISTORY,
                                f"检测到岸站序号重置(探测 seq="
                                f"{probe_records[0]['seq']} <= cursor={cursor})，"
                                "已自动回退补采游标重新补采",
                            )
                            await self.db.resolve_error(eid)
                            continue
                    except (httpx.HTTPError, json.JSONDecodeError, KeyError):
                        # 探测失败不影响主流程，下轮常规重试
                        pass

                # 岸站代际检测（非空页路径）：页首 seq 回退到游标之前
                if records and cursor > 0 and records[0]["seq"] <= cursor:
                    await self.db.set_meta(META_HISTORY_CURSOR, "0")
                    eid = await self.db.add_error(
                        ERROR_KIND_HISTORY,
                        f"检测到岸站序号重置(页首 seq={records[0]['seq']} "
                        f"<= cursor={cursor})，已自动回退补采游标",
                    )
                    await self.db.resolve_error(eid)
                    continue

                for raw in records:
                    await self._ingest(raw, "history")
                self.last_history_at = time.time()

                if records:
                    self.last_backfill_seq = records[-1]["seq"]
                    # 以本页最后一条 seq 作为下次请求游标，持久化以便重启接续
                    await self.db.set_meta(
                        META_HISTORY_CURSOR, str(records[-1]["seq"])
                    )

                if not page.get("has_more"):
                    # 已补到岸站当前最新；短暂空闲后再轮询新产生的船位
                    try:
                        await asyncio.wait_for(
                            self._stop.wait(), timeout=STATION_CATCHUP_IDLE
                        )
                    except asyncio.TimeoutError:
                        pass
                else:
                    # 还有历史页，尽量连续拉取但让出事件循环
                    await asyncio.sleep(0)
