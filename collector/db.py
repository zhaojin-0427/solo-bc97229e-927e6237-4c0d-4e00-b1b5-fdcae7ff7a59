"""SQLite 持久化层。

关键设计：
- positions 以岸站全局序号 seq 为主键，INSERT OR IGNORE 天然去重，
  SSE 与历史补采重叠的记录只入库一次。
- 连续进度 contiguous_seq 持久化在 meta 表：只有 seq=1..w 全部存在时
  水位才允许推进到 w；记录只增不改，水位单调不减，重启后自动恢复。
- ingest_errors 记录断流、翻页失败等错误，恢复后写 resolved_at。
- port_events / vessel_port_state 持久化港区进出事件与同船衔接状态，
  事件处理进度 event_processed_seq 同样落在 meta 表；
  缺口演练回退时三者在同一事务内回退，不漏记、不重复。
"""

from __future__ import annotations

import time
from typing import Any

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS positions (
    seq        INTEGER PRIMARY KEY,
    mmsi       TEXT    NOT NULL,
    utc_ts     REAL    NOT NULL,
    latitude   REAL    NOT NULL,
    longitude  REAL    NOT NULL,
    source     TEXT    NOT NULL CHECK (source IN ('sse', 'history')),
    received_at REAL   NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_positions_mmsi_seq ON positions(mmsi, seq);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_errors (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,
    message     TEXT NOT NULL,
    occurred_at REAL NOT NULL,
    resolved_at REAL
);
CREATE INDEX IF NOT EXISTS idx_errors_occurred ON ingest_errors(occurred_at);

-- 港区进出事件：direction=enter 外转内（进港），exit 内转外（出港）。
-- 记录跨越边界的相邻两点序号与各自 UTC 时刻；after_seq 唯一，幂等去重。
CREATE TABLE IF NOT EXISTS port_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    mmsi            TEXT NOT NULL,
    direction       TEXT NOT NULL CHECK (direction IN ('enter', 'exit')),
    before_seq      INTEGER NOT NULL,
    after_seq       INTEGER NOT NULL UNIQUE,
    before_utc_ts   REAL NOT NULL,
    after_utc_ts    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_port_events_mmsi_seq
    ON port_events(mmsi, after_seq);

-- 同船衔接状态：事件引擎处理过的每艘船最后一个船位及其港内/港外状态，
-- 用于与下一个同船船位比较（首点不记事件）。last_seq <= 处理进度。
CREATE TABLE IF NOT EXISTS vessel_port_state (
    mmsi      TEXT PRIMARY KEY,
    last_seq  INTEGER NOT NULL,
    inside    INTEGER NOT NULL CHECK (inside IN (0, 1))
);
"""

ERROR_KIND_SSE = "sse"
ERROR_KIND_HISTORY = "history_page"
ERROR_KIND_PARSE = "parse"


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        # WAL 允许采集写入与轨迹查询并发；busy_timeout 防偶发锁冲突
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute("PRAGMA busy_timeout=5000")
        await self._conn.executescript(SCHEMA)
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        assert self._conn is not None, "Database 尚未 connect"
        return self._conn

    # ----------------------------- meta -----------------------------

    async def get_meta(self, key: str, default: str | None = None) -> str | None:
        cur = await self.conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        )
        row = await cur.fetchone()
        return row["value"] if row else default

    async def set_meta(self, key: str, value: str) -> None:
        await self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        await self.conn.commit()

    # ----------------------------- 船位写入 -----------------------------

    async def upsert_position(self, pos: dict[str, Any], source: str) -> bool:
        """插入一条船位；若 seq 已存在则忽略。返回是否为新记录。"""
        cur = await self.conn.execute(
            "INSERT OR IGNORE INTO positions"
            "(seq, mmsi, utc_ts, latitude, longitude, source, received_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            (
                pos["seq"],
                pos["mmsi"],
                pos["utc_ts"],
                pos["latitude"],
                pos["longitude"],
                source,
                time.time(),
            ),
        )
        await self.conn.commit()
        return cur.rowcount > 0

    async def seq_exists(self, seq: int) -> bool:
        cur = await self.conn.execute(
            "SELECT 1 FROM positions WHERE seq = ?", (seq,)
        )
        return await cur.fetchone() is not None

    async def advance_watermark(self) -> int:
        """从当前连续水位向后推进，遇到第一个缺口即停。返回最新水位。"""
        w = int(await self.get_meta("contiguous_seq", "0"))
        # 每个序号最多触发一次主键查询；有缺口时每次写入仅一次探测，代价可忽略
        while await self.seq_exists(w + 1):
            w += 1
        if w != int(await self.get_meta("contiguous_seq", "0")):
            await self.set_meta("contiguous_seq", str(w))
        return w

    async def get_watermark(self) -> int:
        return int(await self.get_meta("contiguous_seq", "0"))

    # ----------------------------- 查询 -----------------------------

    async def count_positions(self) -> int:
        cur = await self.conn.execute("SELECT COUNT(*) c FROM positions")
        return (await cur.fetchone())["c"]

    async def count_mmsi(self) -> int:
        cur = await self.conn.execute("SELECT COUNT(DISTINCT mmsi) c FROM positions")
        return (await cur.fetchone())["c"]

    async def first_missing_after(
        self, watermark: int, upper_bound: int, probe: int = 100
    ) -> list[int]:
        """返回水位之后、upper_bound 以内真实缺失的 seq（最多 20 个）。"""
        upper = min(watermark + probe, upper_bound)
        if upper <= watermark:
            return []
        cur = await self.conn.execute(
            "SELECT seq FROM positions WHERE seq > ? AND seq <= ? ORDER BY seq",
            (watermark, upper),
        )
        present = {row["seq"] for row in await cur.fetchall()}
        missing = [s for s in range(watermark + 1, upper + 1) if s not in present]
        return missing[:20]

    async def list_vessels(self) -> list[aiosqlite.Row]:
        cur = await self.conn.execute(
            "SELECT mmsi, COUNT(*) AS cnt, MIN(seq) AS min_seq, MAX(seq) AS max_seq "
            "FROM positions GROUP BY mmsi ORDER BY mmsi"
        )
        return await cur.fetchall()

    async def trajectory(
        self, mmsi: str, watermark: int, limit: int
    ) -> tuple[list[aiosqlite.Row], int, int]:
        """只返回 seq <= 连续水位的船位（缺口之后的记录不可见）。

        返回 (可见行[按 seq 倒序限量], 该 MMSI 总接收数, 水位内可见总数)。
        """
        cur = await self.conn.execute(
            "SELECT seq, mmsi, utc_ts, latitude, longitude, source, received_at "
            "FROM positions WHERE mmsi = ? AND seq <= ? ORDER BY seq DESC LIMIT ?",
            (mmsi, watermark, limit),
        )
        rows = await cur.fetchall()
        cur2 = await self.conn.execute(
            "SELECT COUNT(*) c FROM positions WHERE mmsi = ?", (mmsi,)
        )
        total = (await cur2.fetchone())["c"]
        cur3 = await self.conn.execute(
            "SELECT COUNT(*) c FROM positions WHERE mmsi = ? AND seq <= ?",
            (mmsi, watermark),
        )
        visible = (await cur3.fetchone())["c"]
        return rows, total, visible

    # ----------------------------- 错误记录 -----------------------------

    async def add_error(self, kind: str, message: str) -> int:
        cur = await self.conn.execute(
            "INSERT INTO ingest_errors(kind, message, occurred_at) VALUES(?, ?, ?)",
            (kind, message[:2000], time.time()),
        )
        await self.conn.commit()
        return cur.lastrowid

    async def resolve_error(self, error_id: int) -> None:
        await self.conn.execute(
            "UPDATE ingest_errors SET resolved_at = ? WHERE id = ? AND resolved_at IS NULL",
            (time.time(), error_id),
        )
        await self.conn.commit()

    async def list_errors(self, limit: int, include_resolved: bool) -> list[aiosqlite.Row]:
        if include_resolved:
            sql = (
                "SELECT * FROM ingest_errors ORDER BY occurred_at DESC LIMIT ?"
            )
            params: tuple[Any, ...] = (limit,)
        else:
            sql = (
                "SELECT * FROM ingest_errors WHERE resolved_at IS NULL "
                "ORDER BY occurred_at DESC LIMIT ?"
            )
            params = (limit,)
        cur = await self.conn.execute(sql, params)
        return await cur.fetchall()

    async def count_open_errors(self) -> int:
        cur = await self.conn.execute(
            "SELECT COUNT(*) c FROM ingest_errors WHERE resolved_at IS NULL"
        )
        return (await cur.fetchone())["c"]

    async def reset_all(self) -> None:
        # 清空归档必须同时清空事件；epoch 推进使任何在途旧计算写回失效
        await self.conn.executescript(
            "DELETE FROM positions; DELETE FROM meta; DELETE FROM ingest_errors; "
            "DELETE FROM port_events; DELETE FROM vessel_port_state;"
        )
        await self.conn.commit()

    async def recompute_watermark(self) -> int:
        """从头重算连续水位（仅故障注入演练用：删除记录后水位必须回退）。"""
        w = 0
        while await self.seq_exists(w + 1):
            w += 1
        await self.set_meta("contiguous_seq", str(w))
        return w

    async def delete_positions(self, seqs: list[int]) -> int:
        if not seqs:
            return 0
        placeholders = ",".join("?" for _ in seqs)
        cur = await self.conn.execute(
            f"DELETE FROM positions WHERE seq IN ({placeholders})", seqs
        )
        await self.conn.commit()
        return cur.rowcount

    # ----------------------------- 港区事件 -----------------------------

    async def get_event_seq(self) -> int:
        return int(await self.get_meta("event_processed_seq", "0"))

    async def set_event_seq(self, seq: int) -> None:
        await self.set_meta("event_processed_seq", str(seq))

    async def get_event_epoch(self) -> int:
        return int(await self.get_meta("event_epoch", "0"))

    async def positions_in_range(
        self, seq_from: int, seq_to: int
    ) -> list[aiosqlite.Row]:
        """按 seq 升序返回 (seq_from, seq_to] 区间内的船位。"""
        cur = await self.conn.execute(
            "SELECT seq, mmsi, utc_ts, latitude, longitude "
            "FROM positions WHERE seq > ? AND seq <= ? ORDER BY seq",
            (seq_from, seq_to),
        )
        return await cur.fetchall()

    async def get_vessel_state(self, mmsi: str) -> aiosqlite.Row | None:
        cur = await self.conn.execute(
            "SELECT mmsi, last_seq, inside FROM vessel_port_state WHERE mmsi = ?",
            (mmsi,),
        )
        return await cur.fetchone()

    async def apply_event_batch(
        self,
        target_seq: int,
        events: list[dict[str, Any]],
        state_upserts: dict[str, tuple[int, int]],
    ) -> None:
        """原子提交一批事件处理结果：事件、同船衔接状态、处理进度同事务。

        依赖 sqlite3 隐式事务：首批 DML 开启事务，末尾单次 commit 原子提交；
        事件 after_seq 唯一约束保证即使重复提交也不会产生重复事件。
        小批量提交使历史扫描不形成长写事务。
        """
        conn = self.conn
        try:
            for ev in events:
                await conn.execute(
                    "INSERT OR IGNORE INTO port_events"
                    "(mmsi, direction, before_seq, after_seq, "
                    " before_utc_ts, after_utc_ts) VALUES(?, ?, ?, ?, ?, ?)",
                    (
                        ev["mmsi"],
                        ev["direction"],
                        ev["before_seq"],
                        ev["after_seq"],
                        ev["before_utc_ts"],
                        ev["after_utc_ts"],
                    ),
                )
            for mmsi, (last_seq, inside) in state_upserts.items():
                await conn.execute(
                    "INSERT INTO vessel_port_state(mmsi, last_seq, inside) "
                    "VALUES(?, ?, ?) "
                    "ON CONFLICT(mmsi) DO UPDATE SET "
                    "last_seq=excluded.last_seq, inside=excluded.inside",
                    (mmsi, last_seq, inside),
                )
            await conn.execute(
                "INSERT INTO meta(key, value) VALUES('event_processed_seq', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(target_seq),),
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    async def rollback_events_to(
        self, watermark: int, new_epoch: int, inside_fn
    ) -> tuple[int, int]:
        """缺口回退：在单事务内把事件视图回退到新的连续水位。

        - 删除所有 after_seq > watermark 的事件（越过进度的事件立即不可见）；
        - 同船衔接状态不直接删除，而是截断重建为该船在水位内最后一个
          船位的内/外状态：这样补齐后跨越回退边界的相邻船位仍能正确比较，
          不会把边界后的第一个船位误判成“首点”而漏记事件；
          若该船在水位内没有任何船位，才删除其状态行（首点不记事件）；
        - 处理进度不超过新水位，并推进 epoch 作废旧计算。
        inside_fn(latitude, longitude) -> bool 由事件层注入，边界判定口径唯一。
        返回 (删除事件数, 重建状态行数)。
        """
        conn = self.conn
        try:
            cur = await conn.execute(
                "DELETE FROM port_events WHERE after_seq > ?", (watermark,)
            )
            deleted_events = cur.rowcount

            cur = await conn.execute(
                "SELECT mmsi FROM vessel_port_state WHERE last_seq > ?",
                (watermark,),
            )
            affected = [row["mmsi"] for row in await cur.fetchall()]
            rebuilt_states = 0
            for mmsi in affected:
                cur = await conn.execute(
                    "SELECT seq, latitude, longitude FROM positions "
                    "WHERE mmsi = ? AND seq <= ? ORDER BY seq DESC LIMIT 1",
                    (mmsi, watermark),
                )
                last_row = await cur.fetchone()
                if last_row is None:
                    # 该船在水位内无船位：删除状态，下次出现按首点处理
                    await conn.execute(
                        "DELETE FROM vessel_port_state WHERE mmsi = ?", (mmsi,)
                    )
                else:
                    inside = 1 if inside_fn(
                        last_row["latitude"], last_row["longitude"]
                    ) else 0
                    await conn.execute(
                        "UPDATE vessel_port_state SET last_seq = ?, inside = ? "
                        "WHERE mmsi = ?",
                        (last_row["seq"], inside, mmsi),
                    )
                    rebuilt_states += 1

            await conn.execute(
                "INSERT INTO meta(key, value) VALUES('event_processed_seq', ?) "
                "ON CONFLICT(key) DO UPDATE SET "
                "value=CAST(MIN(CAST(value AS INTEGER), "
                "           CAST(excluded.value AS INTEGER)) AS TEXT)",
                (str(watermark),),
            )
            await conn.execute(
                "INSERT INTO meta(key, value) VALUES('event_epoch', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(new_epoch),),
            )
            await conn.commit()
            return deleted_events, rebuilt_states
        except Exception:
            await conn.rollback()
            raise

    async def count_port_events(self) -> int:
        cur = await self.conn.execute("SELECT COUNT(*) c FROM port_events")
        return (await cur.fetchone())["c"]

    async def list_port_events(
        self,
        mmsi: str,
        after_seq: int,
        limit: int,
        direction: str | None = None,
        visible_bound: int | None = None,
    ) -> tuple[list[aiosqlite.Row], bool]:
        """按 MMSI 分页读取事件（after_seq 升序）。

        visible_bound 给定时，只返回 after_seq <= visible_bound 的事件，
        用于把事件锁定在请求开始时的处理进度快照上，保证同次查询一致。
        多取 1 行判断是否还有下一页。返回 (事件行, has_more)。
        """
        bound_clause = "AND e.after_seq <= ?" if visible_bound is not None else ""
        if direction:
            sql = (
                "SELECT e.mmsi, e.direction, e.before_seq, e.after_seq, "
                "e.before_utc_ts, e.after_utc_ts FROM port_events e "
                f"WHERE e.mmsi = ? AND e.after_seq > ? {bound_clause} "
                "AND e.direction = ? ORDER BY e.after_seq LIMIT ?"
            )
            params: list[Any] = [mmsi, after_seq]
            if visible_bound is not None:
                params.append(visible_bound)
            params.extend([direction, limit + 1])
        else:
            sql = (
                "SELECT e.mmsi, e.direction, e.before_seq, e.after_seq, "
                "e.before_utc_ts, e.after_utc_ts FROM port_events e "
                f"WHERE e.mmsi = ? AND e.after_seq > ? {bound_clause} "
                "ORDER BY e.after_seq LIMIT ?"
            )
            params = [mmsi, after_seq]
            if visible_bound is not None:
                params.append(visible_bound)
            params.append(limit + 1)
        cur = await self.conn.execute(sql, tuple(params))
        rows = await cur.fetchall()
        has_more = len(rows) > limit
        return rows[:limit], has_more

    async def reset_event_state(self) -> None:
        """清空归档时同步清空事件与衔接状态（在 reset_all 同一脚本内完成）。"""
        await self.conn.execute("DELETE FROM port_events")
        await self.conn.execute("DELETE FROM vessel_port_state")
