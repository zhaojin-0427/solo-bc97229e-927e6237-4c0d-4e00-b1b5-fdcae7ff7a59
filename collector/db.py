"""SQLite 持久化层。

关键设计：
- positions 以岸站全局序号 seq 为主键，INSERT OR IGNORE 天然去重，
  SSE 与历史补采重叠的记录只入库一次。
- 连续进度 contiguous_seq 持久化在 meta 表：只有 seq=1..w 全部存在时
  水位才允许推进到 w；记录只增不改，水位单调不减，重启后自动恢复。
- ingest_errors 记录断流、翻页失败等错误，恢复后写 resolved_at。
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
        await self.conn.executescript(
            "DELETE FROM positions; DELETE FROM meta; DELETE FROM ingest_errors;"
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
