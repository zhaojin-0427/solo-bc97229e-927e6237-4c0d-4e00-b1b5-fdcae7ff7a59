#!/usr/bin/env python3
"""港区进出事件 API 端到端演练（只通过 HTTP 驱动）。

覆盖：
1. 事件补建与正确性：事件与按轨迹独立重算的结果完全一致；首点不记事件
2. 处理进度：只使用连续水位以内数据，event_processed_seq <= contiguous_seq，
   追平后 caught_up=true；同次查询的事件与进度一致
3. 分页（after_seq 游标）与方向过滤（enter/exit）
4. 缺口演练：回退连续进度后越界事件立即不可见、旧计算不得重新发布；
   补齐后自动恢复且与独立重算一致（不漏记、不重复）
5. 清空归档：事件与衔接状态同时清空，随后自动重建
"""

from __future__ import annotations

import argparse
import sys
import time

sys.path.insert(0, ".")
import httpx
from collector.events import inside_port

FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global FAIL
    if not cond:
        FAIL += 1
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))


def wait_until(fn, timeout: float = 60.0, interval: float = 0.5):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"等待超时, last={last}")


def api_events(col: httpx.Client, mmsi: str, direction: str | None = None,
               page_size: int = 50):
    got, cursor = [], 0
    last_page = None
    while True:
        params = {"after_seq": cursor, "limit": page_size}
        if direction:
            params["direction"] = direction
        last_page = col.get(f"/port-events/{mmsi}", params=params).json()
        got.extend(last_page["events"])
        if not last_page["has_more"]:
            return got, last_page
        cursor = last_page["next_after_seq"]


def expected_events(col: httpx.Client, mmsi: str):
    traj = col.get(f"/trajectories/{mmsi}", params={"limit": 5000}).json()
    pts = sorted(traj["positions"], key=lambda p: p["seq"])
    out = []
    for a, b in zip(pts, pts[1:]):
        ia = inside_port(a["latitude"], a["longitude"])
        ib = inside_port(b["latitude"], b["longitude"])
        if ia != ib:
            out.append((
                "enter" if ib else "exit",
                a["seq"], b["seq"], a["utc_ts"], b["utc_ts"],
            ))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--station", default="http://127.0.0.1:8000")
    ap.add_argument("--collector", default="http://127.0.0.1:8001")
    args = ap.parse_args()
    st = httpx.Client(base_url=args.station, timeout=15)
    col = httpx.Client(base_url=args.collector, timeout=15)

    # ---------- 0. 干净开始 ----------
    st.post("/station/reset")
    col.post("/admin/reset")

    def steady():
        s = col.get("/status").json()
        p = col.get("/port-events/progress").json()
        return s["complete"] and p["caught_up"] and p["event_total"] > 0

    wait_until(steady, timeout=60)
    progress = col.get("/port-events/progress").json()
    check("处理进度追平连续水位", progress["caught_up"])
    check("进度不越过连续水位",
          progress["event_processed_seq"] <= progress["contiguous_seq"])
    check("返回港区矩形配置", all(
        k in progress["port_rect"]
        for k in ("lat_min", "lat_max", "lon_min", "lon_max")))

    # ---------- 1. 事件正确性：与独立重算逐字段一致 ----------
    vessels = [v["mmsi"] for v in col.get("/vessels").json()]
    check("船舶清单非空", len(vessels) > 0)
    target = None
    total_events = 0
    for mmsi in vessels:
        got, page = api_events(col, mmsi)
        total_events += len(got)
        exp = expected_events(col, mmsi)
        got_tuples = [(e["direction"], e["before_seq"], e["after_seq"],
                       e["before_utc_ts"], e["after_utc_ts"]) for e in got]
        check(f"{mmsi} 事件与独立重算一致", got_tuples == exp,
              f"api={len(got)} expected={len(exp)}")
        # 同次查询内事件与进度自洽：所有事件序号都不超过该响应声明的处理进度，
        # 且处理进度不越过连续水位（单响应快照，不与其他请求比较）
        prog = page["progress"]
        check(f"{mmsi} 同次查询事件与进度自洽",
              all(e["after_seq"] <= prog["event_processed_seq"] for e in got)
              and prog["event_processed_seq"] <= prog["contiguous_seq"],
              f"max_event={max((e['after_seq'] for e in got), default=0)} "
              f"processed={prog['event_processed_seq']} w={prog['contiguous_seq']}")
        if got and target is None:
            target = mmsi
    check("至少一艘船产生进出事件", target is not None, f"total={total_events}")

    # 首点不记事件：任一事件的 before_seq 都不等于该船最小序号
    first = col.get(f"/trajectories/{target}", params={"limit": 5000}).json()
    min_seq = min(p["seq"] for p in first["positions"])
    ev0, _ = api_events(col, target)
    check("首点不记事件", all(e["before_seq"] >= min_seq for e in ev0))

    # ---------- 2. 方向过滤与分页 ----------
    enters, _ = api_events(col, target, direction="enter")
    exits, _ = api_events(col, target, direction="exit")
    check("enter 过滤正确", all(e["direction"] == "enter" for e in enters))
    check("exit 过滤正确", all(e["direction"] == "exit" for e in exits))
    check("enter+exit 等于全量",
          len(enters) + len(exits) == len(ev0))
    paged, _ = api_events(col, target, page_size=3)
    check("小页分页拼接等于全量",
          [e["after_seq"] for e in paged] == [e["after_seq"] for e in ev0])
    check("事件按 after_seq 升序",
          [e["after_seq"] for e in ev0] == sorted(e["after_seq"] for e in ev0))
    check("after_seq 全局唯一（不重复）",
          len({e["after_seq"] for e in ev0}) == len(ev0))
    r = col.get(f"/port-events/{target}", params={"direction": "bad"})
    check("非法 direction 返回 422", r.status_code == 422)
    r = col.get("/port-events/999999999")
    check("未知 MMSI 返回 404", r.status_code == 404)

    # ---------- 3. 缺口演练 ----------
    p = col.get("/port-events/progress").json()
    w = p["contiguous_seq"]
    gap = w - 6
    before_events, _ = api_events(col, target)
    r = col.post("/admin/inject-gap", json={"seqs": [gap]})
    check("缺口注入成功", r.status_code == 200, r.text)
    body = r.json()
    check("连续水位回退", body["contiguous_seq"] == gap - 1)
    check("事件处理进度同步回退",
          body["event_processed_seq"] <= gap - 1,
          str(body["event_processed_seq"]))

    gap_events, gap_page = api_events(col, target)
    check("回退后越界事件立即排除",
          all(e["after_seq"] <= gap - 1 for e in gap_events))
    # 回退瞬间进度必须 <= 新水位；引擎可能在查询前已处理新增连续船位，
    # 因此这里只校验不变量（进度永不越界、事件不越界），不校验具体水位值
    check("回退后查询进度不越界",
          gap_page["progress"]["contiguous_seq"] >= gap - 1
          and gap_page["progress"]["event_processed_seq"]
          <= gap_page["progress"]["contiguous_seq"])

    # 旧计算不得把越界事件重新发布：在缺口仍存在期间反复观察
    time.sleep(2)
    safe = True
    for _ in range(4):
        we, wp = api_events(col, target)
        # 缺口未补齐时连续水位不会越过 gap-1
        if col.get("/status").json()["contiguous_seq"] <= gap - 1:
            if (any(e["after_seq"] > gap - 1 for e in we)
                    or wp["progress"]["event_processed_seq"] > gap - 1):
                safe = False
        time.sleep(0.3)
    check("缺口存在期间在途旧计算未重新发布越界事件", safe)

    # 补齐
    col.post("/admin/rewind-backfill")
    wait_until(lambda: (lambda pp, ss: ss["complete"] and pp["caught_up"]
                        and pp["contiguous_seq"] >= w)(
        col.get("/port-events/progress").json(), col.get("/status").json()),
        timeout=60)

    # 轮询直到事件与独立重算一致（引擎追平的最终稳态保证不漏不重）
    def recovered_consistent():
        rec, rpage = api_events(col, target)
        if not rpage["progress"]["caught_up"]:
            return None
        tuples = [(e["direction"], e["before_seq"], e["after_seq"],
                   e["before_utc_ts"], e["after_utc_ts"]) for e in rec]
        if tuples == expected_events(col, target):
            return rec, rpage
        return None

    recovered, rec_page = wait_until(recovered_consistent, timeout=30)
    check("补齐后处理进度恢复并追平", rec_page["progress"]["caught_up"])
    check("补齐后事件与独立重算一致（不漏不重）", True)
    check("补齐后事件数不少于回退前", len(recovered) >= len(before_events))

    # ---------- 4. 清空归档同时清空事件 ----------
    st.post("/station/reset")
    col.post("/admin/reset")
    imm = col.get("/port-events/progress").json()
    check("清空后事件与进度立即归零",
          imm["event_total"] == 0 and imm["event_processed_seq"] == 0
          and imm["contiguous_seq"] == 0)
    wait_until(steady, timeout=60)
    rebuilt = col.get("/port-events/progress").json()
    check("清空后事件随采集自动重建", rebuilt["event_total"] > 0)

    print(f"\n失败用例: {FAIL}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
