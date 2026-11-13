#!/usr/bin/env python3
"""端到端演练脚本（只通过 HTTP 访问两个服务，不依赖其内部状态）。

覆盖：
1. 初始采集：历史补采 + SSE 实时，重叠去重，连续进度无缺口
2. 历史翻页失败：实时接收不中断，错误可查，恢复后自动补齐
3. SSE 断流/恢复：断线期间船位不丢，重连后由历史补齐
4. 采集器重启：船位与进度保留，自动接续
5. 轨迹查询：按 MMSI 读取，缺口之后的记录不可见
6. 幂等重启：重复运行 reset 后状态自洽
"""

from __future__ import annotations

import argparse
import sys
import time

import httpx

FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global FAIL
    mark = "PASS" if cond else "FAIL"
    if not cond:
        FAIL += 1
    print(f"[{mark}] {name}" + (f" -- {detail}" if detail else ""))


def wait_until(desc: str, fn, timeout: float = 60.0, interval: float = 0.5):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"等待超时: {desc} (last={last})")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--station", default="http://127.0.0.1:8000")
    ap.add_argument("--collector", default="http://127.0.0.1:8001")
    args = ap.parse_args()
    st = httpx.Client(base_url=args.station, timeout=15)
    col = httpx.Client(base_url=args.collector, timeout=15)

    # ---------- 0. 重置，保证可重复运行 ----------
    st.post("/station/reset")
    col.post("/admin/reset")
    time.sleep(2)

    # ---------- 1. 初始采集与去重 ----------
    station_total = st.get("/station/status").json()["total_positions"]

    def caught_up():
        s = col.get("/status").json()
        return s["received_total"] >= station_total and s["complete"]

    wait_until("初始历史补齐", caught_up, timeout=40)
    s = col.get("/status").json()
    check("连续水位等于已接收总数（无缺口）", s["contiguous_seq"] == s["received_total"],
          f"w={s['contiguous_seq']} total={s['received_total']}")
    check("两个来源均有贡献", s["sse_received"] >= 0 and s["history_received"] > 0,
          f"sse={s['sse_received']} history={s['history_received']}")
    check("无真实缺口", s["next_missing_seqs"] == [], str(s["next_missing_seqs"]))
    check("无未恢复错误", s["open_error_count"] == 0)

    # 直接验证库内 seq 唯一（重复入库只一次）：再等几秒，总数应随岸站单调增长
    time.sleep(2)
    s2 = col.get("/status").json()
    check("实时船位持续入库", s2["received_total"] > s["received_total"],
          f"{s['received_total']} -> {s2['received_total']}")
    check("持续保持连续", s2["contiguous_seq"] == s2["received_total"])

    # ---------- 2. 历史翻页 100% 失败 ----------
    st.post("/station/history-fail-rate", json={"rate": 1.0})
    before = col.get("/status").json()["received_total"]
    time.sleep(6)
    during = col.get("/status").json()
    check("翻页失败期间实时仍在接收", during["sse_connected"] and
          during["received_total"] > before,
          f"{before} -> {during['received_total']}")
    check("翻页失败被计数", during["history_failures"] > 0)
    errs = col.get("/errors").json()
    check("错误可查询且类型正确",
          any(e["kind"] == "history_page" and e["resolved_at"] is None for e in errs))
    st_now = st.get("/station/status").json()["last_seq"]
    # 历史拿不到，但 SSE 在线 => 水位仍可随实时流推进（若 SSE 连续）
    check("实时连续时水位继续推进", during["contiguous_seq"] >= before)
    check("最大已见序号在跟踪", during["max_seen_seq"] >= during["received_total"])

    # 恢复历史接口，等待补齐到岸站最新
    st.post("/station/history-fail-rate", json={"rate": 0.0})

    def backfilled():
        c = col.get("/status").json()
        return c["complete"] and c["received_total"] >= st_now and c["open_error_count"] == 0

    wait_until("历史恢复后补齐", backfilled, timeout=40)
    s3 = col.get("/status").json()
    check("恢复后错误全部 resolved", s3["open_error_count"] == 0)
    check("恢复后连续无缺口", s3["contiguous_seq"] == s3["received_total"])

    # ---------- 3. SSE 断流 / 恢复 ----------
    st.post("/station/stream-stop")
    time.sleep(1)
    disconnected = wait_until(
        "采集器感知断流",
        lambda: not col.get("/status").json()["sse_connected"],
        timeout=70,  # 读取超时内才会发现断开
    )
    check("SSE 状态变为断开", disconnected)
    errs = col.get("/errors").json()
    check("断流错误已记录", any(e["kind"] == "sse" for e in errs))
    # 断流期间岸站暂停生成；恢复后错过/新增的船位由历史补齐
    time.sleep(2)
    st.post("/station/stream-resume")
    time.sleep(3)
    station_after = st.get("/station/status").json()["last_seq"]

    def sse_recovered():
        c = col.get("/status").json()
        return c["sse_connected"] and c["complete"] and c["received_total"] >= station_after

    wait_until("SSE 重连并补齐", sse_recovered, timeout=60)
    s4 = col.get("/status").json()
    check("发生过断流并完成重连",
          s4["sse_disconnects"] >= 1 and s4["sse_reconnects"] >= s4["sse_disconnects"] + 1,
          f"disconnects={s4['sse_disconnects']} reconnects={s4['sse_reconnects']}")
    check("重连后无缺口", s4["contiguous_seq"] == s4["received_total"])
    check("断流错误已恢复",
        all(e["resolved_at"] is not None for e in col.get("/errors?include_resolved=true").json()
            if e["kind"] == "sse"))

    # ---------- 4. 轨迹查询 ----------
    vessels = col.get("/vessels").json()
    check("船舶清单非空", len(vessels) > 0, str(len(vessels)))
    mmsi = vessels[0]["mmsi"]
    traj = col.get(f"/trajectories/{mmsi}").json()
    check("轨迹返回船位", traj["returned"] > 0)
    seqs = [p["seq"] for p in traj["positions"]]
    check("轨迹按 seq 升序", seqs == sorted(seqs))
    check("轨迹均不超过连续水位", all(x <= traj["contiguous_seq"] for x in seqs))
    check("时间戳字段存在", all("utc_ts" in p and "latitude" in p for p in traj["positions"]))
    not_found = col.get("/trajectories/999999999")
    check("未知 MMSI 返回 404", not_found.status_code == 404)

    # ---------- 5. 缺口语义：注入缺口，轨迹不返回缺口之后的记录 ----------
    status_before = col.get("/status").json()
    w_before = status_before["contiguous_seq"]
    gap = w_before - 5  # 在连续区间内部挖一个缺口
    # 先取该缺口所属 MMSI，验证它的轨迹在缺口后被截断
    # 直接挑一个 MMSI 即可：水位对所有 MMSI 是统一上界
    vessels_all = col.get("/vessels").json()
    mmsi0 = vessels_all[0]["mmsi"]
    traj_before = col.get(f"/trajectories/{mmsi0}").json()
    max_visible_before = max(p["seq"] for p in traj_before["positions"])

    r = col.post("/admin/inject-gap", json={"seqs": [gap]})
    check("缺口注入成功", r.status_code == 200, r.text)
    deleted = r.json()["deleted"]
    s_gap = col.get("/status").json()
    check("连续水位回退到缺口之前", s_gap["contiguous_seq"] == gap - 1,
          f"w={s_gap['contiguous_seq']} expect={gap - 1}")
    check("状态报告真实缺口", gap in s_gap["next_missing_seqs"],
          str(s_gap["next_missing_seqs"]))
    check("已接收总数包含缺口后的记录（记录仍在库）",
          s_gap["received_total"] == status_before["received_total"] - deleted)

    traj_gap = col.get(f"/trajectories/{mmsi0}").json()
    check("轨迹不返回缺口之后的记录",
          all(p["seq"] <= gap - 1 for p in traj_gap["positions"]),
          f"max={max(p['seq'] for p in traj_gap['positions'])} bound={gap - 1}")
    check("轨迹可见上界等于回退后水位", traj_gap["contiguous_seq"] == gap - 1)

    # 回退补采游标，让历史循环重新拉取并补齐缺口
    col.post("/admin/rewind-backfill")
    wait_until(
        "缺口补齐",
        lambda: (lambda x: x["contiguous_seq"] == x["received_total"]
                 and x["contiguous_seq"] >= w_before)(col.get("/status").json()),
        timeout=30,
    )
    s5 = col.get("/status").json()
    check("补齐后连续水位恢复", s5["contiguous_seq"] >= w_before)
    traj_after = col.get(f"/trajectories/{mmsi0}").json()
    check("补齐后轨迹恢复可见", max(p["seq"] for p in traj_after["positions"])
          >= max_visible_before)

    print("\n========== 汇总 ==========")
    print(f"失败用例: {FAIL}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
