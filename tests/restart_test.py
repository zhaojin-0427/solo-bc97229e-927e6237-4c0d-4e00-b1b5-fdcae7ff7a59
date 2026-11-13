#!/usr/bin/env python3
"""中断重启接续测试：真正终止采集器进程后再启动，验证
- 已提交船位、连续水位、历史游标全部保留
- 重启后自动接续 SSE 与历史补采，最终仍无缺口
通过 docker compose / 本地进程均可复用本脚本（仅依赖 HTTP）。
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time

import httpx

FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global FAIL
    if not cond:
        FAIL += 1
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))


def wait_status(client: httpx.Client, pred, timeout: float = 40.0) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = client.get("/status").json()
        if pred(last):
            return last
        time.sleep(0.5)
    raise AssertionError(f"等待超时 last={last}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--station", default="http://127.0.0.1:8000")
    ap.add_argument("--collector", default="http://127.0.0.1:8001")
    ap.add_argument("--db", default="/tmp/colldata/collector.db")
    ap.add_argument("--pylibs", default="/tmp/pylibs")
    args = ap.parse_args()
    col = httpx.Client(base_url=args.collector, timeout=15)
    st = httpx.Client(base_url=args.station, timeout=15)

    st.post("/station/reset")
    col.post("/admin/reset")
    wait_status(col, lambda s: s["complete"] and s["received_total"] >= 60)
    before = col.get("/status").json()
    check("重启前已连续", before["contiguous_seq"] == before["received_total"],
          f"total={before['received_total']}")

    # 杀掉采集器进程（SQLite 文件保留）
    killed = subprocess.run(
        ["pkill", "-f", "[c]ollector.app:app"], capture_output=True
    )
    check("采集器进程已终止", killed.returncode == 0)
    time.sleep(2)
    try:
        col.get("/health", timeout=2)
        check("采集器确实不可达", False)
    except httpx.HTTPError:
        check("采集器确实不可达", True)

    # 停机期间岸站仍在产生新船位
    station_during_stop = st.get("/station/status").json()["last_seq"]
    time.sleep(3)
    station_new = st.get("/station/status").json()["last_seq"]
    check("停机期间岸站持续产生船位", station_new > station_during_stop)

    # 重新启动采集器，沿用同一个 SQLite 文件
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "collector.app:app",
            "--host", "127.0.0.1", "--port", "8001",
        ],
        env={
            "STATION_URL": args.station,
            "DB_PATH": args.db,
            "PYTHONPATH": args.pylibs,
            "PATH": "/usr/bin:/bin",
        },
        stdout=open("/tmp/collector-restart.log", "w"),
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    check("采集器重新拉起", proc.pid > 0)

    # 等健康检查通过
    deadline = time.time() + 30
    up = False
    while time.time() < deadline:
        try:
            if col.get("/health", timeout=2).status_code == 200:
                up = True
                break
        except httpx.HTTPError:
            time.sleep(0.5)
    check("重启后健康检查通过", up)

    after_up = wait_status(
        col, lambda s: s["complete"] and s["received_total"] >= before["received_total"]
    )
    check("重启后已有船位保留",
          after_up["received_total"] >= before["received_total"],
          f"before={before['received_total']} after={after_up['received_total']}")

    # 自动接续：最终补齐停机期间的船位并超过岸站停机时的序号
    final = wait_status(
        col,
        lambda s: s["complete"] and s["contiguous_seq"] >= station_new,
        timeout=40,
    )
    check("重启后自动接续补齐", final["contiguous_seq"] >= station_new,
          f"w={final['contiguous_seq']} station={station_new}")
    check("重启后连续无缺口", final["contiguous_seq"] == final["received_total"])
    check("历史游标已持久化并推进",
          final["history_cursor"] >= before["history_cursor"])

    print(f"\n失败用例: {FAIL}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
