#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RabbitMQ 最大稳定吞吐自动探测脚本（基于 perf-test.jar 的 compact 输出）
- 自动指数探测 + 二分逼近，给出“最大稳定吞吐（msg/s）”估计值
- 稳定判定：avg_received/avg_sent ≥ success_ratio 且全程 p95 ≤ p95_limit_ms
- 生成两份 CSV：summary（每次试验汇总）、timeseries（逐秒数据）
"""

import argparse
import csv
import datetime as dt
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time
from statistics import mean

# 例： "1.000s 173,920 msg/s 84,405 msg/s 1/25/189/312/331 ms"
COMPACT_LINE_RE = re.compile(
    r"^\s*(?P<tsec>\d+(?:\.\d+)?)s\s+"
    r"(?P<sent>[\d,]+)\s+msg/s\s+"
    r"(?P<recv>[\d,]+)\s+msg/s\s+"
    r"(?P<lat>[\d/]+)\s+(?P<unit>µs|μs|us|ms)\s*$"
)

def parse_args():
    p = argparse.ArgumentParser(
        description="Find RabbitMQ max stable throughput using perf-test.jar and save results to CSV."
    )
    # 允许默认/环境变量：PERFTEST_JAR、AMQP_URI
    p.add_argument(
        "--jar",
        default=os.environ.get("PERFTEST_JAR", "perf-test.jar"),
        help="Path to perf-test.jar (default: ./perf-test.jar or $PERFTEST_JAR)",
    )
    p.add_argument(
        "--uri",
        default=os.environ.get("AMQP_URI", "amqp://guest:guest@localhost:5672/%2F"),
        help="AMQP URI (default: amqp://guest:guest@localhost:5672/%2F or $AMQP_URI)",
    )

    p.add_argument("--producers", "-x", type=int, default=4, help="Number of producers")
    p.add_argument("--consumers", "-y", type=int, default=4, help="Number of consumers")
    p.add_argument("--size", "-s", type=int, default=1024, help="Message size (bytes)")
    p.add_argument("--queue", "-u", default="perf_queue", help="Queue name")
    p.add_argument("--duration", "-z", type=int, default=15, help="Seconds per trial")
    p.add_argument("--start-rate", type=int, default=1000, help="Initial target rate (msg/s)")
    p.add_argument("--max-rate", type=int, default=1_000_000, help="Hard cap for target rate (msg/s)")
    p.add_argument("--growth", type=float, default=2.0, help="Growth factor for coarse search (e.g., 1.5~2.0)")
    p.add_argument("--success-ratio", type=float, default=0.95, help="avg_received/avg_sent threshold")
    p.add_argument("--p95-limit-ms", type=int, default=2000, help="Allowable worst p95 latency (ms)")
    p.add_argument("--java-opts", dest="java_opts", default="-Xms512m -Xmx1g", help="JAVA_OPTS for the perf-test JVM")
    p.add_argument("--id-prefix", default="auto", help="Prefix for run id shown by PerfTest")
    p.add_argument("--warmup-rate", type=int, default=0, help="Optional warmup rate (msg/s); 0 = skip warmup")
    # 输出目录与组件名；如未提供 csv-prefix，则按组件命名规范化到 datas/
    p.add_argument("--out-dir", default="datas", help="Output directory (default: datas)")
    p.add_argument("--component-name", default="RabbitMQ", help="Component name to embed in filenames")
    p.add_argument("--csv-prefix", default=None, help="(Optional) legacy prefix; if set, overrides component-based naming")
    p.add_argument("--quiet", action="store_true", help="Do not stream perftest output")
    return p.parse_args()

def ensure_java_and_jar(jar_path: pathlib.Path):
    if shutil.which("java") is None:
        sys.exit("ERROR: 'java' not found in PATH.")
    if not jar_path.exists():
        sys.exit(f"ERROR: jar not found: {jar_path}")

def run_perftest(args, rate: int, run_id: str):
    """
    执行 perf-test.jar（compact 输出），返回 (timeseries_rows, summary_dict)
    """
    cmd = [
        "java", *args.java_opts.split(),
        "-jar", str(args.jar),
        "--uri", args.uri,
        "--metrics-format", "compact",
        "--rate", str(rate),
        "-x", str(args.producers),
        "-y", str(args.consumers),
        "-s", str(args.size),
        "-u", str(args.queue),
        "-z", str(args.duration),
        "--id", run_id,
    ]
    # 可按需添加：--flag persistent、--qos、--confirm 等

    if not args.quiet:
        print(" ".join(cmd), flush=True)

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1,
    )

    timeseries = []
    start_time = time.time()

    for line in proc.stdout:
        if not args.quiet:
            print(line, end="")
        m = COMPACT_LINE_RE.match(line)
        if m:
            tsec = float(m.group("tsec"))
            sent = int(m.group("sent").replace(",", ""))
            recv = int(m.group("recv").replace(",", ""))
            lat_parts = [int(x) for x in m.group("lat").split("/")]

            unit = m.group("unit")
            factor = 0.001 if unit in ("µs", "μs", "us") else 1.0  # 微秒→毫秒

            if len(lat_parts) == 5:
                p50 = int(round(lat_parts[1] * factor))
                p95 = int(round(lat_parts[3] * factor))
                p99 = int(round(lat_parts[4] * factor))
            else:
                p50 = p95 = p99 = -1

            timeseries.append({
                "time_s": tsec,
                "sent_msg_s": sent,
                "received_msg_s": recv,
                "p50_ms": p50, "p95_ms": p95, "p99_ms": p99
            })


    proc.wait()
    rc = proc.returncode
    end_time = time.time()
    duration = end_time - start_time

    if rc != 0 and not timeseries:
        raise RuntimeError(f"PerfTest exited with code {rc} and produced no parsable output")

    if timeseries:
        avg_sent = mean([row["sent_msg_s"] for row in timeseries])
        avg_recv = mean([row["received_msg_s"] for row in timeseries])
        valid_p95 = [row["p95_ms"] for row in timeseries if row["p95_ms"] >= 0]
        worst_p95 = max(valid_p95) if valid_p95 else -1
    else:
        avg_sent = avg_recv = worst_p95 = 0

    # 稳定性判定
    success = True
    note_bits = []
    if avg_sent <= 0:
        success = False
        note_bits.append("no_data")
    else:
        ratio = (avg_recv / avg_sent) if avg_sent > 0 else 0.0
        if ratio < args.success_ratio:
            success = False
            note_bits.append(f"ratio_below_{args.success_ratio}")
        if worst_p95 >= 0 and worst_p95 > args.p95_limit_ms:
            success = False
            note_bits.append(f"p95_over_{args.p95_limit_ms}ms")

    summary = {
        "run_id": run_id,
        "target_rate_msg_s": rate,
        "avg_sent_msg_s": int(avg_sent),
        "avg_received_msg_s": int(avg_recv),
        "worst_p95_ms": int(worst_p95) if worst_p95 >= 0 else -1,
        "success": success,
        "note": ";".join(note_bits),
        "duration_s": int(duration),
        "producers": args.producers,
        "consumers": args.consumers,
        "size_bytes": args.size,
        "queue": args.queue,
    }
    return timeseries, summary

def write_csvs(ts_filename, sum_filename, all_ts_rows, all_sum_rows):
    # 逐秒
    with open(ts_filename, "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "run_id","target_rate_msg_s","time_s",
                "sent_msg_s","received_msg_s","p50_ms","p95_ms","p99_ms"
            ],
        )
        w.writeheader()
        for row in all_ts_rows:
            w.writerow(row)
    # 汇总
    with open(sum_filename, "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "run_id","target_rate_msg_s","avg_sent_msg_s","avg_received_msg_s",
                "worst_p95_ms","success","note","duration_s",
                "producers","consumers","size_bytes","queue"
            ],
        )
        w.writeheader()
        for row in all_sum_rows:
            w.writerow(row)

def main():
    args = parse_args()
    args.jar = pathlib.Path(args.jar).resolve()
    ensure_java_and_jar(args.jar)

    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.csv_prefix:
        prefix = args.csv_prefix
    else:
        os.makedirs(args.out_dir, exist_ok=True)
        prefix = os.path.join(args.out_dir, f"{args.component_name}_perftest")
    ts_csv = f"{prefix}_timeseries_{timestamp}.csv"
    sum_csv = f"{prefix}_summary_{timestamp}.csv"

    all_ts_rows, all_sum_rows = [], []

    def record(run_id, rate, timeseries, summary):
        for row in timeseries:
            all_ts_rows.append({"run_id": run_id, "target_rate_msg_s": rate, **row})
        all_sum_rows.append(summary)

    # 可选预热
    if args.warmup_rate and args.warmup_rate > 0:
        run_id = f"{args.id_prefix}-warmup-{args.warmup_rate}"
        ts, sm = run_perftest(args, args.warmup_rate, run_id)
        record(run_id, args.warmup_rate, ts, sm)

    # 1) 粗搜索（指数增长）找出 [lo, hi) 区间
    rate = args.start_rate
    last_ok = 0
    hi = None
    while rate <= args.max_rate:
        run_id = f"{args.id_prefix}-r{rate}"
        ts, sm = run_perftest(args, rate, run_id)
        record(run_id, rate, ts, sm)
        if sm["success"]:
            last_ok = rate
            # 下一档至少 +1，避免 rate=1 时卡住
            rate = int(max(rate + 1, rate * args.growth))
        else:
            hi = rate
            break

    # 没有任何成功档位
    if last_ok == 0 and hi is None:
        print("未找到成功的速率；请检查 broker/参数/网络。", file=sys.stderr)
        write_csvs(ts_csv, sum_csv, all_ts_rows, all_sum_rows)
        print(f"已写入: {sum_csv}\n        {ts_csv}")
        return

    # 从未失败过（达到上限）
    if hi is None:
        print(f"最大稳定吞吐 ≥ {last_ok} msg/s（达到上限 {args.max_rate}）。")
        write_csvs(ts_csv, sum_csv, all_ts_rows, all_sum_rows)
        print(f"已写入: {sum_csv}\n        {ts_csv}")
        return

    # 2) 二分搜索 [last_ok, hi) 之间的临界
    lo = last_ok
    # 终止条件：间隔 ≤ max(100, 2%*lo)
    while hi - lo > max(100, int(0.02 * max(1, lo))):
        mid = (lo + hi) // 2
        run_id = f"{args.id_prefix}-r{mid}"
        ts, sm = run_perftest(args, mid, run_id)
        record(run_id, mid, ts, sm)
        if sm["success"]:
            lo = mid
        else:
            hi = mid

    print(
        f"估计最大稳定吞吐: {lo} msg/s "
        f"(判定标准: received/sent ≥ {args.success_ratio}, p95 ≤ {args.p95_limit_ms} ms)"
    )

    write_csvs(ts_csv, sum_csv, all_ts_rows, all_sum_rows)
    print(f"已写入: {sum_csv}\n        {ts_csv}")

if __name__ == "__main__":
    main()
