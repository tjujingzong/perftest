#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
在 Docker 容器内逐步提升 kbbench 并发(-c)，多轮运行，并将每次结果保存到 CSV。
基于用户原始脚本扩展：
  1) 支持 clients 扫描（--clients-seq 或 --clients-start/--clients-end/--clients-step）
  2) 支持每个并发跑多次 --repeats 并记录每次结果
  3) CSV 输出字段：timestamp, clients, jobs, duration_s, tps_including, tps_excluding,
                    latency_ms_avg, tx_processed, return_code, error
  4) 仍可像原脚本一样只跑一次（未提供扫描参数时）

示例：
python kbbench_sweep.py \
  --container kingbase --password 123456 --host 127.0.0.1 --port 54321 \
  --db kbbenchdb --user system --jobs 4 --duration 60 \
  --clients-seq 4,8,16,32 --repeats 2 --out results.csv --print-output

或用区间：
python kbbench_sweep.py \
  --container kingbase --password 123456 --host 127.0.0.1 --port 54321 \
  --db kbbenchdb --user system --jobs 4 --duration 60 \
  --clients-start 4 --clients-end 32 --clients-step 4 \
  --out results.csv
"""

import subprocess
import shlex
import re
import sys
import argparse
import csv
import os
import time
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List

TPS_INC_RE = re.compile(r"tps\s*=\s*([0-9.]+)\s*\(including", re.IGNORECASE)
TPS_EXC_RE = re.compile(r"tps\s*=\s*([0-9.]+)\s*\(excluding", re.IGNORECASE)
LAT_AVG_RE = re.compile(r"latency\s+average\s*=\s*([0-9.]+)\s*ms", re.IGNORECASE)
TX_PROC_RE = re.compile(r"number\s+of\s+transactions\s+actually\s+processed:\s*([0-9]+)", re.IGNORECASE)


def parse_tps(text: str) -> Tuple[Optional[float], Optional[float]]:
    inc = exc = None
    m = TPS_INC_RE.search(text)
    if m:
        inc = float(m.group(1))
    m = TPS_EXC_RE.search(text)
    if m:
        exc = float(m.group(1))
    return inc, exc


def parse_latency(text: str) -> Optional[float]:
    m = LAT_AVG_RE.search(text)
    return float(m.group(1)) if m else None


def parse_tx_processed(text: str) -> Optional[int]:
    m = TX_PROC_RE.search(text)
    return int(m.group(1)) if m else None


def run_kbbench(
    container: str = "kingbase",
    password: str = "123456",
    host: str = "127.0.0.1",
    db: str = "kbbenchdb",
    user: str = "system",
    clients: int = 8,
    jobs: int = 4,
    duration: int = 60,
    progress: int = 10,
    port: Optional[int] = None,
) -> Tuple[int, str]:
    """
    在容器内：
      1) 写 ~/.pgpass（libpq 读取；支持 * 通配）
      2) 如有 sys_encpwd，则配置 ~/.encpwd（Kingbase 免密）
      3) 通过 PGPASSWORD / KINGBASE_PASSWORD 环境变量兜底
      4) 执行 kbbench 并返回 (return_code, raw_output)
    """
    kb_cmd = [
        "kbbench",
        "-h", host,
        "-M", "extended",
        "-c", str(clients),
        "-j", str(jobs),
        "-T", str(duration),
        "-P", str(progress),
        "-d", db,
        "-U", user,
        "-r",
    ]
    if port is not None:
        kb_cmd.extend(["-p", str(port)])

    kb_cmd_str = " ".join(shlex.quote(x) for x in kb_cmd)

    pgpass_line = f"{host}:{port if port is not None else '*'}:{db}:{user}:{password}"

    bash_script = f"""
        set -euo pipefail
        umask 077

        printf "%s\\n" "$PGPASSLINE" > "$HOME/.pgpass"
        chmod 600 "$HOME/.pgpass"

        if command -v sys_encpwd >/dev/null 2>&1; then
            HOPT=""; POPT=""
            if [ -n {shlex.quote(host)!r} ]; then HOPT="-H {shlex.quote(host)}"; fi
            if [ -n {('' if port is None else str(port))!r} ]; then POPT="-P {port}"; fi
            sys_encpwd $HOPT $POPT -D {shlex.quote(db)} -U {shlex.quote(user)} -W {shlex.quote(password)} >/dev/null 2>&1 || true
        fi

        exec {kb_cmd_str}
    """

    docker_cmd = [
        "docker", "exec",
        "-e", f"PGPASSWORD={password}",
        "-e", f"KINGBASE_PASSWORD={password}",
        "-e", f"PGPASSLINE={pgpass_line}",
        container,
        "bash", "-lc", bash_script,
    ]

    proc = subprocess.run(
        docker_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc.returncode, proc.stdout


def parse_metrics(output: str) -> Dict[str, Optional[float]]:
    inc, exc = parse_tps(output)
    lat = parse_latency(output)
    txc = parse_tx_processed(output)
    return {
        "tps_including": inc,
        "tps_excluding": exc,
        "latency_ms_avg": lat,
        "tx_processed": txc,
    }


def ensure_header(path: str, fieldnames: List[str]) -> None:
    need_header = not os.path.exists(path) or os.path.getsize(path) == 0
    if need_header:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()


def append_row(path: str, fieldnames: List[str], row: Dict[str, Any]) -> None:
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerow(row)


def expand_clients(args: argparse.Namespace) -> List[int]:
    seq: List[int] = []
    if args.clients_seq:
        try:
            seq = [int(x) for x in args.clients_seq.split(",") if x.strip()]
        except ValueError:
            raise SystemExit("--clients-seq 需要逗号分隔的整数，例如 4,8,16,32")
    elif args.clients_start is not None and args.clients_end is not None:
        step = args.clients_step or 1
        if step <= 0:
            raise SystemExit("--clients-step 必须为正整数")
        seq = list(range(args.clients_start, args.clients_end + 1, step))
    else:
        # 回退到单次运行
        seq = [args.clients]
    return seq


def main():
    ap = argparse.ArgumentParser(description="在 Docker 容器中逐步增加 kbbench 并发并将结果保存到 CSV")
    # 连接/运行参数
    ap.add_argument("--container", default="kingbase", help="容器名")
    ap.add_argument("--password", default="123456", help="数据库密码")
    ap.add_argument("--host", default="127.0.0.1", help="数据库主机（容器内可见）")
    ap.add_argument("--db", default="kbbenchdb", help="数据库名")
    ap.add_argument("--user", default="system", help="用户名")
    ap.add_argument("--port", type=int, default=None, help="-p 端口（可选，未知可不填）")

    # 压测参数
    ap.add_argument("--jobs", type=int, default=4, help="-j 线程数")
    ap.add_argument("--duration", type=int, default=60, help="-T 持续时间（秒）")
    ap.add_argument("--progress", type=int, default=10, help="-P 进度打印间隔（秒）")

    # 并发扫描参数
    ap.add_argument("--clients", type=int, default=8, help="单次运行时的 -c 并发客户端数（当未开启扫描时使用）")
    ap.add_argument("--clients-seq", help="逗号分隔的并发列表，例如 '4,8,16,32'")
    ap.add_argument("--clients-start", type=int, help="范围扫描起点（含）")
    ap.add_argument("--clients-end", type=int, help="范围扫描终点（含）")
    ap.add_argument("--clients-step", type=int, help="范围扫描步长（默认 1）")

    # 运行控制
    ap.add_argument("--repeats", type=int, default=1, help="每个并发跑几次")
    ap.add_argument("--cooldown", type=float, default=2.0, help="相邻两次运行之间的冷却秒数")
    ap.add_argument("--print_output", action="store_true", help="同时打印 kbbench 原始输出")

    # 输出
    ap.add_argument("--out", default=f"kbbench_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    help="结果 CSV 文件路径")

    args = ap.parse_args()

    fieldnames = [
        "timestamp",
        "clients",
        "jobs",
        "duration_s",
        "tps_including",
        "tps_excluding",
        "latency_ms_avg",
        "tx_processed",
        "return_code",
        "error",
    ]

    ensure_header(args.out, fieldnames)

    client_list = expand_clients(args)

    for c in client_list:
        for r in range(1, args.repeats + 1):
            ts = datetime.now().isoformat(timespec="seconds")
            try:
                rc, out = run_kbbench(
                    container=args.container,
                    password=args.password,
                    host=args.host,
                    db=args.db,
                    user=args.user,
                    clients=c,
                    jobs=args.jobs,
                    duration=args.duration,
                    progress=args.progress,
                    port=args.port,
                )
            except Exception as e:
                rc = 1
                out = str(e)

            if args.print_output:
                print("\n=== RUN @", ts, f"c={c} (round {r}/{args.repeats}) ===")
                print(out)
                print("=== END RUN ===\n")

            metrics = parse_metrics(out)
            row = {
                "timestamp": ts,
                "clients": c,
                "jobs": args.jobs,
                "duration_s": args.duration,
                "tps_including": metrics["tps_including"],
                "tps_excluding": metrics["tps_excluding"],
                "latency_ms_avg": metrics["latency_ms_avg"],
                "tx_processed": metrics["tx_processed"],
                "return_code": rc,
                "error": None if rc == 0 else (out[:5000] if out else "unknown error"),
            }
            append_row(args.out, fieldnames, row)

            if (c != client_list[-1]) or (r != args.repeats):
                time.sleep(max(0.0, args.cooldown))

    print(f"结果已写入: {args.out}")


if __name__ == "__main__":
    main()
