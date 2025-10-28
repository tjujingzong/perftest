#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import shlex
import re
import sys
import argparse
from typing import Optional, Tuple

TPS_INC_RE = re.compile(r"tps\s*=\s*([0-9.]+)\s*\(including", re.IGNORECASE)
TPS_EXC_RE = re.compile(r"tps\s*=\s*([0-9.]+)\s*\(excluding", re.IGNORECASE)

def parse_tps(text: str) -> Tuple[Optional[float], Optional[float]]:
    inc = exc = None
    m = TPS_INC_RE.search(text)
    if m:
        inc = float(m.group(1))
    m = TPS_EXC_RE.search(text)
    if m:
        exc = float(m.group(1))
    return inc, exc

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
) -> Tuple[Optional[float], Optional[float], str]:
    """
    在容器内：
      1) 写 ~/.pgpass（libpq 读取；支持 * 通配）
      2) 如有 sys_encpwd，则配置 ~/.encpwd（Kingbase 免密）
      3) 通过 PGPASSWORD / KINGBASE_PASSWORD 环境变量兜底
      4) 执行 kbbench 并解析 TPS
    返回: (tps_including, tps_excluding, raw_output)
    """
    # 组合 kbbench 命令
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

    # ~/.pgpass 一行: host:port:db:user:password
    # 若端口未知，用 '*'，libpq 支持通配。
    pgpass_line = f"{host}:{port if port is not None else '*'}:{db}:{user}:{password}"

    # 在容器里执行的脚本：写密钥文件 -> 可选生成 .encpwd -> 跑 kbbench
    bash_script = f"""
        set -euo pipefail
        umask 077

        # 写 ~/.pgpass（PostgreSQL/Kingbase 的 libpq 路径）
        printf "%s\\n" "$PGPASSLINE" > "$HOME/.pgpass"
        chmod 600 "$HOME/.pgpass"

        # 如存在 Kingbase 的 sys_encpwd，则同时写 ~/.encpwd（免密）
        if command -v sys_encpwd >/dev/null 2>&1; then
            if [ -n "{shlex.quote(host)}" ]; then HOPT="-H {shlex.quote(host)}"; else HOPT=""; fi
            if [ -n "{'' if port is None else str(port)}" ]; then POPT="-P {port}"; else POPT=""; fi
            sys_encpwd $HOPT $POPT -D {shlex.quote(db)} -U {shlex.quote(user)} -W {shlex.quote(password)} >/dev/null 2>&1 || true
        fi

        # 执行 kbbench
        exec {kb_cmd_str}
    """

    docker_cmd = [
        "docker", "exec",
        "-e", f"PGPASSWORD={password}",            # libpq 环境变量
        "-e", f"KINGBASE_PASSWORD={password}",     # 某些 Kingbase 客户端也读取
        "-e", f"PGPASSLINE={pgpass_line}",         # 把行内容传进容器，避免转义问题
        container,
        "bash", "-lc", bash_script,
    ]

    proc = subprocess.run(
        docker_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    output = proc.stdout

    if proc.returncode != 0:
        raise RuntimeError(
            f"kbbench 执行失败，退出码 {proc.returncode}。\n--- 输出开始 ---\n{output}\n--- 输出结束 ---"
        )

    inc, exc = parse_tps(output)
    return inc, exc, output

def main():
    ap = argparse.ArgumentParser(description="在 Docker 容器中运行 kbbench 并提取 TPS（免交互口令）")
    ap.add_argument("--container", default="kingbase", help="容器名")
    ap.add_argument("--password", default="123456", help="数据库密码")
    ap.add_argument("--host", default="127.0.0.1", help="数据库主机（容器内可见）")
    ap.add_argument("--db", default="kbbenchdb", help="数据库名")
    ap.add_argument("--user", default="system", help="用户名")
    ap.add_argument("--clients", type=int, default=8, help="-c 并发客户端数")
    ap.add_argument("--jobs", type=int, default=4, help="-j 线程数")
    ap.add_argument("--duration", type=int, default=60, help="-T 持续时间（秒）")
    ap.add_argument("--progress", type=int, default=10, help="-P 进度打印间隔（秒）")
    ap.add_argument("--port", type=int, default=None, help="-p 端口（可选，未知可不填）")
    ap.add_argument("--print-output", action="store_true", help="同时打印 kbbench 原始输出")
    args = ap.parse_args()

    try:
        inc, exc, out = run_kbbench(
            container=args.container,
            password=args.password,
            host=args.host,
            db=args.db,
            user=args.user,
            clients=args.clients,
            jobs=args.jobs,
            duration=args.duration,
            progress=args.progress,
            port=args.port,
        )
    except Exception as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

    if args.print_output:
        print("--- kbbench 原始输出 ---")
        print(out)
        print("--- 结束 ---")

    print("解析结果：")
    print(f"tps_including = {inc if inc is not None else '未找到'}")
    print(f"tps_excluding = {exc if exc is not None else '未找到'}")

if __name__ == "__main__":
    main()
