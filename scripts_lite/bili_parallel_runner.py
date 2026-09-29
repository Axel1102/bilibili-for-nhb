#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Launch isolated Bilibili crawler worker processes."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import IO, List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE = PROJECT_ROOT / "scripts_lite" / "bili_creator_pipeline.py"


def validate_worker_count(worker_count: int) -> None:
    if worker_count < 2:
        raise ValueError("workers must be at least 2")


def build_worker_command(args: argparse.Namespace, worker_index: int) -> List[str]:
    command = [
        sys.executable,
        "-u",
        str(PIPELINE),
        "crawl",
        "--output-root",
        str(args.output_root),
        "--batch-index",
        str(args.batch_index),
        "--server-headless",
        "--cookie-file",
        str(args.cookie_file),
        "--browser-profile-dir",
        str(args.output_root / "state" / "browser_profiles" / f"worker_{worker_index:02d}"),
        "--shard-count",
        str(args.workers),
        "--shard-index",
        str(worker_index),
        "--min-sleep",
        str(args.min_sleep),
        "--max-sleep",
        str(args.max_sleep),
        "--retries",
        str(args.retries),
    ]
    if args.no_danmaku:
        command.append("--no-danmaku")
    if args.skip_comments:
        command.append("--skip-comments")
    if args.skip_subcomments:
        command.append("--skip-subcomments")
    return command


def terminate_workers(workers: List[Tuple[int, subprocess.Popen[bytes], IO[bytes], Path]]) -> None:
    for _, process, _, _ in workers:
        if process.poll() is None:
            process.terminate()
    deadline = time.time() + 20
    for _, process, _, _ in workers:
        if process.poll() is None:
            try:
                process.wait(timeout=max(0.1, deadline - time.time()))
            except subprocess.TimeoutExpired:
                process.kill()


def run(args: argparse.Namespace) -> int:
    args.output_root = args.output_root.expanduser().resolve()
    args.cookie_file = args.cookie_file.expanduser().resolve()
    validate_worker_count(args.workers)
    if args.workers > 4:
        print(
            f"Warning: starting {args.workers} workers increases resource use and "
            "Bilibili rate-limit risk.",
            flush=True,
        )
    if args.min_sleep < 0 or args.max_sleep < args.min_sleep:
        raise ValueError("sleep range is invalid")
    if not args.cookie_file.is_file():
        raise FileNotFoundError(f"Cookie file not found: {args.cookie_file}")
    selected = args.output_root / "inputs" / "creators_selected.csv"
    if not selected.is_file():
        raise FileNotFoundError(f"Prepared creator list not found: {selected}")

    logs_dir = args.output_root / "state" / "parallel_logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    workers: List[Tuple[int, subprocess.Popen[bytes], IO[bytes], Path]] = []
    environment = {**os.environ, "PYTHONUNBUFFERED": "1"}
    stop_signal: List[int] = []

    def request_stop(signum: int, _frame: object) -> None:
        if not stop_signal:
            stop_signal.append(signum)

    previous_sigterm = signal.signal(signal.SIGTERM, request_stop)
    previous_sigint = signal.signal(signal.SIGINT, request_stop)
    try:
        for worker_index in range(1, args.workers + 1):
            log_path = logs_dir / f"worker_{worker_index:02d}.log"
            log_handle = log_path.open("ab", buffering=0)
            log_handle.write(
                f"\n=== parallel run {int(time.time())}, worker {worker_index}/{args.workers} ===\n".encode()
            )
            command = build_worker_command(args, worker_index)
            process = subprocess.Popen(
                command,
                cwd=PROJECT_ROOT,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                env=environment,
            )
            workers.append((worker_index, process, log_handle, log_path))
            print(
                f"worker {worker_index}/{args.workers} started: pid={process.pid}, log={log_path}",
                flush=True,
            )

        print(f"monitor: tail -f {logs_dir}/worker_*.log", flush=True)
        remaining = {worker_index for worker_index, _, _, _ in workers}
        failure = False
        while remaining:
            if stop_signal:
                print("Stopping parallel workers...", flush=True)
                terminate_workers(workers)
                return 128 + stop_signal[0]
            for worker_index, process, _, log_path in workers:
                if worker_index not in remaining:
                    continue
                exit_code = process.poll()
                if exit_code is None:
                    continue
                remaining.remove(worker_index)
                print(
                    f"worker {worker_index} exited with code {exit_code}; log={log_path}",
                    flush=True,
                )
                if exit_code != 0:
                    failure = True
            if failure:
                print("A worker failed; stopping the remaining workers. Rerun to resume.", flush=True)
                terminate_workers(workers)
                return 1
            if remaining:
                time.sleep(1)
        return 0
    except (KeyboardInterrupt, SystemExit):
        print("Stopping parallel workers...", flush=True)
        terminate_workers(workers)
        return 130
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
        signal.signal(signal.SIGINT, previous_sigint)
        for _, _, log_handle, _ in workers:
            log_handle.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run isolated Bilibili crawler processes in parallel.")
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--cookie-file", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=3, help="at least 2; default: 3")
    parser.add_argument("--batch-index", type=int, default=0, help="0 means all batches")
    parser.add_argument("--min-sleep", type=float, default=3.0)
    parser.add_argument("--max-sleep", type=float, default=6.0)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--no-danmaku", action="store_true")
    parser.add_argument("--skip-comments", action="store_true")
    parser.add_argument("--skip-subcomments", action="store_true")
    return parser


def main() -> int:
    try:
        return run(build_parser().parse_args())
    except Exception as exc:
        print(f"Cannot start parallel crawler: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
