#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Consume pending Bilibili video-understanding work with concurrent API calls."""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    from scripts_lite import bili_generate_descriptions as generator
except ModuleNotFoundError:  # Direct execution from scripts_lite/.
    import bili_generate_descriptions as generator  # type: ignore[no-redef]


def _pending_videos(
    videos: List[Dict[str, Any]],
    saved: Dict[str, Dict[str, Any]],
) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    pending = []
    for video in videos:
        video_id = str(video["video_id"])
        result = saved.get(video_id)
        if result and generator._requires_video_stage(result):
            pending.append((video, result))
    return pending


def _append_jsonl(path: Path, value: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, ensure_ascii=False) + "\n")


def _run_with_retries(
    video: Dict[str, Any],
    base_result: Dict[str, Any],
    args: argparse.Namespace,
    video_prompt: str,
    api_key: str,
) -> Dict[str, Any]:
    video_id = str(video["video_id"])
    item_dir = args.output_dir / "items" / generator._safe_id(video_id)
    result_path = item_dir / "result.json"
    last_error: Exception | None = None
    for attempt in range(1, args.max_attempts + 1):
        try:
            return generator.run_video_understanding(
                base_result,
                video,
                item_dir,
                result_path,
                args,
                video_prompt,
                api_key,
            )
        except Exception as exc:
            last_error = exc
            generator._worker_log(
                args,
                video_id,
                f"attempt {attempt}/{args.max_attempts} failed: "
                f"{type(exc).__name__}: {exc}",
            )
            if attempt == args.max_attempts:
                break
            delay = min(
                args.retry_max_seconds,
                args.retry_base_seconds * (2 ** (attempt - 1)),
            )
            generator._worker_log(args, video_id, f"retrying in {delay:.1f}s")
            time.sleep(delay)
    assert last_error is not None
    raise last_error


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run only queued video understanding, reusing downloaded videos "
            "and existing text/ASR results."
        )
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--input",
        type=Path,
        help=(
            "video metadata JSONL/CSV; default: the dataset export, then the "
            "current video-understanding queue"
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="simultaneous video uploads/model calls; default: 8",
    )
    parser.add_argument("--limit", type=int, default=0, help="0 means all pending rows")
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--retry-base-seconds", type=float, default=2.0)
    parser.add_argument("--retry-max-seconds", type=float, default=60.0)
    parser.add_argument("--video-model", default=generator.DEFAULT_VIDEO_MODEL)
    parser.add_argument("--video-prompt", type=Path, default=generator.DEFAULT_VIDEO_PROMPT)
    parser.add_argument("--video-fps", type=float, default=1.0)
    parser.add_argument(
        "--openai-base-url",
        default=os.getenv("DASHSCOPE_BASE_URL", generator.DEFAULT_OPENAI_BASE_URL),
    )
    parser.add_argument(
        "--api-base-url",
        default=os.getenv("DASHSCOPE_API_BASE_URL", generator.DEFAULT_API_BASE_URL),
    )
    parser.add_argument(
        "--download-missing",
        action="store_true",
        help="download a low-resolution copy when a queued video is not on disk",
    )
    parser.add_argument("--cookie-file", type=Path)
    parser.add_argument("--download-workers", type=int, default=2)
    parser.add_argument("--max-video-height", type=int, default=360)
    parser.add_argument(
        "--delete-video-after-success",
        action="store_true",
        help="delete the local video after its video-understanding result is saved",
    )
    return parser


def _resolve_input(output_dir: Path, input_path: Path | None) -> Path:
    if input_path:
        resolved = input_path.expanduser().resolve()
        if not resolved.is_file():
            raise FileNotFoundError(f"Input metadata not found: {resolved}")
        return resolved
    candidates = [
        output_dir.parent / "exports" / "video_metadata.jsonl",
        output_dir / "video_understanding_queue.jsonl",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        "No input metadata found; pass --input or generate video_understanding_queue.jsonl"
    )


def main() -> int:
    args = build_parser().parse_args()
    if args.workers <= 0 or args.download_workers <= 0 or args.max_attempts <= 0:
        raise ValueError("workers, download-workers, and max-attempts must be positive")
    if args.retry_base_seconds < 0 or args.retry_max_seconds < 0:
        raise ValueError("retry delays cannot be negative")
    if args.video_fps <= 0 or args.max_video_height <= 0:
        raise ValueError("video-fps and max-video-height must be positive")
    api_key = os.getenv("DASHSCOPE_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("DASHSCOPE_API_KEY is missing")

    args.output_dir = args.output_dir.expanduser().resolve()
    if not args.output_dir.is_dir():
        raise FileNotFoundError(f"Description output directory not found: {args.output_dir}")
    input_path = _resolve_input(args.output_dir, args.input)
    video_prompt = args.video_prompt.expanduser().read_text(encoding="utf-8").strip()
    if not video_prompt:
        raise ValueError("video prompt is empty")

    args.worker_logs_dir = args.output_dir / "video_worker_logs"
    args.worker_logs_dir.mkdir(parents=True, exist_ok=True)
    args.keep_video = not args.delete_video_after_success
    args.overwrite = False
    args.local_video_only = not args.download_missing
    args.video_gate = threading.BoundedSemaphore(args.workers)
    args.download_gate = threading.BoundedSemaphore(args.download_workers)
    args.cookie_jar = generator.prepare_cookie_jar(
        args.cookie_file, args.output_dir / "state"
    )

    videos = generator.load_input(input_path)
    saved = generator._load_saved_results(args.output_dir)
    pending = _pending_videos(videos, saved)
    missing = [
        (video, result)
        for video, result in pending
        if generator._find_video(
            args.output_dir / "items" / generator._safe_id(str(video["video_id"])),
            str(video["video_id"]),
        )
        is None
    ]
    missing_ids = {str(video["video_id"]) for video, _ in missing}
    missing_path = args.output_dir / "video_understanding_missing_files.jsonl"
    generator._atomic_write_rows(
        missing_path,
        (
            {
                "video_id": str(video["video_id"]),
                "video_url": str(video.get("video_url") or ""),
                "title": str(video.get("title") or ""),
            }
            for video, _ in missing
        ),
        jsonl=True,
    )
    if not args.download_missing:
        pending = [
            pair for pair in pending if str(pair[0]["video_id"]) not in missing_ids
        ]
    if args.limit > 0:
        pending = pending[: args.limit]

    print(
        f"input={len(videos)}, queued={len(_pending_videos(videos, saved))}, "
        f"local_video_missing={len(missing)}, pending={len(pending)}, "
        f"workers={args.workers}, download_missing={args.download_missing}",
        flush=True,
    )
    print(f"worker logs: {args.worker_logs_dir}/worker_*.log", flush=True)
    if missing and not args.download_missing:
        print(f"missing video list: {missing_path}", flush=True)

    failures_path = args.output_dir / "video_understanding_failures.jsonl"
    completed = 0
    failed = 0
    try:
        with ThreadPoolExecutor(
            max_workers=args.workers, thread_name_prefix="bili-video"
        ) as executor:
            futures: Dict[Future[Dict[str, Any]], Tuple[Dict[str, Any], Dict[str, Any]]] = {
                executor.submit(
                    _run_with_retries,
                    video,
                    result,
                    args,
                    video_prompt,
                    api_key,
                ): (video, result)
                for video, result in pending
            }
            for future in as_completed(futures):
                video, _ = futures[future]
                video_id = str(video["video_id"])
                try:
                    saved[video_id] = future.result()
                    completed += 1
                    print(
                        f"[{completed + failed}/{len(pending)}] ok {video_id}",
                        flush=True,
                    )
                except Exception as exc:
                    failed += 1
                    _append_jsonl(
                        failures_path,
                        {
                            "video_id": video_id,
                            "video_url": str(video.get("video_url") or ""),
                            "error": f"{type(exc).__name__}: {exc}",
                            "failed_at": int(time.time()),
                        },
                    )
                    print(
                        f"[{completed + failed}/{len(pending)}] failed {video_id}: {exc}",
                        flush=True,
                    )
    finally:
        saved = generator._load_saved_results(args.output_dir)
        generator.write_aggregate(args.output_dir, saved)

    remaining = sum(generator._requires_video_stage(item) for item in saved.values())
    print(
        f"finished: completed={completed}, failed={failed}, "
        f"remaining_video_understanding={remaining}",
        flush=True,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(f"Error: {exc}")
        raise SystemExit(1)
