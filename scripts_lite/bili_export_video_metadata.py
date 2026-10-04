#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Export crawled Bilibili video metadata for downstream description generation."""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


FIELDNAMES = [
    "video_id",
    "bvid",
    "aid",
    "creator_id",
    "creator_name",
    "video_url",
    "title",
    "original_description",
    "generated_description",
    "publish_time",
    "duration_seconds",
    "fully_completed",
    "completed_at",
]


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _is_fully_completed(video_dir: Path, complete: Dict[str, Any]) -> bool:
    return (video_dir / "video.json").is_file() and all(
        bool(complete.get(field))
        for field in ["comments_completed", "subcomments_completed", "danmaku_completed"]
    )


def _export_row(detail_path: Path) -> Dict[str, Any]:
    detail = _read_json(detail_path)
    if not detail:
        return {}
    video_dir = detail_path.parent
    complete = _read_json(video_dir / "complete.json")
    bvid = str(detail.get("bvid") or "").strip()
    aid = str(detail.get("aid") or "").strip()
    video_id = bvid or (f"av{aid}" if aid else video_dir.name)
    video_url = str(detail.get("video_url") or "").strip()
    if not video_url:
        video_url = (
            f"https://www.bilibili.com/video/{bvid}"
            if bvid
            else f"https://www.bilibili.com/video/av{aid}"
        )
    return {
        "video_id": video_id,
        "bvid": bvid,
        "aid": aid,
        "creator_id": str(detail.get("creator_id") or ""),
        "creator_name": str(detail.get("creator_name") or ""),
        "video_url": video_url,
        "title": str(detail.get("title") or ""),
        "original_description": str(detail.get("description") or ""),
        "generated_description": "",
        "publish_time": int(detail.get("publish_time") or 0),
        "duration_seconds": int(detail.get("duration") or 0),
        "fully_completed": _is_fully_completed(video_dir, complete),
        "completed_at": int(complete.get("completed_at") or 0),
    }


def collect_video_metadata(
    dataset_root: Path,
    completed_only: bool = False,
) -> Tuple[List[Dict[str, Any]], int]:
    rows_by_id: Dict[str, Dict[str, Any]] = {}
    unreadable_count = 0
    pattern = "batches/batch_*/creators/*/videos/*/detail.json"
    for detail_path in sorted(dataset_root.glob(pattern)):
        row = _export_row(detail_path)
        if not row:
            unreadable_count += 1
            continue
        if completed_only and not row["fully_completed"]:
            continue
        video_id = str(row["video_id"])
        previous = rows_by_id.get(video_id)
        if previous is None or (row["fully_completed"] and not previous["fully_completed"]):
            rows_by_id[video_id] = row
    rows = sorted(
        rows_by_id.values(),
        key=lambda item: (str(item["creator_id"]), int(item["publish_time"]), str(item["video_id"])),
    )
    return rows, unreadable_count


def _atomic_output_path(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.with_name(f".{path.name}.{os.getpid()}.tmp")


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    temporary = _atomic_output_path(path)
    with temporary.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    temporary = _atomic_output_path(path)
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    os.replace(temporary, path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export crawled video URLs, titles, and original descriptions."
    )
    parser.add_argument("--dataset-root", type=Path, required=True)
    parser.add_argument(
        "--output-prefix",
        type=Path,
        help="output path without extension (default: <dataset-root>/exports/video_metadata)",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "jsonl", "both"],
        default="both",
        help="default: both",
    )
    parser.add_argument(
        "--completed-only",
        action="store_true",
        help="only export videos whose comments, sub-comments, and danmaku are all complete",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    dataset_root = args.dataset_root.expanduser().resolve()
    if not dataset_root.is_dir():
        raise FileNotFoundError(f"Dataset root not found: {dataset_root}")
    output_prefix = (
        args.output_prefix.expanduser().resolve()
        if args.output_prefix
        else dataset_root / "exports" / "video_metadata"
    )
    rows, unreadable_count = collect_video_metadata(dataset_root, args.completed_only)
    written: List[Path] = []
    if args.format in {"csv", "both"}:
        csv_path = output_prefix.with_suffix(".csv")
        write_csv(csv_path, rows)
        written.append(csv_path)
    if args.format in {"jsonl", "both"}:
        jsonl_path = output_prefix.with_suffix(".jsonl")
        write_jsonl(jsonl_path, rows)
        written.append(jsonl_path)
    print(
        f"Exported {len(rows)} unique videos; fully completed "
        f"{sum(bool(row['fully_completed']) for row in rows)}; "
        f"unreadable detail files {unreadable_count}."
    )
    for path in written:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
