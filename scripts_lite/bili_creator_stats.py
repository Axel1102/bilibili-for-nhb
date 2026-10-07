#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Report progress and usable content for the Bilibili creator dataset."""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATASET_ROOT = PROJECT_ROOT / "data" / "bili" / "creator_dataset"
CREATOR_CSV_FIELDS = [
    "creator_id",
    "creator_name",
    "batch_index",
    "state",
    "started",
    "catalog_complete",
    "fully_done",
    "catalog_video_count",
    "completed_video_count",
    "remaining_video_count",
    "progress_percent",
    "videos_with_description",
    "videos_with_comments",
    "videos_with_sub_comments",
    "videos_with_danmaku",
    "comment_count",
    "sub_comment_count",
    "danmaku_count",
    "historical_error_attempts",
    "last_video_completed_at",
]


def _read_json(path: Path, default: Any = None) -> Any:
    if not path.exists() or path.stat().st_size == 0:
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                rows.append(value)
    return rows


def _read_creators(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Prepared creator list not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def _atomic_write_creator_csv(path: Path, creators: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=CREATOR_CSV_FIELDS, extrasaction="ignore"
        )
        writer.writeheader()
        for creator in creators:
            catalog_count = int(creator.get("catalog_video_count") or 0)
            completed_count = int(creator.get("completed_video_count") or 0)
            writer.writerow(
                {
                    **creator,
                    "started": int(creator.get("state") != "not_started"),
                    "catalog_complete": int(bool(creator.get("catalog_complete"))),
                    "fully_done": int(bool(creator.get("fully_done"))),
                    "remaining_video_count": max(
                        catalog_count - completed_count, 0
                    ),
                }
            )
    os.replace(temporary, path)


def _video_key(video: Dict[str, Any]) -> str:
    return str(video.get("bvid") or video.get("aid") or "").strip()


def _catalog_video_keys(path: Path) -> List[str]:
    keys: List[str] = []
    seen: Set[str] = set()
    for video in _read_jsonl(path):
        key = _video_key(video)
        if key and key not in seen:
            seen.add(key)
            keys.append(key)
    return keys


def _has_description(detail: Dict[str, Any]) -> bool:
    value = str(detail.get("description") or "").strip()
    return bool(value) and value.lower() not in {"-", "--", "无", "暂无", "暂无简介", "none", "null"}


def _find_creator_dir(dataset_root: Path, row: Dict[str, str]) -> Optional[Path]:
    batch_index = int(row.get("batch_index") or 1)
    creator_id = str(row.get("author_id") or "").strip()
    base = dataset_root / "batches" / f"batch_{batch_index:03d}" / "creators"
    candidates = sorted(path for path in base.glob(f"{creator_id}_*") if path.is_dir())
    return candidates[0] if candidates else None


def _load_error_counts(dataset_root: Path) -> Tuple[Counter, Dict[str, Set[str]], int]:
    attempts: Counter = Counter()
    video_ids: Dict[str, Set[str]] = defaultdict(set)
    total = 0
    for path in [
        dataset_root / "state" / "catalog_errors.jsonl",
        dataset_root / "state" / "crawl_errors.jsonl",
    ]:
        for item in _read_jsonl(path):
            creator_id = str(item.get("creator_id") or "")
            video_id = str(item.get("video_id") or "")
            if creator_id:
                attempts[creator_id] += 1
                if video_id:
                    video_ids[creator_id].add(video_id)
            total += 1
    return attempts, video_ids, total


def _directory_size_bytes(path: Optional[Path]) -> int:
    if path is None or not path.exists():
        return 0
    total = 0
    for child in path.rglob("*"):
        try:
            if child.is_file():
                total += child.stat().st_size
        except OSError:
            continue
    return total


def _video_status(video_dir: Path) -> Dict[str, Any]:
    detail_path = video_dir / "detail.json"
    final_path = video_dir / "video.json"
    complete_path = video_dir / "complete.json"
    detail = _read_json(detail_path, {}) or {}
    complete = _read_json(complete_path, {}) or {}
    title_valid = bool(str(detail.get("title") or "").strip())
    final_exists = final_path.exists() and final_path.stat().st_size > 0
    components_complete = bool(complete) and all(
        bool(complete.get(field))
        for field in ["comments_completed", "subcomments_completed", "danmaku_completed"]
    )
    fully_complete = final_exists and title_valid and components_complete
    meaningful_description = _has_description(detail)
    comments = int(complete.get("comments") or 0)
    sub_comments = int(complete.get("sub_comments") or 0)
    danmaku = int(complete.get("danmaku") or 0)
    return {
        "detail_exists": bool(detail),
        "final_exists": final_exists,
        "components_complete": components_complete,
        "fully_complete": fully_complete,
        "valid": fully_complete,
        "has_description": fully_complete and meaningful_description,
        "comments": comments,
        "sub_comments": sub_comments,
        "danmaku": danmaku,
        "has_comments": comments > 0,
        "has_sub_comments": sub_comments > 0,
        "has_danmaku": danmaku > 0,
        "content_signal": fully_complete and (
            meaningful_description or comments > 0 or sub_comments > 0 or danmaku > 0
        ),
        "completed_at": int(complete.get("completed_at") or 0),
    }


def _creator_status(
    dataset_root: Path,
    row: Dict[str, str],
    error_attempts: Counter,
    error_video_ids: Dict[str, Set[str]],
    include_disk_size: bool,
) -> Dict[str, Any]:
    creator_id = str(row.get("author_id") or "").strip()
    creator_dir = _find_creator_dir(dataset_root, row)
    batch_index = int(row.get("batch_index") or 1)
    catalog_complete = bool(creator_dir and (creator_dir / "catalog" / "complete.json").exists())
    creator_meta = _read_json(creator_dir / "creator.json", {}) if creator_dir else {}
    creator_name = str((creator_meta or {}).get("creator_name") or row.get("author") or "")
    video_keys = _catalog_video_keys(creator_dir / "videos.jsonl") if creator_dir else []

    counts = Counter()
    unresolved_keys: List[str] = []
    last_completed_at = 0
    for key in video_keys:
        status = _video_status(creator_dir / "videos" / key)
        for field in [
            "detail_exists",
            "final_exists",
            "components_complete",
            "fully_complete",
            "valid",
            "has_description",
            "has_comments",
            "has_sub_comments",
            "has_danmaku",
            "content_signal",
        ]:
            counts[field] += int(bool(status[field]))
        counts["comments"] += status["comments"]
        counts["sub_comments"] += status["sub_comments"]
        counts["danmaku"] += status["danmaku"]
        last_completed_at = max(last_completed_at, status["completed_at"])
        if not status["fully_complete"]:
            unresolved_keys.append(key)

    catalog_count = len(video_keys)
    completed_count = counts["fully_complete"]
    if not creator_dir:
        state = "not_started"
    elif not catalog_complete:
        state = "catalog_in_progress"
    elif catalog_count == 0:
        state = "done_no_videos"
    elif completed_count == catalog_count:
        state = "done"
    elif completed_count == 0:
        state = "catalog_done_waiting"
    else:
        state = "crawl_in_progress"

    progress_percent = 100.0 if catalog_complete and catalog_count == 0 else (
        round(completed_count * 100.0 / catalog_count, 1) if catalog_count else 0.0
    )
    historical_error_videos = error_video_ids.get(creator_id, set())
    unresolved_error_videos = sorted(set(unresolved_keys) & historical_error_videos)
    return {
        "batch_index": batch_index,
        "creator_id": creator_id,
        "creator_name": creator_name,
        "state": state,
        "fully_done": state in {"done", "done_no_videos"},
        "catalog_complete": catalog_complete,
        "catalog_video_count": catalog_count,
        "detail_video_count": counts["detail_exists"],
        "final_video_count": counts["final_exists"],
        "completed_video_count": completed_count,
        "valid_video_count": counts["valid"],
        "videos_with_description": counts["has_description"],
        "videos_with_comments": counts["has_comments"],
        "videos_with_sub_comments": counts["has_sub_comments"],
        "videos_with_danmaku": counts["has_danmaku"],
        "videos_with_content_signal": counts["content_signal"],
        "comment_count": counts["comments"],
        "sub_comment_count": counts["sub_comments"],
        "danmaku_count": counts["danmaku"],
        "unresolved_video_count": len(unresolved_keys),
        "unresolved_video_ids": unresolved_keys,
        "progress_percent": progress_percent,
        "historical_error_attempts": int(error_attempts.get(creator_id, 0)),
        "unresolved_videos_with_historical_errors": unresolved_error_videos,
        "last_video_completed_at": last_completed_at,
        "disk_bytes": _directory_size_bytes(creator_dir) if include_disk_size else None,
    }


def build_report(dataset_root: Path, include_disk_size: bool = True) -> Dict[str, Any]:
    creator_rows = _read_creators(dataset_root / "inputs" / "creators_selected.csv")
    error_attempts, error_video_ids, total_error_attempts = _load_error_counts(dataset_root)
    creators = [
        _creator_status(
            dataset_root,
            row,
            error_attempts,
            error_video_ids,
            include_disk_size,
        )
        for row in creator_rows
    ]
    state_counts = Counter(item["state"] for item in creators)
    batches: List[Dict[str, Any]] = []
    for batch_index in sorted({item["batch_index"] for item in creators}):
        batch = [item for item in creators if item["batch_index"] == batch_index]
        batches.append(
            {
                "batch_index": batch_index,
                "creator_count": len(batch),
                "catalog_complete_creators": sum(item["catalog_complete"] for item in batch),
                "fully_done_creators": sum(item["fully_done"] for item in batch),
                "catalog_video_count": sum(item["catalog_video_count"] for item in batch),
                "completed_video_count": sum(item["completed_video_count"] for item in batch),
                "unresolved_video_count": sum(item["unresolved_video_count"] for item in batch),
            }
        )

    summary = {
        "selected_creator_count": len(creators),
        "catalog_complete_creator_count": sum(item["catalog_complete"] for item in creators),
        "fully_done_creator_count": sum(item["fully_done"] for item in creators),
        "creator_state_counts": dict(state_counts),
        "catalog_video_count": sum(item["catalog_video_count"] for item in creators),
        "completed_video_count": sum(item["completed_video_count"] for item in creators),
        "valid_video_count": sum(item["valid_video_count"] for item in creators),
        "videos_with_description": sum(item["videos_with_description"] for item in creators),
        "videos_with_comments": sum(item["videos_with_comments"] for item in creators),
        "videos_with_sub_comments": sum(item["videos_with_sub_comments"] for item in creators),
        "videos_with_danmaku": sum(item["videos_with_danmaku"] for item in creators),
        "videos_with_content_signal": sum(item["videos_with_content_signal"] for item in creators),
        "comment_count": sum(item["comment_count"] for item in creators),
        "sub_comment_count": sum(item["sub_comment_count"] for item in creators),
        "danmaku_count": sum(item["danmaku_count"] for item in creators),
        "unresolved_video_count": sum(item["unresolved_video_count"] for item in creators),
        "historical_error_attempts": total_error_attempts,
        "disk_bytes": sum(item["disk_bytes"] or 0 for item in creators) if include_disk_size else None,
    }
    total_videos = summary["catalog_video_count"]
    summary["video_completion_percent"] = (
        round(summary["completed_video_count"] * 100.0 / total_videos, 1)
        if total_videos
        else 0.0
    )
    return {
        "generated_at": int(time.time()),
        "dataset_root": str(dataset_root),
        "definitions": {
            "fully_done_creator": "catalog complete and every catalog video has video.json plus completed comments, sub-comments, and danmaku flags",
            "valid_video": "fully completed video with readable detail and a non-empty title",
            "content_signal": "valid video with a meaningful description or at least one comment, sub-comment, or danmaku",
        },
        "summary": summary,
        "batches": batches,
        "creators": creators,
    }


def _human_bytes(value: Optional[int]) -> str:
    if value is None:
        return "not scanned"
    size = float(value)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _print_report(report: Dict[str, Any], show: str, limit: int) -> None:
    summary = report["summary"]
    print("Bilibili creator crawl progress")
    print(f"Dataset: {report['dataset_root']}")
    print(
        "Creators: "
        f"selected {summary['selected_creator_count']}, "
        f"catalog complete {summary['catalog_complete_creator_count']}, "
        f"fully done {summary['fully_done_creator_count']}"
    )
    print(f"Creator states: {json.dumps(summary['creator_state_counts'], ensure_ascii=False)}")
    print(
        "Videos: "
        f"cataloged {summary['catalog_video_count']}, "
        f"completed {summary['completed_video_count']} "
        f"({summary['video_completion_percent']:.1f}%), "
        f"valid {summary['valid_video_count']}, "
        f"unresolved {summary['unresolved_video_count']}"
    )
    print(
        "Content: "
        f"meaningful descriptions {summary['videos_with_description']} videos, "
        f"content signals {summary['videos_with_content_signal']} videos"
    )
    print(
        "Interactions: "
        f"comments {summary['comment_count']} in {summary['videos_with_comments']} videos, "
        f"sub-comments {summary['sub_comment_count']} in {summary['videos_with_sub_comments']} videos, "
        f"danmaku {summary['danmaku_count']} in {summary['videos_with_danmaku']} videos"
    )
    print(
        f"Historical error attempts: {summary['historical_error_attempts']} "
        "(resolved retries remain in this count)"
    )
    print(f"Creator data size: {_human_bytes(summary['disk_bytes'])}")

    print("\nBatches:")
    for batch in report["batches"]:
        total = batch["catalog_video_count"]
        completed = batch["completed_video_count"]
        percent = round(completed * 100.0 / total, 1) if total else 0.0
        print(
            f"  batch {batch['batch_index']:03d}: creators done "
            f"{batch['fully_done_creators']}/{batch['creator_count']}, "
            f"videos {completed}/{total} ({percent:.1f}%), "
            f"unresolved {batch['unresolved_video_count']}"
        )

    if show == "summary":
        return
    if show == "done":
        selected = [item for item in report["creators"] if item["fully_done"]]
        title = "Fully completed creators"
    elif show == "unfinished":
        selected = [item for item in report["creators"] if not item["fully_done"]]
        title = "Unfinished creators"
    else:
        selected = list(report["creators"])
        title = "All creators"

    print(f"\n{title} ({len(selected)}):")
    visible = selected if limit <= 0 else selected[:limit]
    for item in visible:
        print(
            f"  [batch {item['batch_index']:03d}] {item['creator_id']} "
            f"{item['creator_name']} | {item['state']} | "
            f"videos {item['completed_video_count']}/{item['catalog_video_count']} "
            f"({item['progress_percent']:.1f}%) | "
            f"comments {item['comment_count']} + sub {item['sub_comment_count']} | "
            f"danmaku {item['danmaku_count']}"
        )
    if len(visible) < len(selected):
        print(f"  ... {len(selected) - len(visible)} more; use --limit 0 or inspect the JSON report")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize Bilibili creator crawl progress.")
    parser.add_argument("--dataset-root", type=Path, default=DEFAULT_DATASET_ROOT)
    parser.add_argument("--show", choices=["summary", "done", "unfinished", "all"], default="done")
    parser.add_argument("--limit", type=int, default=100, help="creator rows printed; 0 means no limit")
    parser.add_argument(
        "--json-output",
        type=Path,
        default=None,
        help="report path (default: <dataset-root>/reports/progress_report.json)",
    )
    parser.add_argument(
        "--creator-csv",
        type=Path,
        default=None,
        help="creator list path (default: <dataset-root>/reports/creator_progress.csv)",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="print only; do not write JSON or creator CSV reports",
    )
    parser.add_argument("--skip-disk-size", action="store_true", help="skip recursive disk usage scan")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    dataset_root = args.dataset_root.expanduser().resolve()
    try:
        report = build_report(dataset_root, include_disk_size=not args.skip_disk_size)
    except (FileNotFoundError, ValueError) as exc:
        print(f"Cannot build report: {exc}")
        return 1
    _print_report(report, args.show, args.limit)
    if not args.no_write:
        output_path = (
            args.json_output.expanduser().resolve()
            if args.json_output
            else dataset_root / "reports" / "progress_report.json"
        )
        _atomic_write_json(output_path, report)
        print(f"\nJSON report: {output_path}")
        creator_csv_path = (
            args.creator_csv.expanduser().resolve()
            if args.creator_csv
            else dataset_root / "reports" / "creator_progress.csv"
        )
        _atomic_write_creator_csv(creator_csv_path, report["creators"])
        print(f"Creator CSV: {creator_csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
