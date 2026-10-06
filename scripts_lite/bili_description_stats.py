#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Summarize Bilibili ASR and description-generation progress."""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Set


AUDIO_SUFFIXES = {".m4a", ".mp3", ".wav", ".flac", ".aac", ".ogg", ".opus"}


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _input_video_ids(path: Path | None) -> Set[str]:
    if path is None or not path.is_file():
        return set()
    rows = []
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
    else:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, dict):
                    rows.append(value)
    return {
        str(row.get("bvid") or row.get("video_id") or row.get("aid") or "").strip()
        for row in rows
        if str(row.get("bvid") or row.get("video_id") or row.get("aid") or "").strip()
    }


def _failure_counts(path: Path) -> tuple[int, int]:
    events = 0
    video_ids: Set[str] = set()
    if not path.is_file():
        return events, 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            events += 1
            if isinstance(value, dict) and value.get("video_id"):
                video_ids.add(str(value["video_id"]))
    return events, len(video_ids)


def collect_stats(output_dir: Path, input_path: Path | None = None) -> Dict[str, Any]:
    items_dir = output_dir / "items"
    input_ids = _input_video_ids(input_path)
    item_dirs = sorted(path for path in items_dir.glob("*") if path.is_dir())
    result_ids: Set[str] = set()
    asr_ids: Set[str] = set()
    transcript_ids: Set[str] = set()
    description_ids: Set[str] = set()
    ok_description_ids: Set[str] = set()
    audio_on_disk_ids: Set[str] = set()
    audio_file_count = 0
    queued_video_ids: Set[str] = set()
    unreadable_asr_count = 0
    unreadable_result_count = 0
    statuses: Counter[str] = Counter()

    for item_dir in item_dirs:
        fallback_id = item_dir.name
        asr_path = item_dir / "asr.json"
        result_path = item_dir / "result.json"
        asr = _read_json(asr_path)
        result = _read_json(result_path)
        video_id = str(result.get("video_id") or fallback_id)

        audio_files = [
            path
            for path in item_dir.iterdir()
            if path.is_file() and path.suffix.lower() in AUDIO_SUFFIXES
        ]
        if audio_files:
            audio_on_disk_ids.add(video_id)
            audio_file_count += len(audio_files)

        if asr_path.exists():
            if asr:
                asr_ids.add(video_id)
                if str(asr.get("transcript") or "").strip():
                    transcript_ids.add(video_id)
            else:
                unreadable_asr_count += 1

        if result_path.exists():
            if result:
                result_ids.add(video_id)
                status = str(result.get("status") or "unknown")
                statuses[status] += 1
                description = str(result.get("generated_description") or "").strip()
                if description:
                    description_ids.add(video_id)
                    if status == "ok":
                        ok_description_ids.add(video_id)
                if bool(result.get("needs_video_understanding")) and not bool(
                    result.get("video_understanding_completed")
                ):
                    queued_video_ids.add(video_id)
            else:
                unreadable_result_count += 1

    audio_ever_ids = asr_ids | audio_on_disk_ids
    processed_ids = result_ids | asr_ids | audio_on_disk_ids
    failure_events, failure_video_count = _failure_counts(
        output_dir / "failures.jsonl"
    )
    report: Dict[str, Any] = {
        "generated_at": int(time.time()),
        "output_dir": str(output_dir),
        "input_path": str(input_path) if input_path else "",
        "input_video_count": len(input_ids) if input_path else None,
        "item_directory_count": len(item_dirs),
        "videos_with_any_artifact": len(processed_ids),
        "result_file_count": len(result_ids),
        "result_status_counts": dict(sorted(statuses.items())),
        "asr_result_count": len(asr_ids),
        "asr_nonempty_transcript_count": len(transcript_ids),
        "description_count": len(description_ids),
        "ok_description_count": len(ok_description_ids),
        "asr_without_description_count": len(asr_ids - description_ids),
        "nonempty_asr_without_description_count": len(
            transcript_ids - description_ids
        ),
        "audio_currently_on_disk_video_count": len(audio_on_disk_ids),
        "audio_currently_on_disk_file_count": audio_file_count,
        "audio_ever_downloaded_confirmed_count": len(audio_ever_ids),
        "video_understanding_queue_count": len(queued_video_ids),
        "historical_failure_event_count": failure_events,
        "historical_failed_video_count": failure_video_count,
        "unreadable_asr_file_count": unreadable_asr_count,
        "unreadable_result_file_count": unreadable_result_count,
    }
    if input_path:
        report.update(
            {
                "input_without_any_artifact_count": len(input_ids - processed_ids),
                "input_without_asr_count": len(input_ids - asr_ids),
                "input_without_description_count": len(input_ids - description_ids),
            }
        )
    report["definitions"] = {
        "result_file": "readable items/<video_id>/result.json",
        "asr_result": "readable items/<video_id>/asr.json, including an empty transcript",
        "description": "non-empty generated_description in result.json",
        "audio_ever_downloaded_confirmed": "ASR result exists or an audio file is still on disk",
        "asr_without_description": "ASR result exists but generated_description is absent",
    }
    return report


def _atomic_write_json(path: Path, value: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _print_report(report: Dict[str, Any]) -> None:
    input_count = report["input_video_count"]
    if input_count is not None:
        print(f"输入视频总数: {input_count}")
    print(f"已有任意处理产物: {report['videos_with_any_artifact']}")
    print(f"已有 result.json: {report['result_file_count']}")
    print(f"已有 ASR 结果: {report['asr_result_count']}")
    print(f"其中 ASR 转写非空: {report['asr_nonempty_transcript_count']}")
    print(f"已有 description: {report['description_count']}")
    print(f"其中 status=ok: {report['ok_description_count']}")
    print(f"已有 ASR 但没有 description: {report['asr_without_description_count']}")
    print(
        "其中非空 ASR 但没有 description: "
        f"{report['nonempty_asr_without_description_count']}"
    )
    print(
        "当前磁盘保留音频: "
        f"{report['audio_currently_on_disk_video_count']} 个视频 / "
        f"{report['audio_currently_on_disk_file_count']} 个文件"
    )
    print(
        "可以确认曾下载音频: "
        f"{report['audio_ever_downloaded_confirmed_count']}"
    )
    print(f"待视频理解: {report['video_understanding_queue_count']}")
    print(f"result 状态: {report['result_status_counts']}")
    if input_count is not None:
        print(f"尚无任何产物: {report['input_without_any_artifact_count']}")
        print(f"尚无 ASR: {report['input_without_asr_count']}")
        print(f"尚无 description: {report['input_without_description_count']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Count Bilibili audio, ASR, and description results."
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--input",
        type=Path,
        help="video_metadata.jsonl or CSV; auto-detected from the dataset when omitted",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        help="default: <output-dir>/description_stats.json",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    output_dir = args.output_dir.expanduser().resolve()
    if not output_dir.is_dir():
        raise FileNotFoundError(f"Description output directory not found: {output_dir}")
    input_path = args.input.expanduser().resolve() if args.input else None
    if input_path is None:
        candidate = output_dir.parent / "exports" / "video_metadata.jsonl"
        if candidate.is_file():
            input_path = candidate
    if input_path is not None and not input_path.is_file():
        raise FileNotFoundError(f"Input metadata not found: {input_path}")
    json_output = (
        args.json_output.expanduser().resolve()
        if args.json_output
        else output_dir / "description_stats.json"
    )
    report = collect_stats(output_dir, input_path)
    _atomic_write_json(json_output, report)
    _print_report(report)
    print(f"统计 JSON: {json_output}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, ValueError) as exc:
        print(f"Error: {exc}")
        raise SystemExit(1)
