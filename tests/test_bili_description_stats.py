from __future__ import annotations

import importlib.util
import json
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts_lite" / "bili_description_stats.py"
SPEC = importlib.util.spec_from_file_location("bili_description_stats", MODULE_PATH)
assert SPEC and SPEC.loader
stats = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(stats)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def test_counts_asr_only_descriptions_and_audio(tmp_path: Path) -> None:
    output_dir = tmp_path / "descriptions"
    input_path = tmp_path / "exports" / "video_metadata.jsonl"
    input_path.parent.mkdir(parents=True)
    input_path.write_text(
        "".join(
            json.dumps(
                {
                    "video_id": f"BV{index}",
                    "video_url": f"https://www.bilibili.com/video/BV{index}",
                }
            )
            + "\n"
            for index in range(1, 5)
        ),
        encoding="utf-8",
    )

    _write_json(output_dir / "items/BV1/asr.json", {"transcript": "转写一"})
    _write_json(
        output_dir / "items/BV1/result.json",
        {"video_id": "BV1", "status": "ok", "generated_description": "描述一"},
    )
    _write_json(output_dir / "items/BV2/asr.json", {"transcript": "转写二"})
    audio = output_dir / "items/BV3/BV3.m4a"
    audio.parent.mkdir(parents=True)
    audio.write_bytes(b"audio")
    _write_json(
        output_dir / "items/BV4/result.json",
        {
            "video_id": "BV4",
            "status": "partial",
            "generated_description": "临时描述",
            "needs_video_understanding": True,
            "video_understanding_completed": False,
        },
    )

    report = stats.collect_stats(output_dir, input_path)
    assert report["input_video_count"] == 4
    assert report["result_file_count"] == 2
    assert report["asr_result_count"] == 2
    assert report["asr_nonempty_transcript_count"] == 2
    assert report["description_count"] == 2
    assert report["ok_description_count"] == 1
    assert report["asr_without_description_count"] == 1
    assert report["audio_currently_on_disk_video_count"] == 1
    assert report["audio_ever_downloaded_confirmed_count"] == 3
    assert report["video_understanding_queue_count"] == 1
    assert report["input_without_any_artifact_count"] == 0


def test_empty_asr_is_counted_separately(tmp_path: Path) -> None:
    output_dir = tmp_path / "descriptions"
    _write_json(output_dir / "items/BV1/asr.json", {"transcript": ""})
    report = stats.collect_stats(output_dir)
    assert report["asr_result_count"] == 1
    assert report["asr_nonempty_transcript_count"] == 0
    assert report["asr_without_description_count"] == 1
