from __future__ import annotations

import csv
import importlib.util
import json
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts_lite" / "bili_export_video_metadata.py"
SPEC = importlib.util.spec_from_file_location("bili_export_video_metadata", MODULE_PATH)
assert SPEC and SPEC.loader
exporter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(exporter)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def _make_video(root: Path, bvid: str, complete: bool) -> Path:
    video_dir = root / "batches/batch_001/creators/1_creator/videos" / bvid
    _write_json(
        video_dir / "detail.json",
        {
            "bvid": bvid,
            "aid": 123,
            "creator_id": "1",
            "creator_name": "UP主",
            "title": "标题",
            "description": "原始简介\n第二行",
            "publish_time": 100,
            "duration": 60,
        },
    )
    if complete:
        _write_json(video_dir / "video.json", {"bvid": bvid})
        _write_json(
            video_dir / "complete.json",
            {
                "comments_completed": True,
                "subcomments_completed": True,
                "danmaku_completed": True,
                "completed_at": 200,
            },
        )
    return video_dir


def test_collects_detail_before_comments_are_complete(tmp_path: Path) -> None:
    _make_video(tmp_path, "BV1", complete=False)
    rows, unreadable = exporter.collect_video_metadata(tmp_path)
    assert unreadable == 0
    assert len(rows) == 1
    assert rows[0]["video_url"] == "https://www.bilibili.com/video/BV1"
    assert rows[0]["title"] == "标题"
    assert rows[0]["original_description"] == "原始简介\n第二行"
    assert rows[0]["generated_description"] == ""
    assert rows[0]["fully_completed"] is False


def test_completed_only_and_csv_output(tmp_path: Path) -> None:
    _make_video(tmp_path, "BV1", complete=False)
    _make_video(tmp_path, "BV2", complete=True)
    rows, _ = exporter.collect_video_metadata(tmp_path, completed_only=True)
    assert [row["video_id"] for row in rows] == ["BV2"]
    output = tmp_path / "exports/videos.csv"
    exporter.write_csv(output, rows)
    with output.open(encoding="utf-8-sig", newline="") as handle:
        saved = list(csv.DictReader(handle))
    assert saved[0]["video_id"] == "BV2"
    assert saved[0]["generated_description"] == ""
