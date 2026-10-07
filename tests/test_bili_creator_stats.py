from __future__ import annotations

import csv
import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts_lite" / "bili_creator_stats.py"
SPEC = importlib.util.spec_from_file_location("bili_creator_stats", MODULE_PATH)
assert SPEC and SPEC.loader
stats = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(stats)


def test_creator_csv_contains_username_and_progress_flags(tmp_path: Path) -> None:
    path = tmp_path / "creator_progress.csv"
    stats._atomic_write_creator_csv(
        path,
        [
            {
                "creator_id": "123",
                "creator_name": "测试博主",
                "batch_index": 2,
                "state": "crawl_in_progress",
                "catalog_complete": True,
                "fully_done": False,
                "catalog_video_count": 10,
                "completed_video_count": 4,
                "progress_percent": 40.0,
            }
        ],
    )
    with path.open(encoding="utf-8-sig", newline="") as handle:
        row = list(csv.DictReader(handle))[0]
    assert row["creator_name"] == "测试博主"
    assert row["started"] == "1"
    assert row["catalog_complete"] == "1"
    assert row["fully_done"] == "0"
    assert row["remaining_video_count"] == "6"

