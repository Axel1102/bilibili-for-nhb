from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts_lite"
    / "bili_run_video_understanding.py"
)
SPEC = importlib.util.spec_from_file_location("bili_run_video_understanding", MODULE_PATH)
assert SPEC and SPEC.loader
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def test_pending_videos_only_selects_unfinished_video_stage() -> None:
    videos = [
        {"video_id": "BV1"},
        {"video_id": "BV2"},
        {"video_id": "BV3"},
    ]
    saved = {
        "BV1": {
            "needs_video_understanding": True,
            "video_understanding_completed": False,
        },
        "BV2": {
            "needs_video_understanding": False,
            "video_understanding_completed": True,
        },
    }
    pending = runner._pending_videos(videos, saved)
    assert [video["video_id"] for video, _ in pending] == ["BV1"]


def test_video_stage_retries_transient_failure(tmp_path: Path, monkeypatch) -> None:
    attempts = []

    def fake_run(*args, **kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("temporary API failure")
        return {"video_id": "BV1", "video_understanding_completed": True}

    monkeypatch.setattr(runner.generator, "run_video_understanding", fake_run)
    monkeypatch.setattr(runner.generator, "_worker_log", lambda *args: None)
    args = SimpleNamespace(
        output_dir=tmp_path,
        max_attempts=2,
        retry_base_seconds=0,
        retry_max_seconds=0,
    )
    result = runner._run_with_retries(
        {"video_id": "BV1"},
        {"video_id": "BV1", "needs_video_understanding": True},
        args,
        "prompt",
        "key",
    )
    assert len(attempts) == 2
    assert result["video_understanding_completed"] is True

