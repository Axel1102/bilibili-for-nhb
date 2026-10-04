from __future__ import annotations

import csv
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts_lite"
    / "bili_generate_descriptions.py"
)
SPEC = importlib.util.spec_from_file_location(
    "bili_generate_descriptions", MODULE_PATH
)
assert SPEC and SPEC.loader
generator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(generator)


def test_load_jsonl_maps_description_and_deduplicates(tmp_path: Path) -> None:
    source = tmp_path / "videos.jsonl"
    row = {
        "video_id": "BV1",
        "bvid": "BV1",
        "video_url": "https://www.bilibili.com/video/BV1",
        "title": "标题",
        "original_description": "简介",
    }
    source.write_text(
        json.dumps(row, ensure_ascii=False)
        + "\n"
        + json.dumps(row, ensure_ascii=False)
        + "\n",
        encoding="utf-8",
    )
    rows = generator.load_input(source)
    assert len(rows) == 1
    assert rows[0]["description"] == "简介"


def test_load_csv(tmp_path: Path) -> None:
    source = tmp_path / "videos.csv"
    with source.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "video_id",
                "video_url",
                "title",
                "original_description",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "video_id": "BV2",
                "video_url": "https://www.bilibili.com/video/BV2",
                "title": "标题2",
                "original_description": "简介2",
            }
        )
    assert generator.load_input(source)[0]["video_id"] == "BV2"


def test_prepare_cookie_jar_converts_raw_header(tmp_path: Path) -> None:
    source = tmp_path / "bilibili.cookie"
    source.write_text("SESSDATA=secret=value; bili_jct=csrf", encoding="utf-8")
    jar = generator.prepare_cookie_jar(source, tmp_path / "state")
    assert jar is not None
    saved = jar.read_text(encoding="utf-8")
    assert saved.startswith("# Netscape HTTP Cookie File")
    assert "SESSDATA\tsecret=value" in saved
    assert jar.stat().st_mode & 0o777 == 0o600


def test_audio_command_uses_cookie_jar_and_full_audio(tmp_path: Path) -> None:
    command = generator.build_audio_command(
        {
            "video_id": "BV1",
            "video_url": "https://www.bilibili.com/video/BV1",
        },
        tmp_path,
        tmp_path / "cookies.txt",
        "/tmp/ffmpeg",
    )
    assert "ba[ext=m4a]/ba/b" in command
    assert "--extract-audio" in command
    assert "--cookies" in command


def test_video_command_limits_resolution(tmp_path: Path) -> None:
    command = generator.build_video_command(
        {
            "video_id": "BV1",
            "video_url": "https://www.bilibili.com/video/BV1",
        },
        tmp_path,
        tmp_path / "cookies.txt",
        "/tmp/ffmpeg",
        360,
    )
    selector = command[command.index("--format") + 1]
    assert "height<=360" in selector
    assert "--merge-output-format" in command
    assert "--cookies" in command


def test_validate_model_result_requires_reason() -> None:
    with pytest.raises(ValueError, match="reason"):
        generator.validate_model_result(
            {
                "description": "信息不足",
                "needs_video_understanding": True,
                "insufficient_information_reason": "",
            }
        )


def test_write_aggregate_has_generated_description(tmp_path: Path) -> None:
    result = {
        "video_id": "BV1",
        "generated_description": "生成内容",
        "status": "ok",
        "asr": {"model": "asr", "error": ""},
        "description_model": {"model": "text"},
    }
    generator.write_aggregate(tmp_path, {"BV1": result})
    saved = json.loads((tmp_path / "results.jsonl").read_text(encoding="utf-8"))
    assert saved["generated_description"] == "生成内容"
    with (tmp_path / "results.csv").open(
        encoding="utf-8-sig", newline=""
    ) as handle:
        assert list(csv.DictReader(handle))[0]["generated_description"] == "生成内容"


def test_summary_does_not_duplicate_asr_transcript(tmp_path: Path) -> None:
    result = {
        "video_id": "BV1",
        "generated_description": "生成内容",
        "status": "ok",
        "asr": {"model": "asr", "transcript": "很长的转写"},
    }
    generator.write_aggregate(tmp_path, {"BV1": result})
    assert "很长的转写" not in (tmp_path / "results.jsonl").read_text(
        encoding="utf-8"
    )


def test_asr_failure_forces_video_understanding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        generator,
        "download_audio",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("ASR source failed")),
    )
    monkeypatch.setattr(
        generator,
        "generate_description",
        lambda *args, **kwargs: (
            {
                "description": "仅根据标题形成的临时描述",
                "needs_video_understanding": False,
                "insufficient_information_reason": "",
            },
            {},
            [],
        ),
    )
    called = []

    def fake_video_stage(base_result, *args, **kwargs):
        called.append(base_result)
        return {**base_result, "video_understanding_completed": True}

    monkeypatch.setattr(generator, "run_video_understanding", fake_video_stage)
    args = SimpleNamespace(
        output_dir=tmp_path,
        overwrite=False,
        retry_partial=False,
        cookie_jar=None,
        download_gate=None,
        keep_audio=False,
        openai_base_url="https://example.invalid",
        text_model="text",
        api_base_url="https://example.invalid",
        asr_model="asr",
        poll_seconds=1,
        asr_timeout_seconds=1,
    )
    result = generator.process_video(
        {
            "video_id": "BV1",
            "video_url": "https://www.bilibili.com/video/BV1",
            "title": "标题",
            "description": "",
        },
        args,
        "text prompt",
        "video prompt",
        "api-key",
    )
    assert called
    assert called[0]["needs_video_understanding"] is True
    assert result["video_understanding_completed"] is True
