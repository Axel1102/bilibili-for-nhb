from __future__ import annotations

import importlib.util
import json
import zipfile
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts_lite"
    / "bili_package_descriptions.py"
)
SPEC = importlib.util.spec_from_file_location("bili_package_descriptions", MODULE_PATH)
assert SPEC and SPEC.loader
packager = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(packager)


def _write(path: Path, content: bytes = b"content") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def test_archive_includes_ignored_outputs_but_excludes_raw_data_and_secrets(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "repo"
    dataset_root = project_root / "creator_video_catalog_server_ready"
    _write(project_root / "README.md")
    _write(project_root / "run.log")
    _write(project_root / ".env", b"secret")
    _write(project_root / "user.cookie", b"secret")
    _write(project_root / ".git/config")
    _write(project_root / ".venv/bin/python")
    _write(project_root / "data/raw.json")
    _write(dataset_root / "batches/batch_001/raw.json")
    _write(dataset_root / "descriptions/items/BV1/result.json")
    _write(dataset_root / "descriptions/items/BV1/asr.json")
    _write(dataset_root / "descriptions/items/BV1/BV1.video.mp4")
    _write(dataset_root / "descriptions/items/BV1/BV1.m4a")
    _write(dataset_root / "descriptions/worker_logs/worker_01.log")
    _write(dataset_root / "exports/video_metadata.jsonl")
    _write(dataset_root / "reports/creator_progress.csv")
    _write(dataset_root / "inputs/creators_selected.csv")
    output = tmp_path / "bundle.zip"

    report = packager.build_archive(
        project_root, dataset_root, output, include_audio=False
    )

    with zipfile.ZipFile(output) as archive:
        names = set(archive.namelist())
        manifest = json.loads(archive.read("repo/PACKAGE_MANIFEST.json"))
    assert "repo/run.log" in names
    assert "repo/creator_video_catalog_server_ready/descriptions/items/BV1/result.json" in names
    assert "repo/creator_video_catalog_server_ready/descriptions/worker_logs/worker_01.log" in names
    assert "repo/creator_video_catalog_server_ready/exports/video_metadata.jsonl" in names
    assert "repo/creator_video_catalog_server_ready/reports/creator_progress.csv" in names
    assert "repo/creator_video_catalog_server_ready/inputs/creators_selected.csv" in names
    assert not any(name.endswith((".mp4", ".m4a", ".cookie")) for name in names)
    assert not any("/batches/" in name or "/data/" in name for name in names)
    assert not any("/.git/" in name or "/.venv/" in name for name in names)
    assert "repo/.env" not in names
    assert report["file_count"] == manifest["file_count"]


def test_include_audio_still_excludes_video(tmp_path: Path) -> None:
    project_root = tmp_path / "repo"
    dataset_root = project_root / "dataset"
    _write(project_root / "README.md")
    _write(dataset_root / "descriptions/items/BV1/BV1.m4a")
    _write(dataset_root / "descriptions/items/BV1/BV1.video.mp4")
    output = tmp_path / "bundle.zip"

    packager.build_archive(
        project_root, dataset_root, output, include_audio=True
    )

    with zipfile.ZipFile(output) as archive:
        names = set(archive.namelist())
    assert "repo/dataset/descriptions/items/BV1/BV1.m4a" in names
    assert not any(name.endswith(".mp4") for name in names)

