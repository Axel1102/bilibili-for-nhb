#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Package description outputs and runnable project files without raw media."""

from __future__ import annotations

import argparse
import json
import os
import time
import zipfile
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATASET_ROOT = PROJECT_ROOT / "creator_video_catalog_server_ready"
DATASET_EXPORT_DIRS = ("descriptions", "exports", "reports", "inputs")
VIDEO_SUFFIXES = {".mp4", ".mkv", ".mov", ".webm", ".avi", ".flv", ".m4v"}
AUDIO_SUFFIXES = {
    ".m4a",
    ".mp3",
    ".wav",
    ".flac",
    ".aac",
    ".ogg",
    ".opus",
    ".wma",
}
SKIP_DIR_NAMES = {
    ".git",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    "node_modules",
    "browser_data",
    "browser_profile",
}
SKIP_FILE_NAMES = {
    ".ds_store",
    ".env",
    "cookies.txt",
    "yt_dlp_cookies.txt",
}


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _is_secret(path: Path) -> bool:
    name = path.name.lower()
    return (
        name in SKIP_FILE_NAMES
        or name.startswith(".env.")
        or name.endswith(".cookie")
    )


def _should_include_file(
    path: Path,
    *,
    include_audio: bool,
    output_path: Path,
) -> bool:
    if path.is_symlink() or path.resolve() == output_path:
        return False
    name = path.name.lower()
    suffix = path.suffix.lower()
    if _is_secret(path) or suffix in VIDEO_SUFFIXES:
        return False
    if suffix in AUDIO_SUFFIXES and not include_audio:
        return False
    if name.endswith((".part", ".ytdl", ".tmp")):
        return False
    if name.startswith("bilibili_description_bundle_") and suffix == ".zip":
        return False
    return path.is_file()


def _walk_files(
    root: Path,
    *,
    prune_paths: Iterable[Path],
    include_audio: bool,
    output_path: Path,
) -> Iterable[Path]:
    resolved_prunes = {path.resolve() for path in prune_paths}
    for current, dirnames, filenames in os.walk(root, topdown=True):
        current_path = Path(current)
        kept_dirs = []
        for dirname in dirnames:
            candidate = current_path / dirname
            if dirname.lower() in SKIP_DIR_NAMES or candidate.is_symlink():
                continue
            resolved = candidate.resolve()
            if resolved in resolved_prunes:
                continue
            kept_dirs.append(dirname)
        dirnames[:] = kept_dirs
        for filename in filenames:
            path = current_path / filename
            if _should_include_file(
                path,
                include_audio=include_audio,
                output_path=output_path,
            ):
                yield path


def collect_files(
    project_root: Path,
    dataset_root: Path,
    output_path: Path,
    *,
    include_audio: bool,
) -> List[Tuple[Path, Path, str]]:
    if project_root == dataset_root:
        raise ValueError("dataset-root cannot be the project root")
    descriptions_dir = dataset_root / "descriptions"
    if not descriptions_dir.is_dir():
        raise FileNotFoundError(f"Description directory not found: {descriptions_dir}")

    bundle_root = project_root.name
    collected: List[Tuple[Path, Path, str]] = []
    seen_archive_paths: Set[Path] = set()

    project_prunes = [
        project_root / ".git",
        project_root / ".venv",
        project_root / "data",
        project_root / "browser_data",
    ]
    if _is_relative_to(dataset_root, project_root):
        project_prunes.append(dataset_root)

    for source in _walk_files(
        project_root,
        prune_paths=project_prunes,
        include_audio=include_audio,
        output_path=output_path,
    ):
        archive_path = Path(bundle_root) / source.relative_to(project_root)
        collected.append((source, archive_path, "project"))
        seen_archive_paths.add(archive_path)

    if _is_relative_to(dataset_root, project_root):
        dataset_archive_root = Path(bundle_root) / dataset_root.relative_to(project_root)
    else:
        dataset_archive_root = Path(bundle_root) / dataset_root.name

    for dirname in DATASET_EXPORT_DIRS:
        source_root = dataset_root / dirname
        if not source_root.is_dir():
            continue
        for source in _walk_files(
            source_root,
            prune_paths=[],
            include_audio=include_audio,
            output_path=output_path,
        ):
            archive_path = dataset_archive_root / source.relative_to(dataset_root)
            if archive_path in seen_archive_paths:
                continue
            collected.append((source, archive_path, f"dataset/{dirname}"))
            seen_archive_paths.add(archive_path)

    return sorted(collected, key=lambda item: item[1].as_posix())


def build_archive(
    project_root: Path,
    dataset_root: Path,
    output_path: Path,
    *,
    include_audio: bool = False,
    verify: bool = True,
) -> Dict[str, object]:
    project_root = project_root.expanduser().resolve()
    dataset_root = dataset_root.expanduser().resolve()
    output_path = output_path.expanduser().resolve()
    files = collect_files(
        project_root,
        dataset_root,
        output_path,
        include_audio=include_audio,
    )
    if not files:
        raise ValueError("No files matched the package policy")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.name}.{os.getpid()}.tmp")
    section_counts = Counter(section for _, _, section in files)
    source_bytes = sum(source.stat().st_size for source, _, _ in files)
    manifest = {
        "created_at": int(time.time()),
        "file_count": len(files),
        "source_bytes": source_bytes,
        "sections": dict(sorted(section_counts.items())),
        "included_dataset_directories": list(DATASET_EXPORT_DIRS),
        "include_audio": include_audio,
        "excluded": [
            "raw crawler data such as batches/ and data/",
            "video files",
            "audio files unless --include-audio is used",
            "cookies, .env files, browser profiles, .git, virtual environments, and caches",
        ],
    }
    try:
        with zipfile.ZipFile(
            temporary,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=6,
            allowZip64=True,
        ) as archive:
            for source, archive_path, _ in files:
                archive.write(source, archive_path.as_posix())
            archive.writestr(
                f"{project_root.name}/PACKAGE_MANIFEST.json",
                json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            )
        if verify:
            with zipfile.ZipFile(temporary, mode="r") as archive:
                broken = archive.testzip()
            if broken:
                raise RuntimeError(f"ZIP integrity check failed at: {broken}")
        os.replace(temporary, output_path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise

    manifest["archive_bytes"] = output_path.stat().st_size
    manifest["output_path"] = str(output_path)
    return manifest


def _human_bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Package Bilibili description outputs without raw video data."
    )
    parser.add_argument("--dataset-root", type=Path, default=DEFAULT_DATASET_ROOT)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        help="ZIP path; default: next to the project directory with a timestamp",
    )
    parser.add_argument(
        "--include-audio",
        action="store_true",
        help="include retained audio files; full video files are always excluded",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="skip the final ZIP CRC integrity check",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    project_root = args.project_root.expanduser().resolve()
    dataset_root = args.dataset_root.expanduser().resolve()
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_path = (
        args.output.expanduser().resolve()
        if args.output
        else project_root.parent / f"bilibili_description_bundle_{timestamp}.zip"
    )
    report = build_archive(
        project_root,
        dataset_root,
        output_path,
        include_audio=args.include_audio,
        verify=not args.no_verify,
    )
    print(f"Archive: {report['output_path']}")
    print(
        f"Files: {report['file_count']}, "
        f"source size: {_human_bytes(int(report['source_bytes']))}, "
        f"ZIP size: {_human_bytes(int(report['archive_bytes']))}"
    )
    print(f"Sections: {json.dumps(report['sections'], ensure_ascii=False)}")
    print("Raw crawler data and full video files were excluded.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, RuntimeError, ValueError, OSError) as exc:
        print(f"Error: {exc}")
        raise SystemExit(1)

