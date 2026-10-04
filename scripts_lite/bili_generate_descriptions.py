#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Generate Bilibili video descriptions with concurrent audio ASR and Qwen."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import httpx


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROMPT = PROJECT_ROOT / "prompts" / "bilibili_description_prompt_zh.txt"
DEFAULT_OPENAI_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DEFAULT_API_BASE_URL = "https://dashscope.aliyuncs.com/api/v1"
DEFAULT_TEXT_MODEL = "qwen3.8-flash"
DEFAULT_ASR_MODEL = "qwen-audio-3.1-asr-flash-filetrans"
CHROME_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
)
CSV_FIELDS = [
    "video_id",
    "bvid",
    "creator_id",
    "creator_name",
    "video_url",
    "title",
    "original_description",
    "generated_description",
    "status",
    "needs_video_understanding",
    "insufficient_information_reason",
    "asr_error",
    "text_model",
    "asr_model",
    "total_tokens",
    "completed_at",
]


def _atomic_write_json(path: Path, value: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _atomic_write_rows(
    path: Path, rows: Iterable[Dict[str, Any]], *, jsonl: bool
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    if jsonl:
        with temporary.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    else:
        with temporary.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=CSV_FIELDS, extrasaction="ignore"
            )
            writer.writeheader()
            writer.writerows(rows)
    os.replace(temporary, path)


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def load_input(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = [dict(row) for row in csv.DictReader(handle)]
    else:
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    value = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"{path}:{line_number} is not valid JSON"
                    ) from exc
                if isinstance(value, dict):
                    rows.append(value)

    unique: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        video_id = str(
            row.get("bvid") or row.get("video_id") or row.get("aid") or ""
        ).strip()
        video_url = str(row.get("video_url") or "").strip()
        if not video_id or not video_url:
            continue
        copied = dict(row)
        copied["video_id"] = video_id
        copied["video_url"] = video_url
        copied["description"] = str(
            row.get("original_description") or row.get("description") or ""
        )
        unique.setdefault(video_id, copied)
    if not unique:
        raise ValueError(f"No usable videos in {path}")
    return list(unique.values())


def _safe_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._") or "unknown"


def prepare_cookie_jar(
    cookie_file: Path | None, state_dir: Path
) -> Path | None:
    """Convert the crawler's raw Cookie header into a private yt-dlp jar."""
    if cookie_file is None:
        return None
    source = cookie_file.expanduser().resolve()
    text = source.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"Cookie file is empty: {source}")
    if text.startswith("# Netscape HTTP Cookie File"):
        return source

    lines = ["# Netscape HTTP Cookie File"]
    for part in text.replace("\n", ";").split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, value = part.split("=", 1)
        name = name.strip()
        if name:
            lines.append(
                f".bilibili.com\tTRUE\t/\tFALSE\t0\t{name}\t{value.strip()}"
            )
    if len(lines) == 1:
        raise ValueError(f"Cookie header could not be parsed: {source}")

    state_dir.mkdir(parents=True, exist_ok=True)
    destination = state_dir / "yt_dlp_cookies.txt"
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    temporary.write_text("\n".join(lines) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    os.replace(temporary, destination)
    return destination


def _find_audio(item_dir: Path, video_id: str) -> Path | None:
    allowed = {".m4a", ".mp3", ".wav", ".flac", ".aac", ".ogg", ".opus"}
    candidates = [
        path
        for path in item_dir.glob(f"{_safe_id(video_id)}.*")
        if path.is_file() and path.suffix.lower() in allowed
    ]
    return sorted(candidates)[0] if candidates else None


def build_audio_command(
    video: Dict[str, Any],
    item_dir: Path,
    cookie_jar: Path | None,
    ffmpeg_path: str,
) -> List[str]:
    command = [
        sys.executable,
        "-m",
        "yt_dlp",
        "--no-playlist",
        "--retries",
        "10",
        "--fragment-retries",
        "10",
        "--retry-sleep",
        "2",
        "--force-ipv4",
        "--user-agent",
        CHROME_USER_AGENT,
        "--add-header",
        "Origin:https://www.bilibili.com",
        "--add-header",
        f"Referer:{video['video_url']}",
        "--format",
        "ba[ext=m4a]/ba/b",
        "--extract-audio",
        "--audio-format",
        "m4a",
        "--audio-quality",
        "5",
        "--ffmpeg-location",
        ffmpeg_path,
        "--no-overwrites",
        "--output",
        str(item_dir / f"{_safe_id(str(video['video_id']))}.%(ext)s"),
        "--print",
        "after_move:filepath",
    ]
    if cookie_jar:
        command.extend(["--cookies", str(cookie_jar)])
    command.append(str(video["video_url"]))
    return command


def download_audio(
    video: Dict[str, Any],
    item_dir: Path,
    cookie_jar: Path | None,
    download_gate: threading.BoundedSemaphore,
) -> Tuple[Path, float, bool]:
    video_id = str(video["video_id"])
    item_dir.mkdir(parents=True, exist_ok=True)
    cached = _find_audio(item_dir, video_id)
    if cached:
        return cached, 0.0, True

    with download_gate:
        cached = _find_audio(item_dir, video_id)
        if cached:
            return cached, 0.0, True
        for pattern in (
            f"{_safe_id(video_id)}*.part",
            f"{_safe_id(video_id)}*.ytdl",
        ):
            for temporary in item_dir.glob(pattern):
                temporary.unlink(missing_ok=True)
        try:
            import imageio_ffmpeg

            ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
        except ImportError as exc:
            raise RuntimeError(
                "Missing imageio-ffmpeg; install requirements.txt"
            ) from exc

        started = time.perf_counter()
        process = subprocess.run(
            build_audio_command(video, item_dir, cookie_jar, ffmpeg_path),
            text=True,
            capture_output=True,
            check=False,
        )
        if process.returncode != 0:
            details = "\n".join(
                part.strip()
                for part in (process.stdout, process.stderr)
                if part.strip()
            )
            last_line = details.splitlines()[-1] if details else "unknown error"
            raise RuntimeError(f"yt-dlp failed: {last_line}")
        audio = _find_audio(item_dir, video_id)
        if not audio:
            raise RuntimeError("yt-dlp succeeded but no audio file was found")
        return audio, time.perf_counter() - started, False


def _object_to_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        return value.model_dump(exclude_none=True)
    return {}


def _token_counts(usage: Dict[str, Any]) -> Tuple[int, int, int]:
    prompt = int(usage.get("prompt_tokens", usage.get("input_tokens")) or 0)
    completion = int(
        usage.get("completion_tokens", usage.get("output_tokens")) or 0
    )
    total = int(usage.get("total_tokens") or prompt + completion)
    return prompt, completion, total


def _upload_file(
    http: httpx.Client,
    api_key: str,
    api_base_url: str,
    model: str,
    path: Path,
) -> str:
    if path.stat().st_size > 1024**3:
        raise ValueError(f"Audio exceeds the 1GB upload limit: {path}")
    response = http.get(
        f"{api_base_url.rstrip('/')}/uploads",
        headers={"Authorization": f"Bearer {api_key}"},
        params={"action": "getPolicy", "model": model},
    )
    response.raise_for_status()
    policy = response.json()["data"]
    key = f"{policy['upload_dir']}/{path.name}"
    with path.open("rb") as handle:
        upload = http.post(
            policy["upload_host"],
            files={
                "OSSAccessKeyId": (None, policy["oss_access_key_id"]),
                "Signature": (None, policy["signature"]),
                "policy": (None, policy["policy"]),
                "x-oss-object-acl": (None, policy["x_oss_object_acl"]),
                "x-oss-forbid-overwrite": (
                    None,
                    policy["x_oss_forbid_overwrite"],
                ),
                "key": (None, key),
                "success_action_status": (None, "200"),
                "file": (path.name, handle, "application/octet-stream"),
            },
        )
        upload.raise_for_status()
    return f"oss://{key}"


def _extract_transcript(value: Dict[str, Any]) -> str:
    texts: List[str] = []
    for transcript in value.get("transcripts") or []:
        if not isinstance(transcript, dict):
            continue
        if isinstance(transcript.get("text"), str):
            texts.append(transcript["text"])
        else:
            texts.extend(
                str(sentence["text"])
                for sentence in transcript.get("sentences") or []
                if isinstance(sentence, dict)
                and isinstance(sentence.get("text"), str)
            )
    for key in ("text", "full_text", "transcription"):
        if not texts and isinstance(value.get(key), str):
            texts.append(value[key])
    return "\n".join(text.strip() for text in texts if text.strip())


def transcribe_audio(
    audio: Path,
    api_key: str,
    api_base_url: str,
    model: str,
    poll_seconds: float,
    timeout_seconds: float,
) -> Tuple[str, Dict[str, Any], str]:
    with httpx.Client(
        timeout=httpx.Timeout(1800.0, connect=30.0), follow_redirects=True
    ) as http:
        oss_url = _upload_file(http, api_key, api_base_url, model, audio)
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "X-DashScope-Async": "enable",
            "X-DashScope-OssResourceResolve": "enable",
        }
        response = http.post(
            f"{api_base_url.rstrip('/')}/services/audio/asr/transcription",
            headers=headers,
            json={
                "model": model,
                "input": {"file_urls": [oss_url]},
                "parameters": {
                    "channel_id": [0],
                    "language_hints": ["zh", "en"],
                },
            },
        )
        response.raise_for_status()
        task_id = str(response.json()["output"]["task_id"])
        deadline = time.monotonic() + timeout_seconds
        while True:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"ASR task timed out: {task_id}")
            task = http.get(
                f"{api_base_url.rstrip('/')}/tasks/{task_id}",
                headers={"Authorization": f"Bearer {api_key}"},
            )
            task.raise_for_status()
            task_value = task.json()
            status = task_value.get("output", {}).get("task_status")
            if status == "SUCCEEDED":
                break
            if status in {"FAILED", "CANCELED", "UNKNOWN"}:
                raise RuntimeError(f"ASR task failed: {task_id} ({status})")
            time.sleep(poll_seconds)

        results = task_value.get("output", {}).get("results") or []
        result_url = next(
            (
                str(item["transcription_url"])
                for item in results
                if item.get("subtask_status") == "SUCCEEDED"
                and item.get("transcription_url")
            ),
            "",
        )
        if not result_url:
            raise RuntimeError(f"ASR result URL missing: {task_id}")
        result_response = http.get(result_url)
        result_response.raise_for_status()
        return (
            _extract_transcript(result_response.json()),
            _object_to_dict(task_value.get("usage")),
            task_id,
        )


def validate_model_result(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("model response is not a JSON object")
    description = value.get("description")
    needs_video = value.get("needs_video_understanding")
    reason = value.get("insufficient_information_reason")
    if (
        not isinstance(description, str)
        or not isinstance(needs_video, bool)
        or not isinstance(reason, str)
    ):
        raise ValueError("model response has invalid field types")
    if needs_video and not reason.strip():
        raise ValueError("video-understanding reason is required")
    if not needs_video and not description.strip():
        raise ValueError("description is required when text is sufficient")
    return {
        "description": description.strip(),
        "needs_video_understanding": needs_video,
        "insufficient_information_reason": reason.strip() if needs_video else "",
    }


def _model_input(
    video: Dict[str, Any], transcript: str, asr_error: str
) -> str:
    source = {
        "title": video.get("title", ""),
        "introduction": video.get("description", ""),
        "asr_transcript": transcript,
        "asr_error": asr_error,
    }
    return (
        "请根据下面的视频文字信息返回规定的 JSON 对象。\n\n"
        "<video_text_data>\n"
        f"{json.dumps(source, ensure_ascii=False)}\n"
        "</video_text_data>"
    )


def generate_description(
    api_key: str,
    base_url: str,
    model: str,
    prompt: str,
    video: Dict[str, Any],
    transcript: str,
    asr_error: str,
) -> Tuple[Dict[str, Any], Dict[str, int], List[str]]:
    from openai import OpenAI

    client = OpenAI(api_key=api_key, base_url=base_url, timeout=1800.0)
    messages: List[Dict[str, str]] = [
        {"role": "system", "content": prompt},
        {
            "role": "user",
            "content": _model_input(video, transcript, asr_error),
        },
    ]
    usage_total = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
    }
    request_ids: List[str] = []
    last_error = "unknown validation error"
    for attempt in range(2):
        request: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": 0,
            "max_tokens": 600,
            "response_format": {"type": "json_object"},
        }
        if model.startswith("qwen3.8-"):
            request["extra_body"] = {"enable_thinking": False}
        response = client.chat.completions.create(**request)
        raw = str(response.choices[0].message.content or "").strip()
        prompt_tokens, completion_tokens, total_tokens = _token_counts(
            _object_to_dict(response.usage)
        )
        usage_total["prompt_tokens"] += prompt_tokens
        usage_total["completion_tokens"] += completion_tokens
        usage_total["total_tokens"] += total_tokens
        request_ids.append(str(getattr(response, "id", "") or ""))
        try:
            result = validate_model_result(json.loads(raw))
            if not any(
                str(value or "").strip()
                for value in (
                    video.get("title"),
                    video.get("description"),
                    transcript,
                )
            ):
                result["needs_video_understanding"] = True
                result["insufficient_information_reason"] = (
                    "标题、简介和 ASR 均无可用信息。"
                )
            return result, usage_total, request_ids
        except (json.JSONDecodeError, ValueError) as exc:
            last_error = str(exc)
            if attempt == 0:
                messages.extend(
                    [
                        {"role": "assistant", "content": raw},
                        {
                            "role": "user",
                            "content": (
                                f"上一个 JSON 无效，原因：{last_error}。"
                                "请只返回修正后的 JSON。"
                            ),
                        },
                    ]
                )
    raise ValueError(f"model returned invalid JSON twice: {last_error}")


def process_video(
    video: Dict[str, Any],
    args: argparse.Namespace,
    prompt: str,
    api_key: str,
) -> Dict[str, Any]:
    video_id = str(video["video_id"])
    item_dir = args.output_dir / "items" / _safe_id(video_id)
    result_path = item_dir / "result.json"
    existing = _read_json(result_path)
    if existing and not args.overwrite and not (
        args.retry_partial and existing.get("status") == "partial"
    ):
        return existing

    started = time.perf_counter()
    audio: Path | None = None
    download_seconds = 0.0
    audio_cached = False
    transcript = ""
    asr_usage: Dict[str, Any] = {}
    asr_task_id = ""
    asr_error = ""
    asr_path = item_dir / "asr.json"
    try:
        audio, download_seconds, audio_cached = download_audio(
            video,
            item_dir,
            args.cookie_jar,
            args.download_gate,
        )
        cached_asr = _read_json(asr_path) if not args.overwrite else {}
        if cached_asr:
            transcript = str(cached_asr.get("transcript") or "")
            asr_usage = _object_to_dict(cached_asr.get("usage"))
            asr_task_id = str(cached_asr.get("task_id") or "")
        else:
            transcript, asr_usage, asr_task_id = transcribe_audio(
                audio,
                api_key,
                args.api_base_url,
                args.asr_model,
                args.poll_seconds,
                args.asr_timeout_seconds,
            )
            _atomic_write_json(
                asr_path,
                {
                    "model": args.asr_model,
                    "task_id": asr_task_id,
                    "transcript": transcript,
                    "usage": asr_usage,
                },
            )
    except Exception as exc:
        asr_error = f"{type(exc).__name__}: {exc}"
    finally:
        if audio and not args.keep_audio and (transcript or asr_path.exists()):
            audio.unlink(missing_ok=True)

    model_result, text_usage, request_ids = generate_description(
        api_key,
        args.openai_base_url,
        args.text_model,
        prompt,
        video,
        transcript,
        asr_error,
    )
    result = {
        "status": "partial" if asr_error else "ok",
        "video_id": video_id,
        "bvid": str(video.get("bvid") or ""),
        "creator_id": str(video.get("creator_id") or ""),
        "creator_name": str(video.get("creator_name") or ""),
        "video_url": str(video["video_url"]),
        "title": str(video.get("title") or ""),
        "original_description": str(video.get("description") or ""),
        "generated_description": model_result["description"],
        "needs_video_understanding": model_result[
            "needs_video_understanding"
        ],
        "insufficient_information_reason": model_result[
            "insufficient_information_reason"
        ],
        "asr": {
            "model": args.asr_model,
            "transcript": transcript,
            "error": asr_error,
            "task_id": asr_task_id,
            "usage": asr_usage,
        },
        "description_model": {
            "model": args.text_model,
            "request_ids": request_ids,
            "usage": text_usage,
        },
        "timing_seconds": {
            "audio_download": round(download_seconds, 3),
            "audio_cached": audio_cached,
            "end_to_end": round(time.perf_counter() - started, 3),
        },
        "total_tokens": _token_counts(asr_usage)[2]
        + _token_counts(text_usage)[2],
        "completed_at": int(time.time()),
    }
    _atomic_write_json(result_path, result)
    return result


def _csv_row(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **result,
        "asr_error": str((result.get("asr") or {}).get("error") or ""),
        "text_model": str(
            (result.get("description_model") or {}).get("model") or ""
        ),
        "asr_model": str((result.get("asr") or {}).get("model") or ""),
    }


def _load_saved_results(output_dir: Path) -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}
    for path in sorted((output_dir / "items").glob("*/result.json")):
        result = _read_json(path)
        video_id = str(result.get("video_id") or "")
        if video_id:
            results[video_id] = result
    return results


def write_aggregate(
    output_dir: Path, results: Dict[str, Dict[str, Any]]
) -> None:
    ordered = sorted(
        (_csv_row(result) for result in results.values()),
        key=lambda item: str(item.get("video_id") or ""),
    )
    _atomic_write_rows(
        output_dir / "results.jsonl", ordered, jsonl=True
    )
    _atomic_write_rows(
        output_dir / "results.csv",
        ordered,
        jsonl=False,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Batch-generate descriptions from Bilibili audio with resumable "
            "concurrency."
        )
    )
    parser.add_argument(
        "--input", type=Path, required=True, help="video_metadata.jsonl or CSV"
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--workers", type=int, default=12, help="total ASR/LLM workers; default: 12"
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        default=3,
        help="simultaneous Bilibili audio downloads; default: 3",
    )
    parser.add_argument(
        "--cookie-file", type=Path, help="raw Cookie header or Netscape cookie jar"
    )
    parser.add_argument("--text-model", default=DEFAULT_TEXT_MODEL)
    parser.add_argument("--asr-model", default=DEFAULT_ASR_MODEL)
    parser.add_argument("--prompt", type=Path, default=DEFAULT_PROMPT)
    parser.add_argument(
        "--openai-base-url",
        default=os.getenv("DASHSCOPE_BASE_URL", DEFAULT_OPENAI_BASE_URL),
    )
    parser.add_argument(
        "--api-base-url",
        default=os.getenv("DASHSCOPE_API_BASE_URL", DEFAULT_API_BASE_URL),
    )
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--asr-timeout-seconds", type=float, default=1800.0)
    parser.add_argument("--limit", type=int, default=0, help="0 means all rows")
    parser.add_argument("--keep-audio", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--retry-partial", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.workers <= 0 or args.download_workers <= 0:
        raise ValueError("workers and download-workers must be greater than zero")
    if args.download_workers > args.workers:
        raise ValueError("download-workers cannot exceed workers")
    api_key = os.getenv("DASHSCOPE_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("DASHSCOPE_API_KEY is missing")

    args.input = args.input.expanduser().resolve()
    args.output_dir = args.output_dir.expanduser().resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    prompt = args.prompt.expanduser().read_text(encoding="utf-8").strip()
    if not prompt or "JSON" not in prompt:
        raise ValueError("prompt is empty or does not request JSON")
    videos = load_input(args.input)
    if args.limit > 0:
        videos = videos[: args.limit]
    args.cookie_jar = prepare_cookie_jar(
        args.cookie_file, args.output_dir / "state"
    )
    args.download_gate = threading.BoundedSemaphore(args.download_workers)

    saved = _load_saved_results(args.output_dir)
    pending = [
        video
        for video in videos
        if args.overwrite
        or str(video["video_id"]) not in saved
        or (
            args.retry_partial
            and saved[str(video["video_id"])].get("status") == "partial"
        )
    ]
    print(
        f"input={len(videos)}, existing={len(videos) - len(pending)}, "
        f"pending={len(pending)}, workers={args.workers}, "
        f"download_workers={args.download_workers}",
        flush=True,
    )

    failures_path = args.output_dir / "failures.jsonl"
    completed = 0
    failed = 0
    queue_size = max(args.workers * 4, 100)
    try:
        with ThreadPoolExecutor(
            max_workers=args.workers, thread_name_prefix="bili-desc"
        ) as executor:
            for start in range(0, len(pending), queue_size):
                chunk = pending[start : start + queue_size]
                futures: Dict[Future[Dict[str, Any]], Dict[str, Any]] = {
                    executor.submit(
                        process_video, video, args, prompt, api_key
                    ): video
                    for video in chunk
                }
                for future in as_completed(futures):
                    video = futures[future]
                    video_id = str(video["video_id"])
                    try:
                        result = future.result()
                        saved[video_id] = result
                        completed += 1
                        print(
                            f"[{completed + failed}/{len(pending)}] ok "
                            f"{video_id} status={result.get('status')}",
                            flush=True,
                        )
                    except Exception as exc:
                        failed += 1
                        failure = {
                            "video_id": video_id,
                            "video_url": video.get("video_url", ""),
                            "error": f"{type(exc).__name__}: {exc}",
                            "failed_at": int(time.time()),
                        }
                        with failures_path.open(
                            "a", encoding="utf-8"
                        ) as handle:
                            handle.write(
                                json.dumps(failure, ensure_ascii=False) + "\n"
                            )
                        print(
                            f"[{completed + failed}/{len(pending)}] failed "
                            f"{video_id}: {failure['error']}",
                            file=sys.stderr,
                            flush=True,
                        )
                    if (completed + failed) % 100 == 0:
                        write_aggregate(args.output_dir, saved)
    except KeyboardInterrupt:
        print(
            "Interrupted; completed item result files are resumable.",
            file=sys.stderr,
        )
        raise

    write_aggregate(args.output_dir, saved)
    ok_count = sum(result.get("status") == "ok" for result in saved.values())
    partial_count = sum(
        result.get("status") == "partial" for result in saved.values()
    )
    print(
        f"finished: new_completed={completed}, failed={failed}, "
        f"total_results={len(saved)}, ok={ok_count}, partial={partial_count}",
        flush=True,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (
        ValueError,
        RuntimeError,
        FileNotFoundError,
        httpx.HTTPError,
    ) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
