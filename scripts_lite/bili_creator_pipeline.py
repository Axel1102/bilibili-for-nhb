#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Resumable Bilibili creator dataset pipeline.

Stages:
1. prepare: select creators from the reviewed workbook and split them into batches.
2. catalog: fetch every selected creator's complete video list first.
3. crawl: fetch video details, descriptions, comments, sub-comments, and danmaku.

The pipeline stores one directory per creator and one directory per video. Every
network page is written atomically before its checkpoint advances, so a rerun can
continue after interruption without starting the whole creator again.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import html
import json
import os
import random
import re
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, TypeVar

import httpx
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_XLSX = PROJECT_ROOT / "保留.xlsx"
DEFAULT_INPUT_CSV = PROJECT_ROOT / "data" / "bilibili" / "csv" / "new.csv"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "bili" / "creator_dataset"
DEFAULT_DECISION_COLUMN = "判断（1：保留，0：排除）"

T = TypeVar("T")


class _DefaultCommentOrder:
    value = 0


DEFAULT_COMMENT_ORDER = _DefaultCommentOrder()


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if pd.isna(value):
        return None
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(text, encoding="utf-8")
    os.replace(tmp_path, path)


def _atomic_write_json(path: Path, payload: Any) -> None:
    _atomic_write_text(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
    )


def _atomic_write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    text = "".join(
        json.dumps(row, ensure_ascii=False, default=_json_default) + "\n"
        for row in rows
    )
    _atomic_write_text(path, text)


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


def _append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, default=_json_default) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _slug(text: Any, fallback: str = "unknown") -> str:
    value = str(text or "").strip()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[\\/:*?\"<>|]", "_", value)
    value = re.sub(r"_+", "_", value).strip("._")
    return value[:80] or fallback


def _normalize_uid(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, bool):
        return ""
    if isinstance(value, (int, float)):
        if float(value).is_integer():
            return str(int(value))
    text = str(value).strip()
    match = re.search(r"space\.bilibili\.com/(\d+)", text)
    if match:
        return match.group(1)
    if re.fullmatch(r"\d+(?:\.0+)?", text):
        return text.split(".", 1)[0]
    return ""


def _is_selected_decision(value: Any, include_ambiguous: bool = False) -> bool:
    if value is None or pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return float(value) == 1.0
    text = str(value).strip().lower()
    if text in {"1", "1.0", "true", "yes", "y", "是", "保留"}:
        return True
    return include_ambiguous and text.startswith("1")


def _pick_column(columns: Sequence[Any], candidates: Sequence[str]) -> Optional[str]:
    normalized = {str(column).strip().lower(): str(column) for column in columns}
    for candidate in candidates:
        hit = normalized.get(candidate.strip().lower())
        if hit:
            return hit
    return None


def _write_csv(path: Path, fieldnames: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)


def prepare_inputs(
    input_xlsx: Path,
    output_root: Path,
    *,
    batch_size: int = 100,
    sheet_name: Optional[str] = None,
    decision_column: str = DEFAULT_DECISION_COLUMN,
    include_ambiguous: bool = False,
) -> Dict[str, Any]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    if not input_xlsx.exists():
        raise FileNotFoundError(input_xlsx)

    frame = pd.read_excel(input_xlsx, sheet_name=sheet_name or 0)
    decision_col = _pick_column(frame.columns, [decision_column])
    uid_col = _pick_column(frame.columns, ["author_id", "uid", "mid", "creator_id"])
    author_col = _pick_column(frame.columns, ["author", "creator_name", "up主", "博主"])
    url_col = _pick_column(frame.columns, ["space_url", "url", "主页", "主页链接"])

    if not decision_col:
        raise ValueError(f"Decision column not found: {decision_column}")
    if not uid_col and not url_col:
        raise ValueError("No creator UID or space URL column was found")

    selected_rows: List[Dict[str, Any]] = []
    seen_uids = set()
    ambiguous_values: Dict[str, int] = {}

    for source_row_number, row in frame.iterrows():
        decision = row.get(decision_col)
        decision_text = "" if pd.isna(decision) else str(decision).strip()
        exact_selected = _is_selected_decision(decision, include_ambiguous=False)
        selected = _is_selected_decision(decision, include_ambiguous=include_ambiguous)
        if decision_text and not exact_selected and decision_text not in {"0", "0.0"}:
            ambiguous_values[decision_text] = ambiguous_values.get(decision_text, 0) + 1
        if not selected:
            continue

        uid = _normalize_uid(row.get(uid_col)) if uid_col else ""
        space_url = "" if not url_col or pd.isna(row.get(url_col)) else str(row.get(url_col)).strip()
        if not uid:
            uid = _normalize_uid(space_url)
        if not uid or uid in seen_uids:
            continue
        seen_uids.add(uid)

        author = "" if not author_col or pd.isna(row.get(author_col)) else str(row.get(author_col)).strip()
        batch_index = len(selected_rows) // batch_size + 1
        selected_rows.append(
            {
                "author_id": uid,
                "author": author,
                "space_url": space_url or f"https://space.bilibili.com/{uid}",
                "decision": decision_text,
                "source_row": int(source_row_number) + 2,
                "batch_index": batch_index,
            }
        )

    return _write_prepared_inputs(
        selected_rows,
        output_root,
        batch_size=batch_size,
        source_path=input_xlsx,
        source_kind="xlsx",
        source_sheet=sheet_name or str(pd.ExcelFile(input_xlsx).sheet_names[0]),
        decision_column=decision_col,
        include_ambiguous=include_ambiguous,
        ambiguous_values=ambiguous_values,
    )


def _write_prepared_inputs(
    selected_rows: List[Dict[str, Any]],
    output_root: Path,
    *,
    batch_size: int,
    source_path: Path,
    source_kind: str,
    source_sheet: Optional[str],
    decision_column: str,
    include_ambiguous: bool,
    ambiguous_values: Dict[str, int],
) -> Dict[str, Any]:
    fieldnames = ["author_id", "author", "space_url", "decision", "source_row", "batch_index"]
    selected_csv = output_root / "inputs" / "creators_selected.csv"
    _write_csv(selected_csv, fieldnames, selected_rows)

    batches_dir = output_root / "inputs" / "batches"
    batch_files: List[str] = []
    for start in range(0, len(selected_rows), batch_size):
        batch_rows = selected_rows[start : start + batch_size]
        batch_index = start // batch_size + 1
        batch_path = batches_dir / f"batch_{batch_index:03d}.csv"
        _write_csv(batch_path, fieldnames, batch_rows)
        batch_files.append(str(batch_path))

    manifest = {
        "input_source": str(source_path),
        "source_kind": source_kind,
        "sheet_name": source_sheet,
        "decision_column": decision_column,
        "include_ambiguous": include_ambiguous,
        "selected_creator_count": len(selected_rows),
        "batch_size": batch_size,
        "batch_count": len(batch_files),
        "selected_csv": str(selected_csv),
        "batch_files": batch_files,
        "ambiguous_decision_values": ambiguous_values,
        "prepared_at": int(time.time()),
    }
    _atomic_write_json(output_root / "inputs" / "manifest.json", manifest)
    return manifest


def prepare_csv_inputs(
    input_csv: Path,
    output_root: Path,
    *,
    batch_size: int = 100,
    decision_column: str = "result",
    include_ambiguous: bool = False,
) -> Dict[str, Any]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    if not input_csv.exists():
        raise FileNotFoundError(input_csv)

    with input_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        source_rows = [dict(row) for row in csv.DictReader(handle)]
    columns = list(source_rows[0]) if source_rows else []
    decision_col = _pick_column(columns, [decision_column, "result", DEFAULT_DECISION_COLUMN])
    uid_col = _pick_column(columns, ["author_id", "uid", "mid", "creator_id"])
    author_col = _pick_column(columns, ["author", "creator_name", "up主", "博主"])
    url_col = _pick_column(columns, ["space_url", "url", "主页", "主页链接"])
    if not uid_col and not url_col:
        raise ValueError("No creator UID or space URL column was found")

    selected_rows: List[Dict[str, Any]] = []
    seen_uids = set()
    ambiguous_values: Dict[str, int] = {}
    for source_row_number, row in enumerate(source_rows, start=2):
        decision = row.get(decision_col) if decision_col else "1"
        decision_text = str(decision or "").strip()
        exact_selected = _is_selected_decision(decision, include_ambiguous=False)
        selected = _is_selected_decision(decision, include_ambiguous=include_ambiguous)
        if decision_text and not exact_selected and decision_text not in {"0", "0.0"}:
            ambiguous_values[decision_text] = ambiguous_values.get(decision_text, 0) + 1
        if decision_col and not selected:
            continue

        uid = _normalize_uid(row.get(uid_col)) if uid_col else ""
        space_url = str(row.get(url_col) or "").strip() if url_col else ""
        if not uid:
            uid = _normalize_uid(space_url)
        if not uid or uid in seen_uids:
            continue
        seen_uids.add(uid)
        author = str(row.get(author_col) or "").strip() if author_col else ""
        selected_rows.append(
            {
                "author_id": uid,
                "author": author,
                "space_url": space_url or f"https://space.bilibili.com/{uid}",
                "decision": decision_text or "1",
                "source_row": source_row_number,
                "batch_index": len(selected_rows) // batch_size + 1,
            }
        )

    return _write_prepared_inputs(
        selected_rows,
        output_root,
        batch_size=batch_size,
        source_path=input_csv,
        source_kind="csv",
        source_sheet=None,
        decision_column=decision_col or "",
        include_ambiguous=include_ambiguous,
        ambiguous_values=ambiguous_values,
    )


def _load_creator_rows(output_root: Path, batch_index: int) -> List[Dict[str, str]]:
    if batch_index < 0:
        raise ValueError("batch_index cannot be negative")
    if batch_index:
        input_path = output_root / "inputs" / "batches" / f"batch_{batch_index:03d}.csv"
    else:
        input_path = output_root / "inputs" / "creators_selected.csv"
    if not input_path.exists():
        raise FileNotFoundError(f"Prepared creator list not found: {input_path}")
    with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _creator_dir(output_root: Path, row: Dict[str, Any]) -> Path:
    batch_index = int(row.get("batch_index") or 1)
    uid = _normalize_uid(row.get("author_id"))
    author = _slug(row.get("author"), "unknown")
    return output_root / "batches" / f"batch_{batch_index:03d}" / "creators" / f"{uid}_{author}"


def _video_key(video: Dict[str, Any]) -> str:
    return str(video.get("bvid") or video.get("aid") or "").strip()


def _normalize_catalog_video(video: Dict[str, Any], creator_id: str) -> Dict[str, Any]:
    aid = int(video.get("aid") or 0)
    bvid = str(video.get("bvid") or "")
    return {
        "creator_id": creator_id,
        "creator_name": str(video.get("author") or ""),
        "aid": aid,
        "bvid": bvid,
        "title": str(video.get("title") or ""),
        "description_preview": str(video.get("description") or ""),
        "publish_time": int(video.get("created") or 0),
        "duration": video.get("length") or "",
        "play_count": video.get("play") or 0,
        "comment_count": video.get("comment") or 0,
        "cover_url": str(video.get("pic") or ""),
        "video_url": (
            f"https://www.bilibili.com/video/{bvid}"
            if bvid
            else f"https://www.bilibili.com/video/av{aid}"
        ),
    }


def _normalize_video_detail(response: Dict[str, Any], fallback: Dict[str, Any]) -> Dict[str, Any]:
    view = response.get("View") or response.get("view") or {}
    owner = view.get("owner") or {}
    pages = []
    for page in view.get("pages") or []:
        pages.append(
            {
                "cid": int(page.get("cid") or 0),
                "page": int(page.get("page") or 0),
                "part": str(page.get("part") or ""),
                "duration": int(page.get("duration") or 0),
                "dimension": page.get("dimension") or {},
            }
        )
    aid = int(view.get("aid") or fallback.get("aid") or 0)
    bvid = str(view.get("bvid") or fallback.get("bvid") or "")
    return {
        "aid": aid,
        "bvid": bvid,
        "title": str(view.get("title") or fallback.get("title") or ""),
        "description": str(view.get("desc") or ""),
        "publish_time": int(view.get("pubdate") or fallback.get("publish_time") or 0),
        "created_time": int(view.get("ctime") or 0),
        "duration": int(view.get("duration") or 0),
        "creator_id": str(owner.get("mid") or fallback.get("creator_id") or ""),
        "creator_name": str(owner.get("name") or fallback.get("creator_name") or ""),
        "cover_url": str(view.get("pic") or fallback.get("cover_url") or ""),
        "category_id": view.get("tid"),
        "category_name": view.get("tname") or "",
        "copyright": view.get("copyright"),
        "stats": view.get("stat") or {},
        "rights": view.get("rights") or {},
        "pages": pages,
        "video_url": (
            f"https://www.bilibili.com/video/{bvid}"
            if bvid
            else f"https://www.bilibili.com/video/av{aid}"
        ),
    }


def _normalize_comment(item: Dict[str, Any]) -> Dict[str, Any]:
    member = item.get("member") or {}
    content = item.get("content") or {}
    reply_control = item.get("reply_control") or {}
    return {
        "rpid": str(item.get("rpid") or ""),
        "root_rpid": str(item.get("root") or item.get("rpid") or ""),
        "parent_rpid": str(item.get("parent") or "0"),
        "created_time": int(item.get("ctime") or 0),
        "like_count": int(item.get("like") or 0),
        "reply_count": int(item.get("rcount") or 0),
        "user_id": str(member.get("mid") or ""),
        "user_name": str(member.get("uname") or ""),
        "user_level": (member.get("level_info") or {}).get("current_level"),
        "message": str(content.get("message") or ""),
        "location": str(reply_control.get("location") or ""),
        "sub_comments": [],
    }


def _parse_danmaku_xml(xml_text: str, cid: int) -> List[Dict[str, Any]]:
    root = ET.fromstring(xml_text)
    rows: List[Dict[str, Any]] = []
    for node in root.findall("d"):
        values = (node.attrib.get("p") or "").split(",")
        values += [""] * (8 - len(values))
        rows.append(
            {
                "cid": cid,
                "progress_seconds": float(values[0] or 0),
                "mode": int(float(values[1] or 0)),
                "font_size": int(float(values[2] or 0)),
                "color": int(float(values[3] or 0)),
                "created_time": int(float(values[4] or 0)),
                "pool": int(float(values[5] or 0)),
                "user_hash": values[6],
                "danmaku_id": values[7],
                "text": html.unescape(node.text or ""),
            }
        )
    return rows


def _is_browser_session_closed_error(exc: BaseException) -> bool:
    """Return whether continuing would only create more per-video failures."""
    error_name = type(exc).__name__.lower()
    message = str(exc).lower()
    return "targetclosed" in error_name or any(
        marker in message
        for marker in (
            "target page, context or browser has been closed",
            "browser has been closed",
            "browser context was closed",
            "connection closed",
        )
    )


def _load_server_cookie(cookie_file: Optional[Path]) -> str:
    if cookie_file is not None:
        path = cookie_file.expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(f"Bilibili cookie file not found: {path}")
        cookie = path.read_text(encoding="utf-8").strip()
        if not cookie:
            raise ValueError(f"Bilibili cookie file is empty: {path}")
        return cookie
    return os.environ.get("BILIBILI_COOKIE", "").strip()


async def _retry(
    operation: Callable[[], Awaitable[T]],
    *,
    label: str,
    attempts: int,
    base_delay: float,
) -> T:
    last_error: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        try:
            return await operation()
        except (KeyboardInterrupt, asyncio.CancelledError):
            raise
        except Exception as exc:
            if _is_browser_session_closed_error(exc):
                raise
            last_error = exc
            if attempt >= attempts:
                break
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, max(0.1, base_delay))
            print(f"{label} failed ({attempt}/{attempts}): {exc}; retry in {delay:.1f}s")
            await asyncio.sleep(delay)
    assert last_error is not None
    raise last_error


class BilibiliSession:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.crawler: Any = None
        self.playwright: Any = None
        self.client: Any = None
        self.browser_context: Any = None

    async def __aenter__(self) -> "BilibiliSession":
        try:
            return await self._open()
        except BaseException:
            await self.__aexit__(*sys.exc_info())
            raise

    async def _open(self) -> "BilibiliSession":
        sys.path.insert(0, str(PROJECT_ROOT))
        from playwright.async_api import async_playwright

        self.playwright = await async_playwright().start()
        if self.args.server_headless:
            from scripts_lite.bili_standalone_client import (
                DEFAULT_USER_AGENT,
                BilibiliApiClient,
                add_cookie_string,
            )

            profile_dir = self.args.browser_profile_dir or (
                self.args.output_root / "state" / "browser_profile"
            )
            profile_dir.mkdir(parents=True, exist_ok=True)
            try:
                profile_dir.chmod(0o700)
            except OSError:
                pass
            self.browser_context = await self.playwright.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),
                accept_downloads=True,
                headless=True,
                viewport={"width": 1920, "height": 1080},
                user_agent=DEFAULT_USER_AGENT,
            )
            stealth_path = PROJECT_ROOT / "libs" / "stealth.min.js"
            if stealth_path.exists():
                await self.browser_context.add_init_script(path=str(stealth_path))
            page = await self.browser_context.new_page()
            await page.goto("https://www.bilibili.com", wait_until="domcontentloaded")
            self.client = await BilibiliApiClient.from_browser(
                self.browser_context,
                page,
                user_agent=DEFAULT_USER_AGENT,
            )
            if not await self.client.pong():
                cookie = _load_server_cookie(self.args.cookie_file)
                if not cookie:
                    raise RuntimeError(
                        "Headless server login is required. Pass --cookie-file PATH or set "
                        "the BILIBILI_COOKIE environment variable."
                    )
                await add_cookie_string(self.browser_context, cookie)
                await self.client.update_cookies(self.browser_context)
                if not await self.client.pong():
                    raise RuntimeError(
                        "Bilibili login verification failed. Refresh the cookie file and restart the command."
                    )
            return self

        import config
        from media_platform.bilibili.core import BilibiliCrawler
        from media_platform.bilibili.login import BilibiliLogin

        config.PLATFORM = "bili"
        self.crawler = BilibiliCrawler()
        if config.ENABLE_CDP_MODE:
            self.crawler.browser_context = await self.crawler.launch_browser_with_cdp(
                self.playwright,
                None,
                self.crawler.user_agent,
                headless=config.CDP_HEADLESS,
            )
        else:
            self.crawler.browser_context = await self.crawler.launch_browser(
                self.playwright.chromium,
                None,
                self.crawler.user_agent,
                headless=config.HEADLESS,
            )
            await self.crawler.browser_context.add_init_script(path=str(PROJECT_ROOT / "libs" / "stealth.min.js"))

        self.crawler.context_page = await self.crawler.browser_context.new_page()
        await self.crawler.context_page.goto(self.crawler.index_url)
        self.client = await self.crawler.create_bilibili_client(None)
        if not await self.client.pong():
            login = BilibiliLogin(
                login_type=config.LOGIN_TYPE,
                login_phone="",
                browser_context=self.crawler.browser_context,
                context_page=self.crawler.context_page,
                cookie_str=config.COOKIES,
            )
            await login.begin()
            await self.client.update_cookies(browser_context=self.crawler.browser_context)
            if not await self.client.pong():
                raise RuntimeError(
                    "Bilibili login verification failed. Refresh the cookie file and restart the command."
                )
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        try:
            if self.crawler is not None:
                await self.crawler.close()
            elif self.browser_context is not None:
                try:
                    await self.browser_context.close()
                except Exception as close_error:
                    if not _is_browser_session_closed_error(close_error):
                        raise
        finally:
            if self.playwright is not None:
                await self.playwright.stop()


def _sleep_range(min_sleep: float, max_sleep: float) -> float:
    if max_sleep < min_sleep:
        max_sleep = min_sleep
    return random.uniform(max(0.0, min_sleep), max(0.0, max_sleep))


async def _polite_sleep(min_sleep: float, max_sleep: float) -> None:
    delay = _sleep_range(min_sleep, max_sleep)
    if delay:
        await asyncio.sleep(delay)


async def _fetch_creator_catalog(
    client: Any,
    row: Dict[str, Any],
    output_root: Path,
    *,
    page_size: int,
    attempts: int,
    min_sleep: float,
    max_sleep: float,
) -> None:
    creator_id = _normalize_uid(row.get("author_id"))
    creator_path = _creator_dir(output_root, row)
    catalog_dir = creator_path / "catalog"
    complete_path = catalog_dir / "complete.json"
    if complete_path.exists():
        print(f"catalog skip complete: {creator_id} {row.get('author', '')}")
        return

    progress_path = catalog_dir / "progress.json"
    progress = _read_json(progress_path, {}) or {}
    page_number = max(1, int(progress.get("next_page") or 1))
    creator_name = str(progress.get("creator_name") or row.get("author") or "")
    total_count = int(progress.get("total_count") or 0)

    while True:
        requested_page = page_number

        async def request_page() -> Dict[str, Any]:
            return await client.get_creator_videos(
                creator_id=creator_id,
                pn=requested_page,
                ps=page_size,
                order_mode="pubdate",
            )

        response = await _retry(
            request_page,
            label=f"catalog uid={creator_id} page={requested_page}",
            attempts=attempts,
            base_delay=max(1.0, min_sleep),
        )
        page_obj = response.get("page") or {}
        raw_videos = (response.get("list") or {}).get("vlist") or []
        videos = [_normalize_catalog_video(video, creator_id) for video in raw_videos]
        if videos and not creator_name:
            creator_name = str(videos[0].get("creator_name") or "")
        total_count = int(page_obj.get("count") or total_count or len(videos))
        has_more = bool(videos) and requested_page * page_size < total_count

        page_payload = {
            "creator_id": creator_id,
            "page": requested_page,
            "page_size": page_size,
            "total_count": total_count,
            "has_more": has_more,
            "videos": videos,
        }
        _atomic_write_json(catalog_dir / "pages" / f"page_{requested_page:05d}.json", page_payload)
        _atomic_write_json(
            progress_path,
            {
                "creator_id": creator_id,
                "creator_name": creator_name,
                "next_page": requested_page + 1,
                "total_count": total_count,
                "updated_at": int(time.time()),
            },
        )
        print(f"catalog uid={creator_id} page={requested_page}: {len(videos)} videos")
        if not has_more:
            break
        page_number += 1
        await _polite_sleep(min_sleep, max_sleep)

    all_videos: List[Dict[str, Any]] = []
    seen = set()
    for page_path in sorted((catalog_dir / "pages").glob("page_*.json")):
        page_payload = _read_json(page_path, {}) or {}
        for video in page_payload.get("videos") or []:
            key = _video_key(video)
            if key and key not in seen:
                seen.add(key)
                all_videos.append(video)
    _atomic_write_jsonl(creator_path / "videos.jsonl", all_videos)

    creator_info: Dict[str, Any] = {}
    try:
        creator_info = await _retry(
            lambda: client.get_creator_info(int(creator_id)),
            label=f"creator info uid={creator_id}",
            attempts=attempts,
            base_delay=max(1.0, min_sleep),
        )
    except Exception as exc:
        creator_info = {"fetch_error": f"{type(exc).__name__}: {exc}"}

    _atomic_write_json(
        creator_path / "creator.json",
        {
            "creator_id": creator_id,
            "creator_name": creator_name or row.get("author") or "",
            "space_url": row.get("space_url") or f"https://space.bilibili.com/{creator_id}",
            "source": row,
            "video_count": len(all_videos),
            "profile": creator_info,
            "catalog_completed_at": int(time.time()),
        },
    )
    _atomic_write_json(
        complete_path,
        {
            "creator_id": creator_id,
            "video_count": len(all_videos),
            "completed_at": int(time.time()),
        },
    )


async def catalog_creators(client: Any, rows: Sequence[Dict[str, Any]], output_root: Path, args: argparse.Namespace) -> None:
    errors_path = output_root / "state" / "catalog_errors.jsonl"
    for index, row in enumerate(rows, start=1):
        uid = _normalize_uid(row.get("author_id"))
        print(f"[{index}/{len(rows)}] catalog {uid} {row.get('author', '')}")
        try:
            await _fetch_creator_catalog(
                client,
                row,
                output_root,
                page_size=args.catalog_page_size,
                attempts=args.retries,
                min_sleep=args.min_sleep,
                max_sleep=args.max_sleep,
            )
        except (KeyboardInterrupt, asyncio.CancelledError):
            raise
        except Exception as exc:
            _append_jsonl(
                errors_path,
                {
                    "creator_id": uid,
                    "creator_name": row.get("author") or "",
                    "stage": "catalog",
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:1000],
                    "time": int(time.time()),
                },
            )
            print(f"catalog failed uid={uid}: {exc}")
            if _is_browser_session_closed_error(exc):
                raise RuntimeError(
                    "Browser session closed; stopping so the process supervisor can restart safely."
                ) from exc
        await _polite_sleep(args.min_sleep, args.max_sleep)


async def _crawl_sub_comments(
    client: Any,
    video_id: str,
    root_comment: Dict[str, Any],
    comments_dir: Path,
    args: argparse.Namespace,
) -> None:
    root_rpid = str(root_comment.get("rpid") or "")
    reply_count = int(root_comment.get("rcount") or root_comment.get("reply_count") or 0)
    if not root_rpid or reply_count <= 0:
        return
    root_dir = comments_dir / "sub" / root_rpid
    complete_path = root_dir / "complete.json"
    if complete_path.exists():
        return
    progress = _read_json(root_dir / "progress.json", {}) or {}
    page_number = max(1, int(progress.get("next_page") or 1))
    total_count = int(progress.get("total_count") or reply_count)

    while True:
        requested_page = page_number

        async def request_page() -> Dict[str, Any]:
            return await client.get_video_level_two_comments(
                video_id,
                int(root_rpid),
                requested_page,
                args.sub_comment_page_size,
                DEFAULT_COMMENT_ORDER,
            )

        response = await _retry(
            request_page,
            label=f"sub-comments aid={video_id} root={root_rpid} page={requested_page}",
            attempts=args.retries,
            base_delay=max(1.0, args.min_sleep),
        )
        raw_replies = response.get("replies") or []
        page_obj = response.get("page") or {}
        total_count = int(page_obj.get("count") or total_count or len(raw_replies))
        replies = [_normalize_comment(item) for item in raw_replies]
        has_more = bool(replies) and requested_page * args.sub_comment_page_size < total_count
        _atomic_write_json(
            root_dir / "pages" / f"page_{requested_page:05d}.json",
            {
                "root_rpid": root_rpid,
                "page": requested_page,
                "total_count": total_count,
                "has_more": has_more,
                "comments": replies,
            },
        )
        _atomic_write_json(
            root_dir / "progress.json",
            {
                "next_page": requested_page + 1,
                "total_count": total_count,
                "updated_at": int(time.time()),
            },
        )
        if not has_more:
            _atomic_write_json(
                complete_path,
                {"root_rpid": root_rpid, "count": total_count, "completed_at": int(time.time())},
            )
            return
        page_number += 1
        await _polite_sleep(args.min_sleep, args.max_sleep)


async def _crawl_comments(client: Any, video_id: str, video_dir: Path, args: argparse.Namespace) -> None:
    comments_dir = video_dir / "comments"
    complete_path = comments_dir / "complete.json"
    complete = _read_json(complete_path, {}) or {}

    async def ensure_saved_sub_comments() -> None:
        if args.skip_subcomments:
            return
        for page_path in sorted((comments_dir / "top").glob("page_*.json")):
            payload = _read_json(page_path, {}) or {}
            for saved_comment in payload.get("comments") or []:
                await _crawl_sub_comments(client, video_id, saved_comment, comments_dir, args)

    if complete:
        fetched_count = int(complete.get("top_level_count") or 0)
        was_limited = bool(complete.get("limited"))
        limit_now_satisfied = (
            args.max_comments_per_video > 0
            and fetched_count >= args.max_comments_per_video
        )
        needs_more_top_comments = was_limited and not limit_now_satisfied
        if not needs_more_top_comments:
            await ensure_saved_sub_comments()
            if not args.skip_subcomments and not complete.get("subcomments_completed"):
                complete["subcomments_completed"] = True
                complete["completed_at"] = int(time.time())
                _atomic_write_json(complete_path, complete)
            return

    progress_path = comments_dir / "progress.json"
    progress = _read_json(progress_path, {}) or {}
    sequence = max(1, int(progress.get("next_sequence") or 1))
    cursor = int(progress.get("next_cursor") or 0)
    total_fetched = int(progress.get("total_fetched") or 0)
    if progress.get("done"):
        await ensure_saved_sub_comments()
        _atomic_write_json(
            complete_path,
            {
                "top_level_count": total_fetched,
                "limited": bool(progress.get("limited")),
                "subcomments_completed": not args.skip_subcomments,
                "completed_at": int(time.time()),
            },
        )
        return

    while True:
        requested_cursor = cursor
        response = await _retry(
            lambda: client.get_video_comments(video_id, next=requested_cursor),
            label=f"comments aid={video_id} cursor={requested_cursor}",
            attempts=args.retries,
            base_delay=max(1.0, args.min_sleep),
        )
        cursor_obj = response.get("cursor") or {}
        raw_replies = response.get("replies") or []
        comments = [_normalize_comment(item) for item in raw_replies]
        next_cursor = int(cursor_obj.get("next") or 0)
        is_end = bool(cursor_obj.get("is_end"))
        total_fetched += len(comments)
        reached_limit = args.max_comments_per_video > 0 and total_fetched >= args.max_comments_per_video

        _atomic_write_json(
            comments_dir / "top" / f"page_{sequence:05d}.json",
            {
                "sequence": sequence,
                "requested_cursor": requested_cursor,
                "next_cursor": next_cursor,
                "is_end": is_end,
                "comments": comments,
            },
        )

        if not args.skip_subcomments:
            for raw_comment in raw_replies:
                await _crawl_sub_comments(client, video_id, raw_comment, comments_dir, args)

        stalled = next_cursor == requested_cursor and not is_end
        done = is_end or not comments or reached_limit or stalled
        _atomic_write_json(
            progress_path,
            {
                "next_sequence": sequence + 1,
                "next_cursor": next_cursor,
                "total_fetched": total_fetched,
                "done": done,
                "limited": reached_limit,
                "updated_at": int(time.time()),
            },
        )
        if done:
            _atomic_write_json(
                complete_path,
                {
                    "top_level_count": total_fetched,
                    "limited": reached_limit,
                    "subcomments_completed": not args.skip_subcomments,
                    "completed_at": int(time.time()),
                },
            )
            return
        sequence += 1
        cursor = next_cursor
        await _polite_sleep(args.min_sleep, args.max_sleep)


def _assemble_comments(video_dir: Path) -> List[Dict[str, Any]]:
    comments_dir = video_dir / "comments"
    top_comments: List[Dict[str, Any]] = []
    seen_top = set()
    for page_path in sorted((comments_dir / "top").glob("page_*.json")):
        payload = _read_json(page_path, {}) or {}
        for comment in payload.get("comments") or []:
            rpid = str(comment.get("rpid") or "")
            if rpid and rpid not in seen_top:
                seen_top.add(rpid)
                top_comments.append(comment)

    for comment in top_comments:
        root_rpid = str(comment.get("rpid") or "")
        sub_comments: List[Dict[str, Any]] = []
        seen_sub = set()
        for page_path in sorted((comments_dir / "sub" / root_rpid / "pages").glob("page_*.json")):
            payload = _read_json(page_path, {}) or {}
            for sub_comment in payload.get("comments") or []:
                rpid = str(sub_comment.get("rpid") or "")
                if rpid and rpid not in seen_sub:
                    seen_sub.add(rpid)
                    sub_comment.pop("sub_comments", None)
                    sub_comments.append(sub_comment)
        comment["sub_comments"] = sub_comments
    return top_comments


async def _fetch_danmaku(client: Any, detail: Dict[str, Any], video_dir: Path, args: argparse.Namespace) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    for page in detail.get("pages") or []:
        cid = int(page.get("cid") or 0)
        if not cid:
            continue
        output_path = video_dir / "danmaku" / f"cid_{cid}.json"
        payload = _read_json(output_path, None)
        if payload is None:
            url = f"https://comment.bilibili.com/{cid}.xml"

            async def request_xml() -> str:
                async with httpx.AsyncClient(
                    proxy=getattr(client, "proxy", None),
                    headers=getattr(client, "headers", None),
                    follow_redirects=True,
                    timeout=getattr(client, "timeout", 60),
                ) as http_client:
                    response = await http_client.get(url)
                    response.raise_for_status()
                    return response.text

            xml_text = await _retry(
                request_xml,
                label=f"danmaku cid={cid}",
                attempts=args.retries,
                base_delay=max(1.0, args.min_sleep),
            )
            rows = _parse_danmaku_xml(xml_text, cid)
            payload = {
                "cid": cid,
                "part": page.get("part") or "",
                "source_url": url,
                "count": len(rows),
                "danmaku": rows,
                "note": "Current XML danmaku snapshot; historical completeness is not guaranteed.",
            }
            _atomic_write_json(output_path, payload)
            await _polite_sleep(args.min_sleep, args.max_sleep)
        all_rows.extend(payload.get("danmaku") or [])
    return all_rows


async def _crawl_video(client: Any, creator_path: Path, catalog_video: Dict[str, Any], args: argparse.Namespace) -> None:
    key = _video_key(catalog_video)
    if not key:
        return
    video_dir = creator_path / "videos" / _slug(key, "unknown_video")
    complete_path = video_dir / "complete.json"
    complete = _read_json(complete_path, {}) or {}
    requested_comments = not args.skip_comments
    requested_subcomments = requested_comments and not args.skip_subcomments
    requested_danmaku = args.danmaku
    complete_satisfies_request = bool(complete) and (
        (not requested_comments or complete.get("comments_completed"))
        and (not requested_subcomments or complete.get("subcomments_completed"))
        and (not requested_danmaku or complete.get("danmaku_completed"))
    )
    if complete_satisfies_request:
        return

    detail_path = video_dir / "detail.json"
    detail = _read_json(detail_path, None)
    if detail is None:
        aid = int(catalog_video.get("aid") or 0)
        bvid = str(catalog_video.get("bvid") or "")
        response = await _retry(
            lambda: client.get_video_info(aid=aid or None, bvid=bvid or None),
            label=f"video detail {bvid or aid}",
            attempts=args.retries,
            base_delay=max(1.0, args.min_sleep),
        )
        detail = _normalize_video_detail(response, catalog_video)
        _atomic_write_json(detail_path, detail)

    if not args.skip_comments:
        await _crawl_comments(client, str(detail.get("aid") or catalog_video.get("aid") or ""), video_dir, args)
        comments = _assemble_comments(video_dir)
    else:
        comments = []

    danmaku: List[Dict[str, Any]] = []
    if args.danmaku:
        danmaku = await _fetch_danmaku(client, detail, video_dir, args)

    final_payload = {
        **detail,
        "comments": comments,
        "comment_count_crawled": len(comments),
        "sub_comment_count_crawled": sum(len(item.get("sub_comments") or []) for item in comments),
        "danmaku": danmaku,
        "danmaku_count_crawled": len(danmaku),
        "completed_at": int(time.time()),
    }
    _atomic_write_json(video_dir / "video.json", final_payload)
    _atomic_write_json(
        complete_path,
        {
            "video_id": key,
            "comments": len(comments),
            "sub_comments": final_payload["sub_comment_count_crawled"],
            "danmaku": len(danmaku),
            "comments_completed": requested_comments,
            "subcomments_completed": requested_subcomments,
            "danmaku_completed": requested_danmaku,
            "completed_at": final_payload["completed_at"],
        },
    )


async def crawl_creators(client: Any, rows: Sequence[Dict[str, Any]], output_root: Path, args: argparse.Namespace) -> None:
    errors_path = output_root / "state" / "crawl_errors.jsonl"
    for creator_index, row in enumerate(rows, start=1):
        creator_path = _creator_dir(output_root, row)
        videos = _read_jsonl(creator_path / "videos.jsonl")
        if not videos:
            print(f"[{creator_index}/{len(rows)}] no catalog, skip uid={row.get('author_id')}")
            continue
        if args.max_videos_per_creator > 0:
            videos = videos[: args.max_videos_per_creator]
        print(f"[{creator_index}/{len(rows)}] crawl uid={row.get('author_id')}: {len(videos)} videos")
        for video_index, video in enumerate(videos, start=1):
            key = _video_key(video)
            print(f"  [{video_index}/{len(videos)}] {key} {video.get('title', '')}")
            try:
                await _crawl_video(client, creator_path, video, args)
            except (KeyboardInterrupt, asyncio.CancelledError):
                raise
            except Exception as exc:
                _append_jsonl(
                    errors_path,
                    {
                        "creator_id": row.get("author_id") or "",
                        "creator_name": row.get("author") or "",
                        "video_id": key,
                        "stage": "crawl",
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:1000],
                        "time": int(time.time()),
                    },
                )
                print(f"  crawl failed {key}: {exc}")
                if _is_browser_session_closed_error(exc):
                    raise RuntimeError(
                        "Browser session closed; stopping so the process supervisor can restart safely."
                    ) from exc
            await _polite_sleep(args.min_sleep, args.max_sleep)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and crawl a reviewed Bilibili creator list.")
    parser.add_argument("command", choices=["prepare", "catalog", "crawl", "all"])
    parser.add_argument("--input-csv", type=Path, default=DEFAULT_INPUT_CSV)
    parser.add_argument("--input-xlsx", type=Path, default=None, help="Optional rebuild source; overrides --input-csv")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--sheet", default=None)
    parser.add_argument("--decision-column", default=DEFAULT_DECISION_COLUMN)
    parser.add_argument("--include-ambiguous", action="store_true")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--batch-index", type=int, default=0, help="1-based batch; 0 means all prepared creators")
    parser.add_argument("--catalog-page-size", type=int, default=30)
    parser.add_argument("--sub-comment-page-size", type=int, default=20)
    parser.add_argument("--max-comments-per-video", type=int, default=0, help="0 means no script-side limit")
    parser.add_argument("--max-videos-per-creator", type=int, default=0, help="0 means all catalog videos")
    parser.add_argument("--skip-comments", action="store_true")
    parser.add_argument("--skip-subcomments", action="store_true")
    parser.add_argument(
        "--danmaku",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="crawl current danmaku snapshots (default: enabled; use --no-danmaku to disable)",
    )
    parser.add_argument("--min-sleep", type=float, default=1.5)
    parser.add_argument("--max-sleep", type=float, default=3.5)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument(
        "--server-headless",
        action="store_true",
        help="use Playwright's bundled headless Chromium; suitable for a Linux server without a GUI",
    )
    parser.add_argument(
        "--cookie-file",
        type=Path,
        default=None,
        help="UTF-8 Bilibili Cookie header value; server mode also accepts BILIBILI_COOKIE",
    )
    parser.add_argument(
        "--browser-profile-dir",
        type=Path,
        default=None,
        help="persistent browser profile (default: <output-root>/state/browser_profile)",
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    args.input_csv = args.input_csv.expanduser().resolve()
    if args.input_xlsx is not None:
        args.input_xlsx = args.input_xlsx.expanduser().resolve()
    args.output_root = args.output_root.expanduser().resolve()
    if args.cookie_file is not None:
        args.cookie_file = args.cookie_file.expanduser().resolve()
    if args.browser_profile_dir is not None:
        args.browser_profile_dir = args.browser_profile_dir.expanduser().resolve()
    if args.retries <= 0:
        raise ValueError("retries must be greater than zero")
    if args.catalog_page_size <= 0 or args.catalog_page_size > 30:
        raise ValueError("catalog_page_size must be between 1 and 30")

    def prepare() -> Dict[str, Any]:
        if args.input_xlsx is not None:
            return prepare_inputs(
                args.input_xlsx,
                args.output_root,
                batch_size=args.batch_size,
                sheet_name=args.sheet,
                decision_column=args.decision_column,
                include_ambiguous=args.include_ambiguous,
            )
        return prepare_csv_inputs(
            args.input_csv,
            args.output_root,
            batch_size=args.batch_size,
            decision_column="result",
            include_ambiguous=args.include_ambiguous,
        )

    if args.command in {"prepare", "all"}:
        manifest = prepare()
        print(
            f"prepared {manifest['selected_creator_count']} creators in "
            f"{manifest['batch_count']} batches -> {manifest['selected_csv']}"
        )
        if manifest["ambiguous_decision_values"] and not args.include_ambiguous:
            print(f"excluded ambiguous decisions: {manifest['ambiguous_decision_values']}")
    if args.command == "prepare":
        return 0

    selected_csv = args.output_root / "inputs" / "creators_selected.csv"
    if not selected_csv.exists():
        manifest = prepare()
        print(f"auto-prepared {manifest['selected_creator_count']} creators")
    rows = _load_creator_rows(args.output_root, args.batch_index)
    if not rows:
        print("No selected creators found.")
        return 1

    original_cwd = Path.cwd()
    os.chdir(PROJECT_ROOT)
    try:
        async with BilibiliSession(args) as session:
            if args.command in {"catalog", "all"}:
                await catalog_creators(session.client, rows, args.output_root, args)
            if args.command in {"crawl", "all"}:
                await crawl_creators(session.client, rows, args.output_root, args)
    finally:
        os.chdir(original_cwd)
    return 0


def main() -> int:
    args = _build_parser().parse_args()
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        print("Interrupted. Rerun the same command to resume from completed pages.")
        return 130
    except Exception as exc:
        print(f"Fatal error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
