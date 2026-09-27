#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Small Bilibili API client used by the headless creator pipeline.

The WBI signing implementation is adapted from MediaCrawler's Bilibili client:
https://github.com/NanmiCoder/MediaCrawler
It remains subject to the repository's NON-COMMERCIAL LEARNING LICENSE 1.1.
"""

from __future__ import annotations

import time
import urllib.parse
from hashlib import md5
from typing import Any, Dict, Optional, Tuple

import httpx


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


class BilibiliApiError(RuntimeError):
    pass


class BilibiliSign:
    _MIXIN_KEY_ENC_TAB = [
        46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
        27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
        37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
        22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
    ]

    def __init__(self, img_key: str, sub_key: str) -> None:
        mixin_key = img_key + sub_key
        self.salt = "".join(mixin_key[index] for index in self._MIXIN_KEY_ENC_TAB)[:32]

    def sign(self, data: Dict[str, Any]) -> Dict[str, Any]:
        signed = {**data, "wts": int(time.time())}
        signed = {
            key: "".join(char for char in str(value) if char not in "!'()*")
            for key, value in sorted(signed.items())
        }
        query = urllib.parse.urlencode(signed)
        signed["w_rid"] = md5((query + self.salt).encode()).hexdigest()
        return signed


def parse_cookie_string(cookie: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for item in cookie.split(";"):
        key, separator, value = item.strip().partition("=")
        if separator and key:
            result[key] = value
    return result


async def add_cookie_string(browser_context: Any, cookie: str) -> None:
    cookies = [
        {"name": key, "value": value, "domain": ".bilibili.com", "path": "/"}
        for key, value in parse_cookie_string(cookie).items()
    ]
    if not cookies:
        raise ValueError("No valid name=value entries were found in the Bilibili cookie.")
    await browser_context.add_cookies(cookies)


class BilibiliApiClient:
    def __init__(
        self,
        *,
        playwright_page: Any,
        headers: Dict[str, str],
        proxy: Optional[str] = None,
        timeout: float = 60,
    ) -> None:
        self.playwright_page = playwright_page
        self.headers = headers
        self.proxy = proxy
        self.timeout = timeout
        self._host = "https://api.bilibili.com"

    @classmethod
    async def from_browser(
        cls,
        browser_context: Any,
        page: Any,
        *,
        user_agent: str = DEFAULT_USER_AGENT,
        proxy: Optional[str] = None,
    ) -> "BilibiliApiClient":
        client = cls(
            playwright_page=page,
            proxy=proxy,
            headers={
                "User-Agent": user_agent,
                "Origin": "https://www.bilibili.com",
                "Referer": "https://www.bilibili.com/",
                "Content-Type": "application/json;charset=UTF-8",
            },
        )
        await client.update_cookies(browser_context)
        return client

    async def update_cookies(self, browser_context: Any) -> None:
        values = await browser_context.cookies()
        self.headers["Cookie"] = "; ".join(
            f"{item['name']}={item['value']}" for item in values
        )

    async def request(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        async with httpx.AsyncClient(
            proxy=self.proxy,
            headers=self.headers,
            follow_redirects=True,
            timeout=self.timeout,
        ) as client:
            response = await client.get(url, params=params)
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise BilibiliApiError(
                f"Bilibili returned non-JSON data (HTTP {response.status_code})."
            ) from exc
        if payload.get("code") != 0:
            raise BilibiliApiError(
                f"Bilibili API code={payload.get('code')}: {payload.get('message') or 'unknown error'}"
            )
        data = payload.get("data")
        return data if isinstance(data, dict) else {}

    async def get_wbi_keys(self) -> Tuple[str, str]:
        storage = await self.playwright_page.evaluate("() => Object.assign({}, window.localStorage)")
        joined_urls = storage.get("wbi_img_urls", "") if isinstance(storage, dict) else ""
        if joined_urls and "-" in joined_urls:
            img_url, sub_url = joined_urls.split("-", 1)
        else:
            nav = await self.request(f"{self._host}/x/web-interface/nav")
            wbi_img = nav.get("wbi_img") or {}
            img_url = str(wbi_img.get("img_url") or "")
            sub_url = str(wbi_img.get("sub_url") or "")
        if not img_url or not sub_url:
            raise BilibiliApiError("Bilibili did not return WBI signing keys.")
        return (
            img_url.rsplit("/", 1)[-1].split(".", 1)[0],
            sub_url.rsplit("/", 1)[-1].split(".", 1)[0],
        )

    async def get(
        self,
        uri: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        sign: bool = True,
    ) -> Dict[str, Any]:
        final_params = dict(params or {})
        if sign and final_params:
            img_key, sub_key = await self.get_wbi_keys()
            final_params = BilibiliSign(img_key, sub_key).sign(final_params)
        return await self.request(f"{self._host}{uri}", final_params)

    async def pong(self) -> bool:
        try:
            response = await self.get("/x/web-interface/nav", sign=False)
        except Exception:
            return False
        return bool(response.get("isLogin"))

    async def get_creator_videos(
        self,
        creator_id: str,
        pn: int,
        ps: int = 30,
        order_mode: str = "pubdate",
    ) -> Dict[str, Any]:
        return await self.get(
            "/x/space/wbi/arc/search",
            {"mid": creator_id, "pn": pn, "ps": ps, "order": order_mode},
        )

    async def get_creator_info(self, creator_id: int) -> Dict[str, Any]:
        return await self.get("/x/space/wbi/acc/info", {"mid": creator_id})

    async def get_video_info(
        self,
        aid: Optional[int] = None,
        bvid: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not aid and not bvid:
            raise ValueError("aid or bvid is required")
        params: Dict[str, Any] = {"aid": aid} if aid else {"bvid": bvid}
        return await self.get("/x/web-interface/view/detail", params, sign=False)

    async def get_video_comments(self, video_id: str, next: int = 0) -> Dict[str, Any]:
        return await self.get(
            "/x/v2/reply/wbi/main",
            {"oid": video_id, "mode": 0, "type": 1, "ps": 20, "next": next},
        )

    async def get_video_level_two_comments(
        self,
        video_id: str,
        level_one_comment_id: int,
        pn: int,
        ps: int,
        order_mode: Any,
    ) -> Dict[str, Any]:
        mode = getattr(order_mode, "value", order_mode)
        return await self.get(
            "/x/v2/reply/reply",
            {
                "oid": video_id,
                "mode": mode,
                "type": 1,
                "ps": ps,
                "pn": pn,
                "root": level_one_comment_id,
            },
        )
