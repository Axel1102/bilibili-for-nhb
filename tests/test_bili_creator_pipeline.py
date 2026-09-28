from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts_lite" / "bili_creator_pipeline.py"
SPEC = importlib.util.spec_from_file_location("bili_creator_pipeline", MODULE_PATH)
assert SPEC and SPEC.loader
pipeline = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pipeline)


def test_prepare_inputs_keeps_exact_decisions_and_batches(tmp_path: Path) -> None:
    source = tmp_path / "reviewed.xlsx"
    frame = pd.DataFrame(
        [
            {"author_id": 1, "author": "A", "space_url": "https://space.bilibili.com/1", pipeline.DEFAULT_DECISION_COLUMN: 1},
            {"author_id": 2, "author": "B", "space_url": "https://space.bilibili.com/2", pipeline.DEFAULT_DECISION_COLUMN: 0},
            {"author_id": 3, "author": "C", "space_url": "https://space.bilibili.com/3", pipeline.DEFAULT_DECISION_COLUMN: "1？"},
            {"author_id": 4, "author": "D", "space_url": "https://space.bilibili.com/4", pipeline.DEFAULT_DECISION_COLUMN: "1"},
            {"author_id": 5, "author": "E", "space_url": "https://space.bilibili.com/5", pipeline.DEFAULT_DECISION_COLUMN: 1},
        ]
    )
    frame.to_excel(source, index=False)

    output = tmp_path / "out"
    manifest = pipeline.prepare_inputs(source, output, batch_size=2)

    assert manifest["selected_creator_count"] == 3
    assert manifest["batch_count"] == 2
    rows = pipeline._load_creator_rows(output, 0)
    assert [row["author_id"] for row in rows] == ["1", "4", "5"]
    assert len(pipeline._load_creator_rows(output, 1)) == 2
    assert len(pipeline._load_creator_rows(output, 2)) == 1


def test_prepare_inputs_can_include_ambiguous_decisions(tmp_path: Path) -> None:
    source = tmp_path / "reviewed.xlsx"
    pd.DataFrame(
        [
            {"author_id": 3, "author": "C", pipeline.DEFAULT_DECISION_COLUMN: "1？"},
            {"author_id": 6, "author": "F", pipeline.DEFAULT_DECISION_COLUMN: "副教授分享"},
        ]
    ).to_excel(source, index=False)

    manifest = pipeline.prepare_inputs(source, tmp_path / "out", include_ambiguous=True)
    assert manifest["selected_creator_count"] == 1


def test_prepare_csv_inputs_uses_result_column(tmp_path: Path) -> None:
    source = tmp_path / "new.csv"
    source.write_text(
        "author_id,author,url,result\n"
        "1,A,https://space.bilibili.com/1,1\n"
        "2,B,https://space.bilibili.com/2,1？\n"
        "3,C,https://space.bilibili.com/3,0\n",
        encoding="utf-8-sig",
    )
    output = tmp_path / "out"
    manifest = pipeline.prepare_csv_inputs(source, output, batch_size=100)
    assert manifest["source_kind"] == "csv"
    assert manifest["selected_creator_count"] == 1
    assert pipeline._load_creator_rows(output, 0)[0]["author_id"] == "1"


def test_normalize_comment_preserves_ids_and_text() -> None:
    result = pipeline._normalize_comment(
        {
            "rpid": 123,
            "root": 100,
            "parent": 100,
            "ctime": 1700000000,
            "like": 8,
            "rcount": 2,
            "member": {"mid": "9", "uname": "评论者", "level_info": {"current_level": 5}},
            "content": {"message": "测试评论"},
            "reply_control": {"location": "IP属地：上海"},
        }
    )
    assert result["rpid"] == "123"
    assert result["root_rpid"] == "100"
    assert result["user_name"] == "评论者"
    assert result["message"] == "测试评论"


def test_parse_danmaku_xml() -> None:
    xml = '<?xml version="1.0" encoding="UTF-8"?><i><d p="1.5,1,25,16777215,1700000000,0,abc,99">A&amp;B</d></i>'
    rows = pipeline._parse_danmaku_xml(xml, 42)
    assert rows == [
        {
            "cid": 42,
            "progress_seconds": 1.5,
            "mode": 1,
            "font_size": 25,
            "color": 16777215,
            "created_time": 1700000000,
            "pool": 0,
            "user_hash": "abc",
            "danmaku_id": "99",
            "text": "A&B",
        }
    ]


def test_subcomment_normalization_exposes_reply_count() -> None:
    result = pipeline._normalize_comment({"rpid": 10, "rcount": 3})
    assert result["reply_count"] == 3


def test_danmaku_is_enabled_by_default() -> None:
    args = pipeline._build_parser().parse_args(["crawl"])
    assert args.danmaku is True
    args = pipeline._build_parser().parse_args(["crawl", "--no-danmaku"])
    assert args.danmaku is False


def test_server_headless_arguments() -> None:
    args = pipeline._build_parser().parse_args(
        ["crawl", "--server-headless", "--cookie-file", "/tmp/bili.cookie"]
    )
    assert args.server_headless is True
    assert args.cookie_file == Path("/tmp/bili.cookie")


def test_load_server_cookie_prefers_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BILIBILI_COOKIE", "from-env")
    cookie_file = tmp_path / "bili.cookie"
    cookie_file.write_text("SESSDATA=from-file; DedeUserID=1\n", encoding="utf-8")
    assert pipeline._load_server_cookie(cookie_file) == "SESSDATA=from-file; DedeUserID=1"
    assert pipeline._load_server_cookie(None) == "from-env"


@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("Target page, context or browser has been closed"),
        RuntimeError("Browser has been closed"),
        type("TargetClosedError", (RuntimeError,), {})("closed"),
    ],
)
def test_browser_session_closed_errors_are_fatal(exc: BaseException) -> None:
    assert pipeline._is_browser_session_closed_error(exc)


def test_normal_api_error_is_not_a_browser_shutdown() -> None:
    assert not pipeline._is_browser_session_closed_error(RuntimeError("risk control"))


def test_video_resume_check_respects_requested_components(tmp_path: Path) -> None:
    creator_path = tmp_path / "creator"
    video = {"bvid": "BV123"}
    complete_path = creator_path / "videos" / "BV123" / "complete.json"
    pipeline._atomic_write_json(
        complete_path,
        {
            "comments_completed": True,
            "subcomments_completed": True,
            "danmaku_completed": False,
        },
    )
    args = SimpleNamespace(skip_comments=False, skip_subcomments=False, danmaku=True)
    assert not pipeline._video_complete_for_request(creator_path, video, args)
    args.danmaku = False
    assert pipeline._video_complete_for_request(creator_path, video, args)


def test_creator_shards_are_complete_unique_and_balanced() -> None:
    rows = [{"author_id": str(index)} for index in range(8)]
    weights = [20, 15, 10, 8, 7, 6, 4, 2]
    assignments, loads = pipeline._assign_creator_shards(rows, weights, 3)
    assigned_ids = [row["author_id"] for shard in assignments for row in shard]
    assert sorted(assigned_ids) == [str(index) for index in range(8)]
    assert len(assigned_ids) == len(set(assigned_ids))
    assert max(loads) - min(loads) <= max(weights)


def test_shard_arguments_are_one_based() -> None:
    args = pipeline._build_parser().parse_args(
        ["crawl", "--shard-count", "4", "--shard-index", "3"]
    )
    assert args.shard_count == 4
    assert args.shard_index == 3


@pytest.mark.asyncio
async def test_comment_crawl_can_add_subcomments_on_a_later_run(tmp_path: Path) -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.top_calls = 0
            self.sub_calls = 0

        async def get_video_comments(self, video_id: str, next: int = 0):
            self.top_calls += 1
            return {
                "cursor": {"next": 1, "is_end": True},
                "replies": [
                    {
                        "rpid": 100,
                        "root": 100,
                        "parent": 0,
                        "rcount": 1,
                        "content": {"message": "top"},
                    }
                ],
            }

        async def get_video_level_two_comments(self, video_id, root, pn, ps, order):
            self.sub_calls += 1
            return {
                "page": {"count": 1},
                "replies": [
                    {
                        "rpid": 101,
                        "root": 100,
                        "parent": 100,
                        "content": {"message": "sub"},
                    }
                ],
            }

    client = FakeClient()
    args = SimpleNamespace(
        skip_subcomments=True,
        max_comments_per_video=0,
        retries=1,
        min_sleep=0,
        max_sleep=0,
        sub_comment_page_size=20,
    )
    await pipeline._crawl_comments(client, "1", tmp_path, args)
    assert client.top_calls == 1
    assert client.sub_calls == 0

    args.skip_subcomments = False
    await pipeline._crawl_comments(client, "1", tmp_path, args)
    comments = pipeline._assemble_comments(tmp_path)
    assert client.top_calls == 1
    assert client.sub_calls == 1
    assert comments[0]["sub_comments"][0]["message"] == "sub"
