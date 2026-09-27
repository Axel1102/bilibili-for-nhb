from __future__ import annotations

from scripts_lite.bili_standalone_client import BilibiliSign, parse_cookie_string


def test_parse_cookie_string_keeps_equals_in_values() -> None:
    assert parse_cookie_string("SESSDATA=a=b==; DedeUserID=123; empty=") == {
        "SESSDATA": "a=b==",
        "DedeUserID": "123",
        "empty": "",
    }


def test_wbi_sign_does_not_mutate_input() -> None:
    original = {"mid": 1, "order": "pubdate"}
    signer = BilibiliSign("7cd084941338484aae1ad9425b84077c", "4932caff0ff746eab6f01bf08b70ac45")
    signed = signer.sign(original)
    assert original == {"mid": 1, "order": "pubdate"}
    assert signed["mid"] == "1"
    assert len(signed["w_rid"]) == 32
    assert signed["wts"].isdigit()
