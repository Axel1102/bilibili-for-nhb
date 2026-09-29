from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts_lite" / "bili_parallel_runner.py"
SPEC = importlib.util.spec_from_file_location("bili_parallel_runner", MODULE_PATH)
assert SPEC and SPEC.loader
parallel_runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(parallel_runner)


def test_worker_count_has_no_upper_limit() -> None:
    parallel_runner.validate_worker_count(12)


def test_worker_count_requires_at_least_two() -> None:
    with pytest.raises(ValueError, match="at least 2"):
        parallel_runner.validate_worker_count(1)
