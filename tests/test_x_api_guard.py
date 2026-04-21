"""Tests for X API circuit breaker helpers."""

from datetime import datetime, timedelta, timezone

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.x_api_guard import (
    get_x_api_block_reason,
    is_x_api_block_error,
    mark_x_api_blocked,
)


class _MetaDB:
    def __init__(self):
        self.meta = {}

    def get_meta(self, key):
        return self.meta.get(key)

    def set_meta(self, key, value):
        self.meta[key] = value


def test_detects_credit_errors():
    assert is_x_api_block_error("402 Payment Required") is True
    assert is_x_api_block_error("does not have any credits") is True
    assert is_x_api_block_error("429 Too Many Requests") is False
    assert is_x_api_block_error("403 reply not allowed") is False


def test_block_reason_active_until_expiry():
    db = _MetaDB()
    now = datetime(2026, 4, 21, tzinfo=timezone.utc)

    mark_x_api_blocked(db, "402 Payment Required", hours=2, now=now)

    assert get_x_api_block_reason(db, now=now + timedelta(hours=1)) is not None
    assert get_x_api_block_reason(db, now=now + timedelta(hours=3)) is None
