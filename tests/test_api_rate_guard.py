"""Tests for persistent API rate-limit helpers."""

from datetime import datetime, timezone
from types import SimpleNamespace

from output.api_rate_guard import (
    optional_api_skip_reason,
    rate_limit_snapshot_from_headers,
    record_snapshot,
)


def test_record_snapshot_persists_and_updates_legacy_meta(db):
    reset_at = datetime(2026, 4, 23, 13, 0, tzinfo=timezone.utc)

    snapshot_id = record_snapshot(
        db,
        "x",
        endpoint="GET /2/tweets",
        remaining="4",
        limit="100",
        reset_at=reset_at,
        raw_metadata={"source": "unit"},
        fetched_at=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
    )

    latest = db.get_latest_api_rate_limit_snapshot("x", "GET /2/tweets")
    assert snapshot_id == latest["id"]
    assert latest["remaining"] == 4
    assert latest["limit"] == 100
    assert latest["reset_at"] == reset_at.isoformat()
    assert latest["raw_metadata"] == {"source": "unit"}
    assert db.get_meta("api_rate_limit:x:remaining") == "4"
    assert db.get_meta("api_rate_limit:x:reset_at") == reset_at.isoformat()


def test_record_snapshot_extracts_common_headers(db):
    headers = {
        "x-ratelimit-remaining": "42",
        "x-ratelimit-limit": "5000",
        "x-ratelimit-reset": "1770000000",
    }

    record_snapshot(db, "github", endpoint="/user/repos", headers=headers)

    latest = db.get_latest_api_rate_limit_snapshot("github", "/user/repos")
    assert latest["remaining"] == 42
    assert latest["limit"] == 5000
    assert latest["reset_at"].startswith("2026-02-")
    assert latest["raw_metadata"]["headers"]["x-ratelimit-remaining"] == "42"


def test_header_snapshot_ignores_responses_without_remaining_budget():
    assert rate_limit_snapshot_from_headers("x", {}, endpoint="GET /2/tweets") is None


def test_optional_skip_reason_still_reads_legacy_meta(db):
    db.set_meta("api_rate_limit:anthropic:remaining", "5")
    config = SimpleNamespace(rate_limits=SimpleNamespace(anthropic_min_remaining=5))

    reason = optional_api_skip_reason(
        config,
        db,
        "anthropic",
        operation="draft replies",
        now=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
    )

    assert "anthropic API remaining budget 5" in reason
    assert "draft replies" in reason
