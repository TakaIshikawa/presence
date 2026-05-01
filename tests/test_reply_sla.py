"""Tests for reply SLA breach reporting."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from engagement.reply_sla import (
    build_reply_sla_report,
    format_json_report,
    format_text_report,
)
from reply_sla import main


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _mock_script_context(db):
    @contextmanager
    def _ctx():
        yield (SimpleNamespace(), db)

    return _ctx


def _insert_reply(db, tweet_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="Nice post",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="Thanks",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (detected_at, reply_id),
    )
    db.conn.commit()


def test_report_buckets_by_priority_threshold_and_orders_next_actions(db):
    breached_high = _insert_reply(db, "breached-high", priority="high", platform="x")
    due_normal = _insert_reply(db, "due-normal", priority="normal", platform="bluesky")
    within_low = _insert_reply(db, "within-low", priority="low", platform="x")
    breached_normal = _insert_reply(db, "breached-normal", priority="normal", platform="x")
    _set_detected_at(db, breached_high, "2026-04-23 05:00:00")
    _set_detected_at(db, due_normal, "2026-04-22 18:00:00")
    _set_detected_at(db, within_low, "2026-04-22 12:00:00")
    _set_detected_at(db, breached_normal, "2026-04-21 11:00:00")

    report = build_reply_sla_report(db, now=NOW)

    assert report["counts"] == {"breached": 2, "due_soon": 1, "within_sla": 1}
    assert report["by_platform"] == {"x": 3, "bluesky": 1}
    assert report["by_priority"] == {"normal": 2, "high": 1, "low": 1}
    assert [item["inbound_tweet_id"] for item in report["items"]] == [
        "breached-high",
        "breached-normal",
        "due-normal",
        "within-low",
    ]
    assert report["items"][0]["sla_status"] == "breached"
    assert report["items"][0]["hours_remaining"] == -1.0
    assert report["items"][2]["sla_status"] == "due_soon"


def test_report_includes_relationship_context_and_stable_json(db):
    ctx = json.dumps(
        {
            "tier_name": "Key Network",
            "dunbar_tier": 2,
            "relationship_strength": 0.8,
            "stage": "warm",
            "extra": "ignored",
        }
    )
    reply_id = _insert_reply(
        db,
        "json-row",
        platform="bluesky",
        priority="high",
        relationship_context=ctx,
    )
    _set_detected_at(db, reply_id, "2026-04-23 06:00:00")

    report = build_reply_sla_report(db, now=NOW)
    decoded = json.loads(format_json_report(report))

    assert decoded["by_relationship_tier"] == {"Key Network (tier 2)": 1}
    assert decoded["items"][0]["relationship_tier"] == "Key Network (tier 2)"
    assert decoded["items"][0]["relationship_context"] == {
        "dunbar_tier": 2,
        "relationship_strength": 0.8,
        "stage": "warm",
        "tier_name": "Key Network",
    }
    assert decoded["items"][0]["age_hours"] == 6.0


def test_text_output_is_stable_for_empty_and_populated_reports(db):
    empty = build_reply_sla_report(db, now=NOW, platform="bluesky")
    assert format_text_report(empty) == "\n".join(
        [
            "Reply SLA Report",
            "Generated: 2026-04-23T12:00:00+00:00",
            "Pending: 0",
            "Filters: platform=bluesky",
            "Thresholds: high=6h, normal=24h, low=72h",
            "Buckets: breached=0, due_soon=0, within_sla=0",
            "Platforms: {}",
            "Priorities: {}",
            "",
            "No pending replies matched.",
        ]
    )

    reply_id = _insert_reply(db, "tw-1", priority="normal", platform="x")
    _set_detected_at(db, reply_id, "2026-04-22 12:00:00")
    populated = build_reply_sla_report(db, now=NOW)

    assert format_text_report(populated) == "\n".join(
        [
            "Reply SLA Report",
            "Generated: 2026-04-23T12:00:00+00:00",
            "Pending: 1",
            "Filters: none",
            "Thresholds: high=6h, normal=24h, low=72h",
            "Buckets: breached=1, due_soon=0, within_sla=0",
            "Platforms: x=1",
            "Priorities: normal=1",
            "",
            "Breached",
            "  #1 24.0h/24h normal x @alice unknown remaining=0.0h target=tw-1",
        ]
    )


def test_partial_reply_queue_schema_does_not_crash_builder():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            draft_text TEXT,
            detected_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO reply_queue (id, draft_text, detected_at) VALUES (?, ?, ?)",
        (7, "hello", "2026-04-23 00:00:00"),
    )

    report = build_reply_sla_report(conn, now=NOW)

    assert report["total_pending"] == 1
    assert report["items"][0]["priority"] == "normal"
    assert report["items"][0]["platform"] == "x"
    assert report["items"][0]["sla_status"] == "within_sla"


def test_limit_and_platform_filter_apply_after_sla_ordering(db):
    x_reply = _insert_reply(db, "x-reply", priority="high", platform="x")
    bsky_reply = _insert_reply(db, "bsky-reply", priority="high", platform="bluesky")
    _set_detected_at(db, x_reply, "2026-04-23 05:00:00")
    _set_detected_at(db, bsky_reply, "2026-04-23 04:00:00")

    report = build_reply_sla_report(db, platform="x", limit=1, now=NOW)

    assert report["total_pending"] == 1
    assert report["items"][0]["inbound_tweet_id"] == "x-reply"
    assert report["filters"] == {"limit": 1, "platform": "x"}


def test_main_json_output(capsys):
    class FakeDb:
        def __init__(self):
            self.conn = sqlite3.connect(":memory:")
            self.conn.execute(
                """CREATE TABLE reply_queue (
                    id INTEGER PRIMARY KEY,
                    status TEXT,
                    detected_at TEXT,
                    priority TEXT,
                    platform TEXT,
                    inbound_author_handle TEXT,
                    inbound_tweet_id TEXT
                )"""
            )
            self.conn.execute(
                """INSERT INTO reply_queue
                   (id, status, detected_at, priority, platform, inbound_author_handle, inbound_tweet_id)
                   VALUES (
                     1, 'pending', '2026-04-23 09:00:00', 'normal', 'x', 'alice', 'tw-1'
                   )"""
            )

    with patch("reply_sla.script_context", _mock_script_context(FakeDb())):
        assert main(["--format", "json", "--platform", "x", "--normal-hours", "12"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"] == {"limit": None, "platform": "x"}
    assert payload["thresholds"]["normal"] == 12.0
    assert payload["items"][0]["author"] == "alice"


@pytest.mark.parametrize(
    "argv",
    [
        ["--high-hours", "0"],
        ["--normal-hours", "-1"],
        ["--low-hours", "0"],
        ["--limit", "0"],
    ],
)
def test_invalid_threshold_arguments_exit_2(argv):
    with pytest.raises(SystemExit) as excinfo:
        main(argv)

    assert excinfo.value.code == 2
