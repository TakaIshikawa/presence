"""Tests for reply draft expiry planning."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from engagement.reply_draft_expiry import (  # noqa: E402
    build_reply_draft_expiry_plan,
    format_reply_draft_expiry_json,
    format_reply_draft_expiry_text,
    inspect_reply_draft_expiry,
)
from plan_reply_draft_expiry import main  # noqa: E402


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
        draft_text="Thanks for sharing this.",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (detected_at, reply_id),
    )
    db.conn.commit()


def test_old_pending_draft_requires_regeneration(db):
    reply_id = _insert_reply(db, "old", platform="x")
    _set_detected_at(db, reply_id, "2026-04-21 11:00:00")

    plan = build_reply_draft_expiry_plan(
        db,
        max_draft_age_hours=24,
        max_context_age_hours=12,
        now=NOW,
    )

    item = plan["items"][0]
    assert item["id"] == reply_id
    assert item["age_hours"] == 49.0
    assert item["reason_codes"] == ["draft_age_exceeded"]
    assert item["recommended_action"] == "regenerate"
    assert item["urgency"] == "high"
    assert plan["groups"]["x"]["high"][0]["reply_id"] == "old"


def test_old_context_timestamp_requires_context_recheck(db):
    reply_id = _insert_reply(
        db,
        "old-context",
        platform="bluesky",
        platform_metadata=json.dumps(
            {
                "root_post_text": "Original",
                "context_refreshed_at": "2026-04-22T06:00:00+00:00",
            }
        ),
    )
    _set_detected_at(db, reply_id, "2026-04-23 10:00:00")

    plan = build_reply_draft_expiry_plan(
        db,
        max_draft_age_hours=24,
        max_context_age_hours=24,
        now=NOW,
    )

    item = plan["items"][0]
    assert item["id"] == reply_id
    assert item["context_age_hours"] == 30.0
    assert item["reason_codes"] == ["context_age_exceeded"]
    assert item["recommended_action"] == "recheck_context"
    assert item["urgency"] == "normal"
    assert plan["groups"]["bluesky"]["normal"][0]["reply_id"] == "old-context"


def test_relationship_context_timestamp_is_considered(db):
    reply_id = _insert_reply(
        db,
        "relationship",
        relationship_context=json.dumps(
            {
                "x_handle": "alice",
                "updated_at": "2026-04-21T12:00:00+00:00",
            }
        ),
    )
    _set_detected_at(db, reply_id, "2026-04-23 11:30:00")

    plan = build_reply_draft_expiry_plan(
        db,
        max_draft_age_hours=24,
        max_context_age_hours=24,
        now=NOW,
    )

    assert plan["items"][0]["reply_id"] == "relationship"
    assert plan["items"][0]["context_age_hours"] == 48.0
    assert plan["items"][0]["recommended_action"] == "recheck_context"


def test_fresh_draft_with_fresh_context_is_kept(db):
    reply_id = _insert_reply(
        db,
        "fresh",
        platform_metadata=json.dumps({"fetched_at": "2026-04-23T11:00:00+00:00"}),
    )
    _set_detected_at(db, reply_id, "2026-04-23 10:00:00")

    plan = build_reply_draft_expiry_plan(
        db,
        max_draft_age_hours=24,
        max_context_age_hours=24,
        now=NOW,
    )

    item = plan["items"][0]
    assert item["stale"] is False
    assert item["reason_codes"] == []
    assert item["recommended_action"] == "keep"
    assert plan["counts"] == {
        "stale": 0,
        "keep": 1,
        "recheck_context": 0,
        "regenerate": 0,
    }


def test_draft_age_takes_priority_over_context_recheck(db):
    row = {
        "id": 10,
        "status": "pending",
        "platform": "x",
        "inbound_tweet_id": "both",
        "inbound_author_handle": "alice",
        "detected_at": "2026-04-21 12:00:00",
        "platform_metadata": json.dumps({"context_at": "2026-04-21T12:00:00+00:00"}),
    }

    item = inspect_reply_draft_expiry(
        row,
        max_draft_age_hours=24,
        max_context_age_hours=24,
        now=NOW,
    )

    assert item["reason_codes"] == ["draft_age_exceeded", "context_age_exceeded"]
    assert item["recommended_action"] == "regenerate"


def test_status_filter_and_partial_schema_are_read_only():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            status TEXT,
            inbound_tweet_id TEXT,
            detected_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO reply_queue (id, status, inbound_tweet_id, detected_at) VALUES (?, ?, ?, ?)",
        (1, "pending", "pending", "2026-04-21 12:00:00"),
    )
    conn.execute(
        "INSERT INTO reply_queue (id, status, inbound_tweet_id, detected_at) VALUES (?, ?, ?, ?)",
        (2, "approved", "approved", "2026-04-21 12:00:00"),
    )

    plan = build_reply_draft_expiry_plan(
        conn,
        status_filter=["approved"],
        max_draft_age_hours=24,
        now=NOW,
    )
    statuses = conn.execute(
        "SELECT inbound_tweet_id, status FROM reply_queue ORDER BY id"
    ).fetchall()

    assert [item["reply_id"] for item in plan["items"]] == ["approved"]
    assert [(row["inbound_tweet_id"], row["status"]) for row in statuses] == [
        ("pending", "pending"),
        ("approved", "approved"),
    ]


def test_text_and_json_formatters_are_deterministic(db):
    old = _insert_reply(db, "old", platform="x", inbound_author_handle="zoe")
    recheck = _insert_reply(
        db,
        "ctx",
        platform="bluesky",
        inbound_author_handle="bob",
        relationship_context=json.dumps({"updated_at": "2026-04-21T12:00:00+00:00"}),
    )
    _set_detected_at(db, old, "2026-04-21 12:00:00")
    _set_detected_at(db, recheck, "2026-04-23 10:00:00")

    plan = build_reply_draft_expiry_plan(
        db,
        max_draft_age_hours=24,
        max_context_age_hours=24,
        now=NOW,
    )
    payload = json.loads(format_reply_draft_expiry_json(plan))
    text = format_reply_draft_expiry_text(plan)

    assert payload["items"][0]["reply_id"] == "ctx"
    assert payload["items"][1]["reply_id"] == "old"
    assert "Reply Draft Expiry Plan" in text
    assert "bluesky" in text
    assert "action=recheck_context reasons=context_age_exceeded" in text
    assert "#1 reply=old @zoe age=48.0h" in text


def test_cli_json_output(capsys):
    class FakeDb:
        conn = sqlite3.connect(":memory:")

    FakeDb.conn.row_factory = sqlite3.Row
    FakeDb.conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            status TEXT,
            platform TEXT,
            inbound_tweet_id TEXT,
            inbound_author_handle TEXT,
            detected_at TEXT
        )"""
    )
    FakeDb.conn.execute(
        """INSERT INTO reply_queue
           (id, status, platform, inbound_tweet_id, inbound_author_handle, detected_at)
           VALUES (1, 'pending', 'x', 'reply-1', 'alice', '2026-04-21 12:00:00')"""
    )

    fixed_plan = build_reply_draft_expiry_plan(
        FakeDb(),
        max_draft_age_hours=24,
        now=NOW,
    )

    with patch("plan_reply_draft_expiry.script_context", _mock_script_context(FakeDb())), patch(
        "plan_reply_draft_expiry.build_reply_draft_expiry_plan",
        return_value=fixed_plan,
    ):
        result = main(
            [
                "--max-draft-age-hours",
                "24",
                "--max-context-age-hours",
                "12",
                "--format",
                "json",
            ]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["thresholds"]["max_draft_age_hours"] == 24
    assert payload["items"][0]["recommended_action"] == "regenerate"
