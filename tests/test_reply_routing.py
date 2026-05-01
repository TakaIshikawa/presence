"""Tests for deterministic inbound reply routing."""

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

from engagement.reply_routing import (
    apply_reply_routes,
    build_reply_routing_report,
    format_json_report,
    format_text_report,
    route_reply,
)
from route_replies import main


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
        inbound_text="How does this handle retries?",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="It retries idempotent operations.",
        intent="question",
        priority="normal",
        platform="x",
        status="pending",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (detected_at, reply_id),
    )
    db.conn.commit()


def test_spam_and_low_quality_route_to_ignore_unless_high_priority():
    spam = route_reply(
        {
            "id": 1,
            "intent": "spam",
            "priority": "low",
            "quality_score": 7.0,
            "inbound_text": "DM me for an investment opportunity",
        },
        now=NOW,
    )
    low_quality = route_reply(
        {
            "id": 2,
            "intent": "question",
            "priority": "normal",
            "quality_score": 2.5,
            "inbound_text": "Can you help?",
        },
        now=NOW,
    )
    high_priority_spam = route_reply(
        {
            "id": 3,
            "intent": "spam",
            "priority": "high",
            "quality_flags": json.dumps(["spam"]),
            "inbound_text": "crypto giveaway",
        },
        now=NOW,
    )

    assert spam.route == "ignore_spam"
    assert spam.reason == "spam signal"
    assert low_quality.route == "ignore_spam"
    assert low_quality.reason == "very low quality"
    assert high_priority_spam.route == "escalate"
    assert high_priority_spam.reason == "high priority overrides spam or low quality"


def test_relationship_rich_questions_route_to_nurture_or_reply_with_higher_urgency():
    relationship_context = json.dumps(
        {
            "tier_name": "Key Network",
            "dunbar_tier": 2,
            "relationship_strength": 0.9,
            "is_known": True,
        }
    )
    nurture = route_reply(
        {
            "id": 1,
            "intent": "question",
            "priority": "normal",
            "relationship_context": relationship_context,
            "inbound_text": "Can you explain the migration path?",
            "detected_at": "2026-04-23T10:00:00+00:00",
        },
        now=NOW,
    )
    urgent = route_reply(
        {
            "id": 2,
            "intent": "question",
            "priority": "high",
            "relationship_context": relationship_context,
            "inbound_text": "Can you explain the migration path?",
            "detected_at": "2026-04-22T08:00:00+00:00",
        },
        now=NOW,
    )

    assert nurture.route == "relationship_nurture"
    assert nurture.reason == "relationship-rich question"
    assert nurture.review_owner == "relationship"
    assert nurture.relationship_tier == "Key Network (tier 2)"
    assert urgent.route == "reply"
    assert urgent.reason == "relationship question with urgency"
    assert urgent.urgency > nurture.urgency


def test_report_orders_by_urgency_and_filters_min_urgency(db):
    bug = _insert_reply(
        db,
        "bug",
        intent="bug_report",
        priority="high",
        inbound_text="This is broken with a security error",
        quality_score=8.0,
    )
    quote = _insert_reply(
        db,
        "quote",
        intent="disagreement",
        priority="normal",
        inbound_text="The real problem is that nobody talks about maintenance",
    )
    spam = _insert_reply(
        db,
        "spam",
        intent="spam",
        priority="low",
        inbound_text="follow back for airdrop",
    )
    _set_detected_at(db, bug, "2026-04-23T08:00:00+00:00")
    _set_detected_at(db, quote, "2026-04-23T09:00:00+00:00")
    _set_detected_at(db, spam, "2026-04-23T10:00:00+00:00")

    report = build_reply_routing_report(db, now=NOW, min_urgency=40)

    assert [item["inbound_tweet_id"] for item in report["items"]] == ["bug", "quote"]
    assert report["by_route"] == {"escalate": 1, "quote_candidate": 1}
    assert report["items"][0]["route"] == "escalate"
    assert report["items"][1]["route"] == "quote_candidate"


def test_text_and_json_output_are_stable(db):
    reply_id = _insert_reply(db, "tw-1", intent="question", priority="normal")
    _set_detected_at(db, reply_id, "2026-04-23T09:00:00+00:00")
    report = build_reply_routing_report(db, now=NOW)

    decoded = json.loads(format_json_report(report))
    assert decoded["filters"] == {"limit": None, "min_urgency": None}
    assert decoded["items"][0]["route"] == "reply"

    assert format_text_report(report) == "\n".join(
        [
            "Reply Routing Matrix",
            "Generated: 2026-04-23T12:00:00+00:00",
            "Pending: 1",
            "Filters: none",
            "Routes: reply=1",
            "Owners: community=1",
            "",
            "#1 u=065 reply community x @alice direct question target=tw-1",
        ]
    )


def test_partial_schema_and_missing_table_do_not_crash():
    conn = sqlite3.connect(":memory:")
    assert build_reply_routing_report(conn, now=NOW)["items"] == []

    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            inbound_text TEXT,
            detected_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO reply_queue (id, inbound_text, detected_at) VALUES (?, ?, ?)",
        (7, "Thanks", "2026-04-23 10:00:00"),
    )

    report = build_reply_routing_report(conn, now=NOW)

    assert report["total_pending"] == 1
    assert report["items"][0]["reply_id"] == 7
    assert report["items"][0]["route"] == "reply"


def test_apply_skips_when_no_compatible_storage_exists(db):
    _insert_reply(db, "skip-apply")
    report = build_reply_routing_report(db, now=NOW)
    result = apply_reply_routes(
        db,
        [route_reply({"id": item["reply_id"], "inbound_text": "How?", "intent": "question"}, now=NOW) for item in report["items"]],
    )

    assert result == {
        "applied": 0,
        "skipped": 1,
        "storage": None,
        "message": "no compatible route storage found",
    }


def test_apply_updates_compatible_reply_queue_columns():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            inbound_text TEXT,
            intent TEXT,
            status TEXT,
            route TEXT,
            route_reason TEXT,
            route_urgency INTEGER,
            review_owner TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO reply_queue (id, inbound_text, intent, status) VALUES (1, 'How?', 'question', 'pending')"
    )

    report = build_reply_routing_report(conn, now=NOW)
    recommendation = route_reply({"id": 1, "inbound_text": "How?", "intent": "question"}, now=NOW)
    result = apply_reply_routes(conn, [recommendation])

    stored = conn.execute(
        "SELECT route, route_reason, route_urgency, review_owner FROM reply_queue WHERE id = 1"
    ).fetchone()
    assert result["applied"] == 1
    assert result["storage"] == "reply_queue"
    assert dict(stored) == {
        "route": report["items"][0]["route"],
        "route_reason": "direct question",
        "route_urgency": report["items"][0]["urgency"],
        "review_owner": "community",
    }


def test_cli_json_dry_run_is_read_only(db, capsys):
    _insert_reply(db, "json-row")

    with patch("route_replies.script_context", _mock_script_context(db)):
        assert main(["--format", "json", "--min-urgency", "40"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"] == {"limit": None, "min_urgency": 40}
    assert payload["items"][0]["route"] == "reply"
    stored = db.conn.execute("SELECT status FROM reply_queue WHERE inbound_tweet_id = 'json-row'").fetchone()
    assert stored["status"] == "pending"


def test_cli_apply_reports_skipped_on_current_schema(db, capsys):
    _insert_reply(db, "apply-skip")

    with patch("route_replies.script_context", _mock_script_context(db)):
        assert main(["--apply", "--format", "json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["apply"] == {
        "applied": 0,
        "skipped": 1,
        "storage": None,
        "message": "no compatible route storage found",
    }


@pytest.mark.parametrize(
    "argv",
    [
        ["--limit", "0"],
        ["--min-urgency", "-1"],
    ],
)
def test_invalid_cli_arguments_exit_2(argv):
    with pytest.raises(SystemExit) as excinfo:
        main(argv)

    assert excinfo.value.code == 2
