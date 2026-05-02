"""Tests for inbound mention response latency reporting."""

from __future__ import annotations

import csv
import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from engagement.mention_response_latency import (
    build_mention_response_latency_report,
    format_mention_response_latency_csv,
    format_mention_response_latency_json,
)
from mention_response_latency import main


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def _mock_script_context(db):
    @contextmanager
    def _ctx():
        yield (SimpleNamespace(), db)

    return _ctx


def _insert_mention(db, tweet_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="Can you explain this?",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="Thanks for asking.",
        status="pending",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_times(db, reply_id: int, **values: str | None) -> None:
    assignments = ", ".join(f"{column} = ?" for column in values)
    db.conn.execute(
        f"UPDATE reply_queue SET {assignments} WHERE id = ?",
        (*values.values(), reply_id),
    )
    db.conn.commit()


def test_report_returns_one_row_per_mention_with_first_draft_latency(db):
    db.conn.execute("ALTER TABLE reply_queue ADD COLUMN draft_created_at TEXT")
    reply_id = _insert_mention(db, "responded")
    _set_times(
        db,
        reply_id,
        detected_at="2026-05-02T09:00:00+00:00",
        draft_created_at="2026-05-02T09:42:00+00:00",
    )

    report = build_mention_response_latency_report(db, days=2, now=NOW)

    assert report["totals"]["counts"] == {
        "total": 1,
        "drafted": 1,
        "published": 0,
        "pending": 0,
        "responded": 1,
    }
    assert report["rows"] == [
        {
            "mention_id": reply_id,
            "received_at": "2026-05-02T09:00:00+00:00",
            "first_reply_draft_at": "2026-05-02T09:42:00+00:00",
            "published_reply_at": None,
            "latency_minutes": 42.0,
            "status": "drafted",
            "relationship_context_present": False,
            "relationship_tier": None,
        }
    ]
    assert report["by_day"][0]["group"] == "2026-05-02"
    assert report["by_day"][0]["latency_minutes"]["median"] == 42.0


def test_pending_mentions_without_draft_or_publication_are_included(db):
    pending_id = _insert_mention(db, "pending", draft_text="")
    _set_times(db, pending_id, detected_at="2026-05-02T08:00:00+00:00")

    report = build_mention_response_latency_report(db, days=1, now=NOW)

    row = report["rows"][0]
    assert row["mention_id"] == pending_id
    assert row["status"] == "pending"
    assert row["first_reply_draft_at"] is None
    assert row["published_reply_at"] is None
    assert row["latency_minutes"] is None
    assert report["totals"]["counts"]["pending"] == 1


def test_published_reply_uses_first_available_response_and_relationship_group(db):
    ctx = json.dumps(
        {
            "tier_name": "Key Network",
            "dunbar_tier": 2,
            "relationship_strength": 0.8,
        }
    )
    reply_id = _insert_mention(
        db,
        "published",
        draft_text="",
        status="posted",
        relationship_context=ctx,
    )
    _set_times(
        db,
        reply_id,
        detected_at="2026-05-02T07:30:00+00:00",
        posted_at="2026-05-02T08:00:00+00:00",
        posted_tweet_id="reply-1",
    )

    report = build_mention_response_latency_report(db, days=1, now=NOW)

    assert report["rows"][0]["status"] == "published"
    assert report["rows"][0]["published_reply_at"] == "2026-05-02T08:00:00+00:00"
    assert report["rows"][0]["latency_minutes"] == 30.0
    assert report["rows"][0]["relationship_context_present"] is True
    assert report["rows"][0]["relationship_tier"] == "Key Network (tier 2)"
    assert report["by_relationship_tier"][0]["group"] == "Key Network (tier 2)"
    assert report["by_relationship_tier"][0]["counts"]["published"] == 1


def test_days_filter_excludes_old_mentions(db):
    recent_id = _insert_mention(db, "recent")
    old_id = _insert_mention(db, "old")
    _set_times(db, recent_id, detected_at="2026-05-02T10:00:00+00:00")
    _set_times(db, old_id, detected_at="2026-04-20T10:00:00+00:00")

    report = build_mention_response_latency_report(db, days=3, now=NOW)

    assert [row["mention_id"] for row in report["rows"]] == [recent_id]


def test_csv_serialization_is_stable_and_item_level(db):
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            detected_at TEXT,
            draft_text TEXT,
            relationship_context TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO reply_queue
           (id, detected_at, draft_text, relationship_context)
           VALUES (?, ?, ?, ?)""",
        (7, "2026-05-02 09:00:00", "hello", '{"tier": 3}'),
    )

    report = build_mention_response_latency_report(conn, days=1, now=NOW)
    text = format_mention_response_latency_csv(report)
    rows = list(csv.DictReader(StringIO(text)))

    assert text.splitlines()[0] == ",".join(
        [
            "mention_id",
            "received_at",
            "first_reply_draft_at",
            "published_reply_at",
            "latency_minutes",
            "status",
            "relationship_context_present",
        ]
    )
    assert rows == [
        {
            "mention_id": "7",
            "received_at": "2026-05-02T09:00:00+00:00",
            "first_reply_draft_at": "2026-05-02T09:00:00+00:00",
            "published_reply_at": "",
            "latency_minutes": "0.0",
            "status": "drafted",
            "relationship_context_present": "true",
        }
    ]


def test_json_serialization_and_missing_table_report_are_stable():
    conn = sqlite3.connect(":memory:")

    report = build_mention_response_latency_report(conn, days=5, now=NOW)
    payload = json.loads(format_mention_response_latency_json(report))

    assert payload["missing_tables"] == ["reply_queue"]
    assert payload["rows"] == []
    assert payload["filters"]["days"] == 5


def test_cli_defaults_to_json_and_writes_csv_output_file(db, tmp_path, capsys):
    reply_id = _insert_mention(db, "cli-row")
    _set_times(db, reply_id, detected_at="2026-05-02T10:00:00+00:00")
    output_path = tmp_path / "latency.csv"

    with patch("mention_response_latency.script_context", _mock_script_context(db)):
        assert main(["--format", "csv", "--days", "2", "--output", str(output_path)]) == 0

    assert capsys.readouterr().out == ""
    assert output_path.read_text(encoding="utf-8").splitlines()[0].startswith(
        "mention_id,received_at"
    )

    with patch("mention_response_latency.script_context", _mock_script_context(db)):
        assert main(["--days", "2"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "mention_response_latency"
    assert payload["filters"]["days"] == 2
