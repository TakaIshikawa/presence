"""Tests for reply context coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import csv
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.reply_context_coverage import (
    build_reply_context_coverage_report,
    format_reply_context_coverage_csv,
    format_reply_context_coverage_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_context_coverage.py"
spec = importlib.util.spec_from_file_location("reply_context_coverage_script", SCRIPT_PATH)
reply_context_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_context_coverage_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _relationship_context(**overrides) -> str:
    payload = {
        "display_name": "Alice",
        "bio": "Builds developer tools.",
        "relationship_strength": 0.8,
        "engagement_stage": 3,
        "dunbar_tier": 2,
        "relationship_notes": "Prefers concrete implementation details.",
    }
    payload.update(overrides)
    return json.dumps(payload, sort_keys=True)


def _conversation_metadata(**overrides) -> str:
    payload = {
        "conversation_id": "conv-1",
        "parent_tweet_id": "our-1",
        "parent_post_text": "Original post",
    }
    payload.update(overrides)
    return json.dumps(payload, sort_keys=True)


def _insert_reply(db, inbound_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=inbound_id,
        inbound_author_handle="alice",
        inbound_author_id=f"{inbound_id}-author",
        inbound_text="Can you clarify this?",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="Thanks for asking.",
        status="pending",
        platform="x",
        relationship_context=_relationship_context(),
        platform_metadata=_conversation_metadata(),
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute("UPDATE reply_queue SET detected_at = ? WHERE id = ?", (detected_at, reply_id))
    db.conn.commit()


def test_enriched_drafts_count_as_context_covered(db):
    reply_id = _insert_reply(db, "covered")
    _set_detected_at(db, reply_id, "2026-05-03T10:00:00+00:00")

    report = build_reply_context_coverage_report(db, now=NOW)
    payload = json.loads(format_reply_context_coverage_json(report))

    assert payload["artifact_type"] == "reply_context_coverage"
    assert report.ok is True
    assert report.totals == {
        "total_mentions": 1,
        "drafted_replies": 1,
        "relationship_context_drafts": 1,
        "conversation_context_drafts": 1,
        "context_covered_drafts": 1,
        "context_missing_drafts": 0,
    }
    assert payload["drafts"][0]["reply_queue_id"] == reply_id
    assert payload["drafts"][0]["has_full_context"] is True


def test_missing_context_draft_is_counted_as_missing(db):
    _insert_reply(
        db,
        "missing",
        relationship_context=None,
        platform_metadata=None,
        our_post_text="",
    )

    report = build_reply_context_coverage_report(db, now=NOW)

    assert report.ok is False
    assert report.totals["total_mentions"] == 1
    assert report.totals["drafted_replies"] == 1
    assert report.totals["relationship_context_drafts"] == 0
    assert report.totals["conversation_context_drafts"] == 0
    assert report.totals["context_missing_drafts"] == 1
    assert report.blocking_issue_count == 1


def test_cultivate_people_rows_can_supply_relationship_context():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
               id INTEGER PRIMARY KEY,
               inbound_tweet_id TEXT,
               inbound_author_handle TEXT,
               inbound_author_id TEXT,
               draft_text TEXT,
               platform_metadata TEXT,
               detected_at TEXT
           )"""
    )
    conn.execute(
        """CREATE TABLE people (
               id TEXT PRIMARY KEY,
               x_handle TEXT,
               display_name TEXT,
               bio TEXT,
               relationship_strength REAL,
               engagement_stage INTEGER,
               dunbar_tier INTEGER
           )"""
    )
    conn.execute(
        """INSERT INTO reply_queue
           (id, inbound_tweet_id, inbound_author_handle, inbound_author_id,
            draft_text, platform_metadata, detected_at)
           VALUES (1, 'mention-1', 'Alice', 'author-a', 'Draft', ?, ?)""",
        (_conversation_metadata(), "2026-05-03T10:00:00+00:00"),
    )
    conn.execute(
        """INSERT INTO people
           (id, x_handle, display_name, bio, relationship_strength, engagement_stage, dunbar_tier)
           VALUES ('p1', 'alice', 'Alice', 'Builds tools', 0.7, 3, 2)"""
    )

    report = build_reply_context_coverage_report(conn, now=NOW)

    assert report.totals["relationship_context_drafts"] == 1
    assert report.totals["conversation_context_drafts"] == 1
    assert report.totals["context_covered_drafts"] == 1
    assert report.missing_cultivate_tables == ()


def test_no_drafts_counts_mentions_without_missing_context(db):
    _insert_reply(db, "classified", draft_text="", relationship_context=None)

    report = build_reply_context_coverage_report(db, now=NOW)

    assert report.ok is True
    assert report.totals["total_mentions"] == 1
    assert report.totals["drafted_replies"] == 0
    assert report.totals["context_covered_drafts"] == 0
    assert report.totals["context_missing_drafts"] == 0
    assert report.drafts == ()


def test_missing_reply_queue_and_missing_cultivate_table_are_graceful(db):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    missing = build_reply_context_coverage_report(conn, now=NOW)
    normal = build_reply_context_coverage_report(db, now=NOW)

    assert missing.missing_tables == ("reply_queue",)
    assert missing.totals["total_mentions"] == 0
    assert normal.missing_cultivate_tables == ("people",)


def test_csv_output_is_one_row_with_totals(db):
    _insert_reply(db, "covered")
    _insert_reply(db, "missing", relationship_context=None, platform_metadata=None, our_post_text="")

    report = build_reply_context_coverage_report(db, now=NOW)
    rows = list(csv.DictReader(format_reply_context_coverage_csv(report).splitlines()))

    assert rows == [
        {
            "generated_at": "2026-05-03T12:00:00+00:00",
            "total_mentions": "2",
            "drafted_replies": "2",
            "relationship_context_drafts": "1",
            "conversation_context_drafts": "1",
            "context_covered_drafts": "1",
            "context_missing_drafts": "1",
        }
    ]


def test_cli_supports_db_filters_and_csv_output(file_db, capsys):
    included = _insert_reply(
        file_db,
        "included",
        inbound_author_handle="Alice",
        our_platform_id="account-1",
    )
    _set_detected_at(file_db, included, "2026-05-03T09:00:00+00:00")
    excluded = _insert_reply(
        file_db,
        "excluded",
        inbound_author_handle="Bob",
        our_platform_id="account-2",
    )
    _set_detected_at(file_db, excluded, "2026-05-03T09:00:00+00:00")

    exit_code = reply_context_coverage_script.main(
        [
            "--db",
            str(file_db.db_path),
            "--start",
            "2026-05-03T00:00:00+00:00",
            "--end",
            "2026-05-04T00:00:00+00:00",
            "--account",
            "account-1",
            "--author",
            "@alice",
            "--format",
            "csv",
        ]
    )
    rows = list(csv.DictReader(capsys.readouterr().out.splitlines()))

    assert exit_code == 0
    assert rows[0]["total_mentions"] == "1"
    assert rows[0]["context_covered_drafts"] == "1"


def test_cli_uses_script_context(db, monkeypatch, capsys):
    _insert_reply(db, "ctx", relationship_context=None, platform_metadata=None, our_post_text="")
    monkeypatch.setattr(
        reply_context_coverage_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = reply_context_coverage_script.main(["--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["totals"]["context_missing_drafts"] == 1
