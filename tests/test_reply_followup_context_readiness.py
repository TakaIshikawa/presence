"""Tests for reply follow-up context readiness reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from engagement.reply_followup_context_readiness import (
    build_reply_followup_context_readiness_report,
    build_reply_followup_context_readiness_report_from_db,
    format_reply_followup_context_readiness_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_followup_context_readiness.py"
spec = importlib.util.spec_from_file_location("reply_followup_context_readiness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE reply_followup_reminders (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            source_id INTEGER,
            source_reply_id INTEGER,
            due_at TEXT,
            status TEXT,
            target_handle TEXT
        );
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            inbound_tweet_id TEXT,
            inbound_url TEXT,
            platform TEXT,
            inbound_author_handle TEXT,
            inbound_author_id TEXT,
            platform_metadata TEXT,
            relationship_context TEXT
        );
        CREATE TABLE people (
            id INTEGER PRIMARY KEY,
            x_handle TEXT,
            display_name TEXT,
            bio TEXT,
            relationship_strength REAL,
            relationship_notes TEXT,
            notes TEXT
        );
        """
    )
    return conn


def test_builder_summarizes_reasons_and_urgency():
    rows = [
        {"followup_id": 1, "due_at": (NOW - timedelta(hours=1)).isoformat(), "status": "pending", "source_type": "reply_queue", "source_exists": 0, "reply_id": None, "mention_id": None, "target_url": None, "platform": None, "relationship_context": None},
        {"followup_id": 2, "due_at": (NOW + timedelta(hours=2)).isoformat(), "status": "pending", "source_type": "reply_queue", "source_exists": 1, "reply_id": 20, "mention_id": "m20", "target_url": "https://x.test/m20", "platform": "x", "relationship_context": json.dumps({"relationship_strength": 0.8, "updated_at": "2026-05-18T00:00:00+00:00"})},
    ]
    report = build_reply_followup_context_readiness_report(rows, now=NOW, max_context_age_hours=24)

    assert report["artifact_type"] == "reply_followup_context_readiness"
    assert report["summary"]["by_urgency_bucket"] == {"due_today": 1, "overdue": 4}
    assert report["summary"]["by_readiness_reason"] == {
        "missing_prior_reply_linkage": 1,
        "missing_relationship_context": 1,
        "missing_target_tweet_metadata": 1,
        "source_mention_unlinked": 1,
        "stale_context": 1,
    }
    assert {
        "followup_id",
        "reply_id",
        "mention_id",
        "due_at",
        "readiness_reason",
        "context_age_hours",
        "target_url",
        "platform",
    }.issubset(report["findings"][0])


def test_db_loader_handles_optional_context_columns_and_people_context():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE reply_followup_reminders (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            source_id INTEGER,
            due_at TEXT,
            status TEXT
        );
        """
    )
    conn.execute("INSERT INTO reply_followup_reminders VALUES (1, 'reply_queue', 42, ?, 'pending')", ((NOW - timedelta(hours=1)).isoformat(),))
    report = build_reply_followup_context_readiness_report_from_db(conn, now=NOW)
    assert report["missing_optional_tables"] == ["people", "reply_queue"]
    assert report["summary"]["finding_count"] >= 1

    full = _conn()
    full.execute(
        "INSERT INTO reply_queue VALUES (20, 'm20', 'https://x.test/m20', 'x', 'alice', 'u1', ?, NULL)",
        (json.dumps({"context_updated_at": "2026-05-20T10:00:00+00:00"}),),
    )
    full.execute("INSERT INTO people VALUES (1, 'alice', 'Alice', 'bio', 0.7, NULL, NULL)")
    full.execute("INSERT INTO reply_followup_reminders VALUES (1, 'reply_queue', 20, NULL, ?, 'pending', 'alice')", ((NOW + timedelta(hours=1)).isoformat(),))
    clean = build_reply_followup_context_readiness_report_from_db(full, now=NOW)
    assert clean["summary"]["finding_count"] == 0


def test_schema_gaps_text_cli_and_validation(tmp_path, capsys):
    missing = build_reply_followup_context_readiness_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["reply_followup_reminders"]
    assert "No reply follow-up context readiness gaps" in format_reply_followup_context_readiness_text(missing)

    db_path = tmp_path / "followups.sqlite"
    conn = _conn(db_path)
    conn.execute("INSERT INTO reply_followup_reminders VALUES (1, 'reply_queue', 42, NULL, ?, 'pending', 'alice')", ((NOW - timedelta(hours=1)).isoformat(),))
    conn.commit()
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--now", "2026-05-20T12:00:00+00:00", "--days-ahead", "2", "--max-context-age-hours", "24", "--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["limit"] == 5
    assert payload["findings"][0]["followup_id"] == 1

    assert script.main(["--db", str(db_path), "--format", "text", "--now", "2026-05-20T12:00:00+00:00"]) == 0
    assert "Reply Follow-up Context Readiness" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
