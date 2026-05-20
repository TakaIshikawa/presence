"""Tests for reply review event timeliness reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from engagement.reply_review_event_timeliness import (
    build_reply_review_event_timeliness_report,
    build_reply_review_event_timeliness_report_from_db,
    format_reply_review_event_timeliness_json,
    format_reply_review_event_timeliness_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_review_event_timeliness.py"
spec = importlib.util.spec_from_file_location("reply_review_event_timeliness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(hours_ago: int) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            status TEXT,
            detected_at TEXT,
            reviewed_at TEXT,
            posted_at TEXT,
            inbound_author_handle TEXT,
            inbound_tweet_id TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE reply_review_events (
            id INTEGER PRIMARY KEY,
            reply_queue_id INTEGER,
            event_type TEXT,
            old_status TEXT,
            new_status TEXT,
            created_at TEXT
        )"""
    )
    return conn


def _reply(conn: sqlite3.Connection, reply_id: int, status: str, detected_hours_ago: int, reviewed_hours_ago: int | None = None, posted_hours_ago: int | None = None) -> None:
    conn.execute(
        "INSERT INTO reply_queue VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            reply_id,
            status,
            _ts(detected_hours_ago),
            _ts(reviewed_hours_ago) if reviewed_hours_ago is not None else None,
            _ts(posted_hours_ago) if posted_hours_ago is not None else None,
            f"author{reply_id}",
            f"tweet{reply_id}",
        ),
    )


def _event(conn: sqlite3.Connection, event_id: int, reply_id: int, event_type: str, old_status: str, new_status: str, created_hours_ago: int) -> None:
    conn.execute(
        "INSERT INTO reply_review_events VALUES (?, ?, ?, ?, ?, ?)",
        (event_id, reply_id, event_type, old_status, new_status, _ts(created_hours_ago)),
    )


def test_row_builder_flags_timeliness_and_consistency_findings():
    rows = [
        {"id": 1, "status": "pending", "detected_at": _ts(30)},
        {"id": 2, "status": "approved", "detected_at": _ts(20), "reviewed_at": _ts(10)},
        {"id": 3, "status": "pending", "detected_at": _ts(5)},
        {"id": 4, "status": "posted", "detected_at": _ts(6), "posted_at": _ts(1)},
    ]
    events = [
        {"id": 20, "reply_queue_id": 2, "event_type": "approved", "old_status": "pending", "new_status": "approved", "created_at": _ts(10)},
        {"id": 30, "reply_queue_id": 3, "event_type": "approved", "old_status": "pending", "new_status": "approved", "created_at": _ts(4)},
        {"id": 40, "reply_queue_id": 4, "event_type": "approved", "old_status": "pending", "new_status": "approved", "created_at": _ts(2)},
    ]

    report = build_reply_review_event_timeliness_report(rows, events, review_threshold_hours=24, post_threshold_hours=6, now=NOW)
    payload = json.loads(format_reply_review_event_timeliness_json(report))

    assert payload["artifact_type"] == "reply_review_event_timeliness"
    assert payload["totals"]["by_issue_type"] == {
        "delayed_reviewed_to_posted_transition": 1,
        "missing_posted_event": 1,
        "overdue_unreviewed_reply": 1,
        "status_mismatch": 2,
    }
    assert [item["severity"] for item in payload["findings"][:3]] == ["critical", "critical", "critical"]
    assert payload["findings"][0]["age_hours"] >= payload["findings"][1]["age_hours"]


def test_db_loader_schema_gaps_empty_state_and_text():
    missing = build_reply_review_event_timeliness_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["reply_queue", "reply_review_events"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE reply_queue (id INTEGER PRIMARY KEY)")
    gaps = build_reply_review_event_timeliness_report_from_db(bad, now=NOW)
    assert gaps["missing_tables"] == ["reply_review_events"]
    assert gaps["missing_columns"] == {"reply_queue": ["detected_at", "status"]}

    clean = _conn()
    _reply(clean, 1, "posted", 5, reviewed_hours_ago=4, posted_hours_ago=1)
    _event(clean, 1, 1, "approved", "pending", "approved", 4)
    _event(clean, 2, 1, "posted", "approved", "posted", 1)
    report = build_reply_review_event_timeliness_report_from_db(clean, now=NOW)
    assert report["totals"]["finding_count"] == 0
    assert "No reply review event timeliness issues found" in format_reply_review_event_timeliness_text(report)


def test_cli_json_output_and_positive_integer_validation(tmp_path, capsys):
    db_path = tmp_path / "reply.sqlite"
    conn = _conn(db_path)
    _reply(conn, 1, "pending", 30)
    conn.commit()
    conn.close()

    original_builder = script.build_reply_review_event_timeliness_report_from_db

    def build_report_with_fixed_now(conn, **kwargs):
        return original_builder(conn, now=NOW, **kwargs)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(script, "build_reply_review_event_timeliness_report_from_db", build_report_with_fixed_now)
        assert script.main(["--db", str(db_path), "--format", "json", "--review-threshold-hours", "24", "--post-threshold-hours", "6"]) == 0
        assert json.loads(capsys.readouterr().out)["totals"]["finding_count"] == 1

    assert script.main(["--db", str(db_path), "--review-threshold-hours", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
