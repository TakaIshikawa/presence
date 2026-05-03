"""Tests for reply edit churn reporting."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import importlib.util
import io
import json
from pathlib import Path
import sqlite3

import pytest

from engagement.reply_edit_churn import (
    build_reply_edit_churn_report,
    format_reply_edit_churn_csv,
    format_reply_edit_churn_json,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_edit_churn.py"
spec = importlib.util.spec_from_file_location("reply_edit_churn_script", SCRIPT_PATH)
reply_edit_churn_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_edit_churn_script)


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            inbound_author_handle TEXT,
            draft_text TEXT,
            intent TEXT,
            priority TEXT,
            quality_score REAL,
            quality_flags TEXT,
            status TEXT,
            detected_at TEXT
        );
        CREATE TABLE reply_review_events (
            id INTEGER PRIMARY KEY,
            reply_queue_id INTEGER,
            event_type TEXT,
            created_at TEXT
        );
        """
    )
    return conn


def _insert_reply(
    conn: sqlite3.Connection,
    reply_id: int,
    *,
    platform: str = "x",
    handle: str = "alice",
    intent: str = "question",
    priority: str = "normal",
    status: str = "approved",
    quality_score: float | None = 8.0,
    quality_flags: list[str] | None = None,
    draft_text: str = "Thanks for asking.",
    detected_at: str = "2026-05-01T09:00:00+00:00",
) -> None:
    conn.execute(
        """INSERT INTO reply_queue
           (id, platform, inbound_author_handle, draft_text, intent, priority,
            quality_score, quality_flags, status, detected_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            reply_id,
            platform,
            handle,
            draft_text,
            intent,
            priority,
            quality_score,
            json.dumps(quality_flags or []),
            status,
            detected_at,
        ),
    )
    conn.commit()


def _insert_event(
    conn: sqlite3.Connection,
    reply_id: int,
    event_type: str,
    created_at: str,
) -> None:
    conn.execute(
        "INSERT INTO reply_review_events (reply_queue_id, event_type, created_at) VALUES (?, ?, ?)",
        (reply_id, event_type, created_at),
    )
    conn.commit()


def test_report_counts_edits_scores_churn_and_sorts_by_edits_then_detected_at():
    conn = _db()
    _insert_reply(
        conn,
        1,
        quality_score=5.5,
        quality_flags=["generic"],
        detected_at="2026-05-01T10:00:00+00:00",
    )
    _insert_reply(conn, 2, detected_at="2026-05-01T09:00:00+00:00")
    _insert_reply(conn, 3, detected_at="2026-05-01T08:00:00+00:00")
    for event_type, created_at in (
        ("edited", "2026-05-01T10:05:00+00:00"),
        ("approved", "2026-05-01T10:10:00+00:00"),
        ("edited", "2026-05-01T10:15:00+00:00"),
    ):
        _insert_event(conn, 1, event_type, created_at)
    for event_type, created_at in (
        ("edited", "2026-05-01T09:05:00+00:00"),
        ("approved", "2026-05-01T09:10:00+00:00"),
    ):
        _insert_event(conn, 2, event_type, created_at)

    report = build_reply_edit_churn_report(conn, min_edits=1, now=NOW)

    assert [row.reply_queue_id for row in report.rows] == [1, 2]
    first = report.rows[0]
    assert first.edit_count == 2
    assert first.review_event_count == 3
    assert first.first_event_at == "2026-05-01T10:05:00+00:00"
    assert first.last_event_at == "2026-05-01T10:15:00+00:00"
    assert first.churn_score == 25.0
    assert first.quality_flags == ("generic",)
    assert first.draft_length == len("Thanks for asking.")
    assert report.totals["reply_count"] == 3
    assert report.totals["edit_event_count"] == 3
    assert report.totals["reply_with_edit_count"] == 2


def test_zero_min_edits_includes_replies_with_no_review_events():
    conn = _db()
    _insert_reply(conn, 1, detected_at="2026-05-01T10:00:00+00:00")

    report = build_reply_edit_churn_report(conn, min_edits=0, now=NOW)

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.reply_queue_id == 1
    assert row.edit_count == 0
    assert row.review_event_count == 0
    assert row.first_event_at is None
    assert row.last_event_at is None
    assert row.churn_score == 0.0


def test_filters_apply_to_reply_rows_and_date_range():
    conn = _db()
    _insert_reply(
        conn,
        1,
        platform="x",
        intent="question",
        priority="high",
        status="posted",
        detected_at="2026-05-01T10:00:00+00:00",
    )
    _insert_reply(
        conn,
        2,
        platform="bluesky",
        intent="question",
        priority="high",
        status="posted",
        detected_at="2026-05-01T10:00:00+00:00",
    )
    _insert_reply(
        conn,
        3,
        platform="x",
        intent="question",
        priority="high",
        status="posted",
        detected_at="2026-04-01T10:00:00+00:00",
    )
    for reply_id in (1, 2, 3):
        _insert_event(conn, reply_id, "edited", "2026-05-01T10:05:00+00:00")

    report = build_reply_edit_churn_report(
        conn,
        platform="x",
        status="posted",
        intent="question",
        priority="high",
        start_date="2026-05-01",
        end_date="2026-05-02",
        now=NOW,
    )

    assert [row.reply_queue_id for row in report.rows] == [1]
    assert report.filters["platform"] == "x"
    assert report.filters["start_date"] == "2026-05-01T00:00:00+00:00"


def test_missing_review_events_table_still_reports_zero_event_replies():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            inbound_author_handle TEXT,
            intent TEXT,
            priority TEXT,
            status TEXT,
            quality_score REAL,
            quality_flags TEXT,
            draft_text TEXT,
            detected_at TEXT
        );
        INSERT INTO reply_queue
            (id, platform, inbound_author_handle, intent, priority, status, draft_text, detected_at)
        VALUES
            (1, 'x', 'alice', 'question', 'normal', 'pending', 'draft', '2026-05-01T09:00:00+00:00');
        """
    )

    report = build_reply_edit_churn_report(conn, min_edits=0, now=NOW)

    assert report.missing_tables == ("reply_review_events",)
    assert report.rows[0].edit_count == 0
    assert report.rows[0].review_event_count == 0


def test_json_and_csv_outputs_are_stable():
    conn = _db()
    _insert_reply(conn, 1, quality_flags=["generic"])
    _insert_event(conn, 1, "edited", "2026-05-01T09:05:00+00:00")

    report = build_reply_edit_churn_report(conn, now=NOW)
    payload = json.loads(format_reply_edit_churn_json(report))
    rows = list(csv.DictReader(io.StringIO(format_reply_edit_churn_csv(report))))

    assert payload["artifact_type"] == "reply_edit_churn"
    assert list(payload) == sorted(payload)
    assert payload["rows"][0]["reply_queue_id"] == 1
    assert rows[0]["reply_queue_id"] == "1"
    assert json.loads(rows[0]["quality_flags"]) == ["generic"]
    assert rows[0]["edit_count"] == "1"


def test_invalid_arguments_raise_value_error():
    conn = _db()
    with pytest.raises(ValueError, match="min_edits must be non-negative"):
        build_reply_edit_churn_report(conn, min_edits=-1, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_reply_edit_churn_report(conn, limit=0, now=NOW)
    with pytest.raises(ValueError, match="start_date must be an ISO-8601"):
        build_reply_edit_churn_report(conn, start_date="not-a-date", now=NOW)
    with pytest.raises(ValueError, match="start_date must be before"):
        build_reply_edit_churn_report(
            conn,
            start_date="2026-05-03",
            end_date="2026-05-01",
            now=NOW,
        )


def test_cli_supports_db_filters_formats_and_invalid_dates(file_db, capsys):
    reply_id = file_db.insert_reply_draft(
        inbound_tweet_id="cli-1",
        inbound_author_handle="alice",
        inbound_author_id="user-1",
        inbound_text="Can you help?",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original",
        draft_text="Draft",
        quality_score=6.0,
        quality_flags=json.dumps(["generic"]),
        platform="x",
        intent="question",
        priority="high",
        status="approved",
    )
    file_db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        ("2026-05-01T09:00:00+00:00", reply_id),
    )
    file_db.record_reply_review_event(
        reply_id,
        "edited",
        created_at="2026-05-01T09:05:00+00:00",
    )

    assert (
        reply_edit_churn_script.main(
            [
                "--db",
                str(file_db.db_path),
                "--platform",
                "x",
                "--status",
                "approved",
                "--intent",
                "question",
                "--priority",
                "high",
                "--start-date",
                "2026-05-01",
                "--end-date",
                "2026-05-02",
                "--format",
                "json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["rows"][0]["reply_queue_id"] == reply_id
    assert payload["filters"]["priority"] == "high"

    assert (
        reply_edit_churn_script.main(
            ["--db", str(file_db.db_path), "--min-edits", "0", "--format", "csv"]
        )
        == 0
    )
    assert "reply_queue_id,platform,inbound_author_handle" in capsys.readouterr().out

    assert reply_edit_churn_script.main(["--db", str(file_db.db_path), "--start-date", "bad"]) == 1
    assert "error: start_date must be an ISO-8601" in capsys.readouterr().err
