"""Tests for publication retry dead-letter candidate reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.publication_retry_dead_letter import (
    build_publication_retry_dead_letter_report,
    format_publication_retry_dead_letter_json,
    format_publication_retry_dead_letter_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "publication_retry_dead_letter.py"
)
spec = importlib.util.spec_from_file_location(
    "publication_retry_dead_letter_script",
    SCRIPT_PATH,
)
publication_retry_dead_letter_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_retry_dead_letter_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _at(hours_ago: float) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _content(db, text: str) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.commit()
    return content_id


def _attempt(
    db,
    content_id: int,
    *,
    platform: str = "x",
    hours_ago: float = 25,
    error: str = "gateway timeout",
    category: str | None = "network",
) -> int:
    return db.record_publication_attempt(
        None,
        content_id,
        platform,
        False,
        attempted_at=_at(hours_ago),
        error=error,
        error_category=category,
    )


def _publication(
    db,
    content_id: int,
    *,
    platform: str = "x",
    status: str = "failed",
    attempt_count: int = 0,
    hours_ago: float = 25,
    next_retry_hours_ago: float | None = None,
    error: str = "gateway timeout",
    category: str | None = "network",
) -> int:
    next_retry_at = None if next_retry_hours_ago is None else _at(next_retry_hours_ago)
    cursor = db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, error, error_category, attempt_count,
            next_retry_at, last_error_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            platform,
            status,
            error,
            category,
            attempt_count,
            next_retry_at,
            _at(hours_ago),
            _at(hours_ago),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _queue(
    db,
    content_id: int,
    *,
    platform: str = "x",
    status: str = "failed",
    hours_ago: float = 25,
    error: str | None = "gateway timeout",
    category: str | None = "network",
    hold_reason: str | None = None,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, error, error_category,
            hold_reason, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            _at(hours_ago),
            platform,
            status,
            error,
            category,
            hold_reason,
            _at(hours_ago),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_groups_repeated_attempts_by_content_and_platform_with_latest_error(db):
    content_id = _content(db, "Repeated network failure")
    _publication(
        db,
        content_id,
        platform="x",
        attempt_count=2,
        hours_ago=27,
        error="older timeout",
    )
    _attempt(db, content_id, platform="x", hours_ago=30, error="first timeout")
    _attempt(db, content_id, platform="x", hours_ago=26, error="second timeout")
    _attempt(
        db,
        content_id,
        platform="x",
        hours_ago=25,
        error="final gateway timeout",
    )
    _attempt(
        db,
        content_id,
        platform="bluesky",
        hours_ago=25,
        error="different target",
    )

    report = build_publication_retry_dead_letter_report(
        db,
        min_failures=3,
        older_than_hours=24,
        now=NOW,
    )

    row = next(item for item in report.rows if item.content_id == content_id and item.platform == "x")
    assert row.failure_count == 3
    assert row.latest_attempt_at == _at(25)
    assert row.latest_error == "final gateway timeout"
    assert row.latest_error_category == "network"
    assert row.dead_letter_candidate is True
    assert row.recommended_action == "manual_replay_or_cancel"
    assert row.sources == ("content_publications", "publication_attempts")

    bluesky = next(item for item in report.rows if item.content_id == content_id and item.platform == "bluesky")
    assert bluesky.failure_count == 1
    assert bluesky.dead_letter_candidate is False


def test_includes_held_publish_queue_without_content_publication(db):
    content_id = _content(db, "Needs operator hold review")
    _queue(
        db,
        content_id,
        platform="x",
        status="held",
        hours_ago=30,
        error=None,
        category=None,
        hold_reason="embargo conflict",
    )

    report = build_publication_retry_dead_letter_report(
        db,
        min_failures=3,
        older_than_hours=24,
        now=NOW,
    )

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.content_id == content_id
    assert row.queue_status == "held"
    assert row.publication_status is None
    assert row.failure_count == 1
    assert row.latest_error == "embargo conflict"
    assert row.recommended_action == "review_hold"
    assert row.dead_letter_candidate is False


def test_thresholds_mark_candidates_without_dropping_failed_rows(db):
    old_repeated = _content(db, "Old repeated")
    recent_repeated = _content(db, "Recent repeated")
    old_single = _content(db, "Old single")
    for hours_ago in (50, 49, 48):
        _attempt(db, old_repeated, hours_ago=hours_ago)
    for hours_ago in (3, 2, 1):
        _attempt(db, recent_repeated, hours_ago=hours_ago)
    _attempt(db, old_single, hours_ago=50)

    report = build_publication_retry_dead_letter_report(
        db,
        min_failures=3,
        older_than_hours=24,
        now=NOW,
    )
    by_content = {row.content_id: row for row in report.rows}

    assert by_content[old_repeated].dead_letter_candidate is True
    assert by_content[recent_repeated].dead_letter_candidate is False
    assert by_content[old_single].dead_letter_candidate is False
    assert report.totals["candidate_count"] == 1


def test_optional_retry_columns_missing_from_test_schema_are_tolerated():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content TEXT
        );
        CREATE TABLE content_publications (
            id INTEGER PRIMARY KEY,
            content_id INTEGER NOT NULL,
            platform TEXT NOT NULL,
            status TEXT NOT NULL,
            error TEXT
        );
        CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY,
            content_id INTEGER NOT NULL,
            platform TEXT NOT NULL,
            success INTEGER NOT NULL,
            attempted_at TEXT NOT NULL,
            error TEXT
        );
        CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            content_id INTEGER NOT NULL,
            platform TEXT,
            status TEXT,
            scheduled_at TEXT,
            error TEXT
        );
        INSERT INTO generated_content (id, content)
        VALUES (10, 'Partial schema content');
        INSERT INTO content_publications (id, content_id, platform, status, error)
        VALUES (1, 10, 'x', 'failed', 'invalid token expired');
        INSERT INTO publication_attempts
            (id, content_id, platform, success, attempted_at, error)
        VALUES
            (1, 10, 'x', 0, '2026-05-01T08:00:00+00:00', 'invalid token expired'),
            (2, 10, 'x', 0, '2026-05-01T09:00:00+00:00', 'invalid token expired'),
            (3, 10, 'x', 0, '2026-05-01T10:00:00+00:00', 'invalid token expired');
        """
    )

    report = build_publication_retry_dead_letter_report(
        conn,
        min_failures=3,
        older_than_hours=24,
        now=NOW,
    )

    assert report.rows[0].content_id == 10
    assert report.rows[0].latest_error_category == "auth"
    assert report.rows[0].next_retry_at is None
    assert report.rows[0].dead_letter_candidate is True
    assert report.missing_columns["content_publications"] == (
        "attempt_count",
        "error_category",
        "last_error_at",
        "next_retry_at",
        "updated_at",
    )
    assert "error_category" in report.missing_columns["publication_attempts"]


def test_failed_queue_only_row_is_included_when_publications_absent():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content TEXT
        );
        CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            content_id INTEGER NOT NULL,
            platform TEXT,
            status TEXT,
            scheduled_at TEXT,
            error TEXT,
            error_category TEXT
        );
        INSERT INTO generated_content (id, content)
        VALUES (10, 'Queue only failure');
        INSERT INTO publish_queue
            (id, content_id, platform, status, scheduled_at, error, error_category)
        VALUES
            (1, 10, 'x', 'failed', '2026-05-01T08:00:00+00:00',
             'media upload failed', 'media');
        """
    )

    report = build_publication_retry_dead_letter_report(conn, now=NOW)

    assert report.rows[0].content_id == 10
    assert report.rows[0].sources == ("publish_queue",)
    assert report.rows[0].publication_status is None
    assert report.missing_tables == (
        "content_publications",
        "publication_attempts",
    )


def test_formatters_cli_and_invalid_args(db, monkeypatch, capsys):
    content_id = _content(db, "CLI candidate")
    for hours_ago in (30, 29, 28):
        _attempt(db, content_id, hours_ago=hours_ago, error="invalid token", category="auth")

    report = build_publication_retry_dead_letter_report(db, now=NOW)
    payload = json.loads(format_publication_retry_dead_letter_json(report))
    text = format_publication_retry_dead_letter_text(report)

    assert payload["artifact_type"] == "publication_retry_dead_letter"
    assert payload["rows"][0]["content_id"] == content_id
    assert "Publication Retry Dead-Letter Report" in text
    assert "manual_fix_before_retry" in text

    monkeypatch.setattr(
        publication_retry_dead_letter_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        publication_retry_dead_letter_script,
        "build_publication_retry_dead_letter_report",
        lambda db, **kwargs: build_publication_retry_dead_letter_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = publication_retry_dead_letter_script.main(
        [
            "--min-failures",
            "3",
            "--older-than-hours",
            "24",
            "--format",
            "json",
            "--limit",
            "5",
        ]
    )
    cli_payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert cli_payload["filters"]["min_failures"] == 3
    assert cli_payload["filters"]["older_than_hours"] == 24.0
    assert cli_payload["filters"]["limit"] == 5
    assert cli_payload["rows"][0]["content_id"] == content_id

    with pytest.raises(ValueError, match="min_failures must be positive"):
        build_publication_retry_dead_letter_report(db, min_failures=0, now=NOW)
    with pytest.raises(ValueError, match="older_than_hours must be positive"):
        build_publication_retry_dead_letter_report(db, older_than_hours=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_publication_retry_dead_letter_report(db, limit=0, now=NOW)
