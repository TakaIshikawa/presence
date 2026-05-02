"""Tests for publish retry advice reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from output.publish_retry_advisor import (
    build_publish_retry_advice_report,
    format_publish_retry_advice_json,
    format_publish_retry_advice_text,
    recommend_publish_retry_action,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "report_publish_retry_advice.py"
spec = importlib.util.spec_from_file_location("report_publish_retry_advice_script", SCRIPT_PATH)
report_publish_retry_advice_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(report_publish_retry_advice_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str, *, retry_count: int = 0) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET retry_count = ?, last_retry_at = ? WHERE id = ?",
        (retry_count, (NOW - timedelta(hours=3)).isoformat(), content_id),
    )
    db.conn.commit()
    return content_id


def _publication_failure(
    db,
    *,
    content_id: int,
    platform: str = "x",
    error: str,
    category: str | None = None,
    attempt_count: int = 1,
    hours_ago: int = 3,
    next_retry_at: str | None = None,
) -> int:
    seen_at = (NOW - timedelta(hours=hours_ago)).isoformat()
    publication_id = db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, error, error_category, attempt_count,
            next_retry_at, last_error_at, updated_at)
           VALUES (?, ?, 'failed', ?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            platform,
            error,
            category,
            attempt_count,
            next_retry_at,
            seen_at,
            seen_at,
        ),
    ).lastrowid
    db.conn.commit()
    return int(publication_id)


def _queue_failure(
    db,
    *,
    content_id: int,
    platform: str = "x",
    error: str,
    category: str | None = None,
    hours_ago: int = 3,
) -> int:
    seen_at = (NOW - timedelta(hours=hours_ago)).isoformat()
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, error, error_category, created_at)
           VALUES (?, ?, ?, 'failed', ?, ?, ?)""",
        (content_id, seen_at, platform, error, category, seen_at),
    ).lastrowid
    db.conn.commit()
    return int(queue_id)


def test_groups_by_normalized_platform_signature_and_action(db):
    first = _content(db, "first")
    second = _content(db, "second")
    _publication_failure(
        db,
        content_id=first,
        error="429 too many requests for request id req-123456",
        category="rate_limit",
        attempt_count=1,
        hours_ago=4,
    )
    queue_id = _queue_failure(
        db,
        content_id=second,
        platform="twitter",
        error="429 too many requests for request id req-999999",
        category="rate_limit",
        hours_ago=4,
    )

    report = build_publish_retry_advice_report(db, days=7, platform="all", now=NOW)

    assert report.totals["records"] == 2
    assert report.totals["by_action"] == {"retry_now": 2}
    assert len(report.groups) == 1
    group = report.groups[0]
    assert group.platform == "x"
    assert group.error_category == "rate_limit"
    assert group.recommended_action == "retry_now"
    assert group.count == 2
    assert group.content_ids == (first, second)
    by_content = {record.content_id: record for record in report.records}
    assert by_content[second].queue_id == queue_id


def test_recommendations_consider_retry_count_age_and_transient_patterns(db):
    retry_now_id = _content(db, "retry now")
    wait_id = _content(db, "wait")
    auth_id = _content(db, "auth")
    exhausted_id = _content(db, "exhausted")
    future_retry = (NOW + timedelta(hours=2)).isoformat()
    _publication_failure(
        db,
        content_id=retry_now_id,
        error="503 service unavailable",
        attempt_count=1,
        hours_ago=2,
    )
    _publication_failure(
        db,
        content_id=wait_id,
        error="timeout connecting to platform",
        attempt_count=1,
        hours_ago=0,
        next_retry_at=future_retry,
    )
    _publication_failure(
        db,
        content_id=auth_id,
        error="invalid app password",
        attempt_count=1,
        hours_ago=5,
    )
    _publication_failure(
        db,
        content_id=exhausted_id,
        error="503 service unavailable",
        attempt_count=3,
        hours_ago=8,
    )

    report = build_publish_retry_advice_report(db, days=7, max_retries=3, now=NOW)
    actions = {record.content_id: record.recommended_action for record in report.records}

    assert actions[retry_now_id] == "retry_now"
    assert actions[wait_id] == "wait"
    assert actions[auth_id] == "review_credentials"
    assert actions[exhausted_id] == "manual_review"
    assert recommend_publish_retry_action(
        error_category="network",
        error="connection reset",
        retry_count=1,
        last_attempt_at=NOW - timedelta(minutes=1),
        next_retry_at=None,
        now=NOW,
    )[0] == "wait"


def test_platform_filter_and_unpublished_retry_rows(db):
    x_content = _content(db, "x")
    bluesky_content = _content(db, "blue")
    generated_retry = _content(db, "unpublished", retry_count=1)
    _publication_failure(db, content_id=x_content, platform="x", error="503 service unavailable")
    _publication_failure(
        db,
        content_id=bluesky_content,
        platform="bluesky",
        error="503 service unavailable",
    )

    report = build_publish_retry_advice_report(db, days=7, platform="x", now=NOW)
    content_ids = {record.content_id for record in report.records}

    assert x_content in content_ids
    assert generated_retry in content_ids
    assert bluesky_content not in content_ids
    assert {record.platform for record in report.records} == {"x"}


def test_formatters_and_cli_json_output(db, monkeypatch, capsys):
    content_id = _content(db, "auth")
    _publication_failure(db, content_id=content_id, error="invalid credentials")
    report = build_publish_retry_advice_report(db, now=NOW)
    payload = json.loads(format_publish_retry_advice_json(report))
    text = format_publish_retry_advice_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "publish_retry_advice"
    assert payload["records"][0]["recommended_action"] == "review_credentials"
    assert "Publish Retry Advice" in text
    assert f"content_id={content_id}" in text

    monkeypatch.setattr(
        report_publish_retry_advice_script,
        "script_context",
        lambda: _script_context(db),
    )
    result = report_publish_retry_advice_script.main(
        ["--platform", "x", "--max-retries", "3", "--format", "json"]
    )
    cli_payload = json.loads(capsys.readouterr().out)

    assert result == 0
    assert cli_payload["filters"]["platform"] == "x"
    assert cli_payload["totals"]["records"] >= 1

    result = report_publish_retry_advice_script.main(["--days", "0"])
    captured = capsys.readouterr()
    assert result == 2
    assert "value must be positive" in captured.err


def test_missing_generated_content_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_publish_retry_advice_report(conn, now=NOW)

    assert report.records == ()
    assert report.groups == ()
    assert report.missing_tables == (
        "generated_content",
        "publish_queue",
        "content_publications",
    )
    assert "No failed or unpublished platform posts" in format_publish_retry_advice_text(report)


def test_works_when_only_publish_queue_exists():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            content_id INTEGER NOT NULL,
            scheduled_at TEXT,
            platform TEXT,
            status TEXT,
            error TEXT,
            error_category TEXT,
            created_at TEXT
        );
        INSERT INTO publish_queue
            (id, content_id, scheduled_at, platform, status, error, error_category, created_at)
        VALUES
            (1, 10, '2026-05-01T08:00:00+00:00', 'x', 'failed',
             '503 service unavailable for request 123', 'network',
             '2026-05-01T08:00:00+00:00');
        """
    )

    report = build_publish_retry_advice_report(conn, days=7, platform="x", now=NOW)

    assert report.missing_tables == ()
    assert report.records[0].content_id == 10
    assert report.records[0].recommended_action == "retry_now"
