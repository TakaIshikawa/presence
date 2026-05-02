"""Tests for platform publication state reconciliation."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.publication_state_reconciliation import (
    build_publication_state_reconciliation_report,
    format_publication_state_reconciliation_json,
    format_publication_state_reconciliation_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "publication_state_reconciliation.py"
)
spec = importlib.util.spec_from_file_location(
    "publication_state_reconciliation_script",
    SCRIPT_PATH,
)
publication_state_reconciliation_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_state_reconciliation_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    text: str,
    *,
    published: int = 0,
    published_url: str | None = None,
    tweet_id: str | None = None,
    created_at: datetime = NOW,
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        """UPDATE generated_content
           SET published = ?, published_url = ?, tweet_id = ?, created_at = ?, published_at = ?
           WHERE id = ?""",
        (
            published,
            published_url,
            tweet_id,
            created_at.isoformat(),
            created_at.isoformat() if published == 1 else None,
            content_id,
        ),
    )
    db.conn.commit()
    return content_id


def _publication(
    db,
    content_id: int,
    platform: str,
    status: str,
    *,
    post_id: str | None = "post-1",
    url: str | None = "https://example.test/post-1",
    updated_at: datetime = NOW,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, platform_post_id, platform_url,
            published_at, updated_at, last_error_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            platform,
            status,
            post_id,
            url,
            updated_at.isoformat() if status == "published" else None,
            updated_at.isoformat(),
            updated_at.isoformat() if status == "failed" else None,
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _attempt(
    db,
    content_id: int,
    platform: str,
    *,
    success: bool,
    post_id: str | None = "post-1",
    url: str | None = "https://example.test/post-1",
    attempted_at: datetime = NOW,
) -> int:
    return db.record_publication_attempt(
        queue_id=None,
        content_id=content_id,
        platform=platform,
        success=success,
        platform_post_id=post_id,
        platform_url=url,
        attempted_at=attempted_at.isoformat(),
    )


def _queue(
    db,
    content_id: int,
    platform: str,
    *,
    status: str = "queued",
    scheduled_at: datetime = NOW,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (
            content_id,
            scheduled_at.isoformat(),
            platform,
            status,
            scheduled_at.isoformat(),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _issues_by_code(report):
    return {issue.issue_code: issue for issue in report.issues}


def test_clean_aligned_rows_produce_no_issues(db):
    content_id = _content(
        db,
        "clean",
        published=1,
        published_url="https://x.test/post-1",
        tweet_id="post-1",
    )
    _publication(
        db,
        content_id,
        "x",
        "published",
        post_id="post-1",
        url="https://x.test/post-1",
    )
    _attempt(
        db,
        content_id,
        "x",
        success=True,
        post_id="post-1",
        url="https://x.test/post-1",
    )

    report = build_publication_state_reconciliation_report(
        db,
        lookback_days=7,
        generated_at=NOW,
    )

    assert report.has_issues is False
    assert report.issues == ()
    assert report.totals["issue_count"] == 0
    assert "No publication state drift" in format_publication_state_reconciliation_text(report)


def test_each_drift_scenario_produces_typed_actionable_issue(db):
    legacy_only = _content(db, "legacy only", published=1)
    missing_legacy_timestamp = _content(db, "missing generated timestamp", published=0)
    platform_publication = _publication(db, missing_legacy_timestamp, "x", "published")
    failed_with_success = _content(db, "failed with success")
    failed_publication = _publication(
        db,
        failed_with_success,
        "x",
        "failed",
        updated_at=NOW - timedelta(hours=2),
    )
    success_attempt = _attempt(
        db,
        failed_with_success,
        "x",
        success=True,
        attempted_at=NOW - timedelta(hours=1),
    )
    queued_after_success = _content(db, "queued after success")
    queue_success = _attempt(
        db,
        queued_after_success,
        "bluesky",
        success=True,
        attempted_at=NOW - timedelta(minutes=5),
    )
    queue_id = _queue(db, queued_after_success, "bluesky", scheduled_at=NOW)
    duplicate = _content(db, "duplicate attempts")
    first_attempt = _attempt(db, duplicate, "x", success=True, post_id="post-a")
    second_attempt = _attempt(
        db,
        duplicate,
        "x",
        success=True,
        post_id="post-b",
        attempted_at=NOW + timedelta(minutes=1),
    )

    report = build_publication_state_reconciliation_report(
        db,
        lookback_days=7,
        generated_at=NOW,
    )
    issues = _issues_by_code(report)

    assert issues["legacy_published_without_platform_record"].content_id == legacy_only
    assert issues["legacy_published_without_platform_record"].platform == "x"
    assert "no platform publication or successful attempt" in issues[
        "legacy_published_without_platform_record"
    ].message
    assert (
        issues["platform_published_without_legacy_timestamp"].content_id
        == missing_legacy_timestamp
    )
    assert (
        issues["platform_published_without_legacy_timestamp"].content_publication_id
        == platform_publication
    )
    assert issues["failed_publication_with_success_attempt"].content_id == failed_with_success
    assert (
        issues["failed_publication_with_success_attempt"].content_publication_id
        == failed_publication
    )
    assert issues["failed_publication_with_success_attempt"].publication_attempt_ids == (
        success_attempt,
    )
    assert issues["queued_after_successful_attempt"].content_id == queued_after_success
    assert issues["queued_after_successful_attempt"].publish_queue_id == queue_id
    assert issues["queued_after_successful_attempt"].publication_attempt_ids == (queue_success,)
    assert issues["duplicate_success_attempts_for_platform"].content_id == duplicate
    assert issues["duplicate_success_attempts_for_platform"].publication_attempt_ids == (
        first_attempt,
        second_attempt,
    )
    assert {issue.content_id for issue in report.issues} >= {
        legacy_only,
        missing_legacy_timestamp,
        failed_with_success,
        queued_after_success,
        duplicate,
    }
    assert all(issue.platform and issue.message for issue in report.issues)


def test_limit_caps_findings_and_reports_total_issue_count(db):
    first = _content(db, "first missing", published=1)
    second = _content(db, "second missing", published=1)

    report = build_publication_state_reconciliation_report(
        db,
        lookback_days=7,
        generated_at=NOW,
        limit=1,
    )

    assert report.filters["limit"] == 1
    assert report.totals["issue_count"] == 1
    assert report.totals["total_issue_count"] == 2
    assert report.totals["limited"] is True
    assert [issue.content_id for issue in report.issues] == [first]
    assert second not in {issue.content_id for issue in report.issues}
    assert report.totals["publication_attempt_count"] == 0


def test_json_and_text_output_are_deterministic(db):
    content_id = _content(db, "json", published=1)

    report = build_publication_state_reconciliation_report(
        db,
        lookback_days=7,
        generated_at=NOW,
    )
    payload = json.loads(format_publication_state_reconciliation_json(report))
    text = format_publication_state_reconciliation_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "publication_state_reconciliation"
    assert payload["issues"][0]["content_id"] == content_id
    assert payload["issues"][0]["issue_code"] == "legacy_published_without_platform_record"
    assert "Publication State Reconciliation" in text
    assert "legacy_published_without_platform_record" in text


def test_schema_gap_handling_reports_missing_metadata():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY)")

    report = build_publication_state_reconciliation_report(conn, now=NOW)

    assert report.issues == ()
    assert report.missing_tables == (
        "content_publications",
        "publish_queue",
        "publication_attempts",
    )
    assert report.missing_columns == {"generated_content": ("published", "published_at")}
    assert report.totals["by_issue_code"]["queued_after_successful_attempt"] == 0


def test_cli_outputs_json_text_and_empty_database_cleanly(db, monkeypatch, capsys):
    content_id = _content(db, "cli", published=1)
    monkeypatch.setattr(
        publication_state_reconciliation_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        publication_state_reconciliation_script,
        "build_publication_state_reconciliation_report",
        lambda db_arg, **kwargs: build_publication_state_reconciliation_report(
            db_arg,
            generated_at=NOW,
            **kwargs,
        ),
    )

    ok_code = publication_state_reconciliation_script.main(
        ["--format", "json", "--lookback-days", "7", "--limit", "5"]
    )
    payload = json.loads(capsys.readouterr().out)

    text_code = publication_state_reconciliation_script.main(["--format", "text"])
    text = capsys.readouterr().out

    assert ok_code == 0
    assert payload["issues"][0]["content_id"] == content_id
    assert payload["filters"]["lookback_days"] == 7
    assert text_code == 0
    assert "content=" in text

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    monkeypatch.setattr(
        publication_state_reconciliation_script,
        "script_context",
        lambda: _script_context(conn),
    )
    monkeypatch.setattr(
        publication_state_reconciliation_script,
        "build_publication_state_reconciliation_report",
        lambda db_arg, **kwargs: build_publication_state_reconciliation_report(
            db_arg,
            generated_at=NOW,
            **kwargs,
        ),
    )

    empty_code = publication_state_reconciliation_script.main(["--format", "json"])
    empty_payload = json.loads(capsys.readouterr().out)

    assert empty_code == 0
    assert empty_payload["issues"] == []
    assert empty_payload["missing_tables"] == [
        "generated_content",
        "content_publications",
        "publish_queue",
        "publication_attempts",
    ]
