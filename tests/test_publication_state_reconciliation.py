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

    report = build_publication_state_reconciliation_report(db, days=7, now=NOW)

    assert report.has_issues is False
    assert report.issues == ()
    assert report.totals["issue_count"] == 0
    assert "No publication state drift" in format_publication_state_reconciliation_text(report)


def test_each_drift_scenario_produces_typed_actionable_issue(db):
    missing_success = _content(db, "missing success", published=1)
    stale_gc = _content(db, "stale generated flag", published=0)
    _publication(db, stale_gc, "bluesky", "published")
    failed_after_success = _content(db, "failed after success")
    _publication(db, failed_after_success, "x", "failed", updated_at=NOW - timedelta(hours=2))
    later_attempt = _attempt(
        db,
        failed_after_success,
        "x",
        success=True,
        attempted_at=NOW - timedelta(hours=1),
    )
    missing_identifier = _content(db, "missing identifier", published=1)
    _publication(db, missing_identifier, "x", "published", post_id=None, url="")
    mismatch = _content(
        db,
        "mismatch",
        published=1,
        published_url="https://x.test/generated",
        tweet_id="generated-post",
    )
    _publication(
        db,
        mismatch,
        "x",
        "published",
        post_id="publication-post",
        url="https://x.test/publication",
    )
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

    report = build_publication_state_reconciliation_report(db, days=7, now=NOW)
    issues = _issues_by_code(report)

    assert issues["generated_published_without_platform_success"].content_id == missing_success
    assert issues["generated_published_without_platform_success"].platform == "x"
    assert "no published publication row or successful attempt" in issues[
        "generated_published_without_platform_success"
    ].message
    assert issues["publication_published_generated_unpublished"].content_id == stale_gc
    assert issues["publication_published_generated_unpublished"].platform == "bluesky"
    assert issues["publication_failed_after_success"].content_id == failed_after_success
    assert issues["publication_failed_after_success"].publication_attempt_ids == (later_attempt,)
    assert issues["published_row_missing_identifier"].content_id == missing_identifier
    assert issues["published_row_missing_identifier"].details["missing_fields"] == [
        "platform_post_id",
        "platform_url",
    ]
    assert issues["x_tweet_id_mismatch"].content_id == mismatch
    assert issues["x_published_url_mismatch"].content_id == mismatch
    assert issues["duplicate_successful_attempt_post_ids"].content_id == duplicate
    assert issues["duplicate_successful_attempt_post_ids"].publication_attempt_ids == (
        first_attempt,
        second_attempt,
    )
    assert {issue.content_id for issue in report.issues} >= {
        missing_success,
        stale_gc,
        failed_after_success,
        missing_identifier,
        mismatch,
        duplicate,
    }
    assert all(issue.platform and issue.message for issue in report.issues)


def test_platform_filter_limits_totals_and_issues(db):
    x_content = _content(db, "x missing", published=1)
    bluesky_content = _content(db, "bluesky stale", published=0)
    _publication(db, bluesky_content, "bluesky", "published")

    report = build_publication_state_reconciliation_report(
        db,
        days=7,
        platforms=("bluesky",),
        now=NOW,
    )

    assert report.filters["platform"] == ["bluesky"]
    assert report.totals["content_publication_count"] == 1
    assert report.totals["publication_attempt_count"] == 0
    assert [issue.content_id for issue in report.issues] == [bluesky_content]
    assert all(issue.platform == "bluesky" for issue in report.issues)
    assert x_content not in {issue.content_id for issue in report.issues}


def test_json_and_text_output_are_deterministic(db):
    content_id = _content(db, "json", published=1)

    report = build_publication_state_reconciliation_report(db, days=7, now=NOW)
    payload = json.loads(format_publication_state_reconciliation_json(report))
    text = format_publication_state_reconciliation_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "publication_state_reconciliation"
    assert payload["issues"][0]["content_id"] == content_id
    assert payload["issues"][0]["issue_code"] == "generated_published_without_platform_success"
    assert "Publication State Reconciliation" in text
    assert "generated_published_without_platform_success" in text


def test_schema_gap_handling_reports_missing_metadata():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY)")

    report = build_publication_state_reconciliation_report(conn, now=NOW)

    assert report.issues == ()
    assert report.missing_tables == ("content_publications", "publication_attempts")
    assert report.missing_columns == {"generated_content": ("published",)}
    assert report.totals["by_issue_code"]["x_tweet_id_mismatch"] == 0


def test_cli_outputs_json_and_fail_on_issues(db, monkeypatch, capsys):
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
            now=NOW,
            **kwargs,
        ),
    )

    ok_code = publication_state_reconciliation_script.main(["--format", "json"])
    payload = json.loads(capsys.readouterr().out)
    fail_code = publication_state_reconciliation_script.main(["--fail-on-issues"])
    text = capsys.readouterr().out

    assert ok_code == 0
    assert payload["issues"][0]["content_id"] == content_id
    assert fail_code == 1
    assert "content=" in text
