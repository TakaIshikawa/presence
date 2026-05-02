"""Tests for publish error signature reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from output.publish_error_signatures import (
    build_publish_error_signature_report,
    format_publish_error_signature_json,
    format_publish_error_signature_text,
    normalize_publish_error_signature,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_error_signatures.py"
spec = importlib.util.spec_from_file_location("publish_error_signatures_script", SCRIPT_PATH)
publish_error_signatures_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publish_error_signatures_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str) -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )


def _queue_failure(
    db,
    *,
    content_id: int,
    platform: str = "x",
    error: str,
    category: str | None = None,
    days_ago: int = 1,
) -> int:
    seen_at = (NOW - timedelta(days=days_ago)).isoformat()
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, error, error_category, created_at)
           VALUES (?, ?, ?, 'failed', ?, ?, ?)""",
        (content_id, seen_at, platform, error, category, seen_at),
    ).lastrowid
    db.conn.commit()
    return int(queue_id)


def _publication_failure(
    db,
    *,
    content_id: int,
    platform: str = "x",
    error: str,
    category: str | None = None,
    days_ago: int = 1,
) -> int:
    seen_at = (NOW - timedelta(days=days_ago)).isoformat()
    publication_id = db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, error, error_category, attempt_count,
            last_error_at, updated_at)
           VALUES (?, ?, 'failed', ?, ?, 1, ?, ?)""",
        (content_id, platform, error, category, seen_at, seen_at),
    ).lastrowid
    db.conn.commit()
    return int(publication_id)


def _attempt_failure(
    db,
    *,
    content_id: int,
    queue_id: int | None = None,
    platform: str = "x",
    error: str,
    category: str | None = None,
    days_ago: int = 1,
) -> int:
    seen_at = (NOW - timedelta(days=days_ago)).isoformat()
    attempt_id = db.conn.execute(
        """INSERT INTO publication_attempts
           (queue_id, content_id, platform, attempted_at, success, error, error_category)
           VALUES (?, ?, ?, ?, 0, ?, ?)""",
        (queue_id, content_id, platform, seen_at, error, category),
    ).lastrowid
    db.conn.commit()
    return int(attempt_id)


def test_normalization_collapses_urls_ids_and_timestamps():
    first = normalize_publish_error_signature(
        "Upload failed for post id 123456 at 2026-05-01T12:30:45+00:00: "
        "https://cdn.example.test/media/abc.png?token=aaa"
    )
    second = normalize_publish_error_signature(
        "Upload failed for post id 987654 at 2026-05-02T09:15:00Z: "
        "https://cdn.example.test/media/def.png?token=bbb"
    )

    assert first == second
    assert "<url>" in first
    assert "<timestamp>" in first
    assert "post id <id>" in first


def test_groups_failures_across_queue_publications_and_attempts(db):
    first_content = _content(db, "first")
    second_content = _content(db, "second")
    third_content = _content(db, "third")
    first_queue = _queue_failure(
        db,
        content_id=first_content,
        error="429 too many requests for request id req-123456 at 2026-05-01T11:00:00Z",
    )
    publication_id = _publication_failure(
        db,
        content_id=second_content,
        error="429 too many requests for request id req-999999 at 2026-05-01T11:05:00Z",
    )
    attempt_id = _attempt_failure(
        db,
        content_id=third_content,
        queue_id=first_queue,
        error="429 too many requests for request id req-888888 at 2026-05-01T11:10:00Z",
    )

    report = build_publish_error_signature_report(
        db,
        days=7,
        min_count=2,
        platform="x",
        now=NOW,
    )

    assert len(report.signatures) == 1
    signature = report.signatures[0]
    assert signature.count == 3
    assert signature.platform == "x"
    assert signature.error_category == "rate_limit"
    assert signature.suggested_action == "retry_later"
    assert signature.queue_ids == (first_queue,)
    assert signature.publication_ids == (publication_id,)
    assert signature.attempt_ids == (attempt_id,)
    assert signature.content_ids == (first_content, second_content, third_content)
    assert signature.source_counts == {
        "content_publications": 1,
        "publication_attempts": 1,
        "publish_queue": 1,
    }


def test_filters_by_days_platform_and_min_count(db):
    x_one = _content(db, "x one")
    x_two = _content(db, "x two")
    old = _content(db, "old")
    bluesky = _content(db, "blue")
    _queue_failure(db, content_id=x_one, platform="x", error="invalid app password for user 111")
    _queue_failure(db, content_id=x_two, platform="x", error="invalid app password for user 222")
    _queue_failure(
        db,
        content_id=old,
        platform="x",
        error="invalid app password for user 333",
        days_ago=40,
    )
    _queue_failure(
        db,
        content_id=bluesky,
        platform="bluesky",
        error="invalid app password for user 444",
    )

    report = build_publish_error_signature_report(
        db,
        days=30,
        min_count=2,
        platform="x",
        now=NOW,
    )
    strict = build_publish_error_signature_report(
        db,
        days=30,
        min_count=3,
        platform="x",
        now=NOW,
    )

    assert len(report.signatures) == 1
    assert report.signatures[0].count == 2
    assert report.signatures[0].content_ids == (x_one, x_two)
    assert report.signatures[0].error_category == "auth"
    assert strict.signatures == ()


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
            (1, 10, '2026-04-30T12:00:00+00:00', 'x', 'failed',
             'media upload failed for media id 123456', 'media',
             '2026-04-30T12:00:00+00:00'),
            (2, 11, '2026-04-30T12:05:00+00:00', 'x', 'failed',
             'media upload failed for media id 999999', 'media',
             '2026-04-30T12:05:00+00:00');
        """
    )

    report = build_publish_error_signature_report(conn, days=7, min_count=2, now=NOW)

    assert report.availability["publish_queue"] is True
    assert report.availability["content_publications"] is False
    assert report.signatures[0].queue_ids == (1, 2)
    assert report.signatures[0].content_ids == (10, 11)


def test_formatters_are_deterministic_and_cli_supports_flags(db, capsys):
    first = _content(db, "first")
    second = _content(db, "second")
    _queue_failure(db, content_id=first, error="duplicate status id 123456")
    _queue_failure(db, content_id=second, error="duplicate status id 654321")
    fixed_report = build_publish_error_signature_report(
        db,
        days=7,
        min_count=2,
        platform="all",
        now=NOW,
    )
    payload = json.loads(format_publish_error_signature_json(fixed_report))
    text = format_publish_error_signature_text(fixed_report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["artifact_type"] == "publish_error_signatures"
    assert payload["signatures"][0]["suggested_action"] == "cancel_duplicate"
    assert "Publish Error Signatures" in text
    assert "duplicate status id <id>" in text

    with patch.object(
        publish_error_signatures_script,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        publish_error_signatures_script,
        "build_publish_error_signature_report",
        return_value=fixed_report,
    ):
        result = publish_error_signatures_script.main(
            ["--days", "7", "--min-count", "2", "--platform", "all", "--format", "json"]
        )

    assert result == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"] == {
        "days": 7,
        "min_count": 2,
        "platform": "all",
    }
