"""Tests for review queue aging reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.review_queue_aging import (
    build_review_queue_aging_report,
    format_review_queue_aging_json,
    format_review_queue_aging_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "review_queue_aging.py"
spec = importlib.util.spec_from_file_location("review_queue_aging_script", SCRIPT_PATH)
review_queue_aging_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(review_queue_aging_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            content TEXT,
            created_at TEXT,
            updated_at TEXT,
            review_status TEXT,
            status TEXT,
            curation_quality TEXT,
            published INTEGER DEFAULT 0,
            published_at TEXT,
            published_url TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            status TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE content_publications (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            status TEXT,
            published_at TEXT
        )"""
    )
    return conn


def _content(
    conn: sqlite3.Connection,
    *,
    age_days: int,
    content_type: str = "x_post",
    review_status: str | None = None,
    status: str | None = None,
    curation_quality: str | None = None,
    published: int = 0,
    published_at: str | None = None,
) -> int:
    created_at = (NOW - timedelta(days=age_days)).isoformat()
    cursor = conn.execute(
        """INSERT INTO generated_content
           (content_type, content, created_at, updated_at, review_status, status,
            curation_quality, published, published_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            content_type,
            f"{content_type} content {age_days}",
            created_at,
            created_at,
            review_status,
            status,
            curation_quality,
            published,
            published_at,
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def test_empty_database_returns_zeroed_report():
    conn = _conn()

    report = build_review_queue_aging_report(conn, now=NOW)

    assert report["totals"]["pending_count"] == 0
    assert report["totals"]["by_age_bucket"] == {
        "0-1d": 0,
        "2-3d": 0,
        "4-7d": 0,
        "8-14d": 0,
        "15d+": 0,
    }
    assert report["groups"] == []
    assert "No pending review queue items found." in format_review_queue_aging_text(report)
    conn.close()


def test_mixed_statuses_exclude_published_and_resolved_content():
    conn = _conn()
    pending = _content(conn, age_days=10, review_status="needs_review")
    queued = _content(conn, age_days=5, content_type="newsletter")
    _content(conn, age_days=20, review_status="approved", published=1)
    _content(conn, age_days=20, review_status="dismissed")
    published_by_publication = _content(conn, age_days=30, review_status="needs_review")
    conn.execute("INSERT INTO publish_queue (content_id, status) VALUES (?, 'queued')", (queued,))
    conn.execute(
        "INSERT INTO content_publications (content_id, status, published_at) VALUES (?, 'published', ?)",
        (published_by_publication, NOW.isoformat()),
    )
    conn.commit()

    report = build_review_queue_aging_report(conn, now=NOW)

    assert report["totals"]["pending_count"] == 2
    assert report["totals"]["by_status"] == {"needs_review": 1, "queued": 1}
    assert report["totals"]["by_content_type"] == {"newsletter": 1, "x_post": 1}
    groups = {(group["status"], group["content_type"]): group for group in report["groups"]}
    assert groups[("needs_review", "x_post")]["oldest_item"]["content_id"] == pending
    assert groups[("queued", "newsletter")]["oldest_item"]["content_id"] == queued
    conn.close()


def test_age_bucket_boundaries_are_deterministic():
    conn = _conn()
    ids = [
        _content(conn, age_days=0, review_status="needs_review"),
        _content(conn, age_days=1, review_status="needs_review"),
        _content(conn, age_days=2, review_status="needs_review"),
        _content(conn, age_days=3, review_status="needs_review"),
        _content(conn, age_days=4, review_status="needs_review"),
        _content(conn, age_days=7, review_status="needs_review"),
        _content(conn, age_days=8, review_status="needs_review"),
        _content(conn, age_days=14, review_status="needs_review"),
        _content(conn, age_days=15, review_status="needs_review"),
    ]

    report = build_review_queue_aging_report(conn, now=NOW)

    assert report["totals"]["by_age_bucket"] == {
        "0-1d": 2,
        "2-3d": 2,
        "4-7d": 2,
        "8-14d": 2,
        "15d+": 1,
    }
    group = report["groups"][0]
    assert group["oldest_item"]["content_id"] == ids[-1]
    assert group["oldest_item"]["age_bucket"] == "15d+"
    conn.close()


def test_status_grouping_oldest_item_and_formatters():
    conn = _conn()
    newer = _content(conn, age_days=4, content_type="x_thread", review_status="needs_review")
    older = _content(conn, age_days=12, content_type="x_thread", review_status="needs_review")
    held = _content(conn, age_days=8, content_type="x_thread")
    conn.execute("INSERT INTO publish_queue (content_id, status) VALUES (?, 'held')", (held,))
    conn.commit()

    report = build_review_queue_aging_report(conn, limit=1, now=NOW)
    payload = json.loads(format_review_queue_aging_json(report))
    text = format_review_queue_aging_text(report)

    assert list(payload.keys()) == sorted(report.keys())
    assert len(report["groups"]) == 1
    assert report["totals"]["group_count"] == 2
    assert report["groups"][0]["status"] == "needs_review"
    assert report["groups"][0]["oldest_item"]["content_id"] == older
    assert newer != older
    assert "Review Queue Aging" in text
    assert "oldest=#" in text
    conn.close()


def test_cli_json_and_invalid_arguments(monkeypatch, capsys):
    conn = _conn()
    content_id = _content(conn, age_days=5, review_status="needs_review")
    monkeypatch.setattr(review_queue_aging_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        review_queue_aging_script,
        "build_review_queue_aging_report",
        lambda db, **kwargs: build_review_queue_aging_report(db, now=NOW, **kwargs),
    )

    assert review_queue_aging_script.main(["--bucket-days", "2,5", "--limit", "3", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["bucket_days"] == [2, 5]
    assert payload["groups"][0]["oldest_item"]["content_id"] == content_id

    assert review_queue_aging_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    conn.close()


def test_missing_schema_and_invalid_builder_args():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_review_queue_aging_report(conn, now=NOW)
    assert report["missing_tables"] == ["generated_content"]

    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY)")
    report = build_review_queue_aging_report(conn, now=NOW)
    assert report["missing_columns"] == {"generated_content": ["created_at"]}

    with pytest.raises(ValueError, match="bucket_days must not be empty"):
        build_review_queue_aging_report(conn, bucket_days=(), now=NOW)
    with pytest.raises(ValueError, match="bucket_days values must be positive"):
        build_review_queue_aging_report(conn, bucket_days=(1, 0), now=NOW)
    with pytest.raises(ValueError, match="bucket_days values must be unique"):
        build_review_queue_aging_report(conn, bucket_days=(1, 1), now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_review_queue_aging_report(conn, limit=0, now=NOW)
    conn.close()
