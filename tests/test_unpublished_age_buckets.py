"""Tests for unpublished content age bucket reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from output.unpublished_age_buckets import (
    build_unpublished_age_bucket_report,
    format_unpublished_age_bucket_json,
    format_unpublished_age_bucket_markdown,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "report_unpublished_age_buckets.py"
spec = importlib.util.spec_from_file_location("report_unpublished_age_buckets_script", SCRIPT_PATH)
report_unpublished_age_buckets_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(report_unpublished_age_buckets_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_content(db, text: str, *, hours_ago: float, published: int = 0) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        content=text,
        eval_score=8.0,
        eval_feedback="needs review",
    )
    created_at = (NOW - timedelta(hours=hours_ago)).isoformat()
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published = ? WHERE id = ?",
        (created_at, published, content_id),
    )
    db.conn.commit()
    return content_id


def test_empty_input_returns_empty_buckets():
    report = build_unpublished_age_bucket_report([], now=NOW)

    assert report["counts"]["rows_scanned"] == 0
    assert report["counts"]["records"] == 0
    assert report["buckets"] == []
    assert json.loads(format_unpublished_age_bucket_json(report))["artifact_type"] == (
        "unpublished_age_buckets"
    )
    assert "| none | 0 |" in format_unpublished_age_bucket_markdown(report)


def test_boundary_timestamps_are_bucketed_deterministically():
    rows = [
        {"id": 1, "created_at": (NOW - timedelta(hours=23, minutes=59)).isoformat()},
        {"id": 2, "created_at": (NOW - timedelta(hours=24)).isoformat()},
        {"id": 3, "created_at": (NOW - timedelta(hours=72)).isoformat()},
        {"id": 4, "created_at": (NOW - timedelta(hours=168)).isoformat()},
        {"id": 5, "created_at": (NOW - timedelta(hours=336)).isoformat()},
    ]

    report = build_unpublished_age_bucket_report(rows, now=NOW)

    assert [bucket["label"] for bucket in report["buckets"]] == ["14d", "7d", "3d", "1d"]
    assert report["counts"]["by_bucket"] == {"14d": 1, "7d": 1, "3d": 1, "1d": 1}
    assert report["buckets"][-1]["records"][0]["content_id"] == 2


def test_database_rows_exclude_young_published_abandoned_and_attempted_content(db):
    young = _insert_content(db, "young", hours_ago=12)
    stale = _insert_content(db, "stale", hours_ago=80)
    published = _insert_content(db, "published", hours_ago=120, published=1)
    abandoned = _insert_content(db, "abandoned", hours_ago=120, published=-1)
    attempted = _insert_content(db, "attempted", hours_ago=120)
    db.record_publication_attempt(None, attempted, "x", False, attempted_at=NOW.isoformat())
    db.upsert_publication_queued(stale, "x")
    db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, hold_reason, created_at)
           VALUES (?, ?, 'x', 'held', 'waiting on review', ?)""",
        (stale, (NOW - timedelta(hours=12)).isoformat(), (NOW - timedelta(hours=80)).isoformat()),
    )
    db.conn.commit()

    report = build_unpublished_age_bucket_report(db, now=NOW)

    assert report["counts"]["rows_scanned"] == 4
    assert [record["content_id"] for bucket in report["buckets"] for record in bucket["records"]] == [
        stale,
    ]
    record = report["buckets"][0]["records"][0]
    assert record["status"] == "mixed:held,queued"
    assert record["reason"] == "waiting on review"
    assert young not in [record["content_id"] for bucket in report["buckets"] for record in bucket["records"]]
    assert published not in [record["content_id"] for bucket in report["buckets"] for record in bucket["records"]]
    assert abandoned not in [record["content_id"] for bucket in report["buckets"] for record in bucket["records"]]


def test_markdown_and_cli_json_output(db, monkeypatch, capsys):
    content_id = _insert_content(db, "cli", hours_ago=25)
    monkeypatch.setattr(
        report_unpublished_age_buckets_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        report_unpublished_age_buckets_script,
        "build_unpublished_age_bucket_report",
        lambda db, **kwargs: build_unpublished_age_bucket_report(db, now=NOW, **kwargs),
    )

    exit_code = report_unpublished_age_buckets_script.main(["--format", "json"])
    payload = json.loads(capsys.readouterr().out)
    markdown = format_unpublished_age_bucket_markdown(
        build_unpublished_age_bucket_report(db, now=NOW)
    )

    assert exit_code == 0
    assert payload["artifact_type"] == "unpublished_age_buckets"
    assert payload["buckets"][0]["records"][0]["content_id"] == content_id
    assert "| Bucket | Count | Newest | Oldest | Statuses | Reasons |" in markdown
    assert "| 1d | 1 |" in markdown


def test_invalid_arguments_and_missing_schema():
    with pytest.raises(ValueError, match="thresholds_hours must not be empty"):
        build_unpublished_age_bucket_report([], thresholds_hours=(), now=NOW)
    with pytest.raises(ValueError, match="thresholds_hours values must be positive"):
        build_unpublished_age_bucket_report([], thresholds_hours=(24, 0), now=NOW)
    with pytest.raises(ValueError, match="min_age_hours must be non-negative"):
        build_unpublished_age_bucket_report([], min_age_hours=-1, now=NOW)

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_unpublished_age_bucket_report(conn, now=NOW)
    assert report["missing_tables"] == ["generated_content"]
    assert report["buckets"] == []
