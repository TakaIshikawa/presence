"""Tests for curated source recrawl backlog planning."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from knowledge.source_recrawl_backlog import (
    BACKOFF,
    HEALTHY,
    NEEDS_FEED_URL,
    RECRAWL_NOW,
    build_source_recrawl_backlog_report,
    format_source_recrawl_backlog_json,
    format_source_recrawl_backlog_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_recrawl_backlog.py"
spec = importlib.util.spec_from_file_location("source_recrawl_backlog_script", SCRIPT_PATH)
source_recrawl_backlog_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(source_recrawl_backlog_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _source(
    db,
    *,
    source_type: str = "blog",
    identifier: str,
    status: str = "active",
    feed_url: str | None = "https://example.com/feed.xml",
    last_success_at: datetime | None = None,
    last_failure_at: datetime | None = None,
    consecutive_failures: int = 0,
    feed_etag: str | None = None,
    feed_last_modified: str | None = None,
) -> int:
    row_id = db.conn.execute(
        """INSERT INTO curated_sources
           (source_type, identifier, status, active, feed_url, last_success_at,
            last_failure_at, consecutive_failures, feed_etag, feed_last_modified)
           VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?)""",
        (
            source_type,
            identifier,
            status,
            feed_url,
            last_success_at.isoformat() if last_success_at else None,
            last_failure_at.isoformat() if last_failure_at else None,
            consecutive_failures,
            feed_etag,
            feed_last_modified,
        ),
    ).lastrowid
    db.conn.commit()
    return int(row_id)


def _knowledge(
    db,
    *,
    source_type: str = "curated_article",
    source_id: str = "https://example.com/post",
    source_url: str = "https://example.com/post",
    author: str = "example.com",
    published_at: datetime | None = None,
    ingested_at: datetime | None = None,
) -> int:
    row_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, approved,
            published_at, ingested_at, created_at)
           VALUES (?, ?, ?, ?, 'knowledge', 1, ?, ?, ?)""",
        (
            source_type,
            source_id,
            source_url,
            author,
            published_at.isoformat() if published_at else None,
            ingested_at.isoformat() if ingested_at else None,
            (ingested_at or published_at or NOW).isoformat(),
        ),
    ).lastrowid
    db.conn.commit()
    return int(row_id)


def test_stale_successful_sources_rank_ahead_of_healthy_sources(db):
    stale_id = _source(
        db,
        identifier="stale.example",
        feed_url="https://stale.example/feed.xml",
        last_success_at=NOW - timedelta(days=45),
    )
    healthy_id = _source(
        db,
        identifier="healthy.example",
        feed_url="https://healthy.example/feed.xml",
        last_success_at=NOW - timedelta(days=2),
    )

    report = build_source_recrawl_backlog_report(db, stale_days=14, now=NOW)

    assert [item.source_id for item in report.recommendations] == [stale_id, healthy_id]
    assert [item.recommendation for item in report.recommendations] == [RECRAWL_NOW, HEALTHY]
    assert report.recommendations[0].reason == "stale_success"
    assert report.totals["recrawl_now_count"] == 1
    assert report.totals["healthy_count"] == 1


def test_repeated_recent_failures_are_marked_backoff_with_next_eligible_date(db):
    source_id = _source(
        db,
        identifier="broken.example",
        feed_url="https://broken.example/feed.xml",
        last_success_at=NOW - timedelta(days=30),
        last_failure_at=NOW - timedelta(days=1),
        consecutive_failures=3,
    )

    report = build_source_recrawl_backlog_report(
        db,
        failure_backoff_days=3,
        now=NOW,
    )
    item = report.recommendations[0]

    assert item.source_id == source_id
    assert item.recommendation == BACKOFF
    assert item.reason == "recent_repeated_failures"
    assert item.next_eligible_at == (NOW - timedelta(days=1) + timedelta(days=3)).isoformat()


def test_elapsed_backoff_recrawls_failed_sources(db):
    _source(
        db,
        identifier="retry.example",
        feed_url="https://retry.example/feed.xml",
        last_success_at=NOW - timedelta(days=30),
        last_failure_at=NOW - timedelta(days=10),
        consecutive_failures=2,
    )

    report = build_source_recrawl_backlog_report(db, failure_backoff_days=3, now=NOW)

    assert report.recommendations[0].recommendation == RECRAWL_NOW
    assert report.recommendations[0].reason == "failure_backoff_elapsed"


def test_sources_missing_feed_url_are_reported_separately(db):
    missing_id = _source(
        db,
        source_type="newsletter",
        identifier="weekly.example",
        feed_url=None,
        last_success_at=NOW - timedelta(days=1),
    )
    _source(
        db,
        source_type="blog",
        identifier="ready.example",
        feed_url="https://ready.example/rss",
        last_success_at=NOW - timedelta(days=1),
    )

    report = build_source_recrawl_backlog_report(db, now=NOW)

    assert [item.source_id for item in report.missing_feed_url] == [missing_id]
    assert report.missing_feed_url[0].recommendation == NEEDS_FEED_URL
    assert report.totals["needs_feed_url_count"] == 1
    assert "Missing feed_url" in format_source_recrawl_backlog_text(report)


def test_knowledge_freshness_can_keep_source_healthy(db):
    _source(
        db,
        identifier="example.com",
        feed_url="https://example.com/feed.xml",
        last_success_at=NOW - timedelta(days=60),
    )
    _knowledge(
        db,
        author="example.com",
        source_url="https://example.com/post",
        published_at=NOW - timedelta(days=2),
    )

    report = build_source_recrawl_backlog_report(db, stale_days=14, now=NOW)
    item = report.recommendations[0]

    assert item.recommendation == HEALTHY
    assert item.knowledge_item_count == 1
    assert item.latest_knowledge_at == (NOW - timedelta(days=2)).isoformat()
    assert item.freshness_at == (NOW - timedelta(days=2)).isoformat()


def test_missing_schema_and_invalid_args_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_source_recrawl_backlog_report(conn, now=NOW)

    assert report.recommendations == ()
    assert report.missing_tables == ("curated_sources",)
    assert report.totals["source_count"] == 0
    with pytest.raises(ValueError, match="stale_days must be positive"):
        build_source_recrawl_backlog_report(conn, stale_days=0, now=NOW)
    with pytest.raises(ValueError, match="failure_backoff_days must be positive"):
        build_source_recrawl_backlog_report(conn, failure_backoff_days=0, now=NOW)
    conn.close()


def test_formatters_emit_deterministic_text_and_json(db):
    source_id = _source(
        db,
        source_type="blog",
        identifier="cached.example",
        feed_url="https://cached.example/rss",
        last_success_at=NOW - timedelta(days=20),
        feed_etag='"v1"',
        feed_last_modified=(NOW - timedelta(days=19)).isoformat(),
    )

    report = build_source_recrawl_backlog_report(db, source_type="blog", stale_days=14, now=NOW)
    text = format_source_recrawl_backlog_text(report)
    payload = json.loads(format_source_recrawl_backlog_json(report))

    assert "Curated Source Recrawl Backlog" in text
    assert "blog:cached.example" in text
    assert "feed_cache=etag:\"v1\"" in text
    assert payload["artifact_type"] == "source_recrawl_backlog"
    assert payload["recommendations"][0]["source_id"] == source_id
    assert list(payload.keys()) == sorted(payload.keys())
    assert format_source_recrawl_backlog_json(report) == format_source_recrawl_backlog_json(report)


def test_cli_outputs_json_for_configured_and_file_db(db, file_db, monkeypatch, capsys):
    _source(
        db,
        source_type="x_account",
        identifier="@alice",
        feed_url=None,
        last_success_at=NOW - timedelta(days=1),
    )
    _source(
        file_db,
        source_type="blog",
        identifier="file.example",
        feed_url="https://file.example/rss",
        last_success_at=NOW - timedelta(days=30),
    )
    monkeypatch.setattr(
        source_recrawl_backlog_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        source_recrawl_backlog_script,
        "build_source_recrawl_backlog_report",
        lambda db, **kwargs: build_source_recrawl_backlog_report(db, now=NOW, **kwargs),
    )

    assert source_recrawl_backlog_script.main(["--source-type", "x_account", "--format", "json"]) == 0
    configured_payload = json.loads(capsys.readouterr().out)
    assert configured_payload["recommendations"][0]["identifier"] == "@alice"

    assert (
        source_recrawl_backlog_script.main(
            ["--db", str(file_db.db_path), "--source-type", "blog", "--format", "json"]
        )
        == 0
    )
    file_payload = json.loads(capsys.readouterr().out)
    assert file_payload["recommendations"][0]["identifier"] == "file.example"
