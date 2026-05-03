"""Tests for curated source knowledge ingest failure reporting."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from knowledge.source_ingest_failures import (
    build_knowledge_source_ingest_failure_report,
    format_knowledge_source_ingest_failures_json,
    format_knowledge_source_ingest_failures_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "knowledge_ingest_failures.py"
)
spec = importlib.util.spec_from_file_location("knowledge_ingest_failures_script", SCRIPT_PATH)
knowledge_ingest_failures_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(knowledge_ingest_failures_script)


def _iso(days_ago: int) -> str:
    return (NOW - timedelta(days=days_ago)).isoformat()


def _add_source(
    db,
    source_type: str,
    identifier: str,
    *,
    name: str | None = None,
    feed_url: str | None = None,
    failures: int = 0,
    fetch_status: str | None = "success",
    last_success_at: str | None = None,
    last_failure_at: str | None = None,
    last_error: str | None = None,
) -> int:
    db.sync_config_sources(
        [{"identifier": identifier, "name": name or identifier, "feed_url": feed_url}],
        source_type,
    )
    db.conn.execute(
        """UPDATE curated_sources
           SET consecutive_failures = ?,
               last_fetch_status = ?,
               last_success_at = ?,
               last_failure_at = ?,
               last_error = ?
           WHERE source_type = ? AND identifier = ?""",
        (
            failures,
            fetch_status,
            last_success_at,
            last_failure_at,
            last_error,
            source_type,
            identifier,
        ),
    )
    db.conn.commit()
    return int(db.get_curated_source(source_type, identifier)["id"])


def _add_knowledge(
    db,
    *,
    source_type: str,
    source_id: str,
    source_url: str,
    author: str | None = None,
    days_ago: int = 1,
) -> None:
    timestamp = _iso(days_ago)
    db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            approved, published_at, ingested_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)""",
        (
            source_type,
            source_id,
            source_url,
            author,
            f"content {source_id}",
            f"insight {source_id}",
            timestamp,
            timestamp,
            timestamp,
        ),
    )
    db.conn.commit()


def test_failed_sources_and_text_output_include_operator_fields(db):
    source_id = _add_source(
        db,
        "blog",
        "example.com",
        name="Example Blog",
        feed_url="https://example.com/rss",
        failures=3,
        fetch_status="failure",
        last_success_at=_iso(10),
        last_failure_at=_iso(1),
        last_error="TimeoutError: feed request timed out",
    )
    _add_knowledge(
        db,
        source_type="curated_article",
        source_id="example.com",
        source_url="https://example.com/post",
        days_ago=3,
    )

    report = build_knowledge_source_ingest_failure_report(db, days=30, now=NOW)
    text = format_knowledge_source_ingest_failures_text(report)

    assert report.rows[0].id == source_id
    assert report.rows[0].failure_bucket == "failure:network"
    assert report.rows[0].last_success_age_days == 10
    assert report.rows[0].recent_knowledge_count == 1
    assert report.totals["by_source_type_and_failure_bucket"] == {
        "blog": {"failure:network": 1}
    }
    assert "Example Blog" in text
    assert "https://example.com/rss" in text
    assert "TimeoutError" in text
    assert "action=Retry ingestion" in text


def test_stale_success_and_missing_recent_knowledge_are_reported(db):
    _add_source(
        db,
        "newsletter",
        "letter.example.com",
        feed_url="https://letter.example.com/feed",
        last_success_at=_iso(45),
    )
    _add_knowledge(
        db,
        source_type="curated_newsletter",
        source_id="letter.example.com",
        source_url="https://letter.example.com/old",
        days_ago=60,
    )
    _add_source(
        db,
        "blog",
        "quiet.example.com",
        feed_url="https://quiet.example.com/rss",
        last_success_at=_iso(5),
    )

    report = build_knowledge_source_ingest_failure_report(db, days=30, now=NOW)
    by_identifier = {row.identifier: row for row in report.rows}

    assert by_identifier["letter.example.com"].failure_bucket == "stale_last_success"
    assert by_identifier["letter.example.com"].total_knowledge_count == 1
    assert by_identifier["letter.example.com"].recent_knowledge_count == 0
    assert by_identifier["quiet.example.com"].failure_bucket == "no_recent_knowledge"


def test_missing_last_success_takes_precedence_over_recent_rows(db):
    _add_source(db, "x_account", "alice", fetch_status=None, last_success_at=None)
    _add_knowledge(
        db,
        source_type="curated_x",
        source_id="tweet-1",
        source_url="https://x.com/alice/status/1",
        author="alice",
        days_ago=2,
    )

    report = build_knowledge_source_ingest_failure_report(db, days=30, now=NOW)

    assert report.rows[0].identifier == "alice"
    assert report.rows[0].failure_bucket == "missing_last_success"
    assert report.rows[0].recent_knowledge_count == 1


def test_healthy_filtering_and_source_type_filter(db):
    _add_source(db, "blog", "healthy.example.com", last_success_at=_iso(2))
    _add_knowledge(
        db,
        source_type="curated_article",
        source_id="healthy.example.com",
        source_url="https://healthy.example.com/post",
        days_ago=1,
    )
    _add_source(db, "newsletter", "broken.example.com", last_success_at=None)

    filtered = build_knowledge_source_ingest_failure_report(
        db,
        days=30,
        source_type="blog",
        now=NOW,
    )
    with_healthy = build_knowledge_source_ingest_failure_report(
        db,
        days=30,
        source_type="blog",
        include_healthy=True,
        now=NOW,
    )

    assert filtered.rows == ()
    assert filtered.totals["sources_scanned"] == 1
    assert with_healthy.rows[0].identifier == "healthy.example.com"
    assert with_healthy.rows[0].failure_bucket == "healthy"


def test_malformed_metadata_is_reported_without_crashing():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE curated_sources (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            identifier TEXT,
            name TEXT,
            metadata TEXT
        );
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            source_id TEXT,
            source_url TEXT,
            author TEXT,
            approved INTEGER,
            published_at TEXT
        );
        INSERT INTO curated_sources (id, source_type, identifier, name, metadata)
        VALUES (1, 'blog', 'badmeta.example.com', 'Bad Metadata', '{not-json');
        """
    )

    report = build_knowledge_source_ingest_failure_report(conn, days=30, now=NOW)

    assert report.rows[0].failure_bucket == "malformed_metadata"
    assert report.rows[0].malformed_metadata is True
    assert report.totals["malformed_metadata_count"] == 1
    assert "last_success_at" in report.missing_columns["curated_sources"]


def test_json_output_is_deterministic_and_cli_supports_db(capsys, tmp_path):
    conn = sqlite3.connect(tmp_path / "knowledge.db")
    conn.executescript(
        """
        CREATE TABLE curated_sources (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            identifier TEXT,
            name TEXT,
            feed_url TEXT,
            status TEXT,
            last_fetch_status TEXT,
            consecutive_failures INTEGER,
            last_success_at TEXT,
            last_failure_at TEXT,
            last_error TEXT
        );
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            source_id TEXT,
            source_url TEXT,
            author TEXT,
            approved INTEGER,
            published_at TEXT
        );
        INSERT INTO curated_sources
          (id, source_type, identifier, name, feed_url, status, last_fetch_status,
           consecutive_failures, last_success_at, last_failure_at, last_error)
        VALUES
          (1, 'blog', 'cli.example.com', 'CLI Example', 'https://cli.example.com/rss',
           'active', 'failure', 2, NULL, '2026-04-30T12:00:00+00:00',
           'HTTP 404 not found');
        """
    )
    conn.commit()
    conn.close()

    direct = sqlite3.connect(tmp_path / "knowledge.db")
    direct.row_factory = sqlite3.Row
    report = build_knowledge_source_ingest_failure_report(direct, now=NOW)
    payload = json.loads(format_knowledge_source_ingest_failures_json(report))
    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "knowledge_source_ingest_failures"
    assert payload["rows"][0]["failure_bucket"] == "failure:not_found"
    direct.close()

    assert knowledge_ingest_failures_script.main(
        ["--db", str(tmp_path / "knowledge.db"), "--format", "json"]
    ) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["rows"][0]["identifier"] == "cli.example.com"

    assert knowledge_ingest_failures_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
