"""Tests for curated source ingestion failure digest."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from knowledge.curated_ingestion_failure_digest import (
    build_curated_ingestion_failure_digest,
    build_curated_ingestion_failure_digest_report,
    format_curated_ingestion_failure_digest_json,
    normalize_curated_ingestion_error_category,
    retryability_for_error_category,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "curated_ingestion_failures.py"
spec = importlib.util.spec_from_file_location("curated_ingestion_failures_script", SCRIPT_PATH)
curated_ingestion_failures_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(curated_ingestion_failures_script)


@contextmanager
def _script_context(conn: sqlite3.Connection):
    yield SimpleNamespace(), SimpleNamespace(conn=conn)


def _conn(*, full_schema: bool = True) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    if full_schema:
        conn.execute(
            """CREATE TABLE curated_sources (
                id INTEGER PRIMARY KEY,
                source_type TEXT,
                identifier TEXT,
                name TEXT,
                feed_url TEXT,
                homepage_url TEXT,
                status TEXT,
                last_fetch_status TEXT,
                consecutive_failures INTEGER,
                last_success_at TEXT,
                last_failure_at TEXT,
                last_error TEXT
            )"""
        )
    else:
        conn.execute(
            """CREATE TABLE curated_sources (
                id INTEGER PRIMARY KEY,
                source_type TEXT,
                identifier TEXT
            )"""
        )
    return conn


def _insert_source(
    conn: sqlite3.Connection,
    *,
    source_type: str,
    identifier: str,
    failures: int,
    last_error: str,
    last_failure_at: str,
    last_success_at: str | None = None,
    feed_url: str | None = None,
    status: str = "active",
    fetch_status: str = "failure",
) -> None:
    conn.execute(
        """INSERT INTO curated_sources
           (source_type, identifier, name, feed_url, status, last_fetch_status,
            consecutive_failures, last_success_at, last_failure_at, last_error)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            source_type,
            identifier,
            identifier.title(),
            feed_url,
            status,
            fetch_status,
            failures,
            last_success_at,
            last_failure_at,
            last_error,
        ),
    )
    conn.commit()


def test_failure_rows_group_by_source_category_and_retryability():
    rows = [
        {
            "source_type": "blog",
            "identifier": "https://www.example.com/blog",
            "consecutive_failures": 2,
            "last_error": "TimeoutError: feed request timed out",
            "last_failure_at": "2026-05-02T10:00:00+00:00",
            "last_success_at": "2026-04-01T00:00:00+00:00",
        },
        {
            "source_type": "blog",
            "identifier": "example.com",
            "consecutive_failures": 1,
            "last_error": "Connection reset by peer",
            "last_failure_at": "2026-05-02T11:00:00+00:00",
            "last_success_at": "2026-04-15T00:00:00+00:00",
        },
        {
            "source_type": "x_account",
            "identifier": "MissingUser",
            "consecutive_failures": 1,
            "last_error": "User @MissingUser not found",
            "last_failure_at": "2026-05-02T09:00:00+00:00",
        },
    ]

    report = build_curated_ingestion_failure_digest(rows, now=NOW)

    assert report["totals"]["failure_groups"] == 2
    assert report["totals"]["failures"] == 4
    first = report["groups"][0]
    assert first["source"] == "example.com"
    assert first["error_category"] == "network"
    assert first["retryability"] == "retryable"
    assert first["failure_count"] == 3
    assert first["source_count"] == 2
    assert first["stale_success_age_days"] == 31
    assert report["groups"][1]["retryability"] == "non_retryable"


def test_repeated_failures_rank_ahead_of_stale_one_off_failures():
    rows = [
        {
            "source_type": "newsletter",
            "identifier": "very-stale.example.com",
            "consecutive_failures": 1,
            "last_error": "FeedFetchError: malformed XML",
            "last_failure_at": "2026-05-02T10:00:00+00:00",
            "last_success_at": "2026-01-01T00:00:00+00:00",
        },
        {
            "source_type": "blog",
            "identifier": "repeated.example.com",
            "consecutive_failures": 3,
            "last_error": "HTTP Error 503: Service Unavailable",
            "last_failure_at": "2026-05-02T11:00:00+00:00",
            "last_success_at": "2026-04-30T00:00:00+00:00",
        },
    ]

    report = build_curated_ingestion_failure_digest(rows, now=NOW)

    assert [group["source"] for group in report["groups"]] == [
        "repeated.example.com",
        "very-stale.example.com",
    ]


def test_error_category_and_retryability_classification():
    assert normalize_curated_ingestion_error_category("Rate limit exceeded") == "rate_limit"
    assert normalize_curated_ingestion_error_category("HTTPError: 403 Forbidden") == "auth"
    assert normalize_curated_ingestion_error_category("FeedParseError: bad RSS XML") == "parse"
    assert retryability_for_error_category("rate_limit") == "retryable"
    assert retryability_for_error_category("auth") == "non_retryable"
    assert retryability_for_error_category("unknown") == "unknown"


def test_database_report_filters_recent_failures_and_min_count():
    conn = _conn()
    _insert_source(
        conn,
        source_type="blog",
        identifier="kept.example.com",
        failures=2,
        last_error="TimeoutError: timed out",
        last_failure_at="2026-05-01T12:00:00+00:00",
        last_success_at="2026-04-01T00:00:00+00:00",
    )
    _insert_source(
        conn,
        source_type="blog",
        identifier="old.example.com",
        failures=5,
        last_error="TimeoutError: timed out",
        last_failure_at="2026-04-01T12:00:00+00:00",
    )
    _insert_source(
        conn,
        source_type="newsletter",
        identifier="one-off.example.com",
        failures=1,
        last_error="HTTP 503",
        last_failure_at="2026-05-01T12:00:00+00:00",
    )

    report = build_curated_ingestion_failure_digest_report(
        conn,
        days=7,
        min_failures=2,
        now=NOW,
    )
    payload = json.loads(format_curated_ingestion_failure_digest_json(report))

    assert payload["artifact_type"] == "curated_ingestion_failure_digest"
    assert payload["filters"]["min_failures"] == 2
    assert payload["totals"]["failure_rows_scanned"] == 2
    assert [group["source"] for group in payload["groups"]] == ["kept.example.com"]


def test_missing_optional_columns_are_reported_without_failure():
    conn = _conn(full_schema=False)
    conn.execute(
        "INSERT INTO curated_sources (source_type, identifier) VALUES ('blog', 'example.com')"
    )
    conn.commit()

    report = build_curated_ingestion_failure_digest_report(conn, now=NOW)

    assert report["schema_gaps"]["missing_columns"]["curated_sources"]
    assert report["totals"]["failure_rows_scanned"] == 1
    assert report["groups"][0]["source"] == "example.com"
    assert report["groups"][0]["error_category"] == "unknown"


def test_missing_table_returns_schema_gap():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_curated_ingestion_failure_digest_report(conn, now=NOW)

    assert report["schema_gaps"]["missing_tables"] == ["curated_sources"]
    assert report["source_table"] is None
    assert report["totals"]["failures"] == 0


def test_cli_validates_positive_arguments_and_emits_json(monkeypatch, capsys):
    conn = _conn()
    _insert_source(
        conn,
        source_type="x_account",
        identifier="example",
        failures=2,
        last_error="Rate limit exceeded",
        last_failure_at="2026-05-02T10:00:00+00:00",
    )
    monkeypatch.setattr(
        curated_ingestion_failures_script,
        "script_context",
        lambda: _script_context(conn),
    )
    monkeypatch.setattr(
        curated_ingestion_failures_script,
        "build_curated_ingestion_failure_digest_report",
        lambda db, **kwargs: build_curated_ingestion_failure_digest_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert curated_ingestion_failures_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    exit_code = curated_ingestion_failures_script.main(["--days", "7", "--min-failures", "2"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["groups"][0]["source"] == "@example"
    assert payload["groups"][0]["error_category"] == "rate_limit"
