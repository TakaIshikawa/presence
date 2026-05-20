"""Tests for publication attempt error recovery latency reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.publication_attempt_error_recovery_latency import (
    build_publication_attempt_error_recovery_latency_report,
    build_publication_attempt_error_recovery_latency_report_from_db,
    format_publication_attempt_error_recovery_latency_json,
    format_publication_attempt_error_recovery_latency_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_error_recovery_latency.py"
spec = importlib.util.spec_from_file_location("publication_attempt_error_recovery_latency_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn(path: Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(path or ":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            attempted_at TEXT,
            success INTEGER,
            error TEXT,
            error_category TEXT
        )"""
    )
    return conn


def test_pairs_failures_with_next_later_success_for_same_content_and_platform():
    rows = [
        _row(1, 10, "x", "2026-05-20T01:00:00+00:00", 0, "network"),
        _row(2, 10, "x", "2026-05-20T03:00:00+00:00", 0, "network"),
        _row(3, 10, "x", "2026-05-20T07:00:00+00:00", 1, None),
        _row(4, 10, "bluesky", "2026-05-20T02:00:00+00:00", 1, None),
        _row(5, 11, "x", "2026-05-20T04:00:00+00:00", 0, "auth"),
    ]

    report = build_publication_attempt_error_recovery_latency_report(rows, now=NOW, lookback_days=2)

    network = report["latency_buckets"][0]
    assert network["platform"] == "x"
    assert network["error_category"] == "network"
    assert network["failure_count"] == 2
    assert network["median_recovery_hours"] == 5
    assert network["p95_recovery_hours"] == 6
    assert network["representative_attempt_ids"] == ["1", "2"]
    unresolved = report["unresolved_buckets"][0]
    assert unresolved["error_category"] == "auth"
    assert unresolved["unresolved_failure_count"] == 1
    assert report["summary"]["recovered_failure_count"] == 2
    assert report["summary"]["unresolved_failure_count"] == 1


def test_groups_by_normalized_category_platform_filters_and_min_count():
    rows = [
        _row(1, 10, "x", "2026-05-20T01:00:00+00:00", 0, "not-real"),
        _row(2, 10, "x", "2026-05-20T02:00:00+00:00", 1, None),
        _row(3, 11, "bluesky", "2026-05-20T01:00:00+00:00", 0, "network"),
        _row(4, 11, "bluesky", "2026-05-20T02:00:00+00:00", 1, None),
    ]

    report = build_publication_attempt_error_recovery_latency_report(
        rows,
        now=NOW,
        platform="x",
        min_count=1,
    )

    assert [bucket["platform"] for bucket in report["latency_buckets"]] == ["x"]
    assert report["latency_buckets"][0]["error_category"] == "unknown"

    filtered = build_publication_attempt_error_recovery_latency_report(rows, now=NOW, min_count=2)
    assert filtered["latency_buckets"] == []
    assert filtered["summary"]["recovered_failure_count"] == 2


def test_db_adapter_handles_missing_table_and_loads_attempts():
    missing = build_publication_attempt_error_recovery_latency_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["publication_attempts"]
    assert missing["summary"]["failure_count"] == 0

    conn = _conn()
    conn.executemany(
        "INSERT INTO publication_attempts VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "x", "2026-05-20T00:00:00+00:00", 0, "timeout", "network"),
            (2, 1, "x", "2026-05-20T06:00:00+00:00", 1, None, None),
        ],
    )

    report = build_publication_attempt_error_recovery_latency_report_from_db(conn, now=NOW)

    assert report["latency_buckets"][0]["median_recovery_hours"] == 6


def test_json_text_cli_and_argument_validation(tmp_path, capsys):
    conn = _conn(tmp_path / "attempts.sqlite")
    conn.executemany(
        "INSERT INTO publication_attempts VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "x", "2026-05-20T00:00:00+00:00", 0, "timeout", "network"),
            (2, 1, "x", "2026-05-20T02:00:00+00:00", 1, None, None),
        ],
    )
    conn.commit()
    conn.close()

    report = build_publication_attempt_error_recovery_latency_report([_row(1, 1, "x", "2026-05-20T00:00:00+00:00", 0, "network")], now=NOW)
    payload = json.loads(format_publication_attempt_error_recovery_latency_json(report))
    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "publication_attempt_error_recovery_latency"
    text = format_publication_attempt_error_recovery_latency_text(report)
    assert "Unresolved failure buckets:" in text
    assert "Publication Attempt Error Recovery Latency" in text

    db_path = tmp_path / "attempts.sqlite"
    assert script.main(["--db", str(db_path), "--format", "json", "--platform", "x", "--limit", "5"]) == 0
    assert '"artifact_type": "publication_attempt_error_recovery_latency"' in capsys.readouterr().out
    assert script.main(["--lookback-days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_invalid_builder_arguments_raise():
    with pytest.raises(ValueError, match="lookback_days must be positive"):
        build_publication_attempt_error_recovery_latency_report([], lookback_days=0, now=NOW)
    with pytest.raises(ValueError, match="min_count must be positive"):
        build_publication_attempt_error_recovery_latency_report([], min_count=0, now=NOW)
    with pytest.raises(ValueError, match="invalid platform"):
        build_publication_attempt_error_recovery_latency_report([], platform="mastodon", now=NOW)


def _row(
    attempt_id: int,
    content_id: int,
    platform: str,
    attempted_at: str,
    success: int,
    error_category: str | None,
) -> dict:
    return {
        "attempt_id": attempt_id,
        "content_id": content_id,
        "platform": platform,
        "attempted_at": attempted_at,
        "success": success,
        "error_category": error_category,
        "error": "failed" if not success else None,
    }
