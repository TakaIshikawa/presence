"""Tests for API rate-limit snapshot payload integrity reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.api_rate_limit_snapshot_payload_integrity import (
    build_api_rate_limit_snapshot_payload_integrity_report,
    build_api_rate_limit_snapshot_payload_integrity_report_from_db,
    format_api_rate_limit_snapshot_payload_integrity_json,
    format_api_rate_limit_snapshot_payload_integrity_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "api_rate_limit_snapshot_payload_integrity.py"
spec = importlib.util.spec_from_file_location("api_rate_limit_snapshot_payload_integrity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE api_rate_limit_snapshots (
            id INTEGER PRIMARY KEY,
            provider TEXT,
            endpoint TEXT,
            remaining INTEGER,
            limit_value INTEGER,
            reset_at TEXT,
            raw_metadata TEXT,
            fetched_at TEXT
        )"""
    )
    return conn


def _insert(
    conn: sqlite3.Connection,
    *,
    snapshot_id: int,
    provider: str = "x",
    endpoint: str = "/tweets",
    remaining: int = 1,
    limit_value: int | None = 10,
    reset_at: str | None = None,
    raw_metadata: str | None = "{}",
    fetched_minutes_ago: int = 5,
) -> None:
    conn.execute(
        """INSERT INTO api_rate_limit_snapshots
           (id, provider, endpoint, remaining, limit_value, reset_at, raw_metadata, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            snapshot_id,
            provider,
            endpoint,
            remaining,
            limit_value,
            reset_at,
            raw_metadata,
            (NOW - timedelta(minutes=fetched_minutes_ago)).isoformat(),
        ),
    )


def test_builder_flags_payload_integrity_reasons_in_stable_order():
    rows = [
        {"id": 1, "provider": "x", "endpoint": "default", "remaining": 8, "limit_value": 10, "reset_at": NOW.isoformat(), "raw_metadata": "{", "fetched_at": NOW.isoformat()},
        {"id": 2, "provider": "x", "endpoint": "/2/tweets", "remaining": -1, "limit_value": 10, "reset_at": NOW.isoformat(), "raw_metadata": "{}", "fetched_at": NOW.isoformat()},
        {"id": 3, "provider": "github", "endpoint": "/repos", "remaining": 12, "limit_value": 10, "reset_at": NOW.isoformat(), "raw_metadata": "{}", "fetched_at": NOW.isoformat()},
        {"id": 4, "provider": "openai", "endpoint": "/models", "remaining": 1, "limit_value": 10, "reset_at": None, "raw_metadata": "{}", "fetched_at": NOW.isoformat()},
    ]
    report = build_api_rate_limit_snapshot_payload_integrity_report(rows, low_remaining=5, now=NOW)
    payload = json.loads(format_api_rate_limit_snapshot_payload_integrity_json(report))

    assert payload["artifact_type"] == "api_rate_limit_snapshot_payload_integrity"
    assert [item["reason"] for item in payload["findings"]] == [
        "malformed_raw_metadata_json",
        "negative_remaining",
        "limit_below_remaining",
        "missing_reset_at_low_remaining",
        "default_endpoint_overuse",
    ]
    assert payload["summary"]["by_reason"] == {
        "default_endpoint_overuse": 1,
        "limit_below_remaining": 1,
        "malformed_raw_metadata_json": 1,
        "missing_reset_at_low_remaining": 1,
        "negative_remaining": 1,
    }
    assert {"provider", "endpoint", "reason", "count"}.issubset(payload["groups"][0])


def test_db_loader_cli_and_schema_gaps(tmp_path, capsys):
    conn = _conn()
    _insert(conn, snapshot_id=1, provider="x", endpoint="default", remaining=2, reset_at=NOW.isoformat())
    _insert(conn, snapshot_id=2, provider="x", endpoint="/tweets", remaining=9, reset_at=NOW.isoformat())
    _insert(conn, snapshot_id=3, provider="github", endpoint="/repos", remaining=7, limit_value=5, reset_at=NOW.isoformat())
    report = build_api_rate_limit_snapshot_payload_integrity_report_from_db(conn, low_remaining=5, now=NOW)
    assert report["summary"]["finding_count"] == 2
    assert "API Rate-limit Snapshot Payload Integrity" in format_api_rate_limit_snapshot_payload_integrity_text(report)

    db_path = tmp_path / "snapshots.sqlite"
    conn.commit()
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--format", "json", "--now", NOW.isoformat(), "--days", "2", "--low-remaining", "5", "--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"] == {"days": 2, "limit": 5, "low_remaining": 5}
    assert script.main(["--db", str(db_path), "--format", "text", "--now", NOW.isoformat()]) == 0
    assert "default_endpoint_overuse" in capsys.readouterr().out

    missing = build_api_rate_limit_snapshot_payload_integrity_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["api_rate_limit_snapshots"]
    partial = sqlite3.connect(":memory:")
    partial.execute("CREATE TABLE api_rate_limit_snapshots (provider TEXT)")
    gaps = build_api_rate_limit_snapshot_payload_integrity_report_from_db(partial, now=NOW)
    assert gaps["missing_columns"] == {
        "api_rate_limit_snapshots": ["endpoint", "fetched_at", "limit_value", "raw_metadata", "remaining", "reset_at"]
    }
    assert "No API rate-limit snapshot payload integrity gaps" in format_api_rate_limit_snapshot_payload_integrity_text(gaps)


def test_validation_errors():
    with pytest.raises(ValueError, match="days must be positive"):
        build_api_rate_limit_snapshot_payload_integrity_report([], days=0)
    with pytest.raises(ValueError, match="low_remaining must be non-negative"):
        build_api_rate_limit_snapshot_payload_integrity_report([], low_remaining=-1)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_api_rate_limit_snapshot_payload_integrity_report([], limit=0)
    assert script.main(["--limit", "0"]) == 2
