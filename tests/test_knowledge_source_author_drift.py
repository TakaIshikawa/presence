"""Tests for knowledge source author drift reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from knowledge.source_author_drift import (
    build_source_author_drift_report,
    build_source_author_drift_report_from_db,
    format_source_author_drift_json,
    format_source_author_drift_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_source_author_drift.py"
spec = importlib.util.spec_from_file_location("knowledge_source_author_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE knowledge_sources (
            id INTEGER,
            canonical_url TEXT,
            author_handle TEXT,
            author_name TEXT,
            ingested_at TEXT
        )"""
    )
    return conn


def test_builder_flags_changed_and_missing_recent_authors():
    rows = [
        {"source_id": 1, "url": "https://example.com/post", "author_handle": "alice", "author_name": "Alice", "seen_at": (NOW - timedelta(days=2)).isoformat()},
        {"source_id": 2, "url": "https://example.com/post/", "author_handle": "bob", "author_name": "Bob", "seen_at": (NOW - timedelta(days=1)).isoformat()},
        {"source_id": 3, "url": "https://example.com/other", "author_handle": None, "author_name": None, "seen_at": NOW.isoformat()},
    ]
    report = build_source_author_drift_report(rows, now=NOW)
    assert report["artifact_type"] == "source_author_drift"
    assert report["summary"]["finding_count"] == 2
    assert "same_url_multiple_authors" in report["findings"][0]["drift_reason"]


def test_db_adapter_and_cli(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO knowledge_sources VALUES (1, 'https://example.com/p', 'alice', 'Alice', ?)", ((NOW - timedelta(days=2)).isoformat(),))
    conn.execute("INSERT INTO knowledge_sources VALUES (2, 'https://example.com/p', 'bob', 'Bob', ?)", ((NOW - timedelta(days=1)).isoformat(),))
    conn.commit()
    report = build_source_author_drift_report_from_db(conn, days=30, now=NOW)
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["previous_author"]["handle"] == "alice"
    assert json.loads(format_source_author_drift_json(report))["artifact_type"] == "source_author_drift"
    assert "Knowledge Source Author Drift" in format_source_author_drift_text(report)

    db_path = tmp_path / "knowledge.sqlite"
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--days", "60", "--limit", "5"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Knowledge Source Author Drift" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--days", "0"])


def test_missing_schema_metadata():
    missing = build_source_author_drift_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["knowledge_sources|curated_sources|source_ingests"]
