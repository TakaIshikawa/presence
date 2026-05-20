"""Tests for knowledge timestamp normalization reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.knowledge_timestamp_normalization import (
    build_knowledge_timestamp_normalization_report,
    build_knowledge_timestamp_normalization_report_from_db,
    format_knowledge_timestamp_normalization_json,
    format_knowledge_timestamp_normalization_text,
)


NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_timestamp_normalization.py"
spec = importlib.util.spec_from_file_location("knowledge_timestamp_normalization_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            source_url TEXT,
            author TEXT,
            published_at TEXT,
            created_at TEXT
        )"""
    )
    return conn


def test_builder_flags_invalid_future_ordering_and_curated_missing_timestamp():
    report = build_knowledge_timestamp_normalization_report(
        [
            {"knowledge_id": 1, "source_type": "curated_article", "source_url": "https://example.com/a", "author": "Ada", "published_at": "not-a-date", "created_at": "2026-05-19T00:00:00+00:00"},
            {"knowledge_id": 2, "source_type": "curated_newsletter", "source_url": "https://example.com/b", "author": "Bo", "published_at": "2026-05-22T00:00:00+00:00", "created_at": "2026-05-20T00:00:00+00:00"},
            {"knowledge_id": 3, "source_type": "curated_article", "source_url": "https://example.com/c", "author": "Cy", "published_at": "2026-05-19T00:00:00+00:00", "created_at": "2026-05-10T00:00:00+00:00"},
            {"knowledge_id": 4, "source_type": "curated_newsletter", "source_url": "https://example.com/d", "author": "Di", "published_at": "", "created_at": ""},
            {"knowledge_id": 5, "source_type": "note", "source_url": "", "author": "Em", "published_at": "2026-05-19", "created_at": "bad"},
            {"knowledge_id": 6, "source_type": "note", "source_url": "", "author": "Fi", "published_at": "", "created_at": "2026-05-21T00:00:00+00:00"},
        ],
        now=NOW,
        grace_days=1,
    )
    payload = json.loads(format_knowledge_timestamp_normalization_json(report))

    assert payload["artifact_type"] == "knowledge_timestamp_normalization"
    assert payload["summary"]["by_issue_type"] == {
        "created_at_in_future": 1,
        "curated_source_url_missing_timestamp": 1,
        "invalid_created_at": 1,
        "invalid_published_at": 1,
        "published_at_after_created_at": 2,
        "published_at_in_future": 1,
    }
    flat = [item for group in payload["findings"] for item in group["items"]]
    assert {item["knowledge_id"] for item in flat} == {1, 2, 3, 4, 5, 6}
    future = next(item for item in flat if item["issue_type"] == "published_at_in_future")
    assert future["days_delta"] == 1
    assert "knowledge_id | source_type | issue_type" in format_knowledge_timestamp_normalization_text(report)


def test_from_db_schema_gaps_empty_state_and_limit_ordering():
    missing = build_knowledge_timestamp_normalization_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["knowledge"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE knowledge (id INTEGER PRIMARY KEY)")
    gaps = build_knowledge_timestamp_normalization_report_from_db(bad, now=NOW)
    assert gaps["missing_columns"]["knowledge"] == ["author", "created_at", "published_at", "source_type", "source_url"]

    conn = _conn()
    conn.execute("INSERT INTO knowledge VALUES (1, 'curated_article', 'https://example.com/a', 'Ada', 'bad', NULL)")
    conn.execute("INSERT INTO knowledge VALUES (2, 'note', '', 'Bo', NULL, 'bad')")
    conn.execute("INSERT INTO knowledge VALUES (3, 'curated_newsletter', 'https://example.com/c', 'Cy', NULL, NULL)")
    report = build_knowledge_timestamp_normalization_report_from_db(conn, now=NOW, limit=2)
    flat = [item for group in report["findings"] for item in group["items"]]
    assert [item["issue_type"] for item in flat] == ["invalid_published_at", "invalid_created_at"]
    assert report["summary"]["finding_count"] == 3
    assert report["summary"]["shown_count"] == 2

    clean = _conn()
    clean.execute(
        "INSERT INTO knowledge VALUES (1, 'curated_article', 'https://example.com/a', 'Ada', '2026-05-19T00:00:00+00:00', '2026-05-19T00:00:00+00:00')"
    )
    clean_report = build_knowledge_timestamp_normalization_report_from_db(clean, now=NOW)
    assert clean_report["empty_state"]["is_empty"] is True
    assert "No knowledge timestamp normalization issues found" in format_knowledge_timestamp_normalization_text(clean_report)


def test_cli_json_text_and_validation(tmp_path, capsys):
    db_path = tmp_path / "knowledge.sqlite"
    conn = _conn(db_path)
    conn.execute("INSERT INTO knowledge VALUES (1, 'curated_article', 'https://example.com/a', 'Ada', 'bad', NULL)")
    conn.commit()
    conn.close()

    assert script.main(["--db", str(db_path), "--now", NOW.isoformat(), "--format", "json", "--limit", "1"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "knowledge_timestamp_normalization"
    assert payload["filters"]["now"] == NOW.isoformat()
    assert script.main(["--db", str(db_path), "--now", NOW.isoformat(), "--format", "text", "--grace-days", "0"]) == 0
    assert "Knowledge Timestamp Normalization" in capsys.readouterr().out
    assert script.main(["--limit", "0"]) == 2
    assert script.main(["--now", "not-a-date"]) == 1
    with pytest.raises(ValueError, match="grace_days must be non-negative"):
        build_knowledge_timestamp_normalization_report([], grace_days=-1)
