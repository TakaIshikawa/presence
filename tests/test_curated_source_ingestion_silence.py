"""Tests for curated source ingestion silence reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.curated_source_ingestion_silence import (
    build_curated_source_ingestion_silence_report,
    build_curated_source_ingestion_silence_report_from_db,
    format_curated_source_ingestion_silence_json,
    format_curated_source_ingestion_silence_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "curated_source_ingestion_silence.py"
spec = importlib.util.spec_from_file_location("curated_source_ingestion_silence_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(days: int) -> str:
    return (NOW - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


def test_builder_flags_missing_and_stale_sources_with_reason_codes():
    report = build_curated_source_ingestion_silence_report(
        [
            {"id": "a", "source": "Active", "source_url": "https://example.com/a"},
            {"id": "s", "source": "Stale", "source_url": "https://example.com/s"},
            {"id": "m", "source": "Missing", "source_url": "https://example.com/m"},
        ],
        [
            {"source": "active", "source_url": "https://example.com/a/", "created_at": _ts(2)},
            {"source": "stale", "source_url": "https://example.com/s", "created_at": _ts(90)},
        ],
        stale_days=30,
        now=NOW,
    )

    reasons = {item["source"]: item["reason_codes"] for item in report["silent_sources"]}
    assert reasons["Stale"] == ["stale_ingestion"]
    assert reasons["Missing"] == ["no_matching_knowledge"]
    assert report["artifact_type"] == "curated_source_ingestion_silence"
    assert report["active_sources_count"] == 1
    assert report["thresholds"]["stale_days"] == 30


def test_db_loader_uses_available_source_metadata_and_reports_missing_tables():
    empty = sqlite3.connect(":memory:")
    missing = build_curated_source_ingestion_silence_report_from_db(empty, now=NOW)
    assert missing["missing_schema"]["missing_tables"] == ["curated_sources", "knowledge"]
    assert missing["silent_sources"] == []

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE curated_sources (id TEXT PRIMARY KEY, name TEXT, feed_url TEXT)")
    conn.execute("CREATE TABLE knowledge (id INTEGER PRIMARY KEY, source_url TEXT, updated_at TEXT)")
    conn.execute("INSERT INTO curated_sources VALUES (?, ?, ?)", ("1", "Feed", "https://example.com/feed"))
    conn.execute("INSERT INTO knowledge VALUES (?, ?, ?)", (10, "https://example.com/feed/", _ts(1)))
    conn.commit()

    report = build_curated_source_ingestion_silence_report_from_db(conn, stale_days=30, now=NOW)
    assert report["active_sources_count"] == 1
    assert report["silent_sources"] == []


def test_formatters_and_cli(tmp_path, capsys):
    report = build_curated_source_ingestion_silence_report(
        [{"id": 1, "source": "Silent"}],
        [],
        now=NOW,
    )
    assert json.loads(format_curated_source_ingestion_silence_json(report))["artifact_type"] == "curated_source_ingestion_silence"
    assert "Silent sources" in format_curated_source_ingestion_silence_text(report)

    db_path = tmp_path / "silence.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE curated_sources (id INTEGER PRIMARY KEY, source TEXT)")
    conn.execute("CREATE TABLE knowledge (id INTEGER PRIMARY KEY, source TEXT, created_at TEXT)")
    conn.execute("INSERT INTO curated_sources VALUES (?, ?)", (1, "Silent"))
    conn.commit()
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["silent_sources"][0]["reason_codes"] == ["no_matching_knowledge"]
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Curated Source Ingestion Silence" in capsys.readouterr().out


def test_builder_rejects_invalid_arguments():
    with pytest.raises(ValueError, match="stale_days must be positive"):
        build_curated_source_ingestion_silence_report([], [], stale_days=0)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_curated_source_ingestion_silence_report([], [], limit=0)
