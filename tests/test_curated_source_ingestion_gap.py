"""Tests for curated source ingestion gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from knowledge.curated_source_ingestion_gap import (
    build_curated_source_ingestion_gap_report,
    format_curated_source_ingestion_gap_json,
    format_curated_source_ingestion_gap_text,
)


NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "curated_source_ingestion_gap.py"
spec = importlib.util.spec_from_file_location("curated_source_ingestion_gap_script", SCRIPT_PATH)
curated_source_ingestion_gap_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(curated_source_ingestion_gap_script)


@contextmanager
def _script_context(conn: sqlite3.Connection):
    yield SimpleNamespace(), SimpleNamespace(conn=conn)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE curated_sources (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            identifier TEXT,
            name TEXT,
            status TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            source_id TEXT,
            source_url TEXT,
            author TEXT,
            ingested_at TEXT
        )"""
    )
    return conn


def _source(conn: sqlite3.Connection, source_type: str, identifier: str) -> None:
    conn.execute(
        "INSERT INTO curated_sources (source_type, identifier, name, status) VALUES (?, ?, ?, 'active')",
        (source_type, identifier, identifier.title()),
    )
    conn.commit()


def _knowledge(conn: sqlite3.Connection, source_type: str, source_id: str, ingested_at: str) -> None:
    conn.execute(
        """INSERT INTO knowledge (source_type, source_id, source_url, author, ingested_at)
           VALUES (?, ?, ?, ?, ?)""",
        (source_type, source_id, f"https://example.com/{source_id}", source_id, ingested_at),
    )
    conn.commit()


def test_report_marks_fresh_stale_and_missing_sources():
    conn = _conn()
    _source(conn, "blog", "fresh.example")
    _source(conn, "blog", "stale.example")
    _source(conn, "x_account", "missing")
    _knowledge(conn, "curated_article", "fresh.example", "2026-05-17T12:00:00+00:00")
    _knowledge(conn, "curated_article", "stale.example", "2026-04-01T12:00:00+00:00")

    report = build_curated_source_ingestion_gap_report(conn, stale_days=14, now=NOW)

    assert report["summary"] == {
        "total_sources": 3,
        "fresh_count": 1,
        "stale_count": 1,
        "missing_count": 1,
    }
    statuses = {row["identifier"]: row["status"] for row in report["rows"]}
    assert statuses["fresh.example"] == "fresh"
    assert statuses["stale.example"] == "stale"
    assert statuses["missing"] == "missing"


def test_expected_sources_can_be_supplied_without_curated_table():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE knowledge (source_type TEXT, source_id TEXT, ingested_at TEXT)"
    )
    conn.execute(
        "INSERT INTO knowledge (source_type, source_id, ingested_at) VALUES (?, ?, ?)",
        ("curated_article", "configured.example", "2026-05-18T00:00:00+00:00"),
    )
    conn.commit()

    report = build_curated_source_ingestion_gap_report(
        conn,
        expected_sources=[{"source_type": "blog", "identifier": "configured.example"}],
        now=NOW,
    )

    assert report["summary"]["fresh_count"] == 1
    assert report["rows"][0]["ingested_item_count"] == 1
    assert report["schema_gaps"]["missing_tables"] == ["curated_sources"]


def test_json_text_cli_and_argument_validation(monkeypatch, capsys):
    conn = _conn()
    _source(conn, "blog", "cli.example")
    monkeypatch.setattr(curated_source_ingestion_gap_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        curated_source_ingestion_gap_script,
        "build_curated_source_ingestion_gap_report",
        lambda db, **kwargs: build_curated_source_ingestion_gap_report(db, now=NOW, **kwargs),
    )

    report = build_curated_source_ingestion_gap_report(conn, now=NOW)
    payload = json.loads(format_curated_source_ingestion_gap_json(report))
    text = format_curated_source_ingestion_gap_text(report)
    exit_code = curated_source_ingestion_gap_script.main(["--stale-days", "7", "--format", "json"])
    cli_payload = json.loads(capsys.readouterr().out)

    assert payload["artifact_type"] == "curated_source_ingestion_gap"
    assert "Curated Source Ingestion Gap" in text
    assert cli_payload["filters"]["stale_days"] == 7
    assert exit_code == 0
    with pytest.raises(SystemExit):
        curated_source_ingestion_gap_script.parse_args(["--stale-days", "0"])
