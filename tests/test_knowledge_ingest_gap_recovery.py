"""Tests for knowledge ingest gap recovery reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from knowledge.ingest_gap_recovery import (
    build_knowledge_ingest_gap_recovery_report,
    format_knowledge_ingest_gap_recovery_json,
    format_knowledge_ingest_gap_recovery_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_ingest_gap_recovery.py"
spec = importlib.util.spec_from_file_location("knowledge_ingest_gap_recovery_script", SCRIPT_PATH)
knowledge_ingest_gap_recovery_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(knowledge_ingest_gap_recovery_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _source(db, source_identifier: str, **kwargs) -> int:
    values = {
        "source_type": "blog",
        "identifier": source_identifier,
        "canonical_url": f"https://{source_identifier}/feed",
        "link_title": "Title",
        "consecutive_failures": 0,
        "last_success_at": NOW.isoformat(),
        "last_failure_at": None,
    }
    values.update(kwargs)
    cols = ", ".join(values)
    placeholders = ", ".join("?" for _ in values)
    row = db.conn.execute(f"INSERT INTO curated_sources ({cols}) VALUES ({placeholders})", list(values.values()))
    db.conn.commit()
    return int(row.lastrowid)


def test_distinguishes_recovery_cases(db):
    _source(db, "never.example", last_success_at=None)
    _source(db, "stale.example", last_success_at=(NOW - timedelta(days=90)).isoformat())
    _source(db, "fail.example", consecutive_failures=4, last_failure_at=NOW.isoformat())
    _source(db, "meta.example", canonical_url=None, feed_url=None, identifier="", link_title=None)

    report = build_knowledge_ingest_gap_recovery_report(db, days=30, failure_threshold=3, now=NOW)
    cases = {row.case_type for row in report.sources}

    assert {"never_ingested", "stale_success", "repeated_failure", "metadata_blocked"} <= cases
    assert report.sources[0].recovery_priority == "high"
    assert report.totals["flagged_count"] == 4


def test_formatters_and_missing_optional_tables():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_knowledge_ingest_gap_recovery_report(conn, now=NOW)
    assert report.sources == ()
    assert report.schema_warnings == ("missing optional table: curated_sources", "missing optional table: knowledge")
    assert "Schema warnings:" in format_knowledge_ingest_gap_recovery_text(report)


def test_cli_outputs_json(db, monkeypatch, capsys):
    _source(db, "cli.example", last_success_at=None)
    monkeypatch.setattr(knowledge_ingest_gap_recovery_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        knowledge_ingest_gap_recovery_script,
        "build_knowledge_ingest_gap_recovery_report",
        lambda db, **kwargs: build_knowledge_ingest_gap_recovery_report(db, now=NOW, **kwargs),
    )
    assert knowledge_ingest_gap_recovery_script.main(["--failure-threshold", "2", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "knowledge_ingest_gap_recovery"
    assert json.loads(format_knowledge_ingest_gap_recovery_json(build_knowledge_ingest_gap_recovery_report(db, now=NOW)))["sources"]
