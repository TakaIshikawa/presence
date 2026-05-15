"""Tests for source evidence aging risk reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.source_evidence_aging_risk import (
    build_source_evidence_aging_risk_report,
    build_source_evidence_aging_risk_report_from_db,
    format_source_evidence_aging_risk_text,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_evidence_aging_risk.py"
spec = importlib.util.spec_from_file_location("source_evidence_aging_risk_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _row(content_id: str, source_id: str, source_age_days: int, *, source_type: str = "research") -> dict:
    return {
        "content_id": content_id,
        "content_type": "blog",
        "source_id": source_id,
        "source_type": source_type,
        "generated_at": NOW.isoformat(),
        "source_timestamp": (NOW - timedelta(days=source_age_days)).isoformat(),
    }


def test_buckets_fresh_stale_and_expired_evidence():
    report = build_source_evidence_aging_risk_report(
        [
            _row("fresh-post", "src-1", 5),
            _row("stale-post", "src-2", 31),
            _row("expired-post", "src-3", 100, source_type="interview"),
        ],
        stale_days=30,
        expired_days=90,
        now=NOW,
    )

    buckets = {item["content_id"]: item["risk_bucket"] for item in report["evidence"]}

    assert buckets["fresh-post"] == "fresh"
    assert buckets["stale-post"] == "stale"
    assert buckets["expired-post"] == "expired"
    assert report["totals"]["fresh"] == 1
    assert report["totals"]["stale"] == 1
    assert report["totals"]["expired"] == 1


def test_records_include_required_item_fields_and_stale_subset():
    report = build_source_evidence_aging_risk_report([_row("post-1", "source-1", 45)], now=NOW)

    item = report["stale_evidence"][0]

    assert item["content_id"] == "post-1"
    assert item["content_type"] == "blog"
    assert item["source_id"] == "source-1"
    assert item["source_type"] == "research"
    assert item["source_age_days"] == 45
    assert item["risk_bucket"] == "stale"
    assert "bucket" in format_source_evidence_aging_risk_text(report).lower()


def test_aggregates_by_risk_bucket_source_type_and_content_type():
    report = build_source_evidence_aging_risk_report(
        [
            _row("a", "src-a", 5, source_type="note"),
            _row("b", "src-b", 45, source_type="note"),
            _row("c", "src-c", 120, source_type="memo"),
        ],
        now=NOW,
    )

    by_bucket = report["aggregates"]["by_risk_bucket_and_source_type"]
    stale_note = next(item for item in by_bucket if item["risk_bucket"] == "stale" and item["source_type"] == "note")
    assert stale_note["count"] == 1
    by_type = report["aggregates"]["by_content_type_and_source_type"]
    assert next(item for item in by_type if item["source_type"] == "note")["count"] == 2


def test_missing_timestamps_are_skipped_and_counted():
    report = build_source_evidence_aging_risk_report(
        [
            {"content_id": "missing-source", "generated_at": NOW.isoformat()},
            {"content_id": "missing-content", "source_timestamp": NOW.isoformat()},
        ],
        now=NOW,
    )

    assert report["evidence"] == []
    assert report["totals"]["missing_source_timestamp"] == 1
    assert report["totals"]["missing_content_timestamp"] == 1
    assert report["empty_state"]["is_empty"] is True


def test_empty_dataset_has_empty_state():
    report = build_source_evidence_aging_risk_report([], now=NOW)

    assert report["totals"]["evidence_count"] == 0
    assert report["empty_state"]["message"] == "No source evidence rows with usable timestamps found."


def test_db_loader_and_cli_table_and_json_output(monkeypatch, capsys, tmp_path):
    import sqlite3

    conn = sqlite3.connect(tmp_path / "evidence.db")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE source_evidence (
           id INTEGER PRIMARY KEY,
           content_id TEXT,
           content_type TEXT,
           source_id TEXT,
           source_type TEXT,
           generated_at TEXT,
           source_timestamp TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO source_evidence
           (content_id, content_type, source_id, source_type, generated_at, source_timestamp)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("post-1", "newsletter", "src-1", "memo", NOW.isoformat(), (NOW - timedelta(days=95)).isoformat()),
    )
    conn.commit()

    report = build_source_evidence_aging_risk_report_from_db(conn, now=NOW)
    assert report["evidence"][0]["risk_bucket"] == "expired"

    monkeypatch.setattr(script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        script,
        "build_source_evidence_aging_risk_report_from_db",
        lambda db, **kwargs: build_source_evidence_aging_risk_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main([]) == 0
    assert "Source Evidence Aging Risk" in capsys.readouterr().out

    assert script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "source_evidence_aging_risk"
