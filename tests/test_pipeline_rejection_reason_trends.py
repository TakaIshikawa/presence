"""Tests for pipeline rejection reason trends."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sqlite3

from evaluation.pipeline_rejection_reason_trends import (
    build_pipeline_rejection_reason_trends_report_from_db,
    format_pipeline_rejection_reason_trends_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "pipeline_rejection_reason_trends.py"
spec = importlib.util.spec_from_file_location("pipeline_rejection_reason_trends_script", SCRIPT_PATH)
pipeline_rejection_reason_trends_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(pipeline_rejection_reason_trends_script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE pipeline_runs (
            id INTEGER PRIMARY KEY,
            run_id TEXT,
            outcome TEXT,
            rejection_reason TEXT,
            created_at TEXT
        );
        """
    )
    return conn


def test_report_groups_non_published_rejection_reasons_with_examples():
    conn = _conn()
    conn.executemany(
        "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?)",
        [
            (1, "run-1", "rejected", "Source Gap", "2026-05-20T10:00:00+00:00"),
            (2, "run-2", "rejected", "source_gap", "2026-05-20T09:00:00+00:00"),
            (3, "run-3", "filtered", None, "2026-05-20T08:00:00+00:00"),
            (4, "run-4", "published", "ok", "2026-05-20T07:00:00+00:00"),
            (5, "run-5", "rejected", "old", "2026-05-01T07:00:00+00:00"),
        ],
    )

    report = build_pipeline_rejection_reason_trends_report_from_db(
        conn, now=NOW, window_hours=24, min_count=1, limit=10
    )

    assert report["artifact_type"] == "pipeline_rejection_reason_trends"
    assert report["summary"]["rows_scanned"] == 3
    assert report["rejection_buckets"][0]["rejection_reason"] == "source gap"
    assert report["rejection_buckets"][0]["example_run_ids"] == ["run-1", "run-2"]
    assert "Rejection buckets" in format_pipeline_rejection_reason_trends_text(report)


def test_include_published_keeps_published_rows():
    conn = _conn()
    conn.execute(
        "INSERT INTO pipeline_runs VALUES (1, 'run-1', 'published', 'ok', '2026-05-20T10:00:00+00:00')"
    )
    report = build_pipeline_rejection_reason_trends_report_from_db(
        conn, now=NOW, window_hours=24, include_published=True
    )
    assert report["summary"]["rows_scanned"] == 1
    assert report["rejection_buckets"][0]["outcome"] == "published"


def test_missing_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_pipeline_rejection_reason_trends_report_from_db(conn, now=NOW)
    assert report["missing_tables"] == ["pipeline_runs"]


def test_cli_outputs_json(tmp_path, capsys):
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE pipeline_runs (id INTEGER PRIMARY KEY, outcome TEXT, rejection_reason TEXT, created_at TEXT);
        INSERT INTO pipeline_runs VALUES (1, 'rejected', 'bad', '2026-05-20T10:00:00+00:00');
        """
    )
    conn.close()
    assert pipeline_rejection_reason_trends_script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert '"artifact_type": "pipeline_rejection_reason_trends"' in capsys.readouterr().out
