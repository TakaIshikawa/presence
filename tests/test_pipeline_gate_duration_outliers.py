"""Tests for pipeline gate duration outliers reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.pipeline_gate_duration_outliers import (
    build_pipeline_gate_duration_outliers_report,
    format_pipeline_gate_duration_outliers_json,
    format_pipeline_gate_duration_outliers_text,
)


NOW = datetime(2026, 5, 18, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "pipeline_gate_duration_outliers.py"
spec = importlib.util.spec_from_file_location("pipeline_gate_duration_outliers_script", SCRIPT_PATH)
pipeline_gate_duration_outliers_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(pipeline_gate_duration_outliers_script)


@contextmanager
def _script_context(conn):
    yield SimpleNamespace(), conn


def _conn(with_generated: bool = True) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE pipeline_runs (
            id INTEGER PRIMARY KEY, batch_id TEXT, content_id INTEGER, content_type TEXT, outcome TEXT, created_at TEXT
        )"""
    )
    if with_generated:
        conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY, published_at TEXT)")
    return conn


def test_outliers_use_published_at_for_published_and_now_for_failures():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, ?)", ((NOW - timedelta(hours=10)).isoformat(),))
    conn.execute("INSERT INTO pipeline_runs VALUES (1, 'b1', 1, 'x_post', 'published', ?)", ((NOW - timedelta(hours=50)).isoformat(),))
    conn.execute("INSERT INTO pipeline_runs VALUES (2, 'b2', 2, 'x_post', 'failed', ?)", ((NOW - timedelta(hours=80)).isoformat(),))
    conn.execute("INSERT INTO pipeline_runs VALUES (3, 'b3', 3, 'x_post', 'failed', ?)", ((NOW - timedelta(hours=2)).isoformat(),))
    conn.commit()

    report = build_pipeline_gate_duration_outliers_report(conn, now=NOW, min_duration_hours=24)

    assert [item["pipeline_run_id"] for item in report["outlier_runs"]] == [2, 1]
    assert report["outcome_breakdowns"] == {"failed": 1, "published": 1}


def test_missing_generated_content_is_optional_and_cli_outputs(monkeypatch, capsys):
    conn = _conn(with_generated=False)
    conn.execute("INSERT INTO pipeline_runs VALUES (1, 'b1', 1, 'x_post', 'failed', ?)", ((NOW - timedelta(hours=80)).isoformat(),))
    conn.commit()
    report = build_pipeline_gate_duration_outliers_report(conn, now=NOW, min_duration_hours=24)

    assert report["missing_optional_columns"] == {"generated_content": ["table"]}
    assert json.loads(format_pipeline_gate_duration_outliers_json(report))["artifact_type"] == "pipeline_gate_duration_outliers"
    assert "Pipeline Gate Duration Outliers" in format_pipeline_gate_duration_outliers_text(report)
    monkeypatch.setattr(pipeline_gate_duration_outliers_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        pipeline_gate_duration_outliers_script,
        "build_pipeline_gate_duration_outliers_report",
        lambda db, **kwargs: build_pipeline_gate_duration_outliers_report(db, now=NOW, **kwargs),
    )
    assert pipeline_gate_duration_outliers_script.main(["--format", "text"]) == 0
    assert "Totals: runs=1" in capsys.readouterr().out

    missing = build_pipeline_gate_duration_outliers_report(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["pipeline_runs"]
