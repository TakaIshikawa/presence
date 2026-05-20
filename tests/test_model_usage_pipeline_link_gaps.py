"""Tests for model usage pipeline link gap reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.model_usage_pipeline_link_gaps import (
    build_model_usage_pipeline_link_gaps_report,
    build_model_usage_pipeline_link_gaps_report_from_db,
    format_model_usage_pipeline_link_gaps_json,
    format_model_usage_pipeline_link_gaps_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "model_usage_pipeline_link_gaps.py"
spec = importlib.util.spec_from_file_location("model_usage_pipeline_link_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(hours_ago: int) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE model_usage (
               id INTEGER PRIMARY KEY,
               content_id INTEGER,
               pipeline_run_id INTEGER,
               operation_name TEXT,
               model_name TEXT,
               cost REAL,
               created_at TEXT
           );
           CREATE TABLE generated_content (id INTEGER PRIMARY KEY);
           CREATE TABLE pipeline_runs (id INTEGER PRIMARY KEY);"""
    )
    return conn


def test_builder_reports_link_gaps_and_bucket_summary():
    report = build_model_usage_pipeline_link_gaps_report(
        [
            {"usage_id": 1, "operation_name": "draft", "model_name": "m1", "cost": 0.7},
            {"usage_id": 2, "operation_name": "draft", "model_name": "m1", "cost": 0.6},
            {"usage_id": 3, "operation_name": "review", "model_name": "m2", "content_id": 99, "pipeline_run_id": 42, "cost": 0.1},
        ],
        [{"id": 10}],
        [{"id": 20}],
        cost_threshold=1.0,
        now=NOW,
    )

    assert report["artifact_type"] == "model_usage_pipeline_link_gaps"
    assert [item["issue_type"] for item in report["gap_items"]] == [
        "missing_content_and_pipeline_link",
        "missing_content_and_pipeline_link",
        "high_unattributed_cost_bucket",
        "content_id_missing_generated_content",
        "pipeline_run_id_missing_pipeline_run",
    ]
    assert report["bucket_summary"][0]["operation_name"] == "draft"
    assert report["bucket_summary"][0]["unattributed_cost"] == 1.3
    assert report["bucket_summary"][0]["high_unattributed_cost"] is True


def test_db_loader_filters_window_and_marks_missing_link_checks_unavailable():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE model_usage (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            pipeline_run_id INTEGER,
            operation_name TEXT,
            model_name TEXT,
            cost REAL,
            created_at TEXT
        )"""
    )
    conn.execute("INSERT INTO model_usage VALUES (1, 99, 88, 'draft', 'm1', 0.2, ?)", (_ts(2),))
    conn.execute("INSERT INTO model_usage VALUES (2, NULL, NULL, 'draft', 'm1', 0.2, ?)", (_ts(300),))

    report = build_model_usage_pipeline_link_gaps_report_from_db(conn, window_hours=24, now=NOW)

    assert report["summary"]["usage_count"] == 1
    assert report["gap_items"] == []
    assert report["link_checks"] == {"generated_content": "unavailable_missing_table", "pipeline_runs": "unavailable_missing_table"}


def test_db_loader_checks_existing_links_and_cli_validation(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (10)")
    conn.execute("INSERT INTO pipeline_runs VALUES (20)")
    conn.execute("INSERT INTO model_usage VALUES (1, 99, 20, 'draft', 'm1', 0.1, ?)", (_ts(1),))
    conn.commit()
    db_path = tmp_path / "usage.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--format", "json", "--window-hours", "1000"]) == 0
    assert json.loads(capsys.readouterr().out)["gap_items"][0]["issue_type"] == "content_id_missing_generated_content"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Model Usage Pipeline Link Gaps" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--window-hours", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert script.main(["--db", str(db_path), "--cost-threshold", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err


def test_missing_model_usage_formatters_and_invalid_thresholds():
    missing = build_model_usage_pipeline_link_gaps_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["model_usage"]
    report = build_model_usage_pipeline_link_gaps_report([], [], [], now=NOW)
    assert json.loads(format_model_usage_pipeline_link_gaps_json(report))["artifact_type"] == "model_usage_pipeline_link_gaps"
    assert "No model usage pipeline link gaps found" in format_model_usage_pipeline_link_gaps_text(report)
    with pytest.raises(ValueError, match="window_hours must be positive"):
        build_model_usage_pipeline_link_gaps_report([], [], [], window_hours=0)
    with pytest.raises(ValueError, match="cost_threshold must be non-negative"):
        build_model_usage_pipeline_link_gaps_report([], [], [], cost_threshold=-0.1)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_model_usage_pipeline_link_gaps_report([], [], [], limit=0)
