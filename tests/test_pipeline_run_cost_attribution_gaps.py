"""Tests for pipeline run cost attribution gap reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.pipeline_run_cost_attribution_gaps import (
    build_pipeline_run_cost_attribution_gaps_report,
    build_pipeline_run_cost_attribution_gaps_report_from_db,
    format_pipeline_run_cost_attribution_gaps_json,
    format_pipeline_run_cost_attribution_gaps_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "pipeline_run_cost_attribution_gaps.py"
spec = importlib.util.spec_from_file_location("pipeline_run_cost_attribution_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(hours_ago: int) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE pipeline_runs (
               id INTEGER PRIMARY KEY,
               content_id INTEGER,
               content_type TEXT,
               status TEXT,
               created_at TEXT
           );
           CREATE TABLE model_usage (
               id INTEGER PRIMARY KEY,
               pipeline_run_id INTEGER,
               content_id INTEGER,
               operation_name TEXT,
               created_at TEXT
           );"""
    )
    return conn


def test_builder_filters_and_groups_findings():
    report = build_pipeline_run_cost_attribution_gaps_report(
        [
            {"issue_type": "missing_pipeline_run_id", "model_usage_id": 1, "content_type": "unknown", "operation_name": "draft", "event_at": _ts(1)},
            {"issue_type": "orphaned_pipeline_run_id", "model_usage_id": 2, "pipeline_run_id": 99, "content_type": "unknown", "operation_name": "draft", "event_at": _ts(2)},
            {"issue_type": "missing_usage", "pipeline_run_id": 3, "content_type": "blog", "operation_name": "unknown", "event_at": _ts(3)},
        ],
        issue_type="missing_usage",
        now=NOW,
    )
    assert report["artifact_type"] == "pipeline_run_cost_attribution_gaps"
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["issue_type"] == "missing_usage"


def test_db_loader_detects_all_join_integrity_gaps():
    conn = _conn()
    conn.executemany(
        "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?)",
        [
            (1, 10, "x_post", "completed", _ts(1)),
            (2, 20, "blog", "completed", _ts(2)),
            (3, 30, "blog", "failed", _ts(2)),
        ],
    )
    conn.executemany(
        "INSERT INTO model_usage VALUES (?, ?, ?, ?, ?)",
        [
            (1, None, 10, "draft", _ts(1)),
            (2, 99, 20, "draft", _ts(1)),
            (3, 1, 999, "draft", _ts(1)),
        ],
    )
    report = build_pipeline_run_cost_attribution_gaps_report_from_db(conn, now=NOW)
    issues = [item["issue_type"] for item in report["findings"]]
    assert issues == ["content_id_mismatch", "missing_pipeline_run_id", "missing_usage", "orphaned_pipeline_run_id"]
    assert report["summary"]["by_issue_type"]["missing_usage"] == 1


def test_cli_json_text_and_filters(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO pipeline_runs VALUES (1, 10, 'blog', 'completed', ?)", (_ts(1),))
    conn.commit()
    db_path = tmp_path / "cost.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--content-type", "blog", "--window-hours", "1000", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Pipeline Run Cost Attribution Gaps" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--window-hours", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_formatters_and_invalid_thresholds():
    report = build_pipeline_run_cost_attribution_gaps_report([], now=NOW)
    assert json.loads(format_pipeline_run_cost_attribution_gaps_json(report))["artifact_type"] == "pipeline_run_cost_attribution_gaps"
    assert "No pipeline run cost attribution gaps found" in format_pipeline_run_cost_attribution_gaps_text(report)
    with pytest.raises(ValueError, match="window_hours must be positive"):
        build_pipeline_run_cost_attribution_gaps_report([], window_hours=0)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_pipeline_run_cost_attribution_gaps_report([], limit=0)
