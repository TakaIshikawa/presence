"""Tests for model usage attribution orphan reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.model_usage_attribution_orphans import (
    build_model_usage_attribution_orphans_report_from_db,
    format_model_usage_attribution_orphans_json,
    format_model_usage_attribution_orphans_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "model_usage_attribution_orphans.py"
spec = importlib.util.spec_from_file_location("model_usage_attribution_orphans_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY)")
    conn.execute("CREATE TABLE pipeline_runs (id INTEGER PRIMARY KEY)")
    conn.execute(
        """CREATE TABLE model_usage (
               id INTEGER PRIMARY KEY,
               content_id INTEGER,
               pipeline_run_id INTEGER,
               operation_name TEXT,
               model_name TEXT,
               estimated_cost REAL,
               created_at TEXT
           )"""
    )
    conn.execute("INSERT INTO generated_content VALUES (10)")
    conn.execute("INSERT INTO pipeline_runs VALUES (20)")
    return conn


def _insert(
    conn: sqlite3.Connection,
    usage_id: int,
    *,
    content_id: int | None,
    pipeline_run_id: int | None,
    operation_name: str = "draft",
    model_name: str = "gpt-x",
    estimated_cost: float = 0.1,
    created_at: str = "2026-05-01T12:00:00+00:00",
) -> None:
    conn.execute(
        "INSERT INTO model_usage VALUES (?, ?, ?, ?, ?, ?, ?)",
        (usage_id, content_id, pipeline_run_id, operation_name, model_name, estimated_cost, created_at),
    )


def test_report_flags_null_attribution_and_broken_references():
    conn = _conn()
    _insert(conn, 1, content_id=10, pipeline_run_id=None)
    _insert(conn, 2, content_id=None, pipeline_run_id=None, estimated_cost=0.25)
    _insert(conn, 3, content_id=99, pipeline_run_id=None, estimated_cost=0.5, created_at="2026-05-02T10:00:00+00:00")
    _insert(conn, 4, content_id=None, pipeline_run_id=99, operation_name="score", model_name="gpt-y", estimated_cost=0.75)
    _insert(conn, 5, content_id=404, pipeline_run_id=405, estimated_cost=1.0)

    report = build_model_usage_attribution_orphans_report_from_db(conn, include_details=True, now=NOW)

    assert report["summary"]["usage_count"] == 5
    assert report["summary"]["orphan_count"] == 4
    assert {tuple(item["orphan_reasons"]) for item in report["orphan_examples"]} == {
        ("missing_content_and_pipeline",),
        ("missing_generated_content",),
        ("missing_generated_content", "missing_pipeline_run"),
        ("missing_pipeline_run",),
    }


def test_summary_by_operation_and_model_includes_cost_and_latest_created_at():
    conn = _conn()
    _insert(conn, 1, content_id=None, pipeline_run_id=None, estimated_cost=0.25, created_at="2026-05-01T12:00:00+00:00")
    _insert(conn, 2, content_id=99, pipeline_run_id=None, estimated_cost=0.5, created_at="2026-05-02T10:00:00+00:00")
    _insert(conn, 3, content_id=None, pipeline_run_id=99, operation_name="score", model_name="gpt-y", estimated_cost=0.75)

    report = build_model_usage_attribution_orphans_report_from_db(conn, now=NOW)

    assert report["orphan_examples"] == []
    assert report["summary_rows"] == [
        {
            "operation_name": "draft",
            "model_name": "gpt-x",
            "orphan_count": 2,
            "total_estimated_cost": 0.75,
            "latest_created_at": "2026-05-02T10:00:00+00:00",
        },
        {
            "operation_name": "score",
            "model_name": "gpt-y",
            "orphan_count": 1,
            "total_estimated_cost": 0.75,
            "latest_created_at": "2026-05-01T12:00:00+00:00",
        },
    ]


def test_formatters_cli_details_and_validation(tmp_path, capsys):
    conn = _conn()
    _insert(conn, 1, content_id=None, pipeline_run_id=None)
    conn.commit()
    db_path = tmp_path / "usage.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "model_usage_attribution_orphans"
    assert payload["orphan_examples"] == []

    assert script.main(["--db", str(db_path), "--details", "--format", "text"]) == 0
    assert "usage_id | orphan_reasons" in capsys.readouterr().out

    assert script.main(["--db", str(db_path), "--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    report = build_model_usage_attribution_orphans_report_from_db(conn, include_details=True, now=NOW)
    assert json.loads(format_model_usage_attribution_orphans_json(report))["summary"]["orphan_count"] == 1
    assert "Model Usage Attribution Orphans" in format_model_usage_attribution_orphans_text(report)


def test_missing_model_usage_table_and_invalid_limit():
    report = build_model_usage_attribution_orphans_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert report["missing_tables"] == ["model_usage"]
    assert report["summary_rows"] == []
    with pytest.raises(ValueError, match="limit must be positive"):
        build_model_usage_attribution_orphans_report_from_db(sqlite3.connect(":memory:"), limit=0)
