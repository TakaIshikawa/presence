"""Tests for model usage token ratio drift reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.model_usage_token_ratio_drift import (
    build_model_usage_token_ratio_drift_report,
    build_model_usage_token_ratio_drift_report_from_db,
    format_model_usage_token_ratio_drift_json,
    format_model_usage_token_ratio_drift_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "model_usage_token_ratio_drift.py"
spec = importlib.util.spec_from_file_location("model_usage_token_ratio_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_ratio_drift_insufficient_baseline_and_zero_output():
    rows = [
        {"operation_name": "draft", "model_name": "m1", "input_tokens": 100, "output_tokens": 100, "created_at": "2026-05-01T00:00:00+00:00"},
        {"operation_name": "draft", "model_name": "m1", "input_tokens": 100, "output_tokens": 100, "created_at": "2026-05-02T00:00:00+00:00"},
        {"operation_name": "draft", "model_name": "m1", "input_tokens": 300, "output_tokens": 100, "created_at": "2026-05-19T00:00:00+00:00"},
        {"operation_name": "rare", "model_name": "m1", "input_tokens": 10, "output_tokens": 10, "created_at": "2026-05-19T00:00:00+00:00"},
        {"operation_name": "zero", "model_name": "m2", "input_tokens": 10, "output_tokens": 0, "created_at": "2026-05-19T00:00:00+00:00"},
    ]
    report = build_model_usage_token_ratio_drift_report(rows, now=NOW, drift_threshold=0.5)
    payload = json.loads(format_model_usage_token_ratio_drift_json(report))
    assert payload["artifact_type"] == "model_usage_token_ratio_drift"
    assert [item["issue_type"] for item in payload["issue_items"]] == [
        "ratio_drift",
        "insufficient_baseline",
        "insufficient_baseline",
        "zero_output_tokens",
    ]
    assert payload["issue_items"][0]["current_ratio"] == 3.0
    assert "Model Usage Token Ratio Drift" in format_model_usage_token_ratio_drift_text(report)


def test_from_db_handles_optional_columns_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE model_usage (
            id INTEGER PRIMARY KEY,
            operation_name TEXT,
            model_name TEXT,
            input_tokens INTEGER,
            output_tokens INTEGER,
            created_at TEXT
        );
        INSERT INTO model_usage VALUES (1, 'draft', 'm1', 100, 100, '2026-05-01T00:00:00+00:00');
        INSERT INTO model_usage VALUES (2, 'draft', 'm1', 100, 100, '2026-05-02T00:00:00+00:00');
        INSERT INTO model_usage VALUES (3, 'draft', 'm1', 300, 100, '2026-05-19T00:00:00+00:00');
        """
    )
    report = build_model_usage_token_ratio_drift_report_from_db(conn, now=NOW)
    assert report["summary"]["issue_count"] == 1

    db_path = tmp_path / "usage.sqlite"
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--current-days", "7"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["issue_count"] == 1

    minimal = sqlite3.connect(":memory:")
    minimal.row_factory = sqlite3.Row
    minimal.execute("CREATE TABLE model_usage (id INTEGER PRIMARY KEY)")
    minimal.execute("INSERT INTO model_usage VALUES (1)")
    assert build_model_usage_token_ratio_drift_report_from_db(minimal, now=NOW)["summary"]["issue_count"] == 2
    missing = build_model_usage_token_ratio_drift_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["model_usage"]
