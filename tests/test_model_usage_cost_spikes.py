"""Tests for model usage cost spike reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.model_usage_cost_spikes import (
    build_model_usage_cost_spikes_report,
    build_model_usage_cost_spikes_report_from_db,
    format_model_usage_cost_spikes_json,
    format_model_usage_cost_spikes_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "model_usage_cost_spikes.py"
spec = importlib.util.spec_from_file_location("model_usage_cost_spikes_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _ts(hours_ago: float) -> str:
    return (NOW - timedelta(hours=hours_ago)).strftime("%Y-%m-%d %H:%M:%S")


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE model_usage (
            id INTEGER PRIMARY KEY,
            model_name TEXT,
            operation_name TEXT,
            input_tokens INTEGER,
            output_tokens INTEGER,
            total_tokens INTEGER,
            estimated_cost REAL,
            content_id INTEGER,
            pipeline_run_id INTEGER,
            created_at TEXT
        )"""
    )
    return conn


def _usage(
    conn: sqlite3.Connection,
    *,
    operation_name: str = "synthesis.generate",
    model_name: str = "claude-sonnet",
    total_tokens: int = 100,
    estimated_cost: float = 0.01,
    content_id: int | None = None,
    pipeline_run_id: int | None = None,
    hours_ago: float = 1,
) -> None:
    conn.execute(
        """INSERT INTO model_usage
           (model_name, operation_name, input_tokens, output_tokens, total_tokens,
            estimated_cost, content_id, pipeline_run_id, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            model_name,
            operation_name,
            total_tokens // 2,
            total_tokens - (total_tokens // 2),
            total_tokens,
            estimated_cost,
            content_id,
            pipeline_run_id,
            _ts(hours_ago),
        ),
    )
    conn.commit()


def test_builder_groups_buckets_and_flags_cost_and_token_thresholds():
    report = build_model_usage_cost_spikes_report(
        [
            {
                "operation_name": "synthesis.generate",
                "model_name": "claude-sonnet",
                "input_tokens": 100,
                "output_tokens": 200,
                "total_tokens": 300,
                "estimated_cost": 0.7,
                "content_id": 10,
                "pipeline_run_id": None,
            },
            {
                "operation_name": "synthesis.generate",
                "model_name": "claude-sonnet",
                "input_tokens": 100,
                "output_tokens": 250,
                "total_tokens": 350,
                "estimated_cost": 0.6,
                "content_id": 11,
                "pipeline_run_id": 20,
            },
            {
                "operation_name": "synthesis.rank",
                "model_name": "gpt-4",
                "total_tokens": 20,
                "estimated_cost": 0.01,
            },
        ],
        cost_threshold=1.0,
        token_threshold=600,
        now=NOW,
    )

    assert report["artifact_type"] == "model_usage_cost_spikes"
    assert report["total_spikes"] == 1
    spike = report["spike_items"][0]
    assert spike["operation_name"] == "synthesis.generate"
    assert spike["model_name"] == "claude-sonnet"
    assert spike["call_count"] == 2
    assert spike["estimated_cost"] == 1.3
    assert spike["total_tokens"] == 650
    assert spike["triggered_thresholds"] == ["estimated_cost", "total_tokens"]
    assert spike["examples"][0]["content_id"] == 10


def test_db_loader_filters_recent_rows_and_returns_examples():
    conn = _conn()
    _usage(conn, estimated_cost=0.8, total_tokens=400, content_id=1, hours_ago=1)
    _usage(conn, estimated_cost=0.4, total_tokens=300, pipeline_run_id=2, hours_ago=2)
    _usage(conn, estimated_cost=10.0, total_tokens=9999, hours_ago=48)

    report = build_model_usage_cost_spikes_report_from_db(
        conn,
        window_hours=24,
        cost_threshold=1.0,
        token_threshold=1000,
        now=NOW,
    )

    assert report["summary"]["rows_scanned"] == 2
    assert report["total_spikes"] == 1
    assert report["spike_items"][0]["estimated_cost"] == 1.2
    assert {example["content_id"] for example in report["spike_items"][0]["examples"]} == {1, None}


def test_missing_model_usage_table_returns_empty_report_with_metadata():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_model_usage_cost_spikes_report_from_db(conn, now=NOW)

    assert report["missing_tables"] == ["model_usage"]
    assert report["summary"]["rows_scanned"] == 0
    assert report["total_spikes"] == 0
    assert report["spike_items"] == []


def test_formatters_are_stable_and_text_is_readable():
    report = build_model_usage_cost_spikes_report(
        [{"operation_name": "op", "model_name": "model", "total_tokens": 10, "estimated_cost": 2.0}],
        cost_threshold=1.0,
        token_threshold=100,
        now=NOW,
    )

    payload = json.loads(format_model_usage_cost_spikes_json(report))
    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "model_usage_cost_spikes"
    text = format_model_usage_cost_spikes_text(report)
    assert "Model Usage Cost Spikes" in text
    assert "operation=op" in text
    assert "triggers=estimated_cost" in text


def test_cli_supports_db_json_text_and_argument_validation(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "usage.sqlite"
    conn = _conn()
    _usage(conn, estimated_cost=2.0, total_tokens=10, hours_ago=1)
    conn.backup(sqlite3.connect(db_path))
    conn.close()

    assert script.main(["--db", str(db_path), "--cost-threshold", "1", "--token-threshold", "100", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "model_usage_cost_spikes"
    assert script.main(["--db", str(db_path), "--cost-threshold", "1", "--format", "text"]) == 0
    assert "Model Usage Cost Spikes" in capsys.readouterr().out

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--window-hours", "24", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["spike_items"] == []
    assert script.main(["--window-hours", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_builder_rejects_invalid_thresholds():
    with pytest.raises(ValueError, match="cost_threshold must be non-negative"):
        build_model_usage_cost_spikes_report([], cost_threshold=-1)
    with pytest.raises(ValueError, match="token_threshold must be non-negative"):
        build_model_usage_cost_spikes_report([], token_threshold=-1)
