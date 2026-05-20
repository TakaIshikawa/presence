"""Tests for prompt version evaluation coverage reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.prompt_version_evaluation_coverage import (
    build_prompt_version_evaluation_coverage_report,
    build_prompt_version_evaluation_coverage_report_from_db,
    format_prompt_version_evaluation_coverage_json,
    format_prompt_version_evaluation_coverage_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "prompt_version_evaluation_coverage.py"
spec = importlib.util.spec_from_file_location("prompt_version_evaluation_coverage_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_used_prompts_without_recent_eval_and_unknown_hashes():
    report = build_prompt_version_evaluation_coverage_report(
        [{"prompt_version_id": 1, "prompt_hash": "h1", "prompt_type": "draft", "prompt_version": "v1"}],
        [{"prompt_version_id": 1, "prompt_hash": "h1", "created_at": "2026-04-01T00:00:00+00:00"}],
        [
            {"prompt_version_id": 1, "prompt_hash": "h1", "created_at": "2026-05-19T00:00:00+00:00", "usage_count": 3},
            {"prompt_hash": "missing-hash", "created_at": "2026-05-19T00:00:00+00:00"},
        ],
        now=NOW,
    )
    payload = json.loads(format_prompt_version_evaluation_coverage_json(report))
    assert payload["artifact_type"] == "prompt_version_evaluation_coverage"
    assert payload["summary"]["issue_count"] == 2
    assert [item["issue_type"] for item in payload["issue_items"]] == ["missing_recent_evaluation", "unknown_prompt_hash"]
    assert "Prompt Version Evaluation Coverage" in format_prompt_version_evaluation_coverage_text(report)


def test_from_db_loads_optional_sources_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE prompt_versions (id INTEGER PRIMARY KEY, prompt_hash TEXT, prompt_type TEXT, prompt_version TEXT);
        CREATE TABLE eval_batches (id INTEGER PRIMARY KEY, created_at TEXT);
        CREATE TABLE eval_results (id INTEGER PRIMARY KEY, batch_id INTEGER, prompt_version_id INTEGER, prompt_hash TEXT);
        CREATE TABLE model_usage (id INTEGER PRIMARY KEY, prompt_version_id INTEGER, prompt_hash TEXT, created_at TEXT, usage_count INTEGER);
        CREATE TABLE engagement_predictions (id INTEGER PRIMARY KEY, prompt_hash TEXT, created_at TEXT);
        INSERT INTO prompt_versions VALUES (1, 'h1', 'draft', 'v1');
        INSERT INTO eval_batches VALUES (1, '2026-04-01T00:00:00+00:00');
        INSERT INTO eval_results VALUES (1, 1, 1, 'h1');
        INSERT INTO model_usage VALUES (1, 1, 'h1', '2026-05-19T00:00:00+00:00', 2);
        INSERT INTO engagement_predictions VALUES (1, 'unknown', '2026-05-19T00:00:00+00:00');
        """
    )
    report = build_prompt_version_evaluation_coverage_report_from_db(conn, now=NOW)
    assert report["summary"]["issue_count"] == 2
    assert report["missing_tables"] == []

    db_path = tmp_path / "coverage.sqlite"
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--lookback-days", "30"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["issue_count"] == 2
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Prompt Version Evaluation Coverage" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])

    missing = build_prompt_version_evaluation_coverage_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert "prompt_versions" in missing["missing_tables"]
    assert "model_usage" in missing["missing_tables"]
