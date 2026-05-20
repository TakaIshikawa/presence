"""Tests for prompt version cost efficiency reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.prompt_version_cost_efficiency import (
    build_prompt_version_cost_efficiency_report,
    build_prompt_version_cost_efficiency_report_from_db,
    format_prompt_version_cost_efficiency_json,
    format_prompt_version_cost_efficiency_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "prompt_version_cost_efficiency.py"
spec = importlib.util.spec_from_file_location("prompt_version_cost_efficiency_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_groups_cost_tokens_by_prompt_version_and_unknown():
    report = build_prompt_version_cost_efficiency_report(
        [{"id": "1", "version": "v1"}],
        [
            {"id": 1, "generated_content_id": 10, "prompt_version_id": "1", "input_tokens": 100, "output_tokens": 50, "cost_usd": 2.0},
            {"id": 2, "generated_content_id": 11, "input_tokens": 30, "output_tokens": 20, "cost_usd": 0.5},
        ],
        [{"id": 10}, {"id": 11}],
        expensive_cost=1.0,
        now=NOW,
    )

    assert report["artifact_type"] == "prompt_version_cost_efficiency"
    by_version = {item["prompt_version"]: item for item in report["version_summary"]}
    assert by_version["v1"]["total_cost_usd"] == 2.0
    assert by_version["v1"]["average_cost_per_generated_content_item"] == 2.0
    assert by_version["v1"]["token_mix"]["input_tokens"] == 100
    assert by_version["unknown"]["total_cost_usd"] == 0.5
    assert report["expensive_examples"][0]["prompt_version"] == "v1"


def test_db_loader_supports_direct_generated_content_usage_and_linked_model_usage():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE prompt_versions (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY, prompt_version TEXT, cost_usd REAL, total_tokens INTEGER)")
    conn.execute("INSERT INTO prompt_versions VALUES (?, ?)", (1, "pv-one"))
    conn.execute("INSERT INTO generated_content VALUES (?, ?, ?, ?)", (10, "direct-v", 1.5, 300))
    conn.commit()

    direct = build_prompt_version_cost_efficiency_report_from_db(conn, now=NOW)
    assert direct["version_summary"][0]["prompt_version"] == "direct-v"
    assert direct["version_summary"][0]["total_cost_usd"] == 1.5

    conn.execute("CREATE TABLE model_usage (id INTEGER PRIMARY KEY, generated_content_id INTEGER, prompt_version_id INTEGER, input_tokens INTEGER, output_tokens INTEGER, cost REAL)")
    conn.execute("INSERT INTO model_usage VALUES (?, ?, ?, ?, ?, ?)", (1, 20, 1, 10, 20, 0.25))
    conn.execute("INSERT INTO generated_content VALUES (?, ?, ?, ?)", (20, None, None, None))
    conn.commit()

    linked = build_prompt_version_cost_efficiency_report_from_db(conn, now=NOW)
    versions = {item["prompt_version"] for item in linked["version_summary"]}
    assert "pv-one" in versions
    assert linked["missing_schema"]["missing_tables"] == []


def test_missing_schema_metadata_and_formatters_and_cli(tmp_path, capsys):
    empty = sqlite3.connect(":memory:")
    report = build_prompt_version_cost_efficiency_report_from_db(empty, now=NOW)
    assert report["missing_schema"]["missing_tables"] == ["generated_content"]

    payload = build_prompt_version_cost_efficiency_report([], [], [], now=NOW)
    assert json.loads(format_prompt_version_cost_efficiency_json(payload))["artifact_type"] == "prompt_version_cost_efficiency"
    assert "Prompt Version Cost Efficiency" in format_prompt_version_cost_efficiency_text(payload)

    db_path = tmp_path / "prompt.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY, prompt_version TEXT, cost_usd REAL, tokens INTEGER)")
    conn.execute("INSERT INTO generated_content VALUES (?, ?, ?, ?)", (1, "v-cli", 3.0, 100))
    conn.commit()
    conn.close()
    assert script.main(["--db", str(db_path), "--format", "json", "--expensive-cost", "2"]) == 0
    assert json.loads(capsys.readouterr().out)["expensive_examples"][0]["prompt_version"] == "v-cli"


def test_builder_rejects_invalid_arguments():
    with pytest.raises(ValueError, match="expensive_cost must be non-negative"):
        build_prompt_version_cost_efficiency_report([], [], [], expensive_cost=-1)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_prompt_version_cost_efficiency_report([], [], [], limit=0)
