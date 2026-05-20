"""Tests for pipeline candidate diversity gap reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.pipeline_candidate_diversity_gaps import (
    build_pipeline_candidate_diversity_gaps_report,
    build_pipeline_candidate_diversity_gaps_report_from_db,
    format_pipeline_candidate_diversity_gaps_json,
    format_pipeline_candidate_diversity_gaps_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "pipeline_candidate_diversity_gaps.py"
spec = importlib.util.spec_from_file_location("pipeline_candidate_diversity_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE pipeline_candidates (
            id INTEGER PRIMARY KEY,
            pipeline_run_id TEXT,
            format TEXT,
            hook TEXT,
            text TEXT,
            created_at TEXT
        )"""
    )
    return conn


def test_builder_flags_low_format_and_similar_openings():
    rows = [
        {"pipeline_run_id": "run-1", "format": "thread", "hook": "Start with the surprising data point", "created_at": NOW.isoformat()},
        {"pipeline_run_id": "run-1", "format": "thread", "hook": "Start with the surprising data points", "created_at": NOW.isoformat()},
        {"pipeline_run_id": "run-1", "format": "post", "hook": "A different opening", "created_at": NOW.isoformat()},
    ]
    report = build_pipeline_candidate_diversity_gaps_report(rows, min_unique_formats=3, now=NOW)
    assert report["artifact_type"] == "pipeline_candidate_diversity_gaps"
    assert report["summary"]["finding_count"] == 1
    finding = report["findings"][0]
    assert finding["pipeline_run_id"] == "run-1"
    assert finding["candidate_count"] == 3
    assert finding["unique_format_count"] == 2
    assert "near_identical_opening_clauses" in finding["gap_reasons"]


def test_db_adapter_cli_and_missing_schema(tmp_path, capsys):
    conn = _conn()
    for i in range(1, 4):
        conn.execute("INSERT INTO pipeline_candidates VALUES (?, 'run-1', 'post', ?, ?, ?)", (i, "Same hook opening", "body", (NOW - timedelta(days=1)).isoformat()))
    conn.commit()
    report = build_pipeline_candidate_diversity_gaps_report_from_db(conn, min_unique_formats=2, now=NOW)
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["format_counts"] == {"post": 3}
    assert json.loads(format_pipeline_candidate_diversity_gaps_json(report))["artifact_type"] == "pipeline_candidate_diversity_gaps"
    assert "Pipeline Candidate Diversity Gaps" in format_pipeline_candidate_diversity_gaps_text(report)

    db_path = tmp_path / "pipeline.sqlite"
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--days", "30", "--min-unique-formats", "2", "--similarity-threshold", "0.8", "--limit", "5"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Pipeline Candidate Diversity Gaps" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--similarity-threshold", "2"])

    missing = build_pipeline_candidate_diversity_gaps_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["pipeline_candidates|synthesis_candidates|generated_candidates"]
