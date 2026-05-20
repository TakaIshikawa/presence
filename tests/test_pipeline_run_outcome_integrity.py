"""Tests for pipeline run outcome integrity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.pipeline_run_outcome_integrity import (
    build_pipeline_run_outcome_integrity_report_from_db,
    format_pipeline_run_outcome_integrity_json,
    format_pipeline_run_outcome_integrity_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "pipeline_run_outcome_integrity.py"
spec = importlib.util.spec_from_file_location("pipeline_run_outcome_integrity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE pipeline_runs (
             id INTEGER PRIMARY KEY,
             batch_id TEXT,
             content_type TEXT,
             candidates_generated INTEGER,
             best_candidate_index INTEGER,
             refinement_picked TEXT,
             final_score REAL,
             published INTEGER,
             content_id INTEGER,
             outcome TEXT,
             filter_stats TEXT,
             created_at TEXT
           )"""
    )
    return conn


def _insert(conn: sqlite3.Connection, **overrides):
    row = {
        "batch_id": "batch",
        "content_type": "x_post",
        "candidates_generated": 2,
        "best_candidate_index": 1,
        "refinement_picked": "REFINED",
        "final_score": 8.0,
        "published": 1,
        "content_id": 10,
        "outcome": "published",
        "filter_stats": '{"repetition_rejected": 1}',
        "created_at": "2026-05-20T10:00:00+00:00",
    }
    row.update(overrides)
    conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, candidates_generated, best_candidate_index,
            refinement_picked, final_score, published, content_id, outcome,
            filter_stats, created_at)
           VALUES (:batch_id, :content_type, :candidates_generated, :best_candidate_index,
            :refinement_picked, :final_score, :published, :content_id, :outcome,
            :filter_stats, :created_at)""",
        row,
    )
    conn.commit()


def test_flags_contradictory_published_outcome_and_missing_final_score():
    conn = _conn()
    _insert(conn, published=0, content_id=None, outcome="published", final_score=None)

    report = build_pipeline_run_outcome_integrity_report_from_db(conn, now=NOW)

    issue_types = [finding["issue_type"] for finding in report["findings"]]
    assert issue_types == ["missing_final_score", "published_outcome_content_mismatch"]
    mismatch = report["findings"][1]
    assert mismatch["details"]["mismatches"] == [
        "publishable_outcome_without_published_flag",
        "publishable_state_without_content_id",
    ]
    assert report["summary"]["by_issue_type"]["missing_final_score"] == 1


def test_flags_malformed_filter_stats_invalid_candidate_index_and_refinement():
    conn = _conn()
    _insert(
        conn,
        candidates_generated=2,
        best_candidate_index=2,
        refinement_picked="BOTH",
        filter_stats="{bad json",
    )

    report = build_pipeline_run_outcome_integrity_report_from_db(conn, now=NOW)

    assert [finding["issue_type"] for finding in report["findings"]] == [
        "invalid_best_candidate_index",
        "invalid_refinement_picked",
        "malformed_filter_stats",
    ]
    assert report["summary"]["malformed_filter_stats_count"] == 1


def test_flags_missing_or_non_positive_candidates_for_completed_runs():
    conn = _conn()
    _insert(conn, candidates_generated=0, best_candidate_index=None, outcome="below_threshold", published=0, content_id=None)
    _insert(conn, candidates_generated=None, best_candidate_index=None, outcome="pending", published=0, content_id=None)

    report = build_pipeline_run_outcome_integrity_report_from_db(conn, now=NOW)

    assert [finding["issue_type"] for finding in report["findings"]] == ["invalid_candidates_generated"]
    assert report["summary"]["pipeline_run_count"] == 2


def test_missing_schema_is_reported_without_crashing():
    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row

    report = build_pipeline_run_outcome_integrity_report_from_db(empty, now=NOW)

    assert report["artifact_type"] == "pipeline_run_outcome_integrity"
    assert report["missing_tables"] == ["pipeline_runs"]
    assert report["empty_state"] == {"is_empty": True, "reason": "missing_schema"}
    assert report["findings"] == []

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute("CREATE TABLE pipeline_runs (batch_id TEXT, outcome TEXT)")
    report = build_pipeline_run_outcome_integrity_report_from_db(partial, now=NOW)
    assert "candidates_generated" in report["missing_columns"]["pipeline_runs"]
    assert report["summary"]["pipeline_run_count"] == 0


def test_formatters_cli_json_and_argument_validation(tmp_path, monkeypatch, capsys):
    conn = _conn()
    _insert(conn, best_candidate_index=9)
    report = build_pipeline_run_outcome_integrity_report_from_db(conn, now=NOW)
    assert json.loads(format_pipeline_run_outcome_integrity_json(report))["artifact_type"] == "pipeline_run_outcome_integrity"
    assert "Pipeline Run Outcome Integrity" in format_pipeline_run_outcome_integrity_text(report)

    db_path = tmp_path / "runs.sqlite"
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(db_path), "--format", "json", "--limit", "5"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Pipeline Run Outcome Integrity" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["pipeline_runs"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
