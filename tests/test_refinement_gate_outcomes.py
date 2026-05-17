"""Tests for refinement gate outcomes reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.refinement_gate_outcomes import (
    build_refinement_gate_outcomes_report,
    build_refinement_gate_outcomes_report_from_db,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "refinement_gate_outcomes.py"
spec = importlib.util.spec_from_file_location("refinement_gate_outcomes_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_aggregates_outcomes_churn_and_score_movement():
    rows = [
        {
            "candidate_id": "pass",
            "format": "thread",
            "source_type": "curated_x",
            "refinement_attempts": 1,
            "before_score": 7,
            "after_score": 8,
            "final_gate_outcome": "passed",
            "created_at": NOW.isoformat(),
        },
        {
            "candidate_id": "improved-fail",
            "format": "thread",
            "source_type": "curated_x",
            "refinement_attempts": 4,
            "before_score": 5,
            "after_score": 7,
            "final_gate_outcome": "failed",
            "failure_reason": "claim check failed",
            "created_at": NOW.isoformat(),
        },
        {
            "candidate_id": "regressed",
            "format": "blog",
            "source_type": "curated_article",
            "refinement_attempts": 2,
            "before_score": 8,
            "after_score": 6,
            "rejection_reason": "too long",
            "created_at": NOW.isoformat(),
        },
        {
            "candidate_id": "unchanged",
            "format": "blog",
            "source_type": "curated_article",
            "refinement_attempts": 1,
            "before_score": 6,
            "after_score": 6,
            "final_gate_outcome": "failed",
            "created_at": NOW.isoformat(),
        },
    ]

    report = build_refinement_gate_outcomes_report(rows, now=NOW, high_churn_attempts=3)

    assert report["summary"]["passed"] == 1
    assert report["summary"]["failed"] == 3
    assert report["summary"]["movement_counts"]["improved"] == 2
    assert report["summary"]["movement_counts"]["regressed"] == 1
    assert report["summary"]["movement_counts"]["unchanged"] == 1
    assert report["summary"]["failure_reason_counts"]["unspecified"] == 1
    thread_group = next(group for group in report["aggregate_counts"] if group["format"] == "thread")
    assert thread_group["passed"] == 1
    assert thread_group["failed"] == 1
    assert report["high_churn_groups"][0]["format"] == "thread"
    assert report["improved_but_rejected"][0]["candidate_id"] == "improved-fail"
    assert report["common_final_rejection_reasons"][0]["failure_reason"] in {"claim check failed", "too long", "unspecified"}


def test_empty_dataset_returns_empty_report():
    report = build_refinement_gate_outcomes_report([], now=NOW)

    assert report["summary"]["rows_scanned"] == 0
    assert report["aggregate_counts"] == []
    assert report["high_churn_groups"] == []
    assert report["improved_but_rejected"] == []


def test_loads_pipeline_runs_and_cli_outputs_json_and_table(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE pipeline_runs (
            batch_id TEXT, content_type TEXT, candidates_generated INTEGER,
            best_score_before_refine REAL, best_score_after_refine REAL,
            outcome TEXT, rejection_reason TEXT, published INTEGER, created_at TEXT
        )"""
    )
    conn.executemany(
        "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("b1", "post", 3, 6, 7, "rejected", "persona mismatch", 0, NOW.isoformat()),
            ("b2", "post", 1, 8, 8, "published", None, 1, NOW.isoformat()),
        ],
    )
    db = SimpleNamespace(conn=conn)

    report = build_refinement_gate_outcomes_report_from_db(db, now=NOW)

    assert report["summary"]["rows_scanned"] == 2
    assert report["aggregate_counts"][0]["format"] == "post"
    assert report["improved_but_rejected"][0]["candidate_id"] == "b1"

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_refinement_gate_outcomes_report_from_db",
        lambda db, **kwargs: build_refinement_gate_outcomes_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "refinement_gate_outcomes"
    assert script.main(["--table"]) == 0
    assert "Aggregate counts:" in capsys.readouterr().out
