"""Tests for pipeline run outcome window reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.pipeline_run_outcome_windows import (
    build_pipeline_run_outcome_windows_report,
    format_pipeline_run_outcome_windows_json,
    format_pipeline_run_outcome_windows_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "pipeline_run_outcome_windows.py"
spec = importlib.util.spec_from_file_location("pipeline_run_outcome_windows_script", SCRIPT_PATH)
pipeline_run_outcome_windows_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(pipeline_run_outcome_windows_script)


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
            outcome TEXT,
            rejection_reason TEXT,
            final_score REAL,
            refinement_picked TEXT,
            best_score_before_refine REAL,
            best_score_after_refine REAL,
            created_at TEXT
        )"""
    )
    return conn


def _insert(
    conn: sqlite3.Connection,
    *,
    batch_id: str,
    content_type: str,
    outcome: str,
    rejection_reason: str | None = None,
    final_score: float | None = None,
    refinement_picked: str | None = None,
    before: float | None = None,
    after: float | None = None,
    created_at: str = "2026-05-02T10:00:00+00:00",
) -> None:
    conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, outcome, rejection_reason, final_score,
            refinement_picked, best_score_before_refine, best_score_after_refine, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            batch_id,
            content_type,
            outcome,
            rejection_reason,
            final_score,
            refinement_picked,
            before,
            after,
            created_at,
        ),
    )
    conn.commit()


def test_report_groups_outcomes_and_computes_score_averages():
    conn = _conn()
    _insert(
        conn,
        batch_id="batch-1",
        content_type="x_thread",
        outcome="published",
        final_score=8.0,
        refinement_picked="REFINED",
        before=7.0,
        after=8.0,
        created_at="2026-05-02T10:00:00+00:00",
    )
    _insert(
        conn,
        batch_id="batch-2",
        content_type="x_thread",
        outcome="published",
        final_score=9.0,
        refinement_picked="REFINED",
        before=8.0,
        after=8.5,
        created_at="2026-05-02T09:00:00+00:00",
    )
    _insert(
        conn,
        batch_id="batch-3",
        content_type="x_post",
        outcome="below_threshold",
        rejection_reason="Below threshold",
        final_score=5.0,
        refinement_picked="ORIGINAL",
        before=6.0,
        after=5.5,
        created_at="2026-05-01T09:00:00+00:00",
    )
    _insert(
        conn,
        batch_id="old",
        content_type="x_post",
        outcome="published",
        final_score=10.0,
        created_at="2026-04-01T09:00:00+00:00",
    )

    report = build_pipeline_run_outcome_windows_report(conn, window_days=7, limit=10, now=NOW)

    assert report["artifact_type"] == "pipeline_run_outcome_windows"
    assert report["totals"] == {
        "runs": 3,
        "content_types": 2,
        "outcomes": 2,
        "average_final_score": pytest.approx(7.3333),
        "average_refinement_delta": pytest.approx(0.3333),
    }
    group = report["groups"][0]
    assert group["content_type"] == "x_thread"
    assert group["outcome"] == "published"
    assert group["rejection_reason"] is None
    assert group["refinement_picked"] == "REFINED"
    assert group["count"] == 2
    assert group["average_final_score"] == 8.5
    assert group["average_refinement_delta"] == 0.75
    assert group["representative_batch_ids"] == ["batch-1", "batch-2"]
    conn.close()


def test_items_include_required_run_fields_and_respect_limit():
    conn = _conn()
    _insert(
        conn,
        batch_id="newer",
        content_type="x_thread",
        outcome="published",
        final_score=8.1,
        refinement_picked="REFINED",
        before=7.0,
        after=8.1,
        created_at="2026-05-02T11:00:00+00:00",
    )
    _insert(
        conn,
        batch_id="older",
        content_type="x_post",
        outcome="all_filtered",
        rejection_reason="All candidates filtered",
        final_score=None,
        refinement_picked=None,
        created_at="2026-05-01T11:00:00+00:00",
    )

    report = build_pipeline_run_outcome_windows_report(conn, limit=1, now=NOW)

    assert len(report["items"]) == 1
    assert report["items"][0] == {
        "batch_id": "newer",
        "content_type": "x_thread",
        "outcome": "published",
        "rejection_reason": None,
        "final_score": 8.1,
        "refinement_picked": "REFINED",
        "best_score_before_refine": 7.0,
        "best_score_after_refine": 8.1,
        "refinement_delta": 1.1,
        "created_at": "2026-05-02T11:00:00+00:00",
    }
    assert report["totals"]["runs"] == 2
    conn.close()


def test_missing_optional_score_columns_are_reported_without_failing():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE pipeline_runs (
            batch_id TEXT,
            content_type TEXT,
            outcome TEXT,
            created_at TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO pipeline_runs (batch_id, content_type, outcome, created_at)
           VALUES ('minimal', 'x_post', 'published', '2026-05-02T10:00:00+00:00')"""
    )

    report = build_pipeline_run_outcome_windows_report(conn, now=NOW)

    assert report["missing_optional_columns"]["pipeline_runs"] == [
        "best_score_after_refine",
        "best_score_before_refine",
        "final_score",
        "refinement_picked",
        "rejection_reason",
    ]
    assert report["items"][0]["batch_id"] == "minimal"
    assert report["items"][0]["final_score"] is None
    assert "Missing optional columns:" in format_pipeline_run_outcome_windows_text(report)
    conn.close()


def test_missing_required_schema_and_invalid_builder_args_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_pipeline_run_outcome_windows_report(conn, now=NOW)
    assert report["missing_tables"] == ["pipeline_runs"]
    assert report["groups"] == []

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute("CREATE TABLE pipeline_runs (batch_id TEXT, created_at TEXT)")
    report = build_pipeline_run_outcome_windows_report(partial, now=NOW)
    assert report["missing_columns"]["pipeline_runs"] == ["content_type", "outcome"]

    with pytest.raises(ValueError, match="window_days must be positive"):
        build_pipeline_run_outcome_windows_report(conn, window_days=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_pipeline_run_outcome_windows_report(conn, limit=0, now=NOW)
    conn.close()
    partial.close()


def test_json_text_and_cli_formatting_are_stable(monkeypatch, capsys):
    conn = _conn()
    _insert(
        conn,
        batch_id="cli",
        content_type="x_thread",
        outcome="published",
        final_score=8.0,
        refinement_picked="REFINED",
        before=7.0,
        after=8.0,
    )

    report = build_pipeline_run_outcome_windows_report(conn, window_days=7, limit=3, now=NOW)
    payload = json.loads(format_pipeline_run_outcome_windows_json(report))
    text = format_pipeline_run_outcome_windows_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["groups"][0]["outcome"] == "published"
    assert "Pipeline Run Outcome Windows" in text
    assert "Outcome groups:" in text
    assert "Recent runs:" in text

    monkeypatch.setattr(
        pipeline_run_outcome_windows_script,
        "script_context",
        lambda: _script_context(conn),
    )
    monkeypatch.setattr(
        pipeline_run_outcome_windows_script,
        "build_pipeline_run_outcome_windows_report",
        lambda db, **kwargs: build_pipeline_run_outcome_windows_report(db, now=NOW, **kwargs),
    )

    assert pipeline_run_outcome_windows_script.main(["--window-days", "7", "--limit", "3", "--format", "json"]) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["window_days"] == 7
    assert cli_payload["filters"]["limit"] == 3

    assert pipeline_run_outcome_windows_script.main(["--window-days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    conn.close()
