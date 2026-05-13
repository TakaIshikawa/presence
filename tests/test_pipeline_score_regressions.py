"""Tests for pipeline score regression reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.pipeline_score_regressions import (
    build_pipeline_score_regressions_report,
    format_pipeline_score_regressions_json,
    format_pipeline_score_regressions_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "pipeline_score_regressions.py"
spec = importlib.util.spec_from_file_location("pipeline_score_regressions_script", SCRIPT_PATH)
pipeline_score_regressions_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(pipeline_score_regressions_script)


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
            final_score REAL,
            rejection_reason TEXT,
            created_at TEXT
        )"""
    )
    return conn


def _insert(
    conn: sqlite3.Connection,
    *,
    batch_id: str,
    content_type: str,
    final_score: float | None,
    rejection_reason: str | None = None,
    created_at: str,
) -> None:
    conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, final_score, rejection_reason, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (batch_id, content_type, final_score, rejection_reason, created_at),
    )
    conn.commit()


def test_report_surfaces_score_drop_and_sorts_worst_delta_first():
    conn = _conn()
    for idx, score in enumerate((9.0, 9.5, 8.5), start=1):
        _insert(
            conn,
            batch_id=f"thread-prev-{idx}",
            content_type="x_thread",
            final_score=score,
            created_at=f"2026-04-2{idx}T10:00:00+00:00",
        )
    for idx, score in enumerate((7.0, 7.5, 7.0), start=1):
        _insert(
            conn,
            batch_id=f"thread-current-{idx}",
            content_type="x_thread",
            final_score=score,
            created_at=f"2026-04-{idx + 27:02d}T10:00:00+00:00",
        )
    for idx, score in enumerate((8.0, 8.0, 8.0), start=1):
        _insert(
            conn,
            batch_id=f"post-prev-{idx}",
            content_type="x_post",
            final_score=score,
            created_at=f"2026-04-2{idx}T11:00:00+00:00",
        )
        _insert(
            conn,
            batch_id=f"post-current-{idx}",
            content_type="x_post",
            final_score=score - 0.5,
            created_at=f"2026-04-{idx + 27:02d}T11:00:00+00:00",
        )

    report = build_pipeline_score_regressions_report(conn, window_days=7, min_runs=3, limit=10, now=NOW)

    assert report["artifact_type"] == "pipeline_score_regressions"
    assert report["totals"] == {
        "current_runs": 6,
        "previous_runs": 6,
        "groups": 2,
        "regressions": 2,
    }
    assert [item["content_type"] for item in report["items"]] == ["x_thread", "x_post"]
    thread = report["items"][0]
    assert thread["current_run_count"] == 3
    assert thread["previous_run_count"] == 3
    assert thread["current_average_final_score"] == pytest.approx(7.1667)
    assert thread["previous_average_final_score"] == 9.0
    assert thread["score_delta"] == pytest.approx(-1.8333)
    assert thread["current_rejection_rate"] == 0.0
    assert thread["previous_rejection_rate"] == 0.0
    assert thread["rejection_rate_delta"] == 0.0
    assert thread["latest_batch_ids"] == ["thread-current-3", "thread-current-2", "thread-current-1"]
    assert thread["latest_created_at"] == "2026-04-30T10:00:00+00:00"
    conn.close()


def test_report_surfaces_rejection_rate_increase_without_score_drop():
    conn = _conn()
    for idx in range(3):
        _insert(
            conn,
            batch_id=f"prev-{idx}",
            content_type="blog",
            final_score=8.0,
            created_at=f"2026-04-2{idx + 1}T10:00:00+00:00",
        )
    for idx in range(3):
        _insert(
            conn,
            batch_id=f"current-{idx}",
            content_type="blog",
            final_score=8.0,
            rejection_reason="Below threshold" if idx < 2 else None,
            created_at=f"2026-04-{idx + 28:02d}T10:00:00+00:00",
        )

    report = build_pipeline_score_regressions_report(conn, window_days=7, min_runs=3, now=NOW)

    assert len(report["items"]) == 1
    assert report["items"][0]["content_type"] == "blog"
    assert report["items"][0]["score_delta"] == 0.0
    assert report["items"][0]["current_rejection_rate"] == pytest.approx(0.6667)
    assert report["items"][0]["rejection_rate_delta"] == pytest.approx(0.6667)
    conn.close()


def test_min_runs_filter_keeps_groups_but_removes_regression_items():
    conn = _conn()
    _insert(
        conn,
        batch_id="prev",
        content_type="short",
        final_score=9.0,
        created_at="2026-04-24T10:00:00+00:00",
    )
    _insert(
        conn,
        batch_id="current",
        content_type="short",
        final_score=4.0,
        created_at="2026-04-30T10:00:00+00:00",
    )

    report = build_pipeline_score_regressions_report(conn, window_days=7, min_runs=2, now=NOW)

    assert report["groups"][0]["content_type"] == "short"
    assert report["groups"][0]["score_delta"] == -5.0
    assert report["items"] == []
    assert report["totals"]["regressions"] == 0
    conn.close()


def test_missing_schema_and_invalid_builder_args_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_pipeline_score_regressions_report(conn, now=NOW)
    assert report["missing_tables"] == ["pipeline_runs"]
    assert report["missing_columns"] == {}
    assert report["items"] == []

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute("CREATE TABLE pipeline_runs (batch_id TEXT, created_at TEXT)")
    report = build_pipeline_score_regressions_report(partial, now=NOW)
    assert report["missing_columns"]["pipeline_runs"] == [
        "content_type",
        "final_score",
        "rejection_reason",
    ]

    with pytest.raises(ValueError, match="window_days must be positive"):
        build_pipeline_score_regressions_report(conn, window_days=0, now=NOW)
    with pytest.raises(ValueError, match="min_runs must be positive"):
        build_pipeline_score_regressions_report(conn, min_runs=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_pipeline_score_regressions_report(conn, limit=0, now=NOW)
    conn.close()
    partial.close()


def test_json_text_and_cli_formatting_are_stable(monkeypatch, capsys):
    conn = _conn()
    for idx in range(3):
        _insert(
            conn,
            batch_id=f"prev-{idx}",
            content_type="x_thread",
            final_score=9.0,
            created_at=f"2026-04-2{idx + 1}T10:00:00+00:00",
        )
        _insert(
            conn,
            batch_id=f"current-{idx}",
            content_type="x_thread",
            final_score=7.0,
            created_at=f"2026-04-{idx + 28:02d}T10:00:00+00:00",
        )

    report = build_pipeline_score_regressions_report(conn, window_days=7, min_runs=3, limit=1, now=NOW)
    payload = json.loads(format_pipeline_score_regressions_json(report))
    text = format_pipeline_score_regressions_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["items"][0]["content_type"] == "x_thread"
    assert "Pipeline Score Regressions" in text
    assert "Regressions:" in text

    monkeypatch.setattr(
        pipeline_score_regressions_script,
        "script_context",
        lambda: _script_context(conn),
    )
    monkeypatch.setattr(
        pipeline_score_regressions_script,
        "build_pipeline_score_regressions_report",
        lambda db, **kwargs: build_pipeline_score_regressions_report(db, now=NOW, **kwargs),
    )

    assert pipeline_score_regressions_script.main(["--window-days", "7", "--min-runs", "3", "--limit", "1", "--format", "json"]) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["window_days"] == 7
    assert cli_payload["filters"]["min_runs"] == 3
    assert cli_payload["filters"]["limit"] == 1

    assert pipeline_score_regressions_script.main(["--window-days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    conn.close()
