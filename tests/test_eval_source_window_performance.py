"""Tests for eval source window performance reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.eval_source_window_performance import (
    build_eval_source_window_performance_report,
    format_eval_source_window_performance_json,
    format_eval_source_window_performance_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "eval_source_window_performance.py"
spec = importlib.util.spec_from_file_location("eval_source_window_performance_script", SCRIPT_PATH)
eval_source_window_performance_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(eval_source_window_performance_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE eval_results (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            threshold REAL,
            source_window_hours INTEGER,
            prompt_count INTEGER,
            commit_count INTEGER,
            candidate_count INTEGER,
            final_score REAL,
            rejection_reason TEXT,
            created_at TEXT
        )"""
    )
    return conn


def _insert(
    conn: sqlite3.Connection,
    *,
    result_id: int,
    content_type: str,
    source_window_hours: int,
    final_score: float | None,
    rejection_reason: str | None = None,
    threshold: float = 7.0,
    prompt_count: int = 2,
    commit_count: int = 3,
    candidate_count: int = 4,
    created_at: str = "2026-05-02T10:00:00+00:00",
) -> None:
    conn.execute(
        """INSERT INTO eval_results
           (id, content_type, threshold, source_window_hours, prompt_count,
            commit_count, candidate_count, final_score, rejection_reason, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            result_id,
            content_type,
            threshold,
            source_window_hours,
            prompt_count,
            commit_count,
            candidate_count,
            final_score,
            rejection_reason,
            created_at,
        ),
    )
    conn.commit()


def test_report_groups_by_content_type_and_source_window_with_pass_metrics():
    conn = _conn()
    _insert(conn, result_id=1, content_type="x_thread", source_window_hours=8, final_score=8.0)
    _insert(
        conn,
        result_id=2,
        content_type="x_thread",
        source_window_hours=8,
        final_score=9.0,
        prompt_count=4,
        commit_count=5,
        candidate_count=6,
        created_at="2026-05-02T11:00:00+00:00",
    )
    _insert(
        conn,
        result_id=3,
        content_type="x_thread",
        source_window_hours=8,
        final_score=8.0,
        rejection_reason="filtered",
        created_at="2026-05-01T10:00:00+00:00",
    )
    _insert(
        conn,
        result_id=4,
        content_type="x_thread",
        source_window_hours=24,
        final_score=6.0,
        rejection_reason="Below threshold",
    )
    _insert(
        conn,
        result_id=5,
        content_type="x_post",
        source_window_hours=8,
        final_score=7.5,
    )
    _insert(
        conn,
        result_id=6,
        content_type="x_post",
        source_window_hours=8,
        final_score=9.0,
        created_at="2026-04-01T10:00:00+00:00",
    )

    report = build_eval_source_window_performance_report(conn, lookback_days=7, limit=10, now=NOW)

    assert report["artifact_type"] == "eval_source_window_performance"
    assert report["totals"] == {
        "run_count": 5,
        "groups": 3,
        "pass_count": 3,
        "pass_rate": 0.6,
        "average_final_score": 7.7,
    }
    best = report["groups"][0]
    assert best["content_type"] == "x_post"
    assert best["source_window_hours"] == 8
    assert best["pass_rate"] == 1.0
    thread = next(group for group in report["groups"] if group["content_type"] == "x_thread" and group["source_window_hours"] == 8)
    assert thread["run_count"] == 3
    assert thread["pass_count"] == 2
    assert thread["pass_rate"] == pytest.approx(0.6667)
    assert thread["average_prompt_count"] == pytest.approx(2.6667)
    assert thread["average_commit_count"] == pytest.approx(3.6667)
    assert thread["average_candidate_count"] == pytest.approx(4.6667)
    assert thread["average_final_score"] == pytest.approx(8.3333)
    assert thread["rejection_reason_counts"] == {"filtered": 1}
    assert thread["representative_result_ids"] == [2, 1, 3]
    assert thread["latest_created_at"] == "2026-05-02T11:00:00+00:00"
    conn.close()


def test_pass_requires_threshold_and_empty_rejection_reason():
    conn = _conn()
    _insert(conn, result_id=1, content_type="blog", source_window_hours=12, final_score=7.0)
    _insert(
        conn,
        result_id=2,
        content_type="blog",
        source_window_hours=12,
        final_score=7.0,
        rejection_reason=" ",
    )
    _insert(
        conn,
        result_id=3,
        content_type="blog",
        source_window_hours=12,
        final_score=7.0,
        rejection_reason="manual reject",
    )
    _insert(conn, result_id=4, content_type="blog", source_window_hours=12, final_score=6.99)

    report = build_eval_source_window_performance_report(conn, now=NOW)

    group = report["groups"][0]
    assert group["run_count"] == 4
    assert group["pass_count"] == 2
    assert group["pass_rate"] == 0.5
    assert group["rejection_reason_counts"] == {"manual reject": 1}
    conn.close()


def test_limit_applies_to_items_not_groups():
    conn = _conn()
    _insert(conn, result_id=1, content_type="a", source_window_hours=8, final_score=8.0)
    _insert(conn, result_id=2, content_type="b", source_window_hours=8, final_score=8.0)
    _insert(conn, result_id=3, content_type="c", source_window_hours=8, final_score=8.0)

    report = build_eval_source_window_performance_report(conn, limit=2, now=NOW)

    assert len(report["groups"]) == 3
    assert len(report["items"]) == 2
    conn.close()


def test_missing_schema_and_invalid_builder_args_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_eval_source_window_performance_report(conn, now=NOW)
    assert report["missing_tables"] == ["eval_results"]
    assert report["missing_columns"] == {}
    assert report["groups"] == []

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute("CREATE TABLE eval_results (id INTEGER, content_type TEXT)")
    report = build_eval_source_window_performance_report(partial, now=NOW)
    assert report["missing_columns"]["eval_results"] == [
        "candidate_count",
        "commit_count",
        "created_at",
        "final_score",
        "prompt_count",
        "rejection_reason",
        "source_window_hours",
        "threshold",
    ]

    with pytest.raises(ValueError, match="lookback_days must be positive"):
        build_eval_source_window_performance_report(conn, lookback_days=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_eval_source_window_performance_report(conn, limit=0, now=NOW)
    conn.close()
    partial.close()


def test_json_text_and_cli_formatting_are_stable(monkeypatch, capsys):
    conn = _conn()
    _insert(conn, result_id=1, content_type="x_thread", source_window_hours=8, final_score=8.0)

    report = build_eval_source_window_performance_report(conn, lookback_days=7, limit=1, now=NOW)
    payload = json.loads(format_eval_source_window_performance_json(report))
    text = format_eval_source_window_performance_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["groups"][0]["content_type"] == "x_thread"
    assert "Eval Source Window Performance" in text
    assert "Source windows:" in text

    monkeypatch.setattr(
        eval_source_window_performance_script,
        "script_context",
        lambda: _script_context(conn),
    )
    monkeypatch.setattr(
        eval_source_window_performance_script,
        "build_eval_source_window_performance_report",
        lambda db, **kwargs: build_eval_source_window_performance_report(db, now=NOW, **kwargs),
    )

    assert eval_source_window_performance_script.main(["--lookback-days", "7", "--limit", "1", "--format", "json"]) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["lookback_days"] == 7
    assert cli_payload["filters"]["limit"] == 1

    assert eval_source_window_performance_script.main(["--lookback-days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    conn.close()
