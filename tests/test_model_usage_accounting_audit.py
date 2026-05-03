"""Tests for model usage accounting audits."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.model_usage_accounting_audit import (
    build_model_usage_accounting_audit_report,
    format_model_usage_accounting_audit_json,
    format_model_usage_accounting_audit_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "model_usage_accounting_audit.py"
spec = importlib.util.spec_from_file_location("model_usage_accounting_audit_script", SCRIPT_PATH)
model_usage_accounting_audit_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(model_usage_accounting_audit_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _ts(days_ago: float) -> str:
    return (NOW - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")


def _content(db) -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Published post",
        eval_score=8.0,
        eval_feedback="usable",
    )


def _pipeline_run(db) -> int:
    return db.insert_pipeline_run(
        batch_id="batch-ok",
        content_type="x_post",
        candidates_generated=3,
        best_candidate_index=0,
        best_score_before_refine=7.0,
    )


def _usage(
    db,
    *,
    model_name: str | None = "claude-sonnet",
    operation_name: str | None = "synthesis.generate_x_post",
    input_tokens: int = 10,
    output_tokens: int = 5,
    total_tokens: int = 15,
    estimated_cost: float = 0.01,
    content_id: int | None = None,
    pipeline_run_id: int | None = None,
    days_ago: float = 1,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO model_usage
           (model_name, operation_name, input_tokens, output_tokens, total_tokens,
            estimated_cost, content_id, pipeline_run_id, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            model_name,
            operation_name,
            input_tokens,
            output_tokens,
            total_tokens,
            estimated_cost,
            content_id,
            pipeline_run_id,
            _ts(days_ago),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_clean_rows_have_no_findings(db):
    content_id = _content(db)
    run_id = _pipeline_run(db)
    _usage(db, content_id=content_id, pipeline_run_id=run_id)

    report = build_model_usage_accounting_audit_report(db, days=7, now=NOW)

    assert report.has_issues is False
    assert report.totals["rows_scanned"] == 1
    assert report.totals["issue_count"] == 0
    assert report.findings == ()


def test_accounting_inconsistencies_have_stable_issue_types_and_totals(db):
    _usage(
        db,
        model_name="",
        operation_name="synthesis.generate_x_post",
        input_tokens=30,
        output_tokens=12,
        total_tokens=50,
        estimated_cost=0,
        content_id=9999,
        pipeline_run_id=8888,
    )
    _usage(
        db,
        model_name="claude-sonnet",
        operation_name=" ",
        input_tokens=4,
        output_tokens=6,
        total_tokens=10,
        estimated_cost=0.002,
    )

    report = build_model_usage_accounting_audit_report(db, days=7, now=NOW)
    payload = json.loads(format_model_usage_accounting_audit_json(report))
    issue_types = [finding["issue_type"] for finding in payload["findings"]]

    assert payload["artifact_type"] == "model_usage_accounting_audit"
    assert payload["has_issues"] is True
    assert set(issue_types) == {
        "blank_model_name",
        "blank_operation_name",
        "missing_generated_content",
        "missing_pipeline_run",
        "token_total_mismatch",
        "zero_cost_with_tokens",
    }
    assert payload["totals"]["issue_count"] == 6
    assert payload["totals"]["rows_scanned"] == 2
    assert payload["totals"]["rows_with_issues"] == 2
    assert payload["totals"]["by_issue_type"]["zero_cost_with_tokens"] == 1
    assert payload["totals"]["by_operation"]["synthesis.generate_x_post"] == 5
    assert payload["totals"]["by_operation"]["(blank)"] == 1
    assert payload["totals"]["by_model"]["(blank)"] == 5
    assert payload["totals"]["by_model"]["claude-sonnet"] == 1

    text = format_model_usage_accounting_audit_text(report)
    assert "type=zero_cost_with_tokens" in text
    assert "type=missing_generated_content" in text
    assert "issue_count=6" in text


def test_days_operation_and_model_filters_limit_rows(db):
    _usage(db, operation_name="included", model_name="claude-sonnet", estimated_cost=0, days_ago=1)
    _usage(db, operation_name="included", model_name="claude-opus", estimated_cost=0, days_ago=1)
    _usage(db, operation_name="excluded", model_name="claude-sonnet", estimated_cost=0, days_ago=1)
    _usage(db, operation_name="included", model_name="claude-sonnet", estimated_cost=0, days_ago=10)

    report = build_model_usage_accounting_audit_report(
        db,
        days=7,
        operation="included",
        model="claude-sonnet",
        now=NOW,
    )

    assert report.totals["rows_scanned"] == 1
    assert [finding.operation_name for finding in report.findings] == ["included"]
    assert [finding.model_name for finding in report.findings] == ["claude-sonnet"]


def test_missing_reference_checks_are_skipped_when_reference_tables_are_absent():
    import sqlite3

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
    conn.execute(
        """INSERT INTO model_usage
           (model_name, operation_name, input_tokens, output_tokens, total_tokens,
            estimated_cost, content_id, pipeline_run_id, created_at)
           VALUES ('model-a', 'op-a', 1, 2, 3, 0.01, 123, 456, ?)""",
        (_ts(1),),
    )

    report = build_model_usage_accounting_audit_report(conn, days=7, now=NOW)

    assert report.findings == ()
    assert report.missing_tables == ()


def test_cli_json_validation_and_fail_on_issues(db, monkeypatch, capsys):
    _usage(db, operation_name="included", model_name="claude-sonnet", estimated_cost=0)
    monkeypatch.setattr(
        model_usage_accounting_audit_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        model_usage_accounting_audit_script,
        "build_model_usage_accounting_audit_report",
        lambda db, **kwargs: build_model_usage_accounting_audit_report(db, now=NOW, **kwargs),
    )

    assert model_usage_accounting_audit_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    exit_code = model_usage_accounting_audit_script.main(
        [
            "--days",
            "7",
            "--operation",
            "included",
            "--model",
            "claude-sonnet",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["filters"]["operation"] == "included"
    assert payload["filters"]["model"] == "claude-sonnet"
    assert payload["totals"]["issue_count"] == 1

    assert model_usage_accounting_audit_script.main(["--fail-on-issues"]) == 1
    assert "type=zero_cost_with_tokens" in capsys.readouterr().out
