"""Tests for model usage budget guard."""

from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.model_budget_guard import (  # noqa: E402
    evaluate_model_budget,
    format_text_report,
    project_monthly_spend,
    summarize_model_spend,
)


def _load_cli_module():
    path = Path(__file__).parent.parent / "scripts" / "model_budget_guard.py"
    spec = importlib.util.spec_from_file_location("model_budget_guard_cli", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _summary_rows():
    return [
        {
            "day": "2026-04-24",
            "operation_name": "synthesis.generate_x_post",
            "model_name": "claude-sonnet-4-6",
            "call_count": 2,
            "input_tokens": 100,
            "output_tokens": 50,
            "total_tokens": 150,
            "estimated_cost": 1.0,
        },
        {
            "day": "2026-04-24",
            "operation_name": "synthesis.generate_x_post",
            "model_name": "claude-haiku-4-6",
            "call_count": 1,
            "input_tokens": 10,
            "output_tokens": 5,
            "total_tokens": 15,
            "estimated_cost": 0.25,
        },
        {
            "day": "2026-04-24",
            "operation_name": "synthesis.evaluate_candidates",
            "model_name": "claude-opus-4-7",
            "call_count": 1,
            "input_tokens": 200,
            "output_tokens": 100,
            "total_tokens": 300,
            "estimated_cost": 0.75,
        },
    ]


def test_summarize_model_spend_aggregates_by_operation_and_model():
    operation_spend, model_spend = summarize_model_spend(_summary_rows())

    generate = next(
        row
        for row in operation_spend
        if row["operation_name"] == "synthesis.generate_x_post"
    )
    assert generate["call_count"] == 3
    assert generate["total_tokens"] == 165
    assert generate["estimated_cost"] == pytest.approx(1.25)

    sonnet = next(
        row
        for row in model_spend
        if row["model_name"] == "claude-sonnet-4-6"
    )
    assert sonnet["operation_name"] == "synthesis.generate_x_post"
    assert sonnet["estimated_cost"] == pytest.approx(1.0)


def test_project_monthly_spend_uses_recent_daily_average():
    projected = project_monthly_spend(
        10.0,
        5,
        today=date(2026, 4, 15),
    )

    assert projected == pytest.approx(60.0)


def test_evaluate_model_budget_reports_total_and_operation_warnings():
    db = SimpleNamespace(get_model_usage_summary=lambda since_days: _summary_rows())

    report = evaluate_model_budget(
        db,
        days=10,
        monthly_budget=5.0,
        operation_budgets={
            "synthesis.generate_x_post": 2.0,
            "synthesis.evaluate_candidates": 99.0,
        },
        today=date(2026, 4, 15),
    )

    assert report.total_spend == pytest.approx(2.0)
    assert report.projected_monthly_spend == pytest.approx(6.0)
    assert [warning.kind for warning in report.warnings] == [
        "total_budget",
        "operation_budget",
    ]
    assert report.warnings[1].operation_name == "synthesis.generate_x_post"


def test_evaluate_model_budget_empty_state_has_zero_projection_and_no_warnings():
    db = SimpleNamespace(get_model_usage_summary=lambda since_days: [])

    report = evaluate_model_budget(
        db,
        days=7,
        monthly_budget=1.0,
        operation_budgets={"missing.operation": 1.0},
        today=date(2026, 4, 15),
    )

    assert report.total_spend == 0
    assert report.projected_monthly_spend == 0
    assert report.operation_spend == []
    assert report.warnings == []
    assert "No model usage found in last 7 days." in format_text_report(report)


def test_json_and_text_formatting_include_budget_details():
    db = SimpleNamespace(get_model_usage_summary=lambda since_days: _summary_rows())
    report = evaluate_model_budget(
        db,
        days=10,
        monthly_budget=5.0,
        today=date(2026, 4, 15),
    )

    payload = report.to_dict()
    text = format_text_report(report)

    assert payload["projected_monthly_spend"] == pytest.approx(6.0)
    assert payload["warnings"][0]["kind"] == "total_budget"
    assert "Model Budget Guard (last 10 days)" in text
    assert "Projected monthly model spend $6.0000 exceeds total budget $5.0000" in text
    assert "synthesis.generate_x_post" in text


def test_cli_json_output_and_exit_success_without_warning(capsys):
    cli = _load_cli_module()
    db = SimpleNamespace(get_model_usage_summary=lambda since_days: [])

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch.object(cli, "script_context", fake_script_context):
        exit_code = cli.main(
            ["--days", "7", "--monthly-budget", "10", "--format", "json"]
        )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["days"] == 7
    assert payload["warnings"] == []


def test_cli_fail_on_warning_returns_nonzero(capsys):
    cli = _load_cli_module()
    db = SimpleNamespace(get_model_usage_summary=lambda since_days: _summary_rows())

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch.object(cli, "script_context", fake_script_context):
        exit_code = cli.main(
            [
                "--days",
                "10",
                "--monthly-budget",
                "5",
                "--operation-budget",
                "synthesis.generate_x_post=2",
                "--fail-on-warning",
            ]
        )

    assert exit_code == 1
    output = capsys.readouterr().out
    assert "Warnings:" in output
    assert "synthesis.generate_x_post" in output
