"""Tests for the model budget guard CLI."""

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from model_budget import format_json_summary, format_text_summary, main
from model_usage import ModelUsageBudgetSummary


def _config(daily=None, monthly=None):
    return SimpleNamespace(
        model_usage=SimpleNamespace(
            max_daily_estimated_cost=daily,
            max_monthly_estimated_cost=monthly,
        )
    )


def test_json_summary_includes_budget_fields():
    summary = ModelUsageBudgetSummary(
        period="daily",
        spend=1.25,
        limit=1.0,
        remaining=-0.25,
        exceeded=True,
        start_at="2026-04-25T00:00:00+00:00",
        end_at="2026-04-26T00:00:00+00:00",
    )

    payload = json.loads(format_json_summary(summary))

    assert payload["spend"] == 1.25
    assert payload["limit"] == 1.0
    assert payload["remaining"] == -0.25
    assert payload["exceeded"] is True


def test_text_summary_reports_missing_budget():
    summary = ModelUsageBudgetSummary(
        period="monthly",
        spend=0.5,
        limit=None,
        remaining=None,
        exceeded=False,
        start_at="2026-04-01T00:00:00+00:00",
        end_at="2026-05-01T00:00:00+00:00",
    )

    text = format_text_summary(summary)

    assert "Spend:     $0.5000" in text
    assert "Limit:     not configured" in text
    assert "Remaining: not configured" in text
    assert "Status:    within budget" in text


def test_main_exits_nonzero_with_fail_on_exceeded(db, capsys):
    db.record_model_usage(
        "claude-sonnet-4-6",
        "synthesis.generate_x_post",
        100,
        25,
        estimated_cost=1.25,
    )

    @contextmanager
    def fake_script_context():
        yield _config(daily=1.0), db

    with patch("model_budget.script_context", fake_script_context), patch(
        "sys.argv", ["model_budget.py", "--json", "--fail-on-exceeded"]
    ):
        with pytest.raises(SystemExit) as exc:
            main()

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["period"] == "daily"
    assert payload["exceeded"] is True


def test_main_allows_boundary_equality_with_fail_on_exceeded(db, capsys):
    db.record_model_usage(
        "claude-sonnet-4-6",
        "synthesis.generate_x_post",
        100,
        25,
        estimated_cost=1.0,
    )

    @contextmanager
    def fake_script_context():
        yield _config(daily=1.0), db

    with patch("model_budget.script_context", fake_script_context), patch(
        "sys.argv", ["model_budget.py", "--json", "--fail-on-exceeded"]
    ):
        main()

    payload = json.loads(capsys.readouterr().out)
    assert payload["spend"] == pytest.approx(1.0)
    assert payload["remaining"] == pytest.approx(0.0)
    assert payload["exceeded"] is False
