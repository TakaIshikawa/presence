"""Tests for model usage helpers."""

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from model_usage import (
    anthropic_usage_tokens,
    estimate_anthropic_cost,
    evaluate_model_usage_budget,
    summarize_model_usage_budget,
)


def test_estimate_anthropic_cost_matches_family_rates():
    assert estimate_anthropic_cost("claude-sonnet-4-6", 1_000_000, 1_000_000) == pytest.approx(18.0)


def test_anthropic_usage_tokens_reads_object_usage():
    response = SimpleNamespace(
        usage=SimpleNamespace(input_tokens=100, output_tokens=25)
    )

    assert anthropic_usage_tokens(response) == (100, 25)


def test_evaluate_model_usage_budget_disabled_without_limits():
    check = evaluate_model_usage_budget(
        None,
        run_started_at=datetime.now(timezone.utc),
    )

    assert check.exceeded is False
    assert check.reason is None


def test_evaluate_model_usage_budget_reports_run_and_daily_excess():
    db = SimpleNamespace(
        get_model_usage_cost_since=lambda started_at: 0.35,
        get_model_usage_cost_for_utc_day=lambda now=None: 1.25,
    )

    check = evaluate_model_usage_budget(
        db,
        run_started_at=datetime.now(timezone.utc),
        max_estimated_cost_per_run=0.2,
        max_daily_estimated_cost=1.0,
    )

    assert check.exceeded is True
    assert check.run_cost == pytest.approx(0.35)
    assert check.daily_cost == pytest.approx(1.25)
    assert "run estimated cost $0.3500 exceeds max $0.2000" in check.reason
    assert "daily estimated cost $1.2500 exceeds max $1.0000" in check.reason


def test_summarize_model_usage_budget_missing_budget_is_not_exceeded():
    db = SimpleNamespace(get_model_usage_cost_for_utc_day=lambda now=None: 1.0)

    summary = summarize_model_usage_budget(
        db,
        period="daily",
        now=datetime(2026, 4, 25, 12, tzinfo=timezone.utc),
    )

    assert summary.spend == pytest.approx(1.0)
    assert summary.limit is None
    assert summary.remaining is None
    assert summary.exceeded is False


def test_summarize_model_usage_budget_boundary_equality_is_not_exceeded():
    db = SimpleNamespace(get_model_usage_cost_since=lambda started_at: 10.0)

    summary = summarize_model_usage_budget(
        db,
        period="monthly",
        monthly_limit=10.0,
        now=datetime(2026, 4, 25, 12, tzinfo=timezone.utc),
    )

    assert summary.spend == pytest.approx(10.0)
    assert summary.limit == pytest.approx(10.0)
    assert summary.remaining == pytest.approx(0.0)
    assert summary.exceeded is False


def test_summarize_model_usage_budget_reports_exceeded_budget():
    db = SimpleNamespace(get_model_usage_cost_for_utc_day=lambda now=None: 1.25)

    summary = summarize_model_usage_budget(
        db,
        period="daily",
        daily_limit=1.0,
        now=datetime(2026, 4, 25, 12, tzinfo=timezone.utc),
    )

    assert summary.spend == pytest.approx(1.25)
    assert summary.limit == pytest.approx(1.0)
    assert summary.remaining == pytest.approx(-0.25)
    assert summary.exceeded is True
