"""Helpers for recording Anthropic model usage."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from output.api_rate_guard import record_snapshot

logger = logging.getLogger(__name__)


ANTHROPIC_PRICING_PER_MILLION = (
    ("opus", 15.0, 75.0),
    ("sonnet", 3.0, 15.0),
    ("haiku", 0.8, 4.0),
)


@dataclass
class ModelUsageBudgetCheck:
    exceeded: bool
    reason: str | None = None
    run_cost: float = 0.0
    daily_cost: float = 0.0


def estimate_anthropic_cost(
    model_name: str, input_tokens: int, output_tokens: int
) -> float:
    """Return estimated USD cost for common Anthropic model families."""
    lowered = (model_name or "").lower()
    for marker, input_rate, output_rate in ANTHROPIC_PRICING_PER_MILLION:
        if marker in lowered:
            return (input_tokens * input_rate + output_tokens * output_rate) / 1_000_000
    return 0.0


def _read_usage_value(usage: Any, name: str) -> Any:
    if isinstance(usage, dict):
        return usage.get(name)
    return getattr(usage, name, None)


def _coerce_token_count(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, float) and value.is_integer():
        return max(0, int(value))
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def anthropic_usage_tokens(response: Any) -> tuple[int, int] | None:
    """Extract input/output tokens from an Anthropic response if present."""
    usage = getattr(response, "usage", None)
    input_tokens = _coerce_token_count(_read_usage_value(usage, "input_tokens"))
    output_tokens = _coerce_token_count(_read_usage_value(usage, "output_tokens"))
    if input_tokens is None and output_tokens is None:
        return None
    return input_tokens or 0, output_tokens or 0


def record_anthropic_usage(
    db: Any,
    response: Any,
    *,
    model_name: str,
    operation_name: str,
    content_id: int | None = None,
    pipeline_run_id: int | None = None,
) -> int | None:
    """Record usage from an Anthropic response when both DB API and usage exist."""
    if db is None or not hasattr(db, "record_model_usage"):
        return None

    tokens = anthropic_usage_tokens(response)
    if tokens is None:
        return None

    input_tokens, output_tokens = tokens
    total_tokens = input_tokens + output_tokens
    estimated_cost = estimate_anthropic_cost(model_name, input_tokens, output_tokens)
    try:
        usage_id = db.record_model_usage(
            model_name=model_name,
            operation_name=operation_name,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
            estimated_cost=estimated_cost,
            content_id=content_id,
            pipeline_run_id=pipeline_run_id,
        )
        try:
            record_snapshot(
                db,
                "anthropic",
                headers=getattr(response, "headers", None),
                endpoint=operation_name,
            )
        except Exception as exc:
            logger.warning("Failed to record Anthropic rate-limit snapshot: %s", exc)
        return usage_id
    except Exception as exc:
        logger.warning("Failed to record model usage: %s", exc)
        return None


def _positive_budget(value: float | int | None) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        budget = float(value)
    except (TypeError, ValueError):
        return None
    return budget if budget > 0 else None


def evaluate_model_usage_budget(
    db: Any,
    *,
    run_started_at: str | datetime,
    max_estimated_cost_per_run: float | int | None = None,
    max_daily_estimated_cost: float | int | None = None,
    now: datetime | None = None,
) -> ModelUsageBudgetCheck:
    """Check configured model-cost budgets using persisted usage estimates."""
    run_budget = _positive_budget(max_estimated_cost_per_run)
    daily_budget = _positive_budget(max_daily_estimated_cost)
    if db is None or (run_budget is None and daily_budget is None):
        return ModelUsageBudgetCheck(exceeded=False)

    run_cost = 0.0
    daily_cost = 0.0
    reasons = []

    if run_budget is not None and hasattr(db, "get_model_usage_cost_since"):
        run_cost = float(db.get_model_usage_cost_since(run_started_at) or 0.0)
        if run_cost > run_budget:
            reasons.append(
                f"run estimated cost ${run_cost:.4f} exceeds max ${run_budget:.4f}"
            )

    if daily_budget is not None and hasattr(db, "get_model_usage_cost_for_utc_day"):
        daily_cost = float(
            db.get_model_usage_cost_for_utc_day(now or datetime.now(timezone.utc))
            or 0.0
        )
        if daily_cost > daily_budget:
            reasons.append(
                f"daily estimated cost ${daily_cost:.4f} exceeds max ${daily_budget:.4f}"
            )

    if not reasons:
        return ModelUsageBudgetCheck(
            exceeded=False,
            run_cost=run_cost,
            daily_cost=daily_cost,
        )
    return ModelUsageBudgetCheck(
        exceeded=True,
        reason="Model usage budget exceeded: " + "; ".join(reasons),
        run_cost=run_cost,
        daily_cost=daily_cost,
    )
