"""Helpers for recording Anthropic model usage."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


ANTHROPIC_PRICING_PER_MILLION = (
    ("opus", 15.0, 75.0),
    ("sonnet", 3.0, 15.0),
    ("haiku", 0.8, 4.0),
)


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
        return db.record_model_usage(
            model_name=model_name,
            operation_name=operation_name,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
            estimated_cost=estimated_cost,
            content_id=content_id,
            pipeline_run_id=pipeline_run_id,
        )
    except Exception as exc:
        logger.warning("Failed to record model usage: %s", exc)
        return None
