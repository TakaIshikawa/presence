"""Forecast model usage cost against configured synthesis budgets."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

KNOWN_CONTENT_TYPES = {
    "x_post",
    "x_thread",
    "x_long_post",
    "x_visual",
    "blog_post",
    "blog_seed",
}

NEAR_LIMIT_RATIO = 0.8


@dataclass(frozen=True)
class OperationCostAverage:
    content_type: str
    operation_name: str
    call_count: int
    total_cost: float
    average_call_cost: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "content_type": self.content_type,
            "operation_name": self.operation_name,
            "call_count": self.call_count,
            "total_cost": round(self.total_cost, 6),
            "average_call_cost": round(self.average_call_cost, 6),
        }


@dataclass(frozen=True)
class ContentTypeCostForecast:
    content_type: str
    recent_run_count: int
    average_run_cost: float
    safe_run_count_today: int | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "content_type": self.content_type,
            "recent_run_count": self.recent_run_count,
            "average_run_cost": round(self.average_run_cost, 6),
            "safe_run_count_today": self.safe_run_count_today,
            "status": self.status,
        }


@dataclass(frozen=True)
class CostForecast:
    generated_at: str
    lookback_days: int
    today_spend: float
    daily_budget: float | None
    per_run_budget: float | None
    remaining_daily_budget: float | None
    average_recent_run_cost: float
    safe_run_count_today: int | None
    status: str
    budget_configured: bool
    message: str
    content_types: list[ContentTypeCostForecast] = field(default_factory=list)
    operations: list[OperationCostAverage] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "lookback_days": self.lookback_days,
            "status": self.status,
            "budget_configured": self.budget_configured,
            "message": self.message,
            "today_spend": round(self.today_spend, 6),
            "daily_budget": self.daily_budget,
            "per_run_budget": self.per_run_budget,
            "remaining_daily_budget": (
                round(self.remaining_daily_budget, 6)
                if self.remaining_daily_budget is not None
                else None
            ),
            "average_recent_run_cost": round(self.average_recent_run_cost, 6),
            "safe_run_count_today": self.safe_run_count_today,
            "content_types": [item.to_dict() for item in self.content_types],
            "operations": [item.to_dict() for item in self.operations],
        }


def positive_budget(value: float | int | None) -> float | None:
    """Return a positive budget value, or None when disabled."""
    if value is None or isinstance(value, bool):
        return None
    try:
        budget = float(value)
    except (TypeError, ValueError):
        return None
    return budget if budget > 0 else None


def fetch_model_usage_rows(db: Any, *, lookback_days: int = 30) -> list[dict[str, Any]]:
    """Read recent model usage rows with best-effort content type context."""
    lookback_days = max(1, int(lookback_days or 1))
    cursor = db.conn.execute(
        """SELECT mu.id,
                  mu.operation_name,
                  mu.estimated_cost,
                  mu.pipeline_run_id,
                  mu.created_at,
                  pr.content_type AS pipeline_content_type,
                  gc.content_type AS content_content_type
           FROM model_usage mu
           LEFT JOIN pipeline_runs pr ON pr.id = mu.pipeline_run_id
           LEFT JOIN generated_content gc ON gc.id = mu.content_id
           WHERE mu.created_at >= datetime('now', ?)
           ORDER BY mu.created_at DESC, mu.id DESC""",
        (f"-{lookback_days} days",),
    )
    return [dict(row) for row in cursor.fetchall()]


def today_model_usage_cost(db: Any, *, now: datetime | None = None) -> float:
    """Read today's UTC model usage cost without modifying the database."""
    now = now or datetime.now(timezone.utc)
    if hasattr(db, "get_model_usage_cost_for_utc_day"):
        return float(db.get_model_usage_cost_for_utc_day(now) or 0.0)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    cursor = db.conn.execute(
        """SELECT COALESCE(SUM(estimated_cost), 0)
           FROM model_usage
           WHERE created_at >= ? AND created_at < ?""",
        (start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")),
    )
    return float(cursor.fetchone()[0] or 0.0)


def build_cost_forecast(
    rows: list[dict[str, Any]],
    *,
    today_spend: float,
    max_estimated_cost_per_run: float | int | None = None,
    max_daily_estimated_cost: float | int | None = None,
    lookback_days: int = 30,
    now: datetime | None = None,
) -> CostForecast:
    """Build a cost forecast from recent model usage rows."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    generated_at = now.astimezone(timezone.utc).isoformat()
    run_budget = positive_budget(max_estimated_cost_per_run)
    daily_budget = positive_budget(max_daily_estimated_cost)
    budget_configured = run_budget is not None or daily_budget is not None

    operations = _operation_averages(rows)
    content_types = _content_type_forecasts(
        rows,
        today_spend=float(today_spend or 0.0),
        daily_budget=daily_budget,
        run_budget=run_budget,
    )
    average_run_cost = _average_recent_run_cost(rows)
    remaining_daily_budget = (
        max(0.0, daily_budget - float(today_spend or 0.0))
        if daily_budget is not None
        else None
    )
    safe_run_count = _safe_run_count(remaining_daily_budget, average_run_cost)

    if not budget_configured:
        status = "ok"
        message = (
            "No synthesis model cost budget is configured; forecast is unlimited "
            "and based on historical usage only."
        )
    else:
        status = _budget_status(
            today_spend=float(today_spend or 0.0),
            daily_budget=daily_budget,
            run_budget=run_budget,
            expected_run_cost=average_run_cost,
        )
        if status == "over_limit":
            message = (
                "Configured model cost budget is already exceeded or the "
                "expected run cost exceeds the per-run cap."
            )
        elif status == "near_limit":
            message = "Configured model cost budget is near its limit for today."
        else:
            message = "Configured model cost budget has remaining capacity today."

    return CostForecast(
        generated_at=generated_at,
        lookback_days=max(1, int(lookback_days or 1)),
        today_spend=float(today_spend or 0.0),
        daily_budget=daily_budget,
        per_run_budget=run_budget,
        remaining_daily_budget=remaining_daily_budget,
        average_recent_run_cost=average_run_cost,
        safe_run_count_today=safe_run_count,
        status=status,
        budget_configured=budget_configured,
        message=message,
        content_types=content_types,
        operations=operations,
    )


def forecast_from_db(
    db: Any,
    *,
    max_estimated_cost_per_run: float | int | None = None,
    max_daily_estimated_cost: float | int | None = None,
    lookback_days: int = 30,
    now: datetime | None = None,
) -> CostForecast:
    """Read usage history and produce a forecast."""
    now = now or datetime.now(timezone.utc)
    rows = fetch_model_usage_rows(db, lookback_days=lookback_days)
    today_spend = today_model_usage_cost(db, now=now)
    return build_cost_forecast(
        rows,
        today_spend=today_spend,
        max_estimated_cost_per_run=max_estimated_cost_per_run,
        max_daily_estimated_cost=max_daily_estimated_cost,
        lookback_days=lookback_days,
        now=now,
    )


def _row_content_type(row: dict[str, Any]) -> str:
    for key in ("content_type", "pipeline_content_type", "content_content_type"):
        value = row.get(key)
        if value:
            return str(value)
    return _infer_content_type(str(row.get("operation_name") or ""))


def _infer_content_type(operation_name: str) -> str:
    parts = [part for part in operation_name.split(".") if part]
    for part in reversed(parts):
        if part in KNOWN_CONTENT_TYPES:
            return part
    for part in reversed(parts):
        if part.startswith(("x_", "blog_")):
            return part
    return "unknown"


def _operation_averages(rows: list[dict[str, Any]]) -> list[OperationCostAverage]:
    grouped: dict[tuple[str, str], list[float]] = {}
    for row in rows:
        key = (_row_content_type(row), str(row.get("operation_name") or "unknown"))
        grouped.setdefault(key, []).append(float(row.get("estimated_cost") or 0.0))

    averages = [
        OperationCostAverage(
            content_type=content_type,
            operation_name=operation_name,
            call_count=len(costs),
            total_cost=sum(costs),
            average_call_cost=sum(costs) / len(costs) if costs else 0.0,
        )
        for (content_type, operation_name), costs in grouped.items()
    ]
    return sorted(averages, key=lambda item: (item.content_type, item.operation_name))


def _pipeline_run_costs(rows: list[dict[str, Any]]) -> dict[str, list[float]]:
    by_run: dict[tuple[str, Any], float] = {}
    fallback_by_content: dict[str, list[float]] = {}

    for row in rows:
        content_type = _row_content_type(row)
        cost = float(row.get("estimated_cost") or 0.0)
        pipeline_run_id = row.get("pipeline_run_id")
        if pipeline_run_id is None:
            fallback_by_content.setdefault(content_type, []).append(cost)
            continue
        by_run[(content_type, pipeline_run_id)] = (
            by_run.get((content_type, pipeline_run_id), 0.0) + cost
        )

    costs_by_content: dict[str, list[float]] = {}
    for (content_type, _pipeline_run_id), cost in by_run.items():
        costs_by_content.setdefault(content_type, []).append(cost)

    for content_type, costs in fallback_by_content.items():
        costs_by_content.setdefault(content_type, []).extend(costs)
    return costs_by_content


def _content_type_forecasts(
    rows: list[dict[str, Any]],
    *,
    today_spend: float,
    daily_budget: float | None,
    run_budget: float | None,
) -> list[ContentTypeCostForecast]:
    forecasts = []
    remaining = (
        max(0.0, daily_budget - today_spend)
        if daily_budget is not None
        else None
    )
    for content_type, run_costs in _pipeline_run_costs(rows).items():
        average = sum(run_costs) / len(run_costs) if run_costs else 0.0
        forecasts.append(
            ContentTypeCostForecast(
                content_type=content_type,
                recent_run_count=len(run_costs),
                average_run_cost=average,
                safe_run_count_today=_safe_run_count(remaining, average),
                status=_budget_status(
                    today_spend=today_spend,
                    daily_budget=daily_budget,
                    run_budget=run_budget,
                    expected_run_cost=average,
                ),
            )
        )
    return sorted(forecasts, key=lambda item: item.content_type)


def _average_recent_run_cost(rows: list[dict[str, Any]]) -> float:
    costs = [
        cost
        for run_costs in _pipeline_run_costs(rows).values()
        for cost in run_costs
    ]
    return sum(costs) / len(costs) if costs else 0.0


def _safe_run_count(
    remaining_daily_budget: float | None, expected_run_cost: float
) -> int | None:
    if remaining_daily_budget is None:
        return None
    if remaining_daily_budget <= 0:
        return 0
    if expected_run_cost <= 0:
        return None
    return max(0, math.floor(remaining_daily_budget / expected_run_cost))


def _budget_status(
    *,
    today_spend: float,
    daily_budget: float | None,
    run_budget: float | None,
    expected_run_cost: float,
) -> str:
    if daily_budget is not None and today_spend >= daily_budget:
        return "over_limit"
    if run_budget is not None and expected_run_cost > run_budget:
        return "over_limit"
    if daily_budget is not None and today_spend >= daily_budget * NEAR_LIMIT_RATIO:
        return "near_limit"
    if (
        daily_budget is not None
        and expected_run_cost > 0
        and today_spend + expected_run_cost >= daily_budget * NEAR_LIMIT_RATIO
    ):
        return "near_limit"
    if (
        run_budget is not None
        and expected_run_cost > 0
        and expected_run_cost >= run_budget * NEAR_LIMIT_RATIO
    ):
        return "near_limit"
    return "ok"
