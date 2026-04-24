"""Budget guard for recorded model usage spend."""

from __future__ import annotations

import calendar
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any


@dataclass(frozen=True)
class BudgetWarning:
    """A model spend budget breach."""

    kind: str
    message: str
    budget: float
    projected_monthly_spend: float
    operation_name: str | None = None


@dataclass(frozen=True)
class ModelBudgetReport:
    """Aggregated model spend and budget warnings."""

    days: int
    total_spend: float
    daily_average: float
    projected_monthly_spend: float
    operation_spend: list[dict[str, Any]]
    model_spend: list[dict[str, Any]]
    warnings: list[BudgetWarning]

    def to_dict(self) -> dict[str, Any]:
        return {
            "days": self.days,
            "total_spend": round(self.total_spend, 6),
            "daily_average": round(self.daily_average, 6),
            "projected_monthly_spend": round(self.projected_monthly_spend, 6),
            "operation_spend": self.operation_spend,
            "model_spend": self.model_spend,
            "warnings": [asdict(warning) for warning in self.warnings],
        }


def _money(value: float) -> str:
    return f"${value:.4f}"


def _coerce_positive_days(days: int) -> int:
    try:
        coerced = int(days)
    except (TypeError, ValueError):
        raise ValueError("days must be an integer") from None
    if coerced < 1:
        raise ValueError("days must be at least 1")
    return coerced


def _month_days(today: date | datetime | None = None) -> int:
    if today is None:
        today = datetime.now(timezone.utc).date()
    if isinstance(today, datetime):
        today = today.date()
    return calendar.monthrange(today.year, today.month)[1]


def project_monthly_spend(
    total_spend: float,
    days: int,
    *,
    today: date | datetime | None = None,
) -> float:
    """Project monthly spend from the recent daily average."""
    days = _coerce_positive_days(days)
    return (float(total_spend or 0.0) / days) * _month_days(today)


def summarize_model_spend(rows: list[dict[str, Any]]) -> tuple[list[dict], list[dict]]:
    """Summarize model usage rows by operation and model."""
    operations: dict[str, dict[str, Any]] = {}
    models: dict[tuple[str, str], dict[str, Any]] = {}

    for row in rows:
        operation_name = str(row.get("operation_name") or "")
        model_name = str(row.get("model_name") or "")
        call_count = int(row.get("call_count") or 0)
        input_tokens = int(row.get("input_tokens") or 0)
        output_tokens = int(row.get("output_tokens") or 0)
        total_tokens = int(row.get("total_tokens") or 0)
        estimated_cost = float(row.get("estimated_cost") or 0.0)

        operation = operations.setdefault(
            operation_name,
            {
                "operation_name": operation_name,
                "call_count": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "estimated_cost": 0.0,
            },
        )
        model = models.setdefault(
            (operation_name, model_name),
            {
                "operation_name": operation_name,
                "model_name": model_name,
                "call_count": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "estimated_cost": 0.0,
            },
        )

        for target in (operation, model):
            target["call_count"] += call_count
            target["input_tokens"] += input_tokens
            target["output_tokens"] += output_tokens
            target["total_tokens"] += total_tokens
            target["estimated_cost"] += estimated_cost

    operation_spend = sorted(
        (_rounded_spend(row) for row in operations.values()),
        key=lambda row: (-row["estimated_cost"], row["operation_name"]),
    )
    model_spend = sorted(
        (_rounded_spend(row) for row in models.values()),
        key=lambda row: (
            -row["estimated_cost"],
            row["operation_name"],
            row["model_name"],
        ),
    )
    return operation_spend, model_spend


def _rounded_spend(row: dict[str, Any]) -> dict[str, Any]:
    rounded = dict(row)
    rounded["estimated_cost"] = round(float(rounded["estimated_cost"]), 6)
    return rounded


def evaluate_model_budget(
    db: Any,
    *,
    days: int = 30,
    monthly_budget: float | None = None,
    operation_budgets: dict[str, float] | None = None,
    today: date | datetime | None = None,
) -> ModelBudgetReport:
    """Evaluate recent model usage against projected monthly spend budgets."""
    days = _coerce_positive_days(days)
    rows = db.get_model_usage_summary(since_days=days)
    operation_spend, model_spend = summarize_model_spend(rows)
    total_spend = sum(float(row["estimated_cost"]) for row in operation_spend)
    projected = project_monthly_spend(total_spend, days, today=today)
    daily_average = total_spend / days

    warnings: list[BudgetWarning] = []
    if monthly_budget is not None and projected > float(monthly_budget):
        warnings.append(
            BudgetWarning(
                kind="total_budget",
                message=(
                    "Projected monthly model spend "
                    f"{_money(projected)} exceeds total budget "
                    f"{_money(float(monthly_budget))}"
                ),
                budget=float(monthly_budget),
                projected_monthly_spend=round(projected, 6),
            )
        )

    operation_lookup = {
        row["operation_name"]: float(row["estimated_cost"]) for row in operation_spend
    }
    for operation_name, budget in sorted((operation_budgets or {}).items()):
        operation_total = operation_lookup.get(operation_name, 0.0)
        operation_projected = project_monthly_spend(
            operation_total,
            days,
            today=today,
        )
        if operation_projected > float(budget):
            warnings.append(
                BudgetWarning(
                    kind="operation_budget",
                    operation_name=operation_name,
                    message=(
                        f"Projected monthly spend for {operation_name} "
                        f"{_money(operation_projected)} exceeds operation budget "
                        f"{_money(float(budget))}"
                    ),
                    budget=float(budget),
                    projected_monthly_spend=round(operation_projected, 6),
                )
            )

    return ModelBudgetReport(
        days=days,
        total_spend=total_spend,
        daily_average=daily_average,
        projected_monthly_spend=projected,
        operation_spend=operation_spend,
        model_spend=model_spend,
        warnings=warnings,
    )


def format_text_report(report: ModelBudgetReport) -> str:
    """Format a budget guard report for console output."""
    lines = [
        "",
        "=" * 78,
        f"Model Budget Guard (last {report.days} days)",
        "=" * 78,
        "",
        f"Total spend:             {_money(report.total_spend)}",
        f"Recent daily average:    {_money(report.daily_average)}",
        f"Projected monthly spend: {_money(report.projected_monthly_spend)}",
        "",
    ]

    if report.warnings:
        lines.append("Warnings:")
        lines.extend(f"- {warning.message}" for warning in report.warnings)
    else:
        lines.append("Warnings: none")

    if report.operation_spend:
        lines.extend(
            [
                "",
                f"{'Operation':44s} {'Calls':>5s} {'Tokens':>8s} {'Cost':>9s}",
                f"{'-' * 44:44s} {'-' * 5:>5s} {'-' * 8:>8s} {'-' * 9:>9s}",
            ]
        )
        for row in report.operation_spend:
            operation = str(row["operation_name"])[:44]
            lines.append(
                f"{operation:44s} "
                f"{int(row['call_count']):5d} "
                f"{int(row['total_tokens']):8d} "
                f"{_money(float(row['estimated_cost'])):>9s}"
            )
    else:
        lines.extend(["", f"No model usage found in last {report.days} days."])

    lines.extend(["", "=" * 78, ""])
    return "\n".join(lines)
