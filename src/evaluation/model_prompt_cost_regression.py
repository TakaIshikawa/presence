"""Summarize model prompt cost and token regressions."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 7
DEFAULT_BASELINE_DAYS = 21
DEFAULT_MIN_COST_INCREASE_PCT = 25.0


@dataclass(frozen=True)
class ModelPromptCostRegression:
    model: str
    prompt_version: str | None
    content_type: str | None
    recent_count: int
    baseline_count: int
    recent_cost_per_item: float
    baseline_cost_per_item: float
    cost_increase_pct: float
    recent_tokens_per_item: float
    baseline_tokens_per_item: float
    token_increase_pct: float
    example_content_ids: tuple[int, ...]
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["example_content_ids"] = list(self.example_content_ids)
        return data


@dataclass(frozen=True)
class ModelPromptCostRegressionReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    regressions: tuple[ModelPromptCostRegression, ...]
    empty_state: dict[str, Any]
    missing_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "model_prompt_cost_regression",
            "empty_state": dict(self.empty_state),
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "regressions": [item.to_dict() for item in self.regressions],
            "totals": dict(self.totals),
        }


def build_model_prompt_cost_regression_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    baseline_days: int = DEFAULT_BASELINE_DAYS,
    min_cost_increase_pct: float = DEFAULT_MIN_COST_INCREASE_PCT,
    model: str | None = None,
    now: datetime | None = None,
) -> ModelPromptCostRegressionReport:
    if days <= 0 or baseline_days <= 0:
        raise ValueError("days and baseline_days must be positive")
    if min_cost_increase_pct < 0:
        raise ValueError("min_cost_increase_pct must be non-negative")
    generated_at = _utc(now or datetime.now(timezone.utc))
    recent_start = generated_at - timedelta(days=days)
    baseline_start = recent_start - timedelta(days=baseline_days)
    filters = {
        "days": days,
        "baseline_days": baseline_days,
        "min_cost_increase_pct": min_cost_increase_pct,
        "model": model,
        "recent_start": recent_start.isoformat(),
        "baseline_start": baseline_start.isoformat(),
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "model_usage" not in schema:
        return _report(generated_at, filters, (), {}, ("model_usage",))

    rows = _load_rows(conn, schema, baseline_start.isoformat(), generated_at.isoformat(), model)
    groups: dict[tuple[str, str | None, str | None], dict[str, Any]] = {}
    totals = {"recent_cost": 0.0, "baseline_cost": 0.0, "recent_tokens": 0, "baseline_tokens": 0}
    for row in rows:
        created = _parse_ts(row.get("created_at"))
        if created is None:
            continue
        window = "recent" if created >= recent_start else "baseline"
        key = (row["model"], row.get("prompt_version"), row.get("content_type"))
        bucket = groups.setdefault(key, {"recent": [], "baseline": []})
        bucket[window].append(row)
        totals[f"{window}_cost"] += float(row.get("cost") or 0)
        totals[f"{window}_tokens"] += int(row.get("tokens") or 0)

    regressions: list[ModelPromptCostRegression] = []
    for (group_model, prompt_version, content_type), bucket in groups.items():
        recent = bucket["recent"]
        baseline = bucket["baseline"]
        if not recent or not baseline:
            continue
        recent_cost = sum(float(row.get("cost") or 0) for row in recent) / len(recent)
        baseline_cost = sum(float(row.get("cost") or 0) for row in baseline) / len(baseline)
        recent_tokens = sum(int(row.get("tokens") or 0) for row in recent) / len(recent)
        baseline_tokens = sum(int(row.get("tokens") or 0) for row in baseline) / len(baseline)
        cost_pct = _pct_change(recent_cost, baseline_cost)
        token_pct = _pct_change(recent_tokens, baseline_tokens)
        if cost_pct < min_cost_increase_pct:
            continue
        regressions.append(
            ModelPromptCostRegression(
                model=group_model,
                prompt_version=prompt_version,
                content_type=content_type,
                recent_count=len(recent),
                baseline_count=len(baseline),
                recent_cost_per_item=round(recent_cost, 8),
                baseline_cost_per_item=round(baseline_cost, 8),
                cost_increase_pct=round(cost_pct, 2),
                recent_tokens_per_item=round(recent_tokens, 2),
                baseline_tokens_per_item=round(baseline_tokens, 2),
                token_increase_pct=round(token_pct, 2),
                example_content_ids=tuple(_examples(recent)),
                recommended_action="Review prompt changes, examples, and max-token settings for this group.",
            )
        )
    regressions.sort(key=lambda item: (-item.cost_increase_pct, item.model, item.prompt_version or ""))
    return _report(generated_at, filters, tuple(regressions), totals, ())


def format_model_prompt_cost_regression_json(report: ModelPromptCostRegressionReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_model_prompt_cost_regression_text(report: ModelPromptCostRegressionReport) -> str:
    lines = [
        "Model Prompt Cost Regression",
        f"Recent: {report.filters['days']} days; baseline={report.filters['baseline_days']} days; model={report.filters.get('model') or 'all'}",
        f"Recent cost={report.totals.get('recent_cost', 0):.4f}; baseline cost={report.totals.get('baseline_cost', 0):.4f}; regressions={len(report.regressions)}",
        "",
    ]
    if not report.regressions:
        lines.append(report.empty_state["message"])
        return "\n".join(lines)
    for item in report.regressions:
        lines.append(
            f"- model={item.model} prompt={item.prompt_version or '-'} type={item.content_type or '-'} "
            f"cost/item {item.baseline_cost_per_item:.4f}->{item.recent_cost_per_item:.4f} (+{item.cost_increase_pct:.1f}%) "
            f"tokens/item {item.baseline_tokens_per_item:.1f}->{item.recent_tokens_per_item:.1f}"
        )
        lines.append(f"  examples={','.join(map(str, item.example_content_ids)) or '-'} action={item.recommended_action}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], start: str, end: str, model: str | None) -> list[dict[str, Any]]:
    mu = schema["model_usage"]
    model_col = _first(mu, ("model", "model_name")) or "model_name"
    prompt_col = _first(mu, ("prompt_version", "prompt_id", "operation_name"))
    content_id_col = _first(mu, ("content_id",))
    created_col = _first(mu, ("created_at", "timestamp", "started_at"))
    cost_col = _first(mu, ("estimated_cost", "cost", "total_cost"))
    tokens_col = _first(mu, ("total_tokens", "tokens"))
    if not created_col or not cost_col or not tokens_col or model_col not in mu:
        return []
    joins = ""
    content_type_expr = "NULL"
    if content_id_col and "generated_content" in schema and "id" in schema["generated_content"]:
        gc_cols = schema["generated_content"]
        type_col = _first(gc_cols, ("content_type", "type", "format"))
        if type_col:
            joins = f" LEFT JOIN generated_content gc ON gc.id = mu.{content_id_col}"
            content_type_expr = f"gc.{type_col}"
    where = [f"mu.{created_col} >= ?", f"mu.{created_col} < ?"]
    params: list[Any] = [start, end]
    if model:
        where.append(f"mu.{model_col} = ?")
        params.append(model)
    sql = f"""SELECT mu.{model_col} AS model,
                     {f'mu.{prompt_col}' if prompt_col else 'NULL'} AS prompt_version,
                     {content_type_expr} AS content_type,
                     {f'mu.{content_id_col}' if content_id_col else 'NULL'} AS content_id,
                     mu.{created_col} AS created_at,
                     mu.{cost_col} AS cost,
                     mu.{tokens_col} AS tokens
              FROM model_usage mu{joins}
              WHERE {' AND '.join(where)}"""
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _report(generated_at: datetime, filters: dict[str, Any], regressions: tuple[ModelPromptCostRegression, ...], totals: dict[str, Any], missing: tuple[str, ...]) -> ModelPromptCostRegressionReport:
    return ModelPromptCostRegressionReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "recent_cost": round(float(totals.get("recent_cost", 0)), 8),
            "baseline_cost": round(float(totals.get("baseline_cost", 0)), 8),
            "recent_tokens": int(totals.get("recent_tokens", 0)),
            "baseline_tokens": int(totals.get("baseline_tokens", 0)),
            "regression_count": len(regressions),
        },
        regressions=regressions,
        empty_state={"is_empty": not regressions, "message": "No model prompt cost regressions found." if not missing else "Model usage schema is unavailable."},
        missing_tables=missing,
    )


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _first(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _pct_change(recent: float, baseline: float) -> float:
    if baseline <= 0:
        return 100.0 if recent > 0 else 0.0
    return ((recent - baseline) / baseline) * 100


def _examples(rows: list[dict[str, Any]]) -> list[int]:
    ids = []
    for row in rows:
        try:
            content_id = int(row.get("content_id"))
        except (TypeError, ValueError):
            continue
        if content_id not in ids:
            ids.append(content_id)
    return ids[:5]
