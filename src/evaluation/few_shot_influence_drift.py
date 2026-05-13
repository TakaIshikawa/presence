"""Connect few-shot example usage to downstream outcomes."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_USES = 3
DEFAULT_MIN_UNDERPERFORMANCE_PCT = 20.0


@dataclass(frozen=True)
class FewShotInfluenceIssue:
    example_id: str
    example_preview: str | None
    usage_count: int
    average_outcome_score: float
    baseline_score: float
    underperformance_pct: float
    issue_type: str
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FewShotInfluenceDriftReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    issues: tuple[FewShotInfluenceIssue, ...]
    empty_state: dict[str, Any]
    missing_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "few_shot_influence_drift",
            "empty_state": dict(self.empty_state),
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "issues": [issue.to_dict() for issue in self.issues],
            "missing_tables": list(self.missing_tables),
            "totals": dict(self.totals),
        }


def build_few_shot_influence_drift_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_uses: int = DEFAULT_MIN_USES,
    min_underperformance_pct: float = DEFAULT_MIN_UNDERPERFORMANCE_PCT,
    content_type: str | None = None,
    now: datetime | None = None,
) -> FewShotInfluenceDriftReport:
    if days <= 0 or min_uses <= 0:
        raise ValueError("days and min_uses must be positive")
    if min_underperformance_pct < 0:
        raise ValueError("min_underperformance_pct must be non-negative")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "min_uses": min_uses, "min_underperformance_pct": min_underperformance_pct, "content_type": content_type}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    usage_table = _usage_table(schema)
    if usage_table is None or "generated_content" not in schema:
        missing = tuple(name for name in ("few_shot_usages", "generated_content") if name not in schema)
        return _report(generated_at, filters, (), 0, missing or ("few_shot_usages",))
    rows = _load_rows(conn, schema, usage_table, cutoff.isoformat(), content_type)
    if not rows:
        return _report(generated_at, filters, (), 0, ())
    baseline = sum(row["score"] for row in rows) / len(rows)
    by_example: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_example.setdefault(str(row["example_id"]), []).append(row)
    issues: list[FewShotInfluenceIssue] = []
    for example_id, group in by_example.items():
        if len(group) < min_uses:
            continue
        avg = sum(row["score"] for row in group) / len(group)
        under = _under_pct(avg, baseline)
        if under < min_underperformance_pct:
            continue
        issues.append(
            FewShotInfluenceIssue(
                example_id=example_id,
                example_preview=_preview(group[0].get("example_text")),
                usage_count=len(group),
                average_outcome_score=round(avg, 4),
                baseline_score=round(baseline, 4),
                underperformance_pct=round(under, 2),
                issue_type="overused_low_performing_example",
                recommended_action="Retire, rewrite, or down-rank this example until its downstream outcomes recover.",
            )
        )
    issues.sort(key=lambda item: (-item.underperformance_pct, item.example_id))
    return _report(generated_at, filters, tuple(issues), len(rows), ())


def format_few_shot_influence_drift_json(report: FewShotInfluenceDriftReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_few_shot_influence_drift_text(report: FewShotInfluenceDriftReport) -> str:
    lines = [
        "Few-Shot Influence Drift",
        f"Window={report.filters['days']} days; min_uses={report.filters['min_uses']}; content_type={report.filters.get('content_type') or 'all'}",
        f"Usages scanned={report.totals['usage_count']}; issues={report.totals['issue_count']}",
        "",
    ]
    if not report.issues:
        lines.append(report.empty_state["message"])
        return "\n".join(lines)
    for issue in report.issues:
        lines.append(f"- example={issue.example_id} uses={issue.usage_count} score={issue.average_outcome_score:.3f} baseline={issue.baseline_score:.3f} under={issue.underperformance_pct:.1f}%")
        lines.append(f"  preview={issue.example_preview or '-'} action={issue.recommended_action}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], table: str, cutoff: str, content_type: str | None) -> list[dict[str, Any]]:
    ucols = schema[table]
    gcols = schema["generated_content"]
    example_col = _first(ucols, ("example_id", "few_shot_id", "example_key"))
    content_col = _first(ucols, ("content_id", "generated_content_id"))
    text_col = _first(ucols, ("example_text", "text", "prompt_example"))
    used_col = _first(ucols, ("used_at", "created_at"))
    if not example_col or not content_col:
        return []
    created_col = _first(gcols, ("created_at", "generated_at"))
    type_col = _first(gcols, ("content_type", "type", "format"))
    eval_col = _first(gcols, ("eval_score", "quality_score", "score"))
    where = []
    params: list[Any] = []
    if used_col:
        where.append(f"u.{used_col} >= ?")
        params.append(cutoff)
    elif created_col:
        where.append(f"gc.{created_col} >= ?")
        params.append(cutoff)
    if content_type and type_col:
        where.append(f"gc.{type_col} = ?")
        params.append(content_type)
    engagement = ""
    score_expr = f"COALESCE(gc.{eval_col}, 0)" if eval_col else "0"
    if "post_engagement" in schema and "content_id" in schema["post_engagement"]:
        ecol = _first(schema["post_engagement"], ("engagement_score", "score", "click_rate"))
        if ecol:
            engagement = " LEFT JOIN post_engagement pe ON pe.content_id = gc.id"
            score_expr = f"COALESCE(pe.{ecol}, gc.{eval_col if eval_col else 'id'} * 0)"
    sql = f"""SELECT u.{example_col} AS example_id,
                     {f'u.{text_col}' if text_col else 'NULL'} AS example_text,
                     {score_expr} AS score
              FROM {table} u
              JOIN generated_content gc ON gc.id = u.{content_col}
              {engagement}
              {('WHERE ' + ' AND '.join(where)) if where else ''}"""
    return [{"example_id": row["example_id"], "example_text": row["example_text"], "score": float(row["score"] or 0)} for row in conn.execute(sql, params).fetchall()]


def _usage_table(schema: dict[str, set[str]]) -> str | None:
    for name in ("few_shot_usages", "few_shot_usage", "prompt_example_usages"):
        if name in schema:
            return name
    return None


def _report(generated_at: datetime, filters: dict[str, Any], issues: tuple[FewShotInfluenceIssue, ...], scanned: int, missing: tuple[str, ...]) -> FewShotInfluenceDriftReport:
    return FewShotInfluenceDriftReport(generated_at.isoformat(), filters, {"usage_count": scanned, "issue_count": len(issues)}, issues, {"is_empty": not issues, "message": "No few-shot influence drift found." if not missing else "Few-shot usage schema is unavailable."}, missing)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _first(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


def _under_pct(avg: float, baseline: float) -> float:
    return 0.0 if baseline <= 0 else max(0.0, ((baseline - avg) / baseline) * 100)


def _preview(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text[:120] or None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
