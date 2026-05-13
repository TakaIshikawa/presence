"""Summarize dry-run evaluation quality by source window."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LIMIT = 20
REQUIRED_COLUMNS = {
    "id",
    "content_type",
    "threshold",
    "source_window_hours",
    "prompt_count",
    "commit_count",
    "candidate_count",
    "final_score",
    "rejection_reason",
    "created_at",
}


def build_eval_source_window_performance_report(
    db_or_conn: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return eval result quality grouped by content type and source window."""

    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    lookback_start = generated_at - timedelta(days=lookback_days)
    filters = {
        "lookback_days": lookback_days,
        "limit": limit,
        "lookback_start": lookback_start.isoformat(),
        "lookback_end": generated_at.isoformat(),
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "eval_results" not in schema:
        return _empty_report(generated_at, filters, ["eval_results"], {})

    missing_required = sorted(REQUIRED_COLUMNS - schema["eval_results"])
    if missing_required:
        return _empty_report(
            generated_at,
            filters,
            [],
            {"eval_results": missing_required},
        )

    rows = _eval_rows(conn, lookback_start=lookback_start, lookback_end=generated_at)
    groups = _groups(rows)
    items = groups[:limit]
    return {
        "artifact_type": "eval_source_window_performance",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _totals(groups),
        "groups": groups,
        "items": items,
        "missing_tables": [],
        "missing_columns": {},
    }


def format_eval_source_window_performance_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_eval_source_window_performance_text(report: dict[str, Any]) -> str:
    """Render a compact operational summary."""

    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Eval Source Window Performance",
        f"Generated: {report['generated_at']}",
        f"Filters: lookback_days={filters['lookback_days']} limit={filters['limit']}",
        (
            f"Totals: runs={totals['run_count']} groups={totals['groups']} "
            f"passes={totals['pass_count']} pass_rate={_fmt_pct(totals['pass_rate'])} "
            f"avg_final_score={_fmt(totals['average_final_score'])}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        formatted = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report["missing_columns"].items())
        ]
        lines.append("Missing columns: " + "; ".join(formatted))
    if not report["groups"]:
        lines.append("No eval results matched the lookback window.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Source windows:")
    for item in report["items"]:
        reasons = ", ".join(
            f"{reason}={count}"
            for reason, count in item["rejection_reason_counts"].items()
        ) or "-"
        lines.append(
            f"- {item['content_type']} {item['source_window_hours']}h "
            f"runs={item['run_count']} pass_rate={_fmt_pct(item['pass_rate'])} "
            f"avg_score={_fmt(item['average_final_score'])} "
            f"prompts={_fmt(item['average_prompt_count'])} "
            f"commits={_fmt(item['average_commit_count'])} "
            f"candidates={_fmt(item['average_candidate_count'])} "
            f"latest={item['latest_created_at'] or '-'} reasons={reasons}"
        )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    try:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
    except sqlite3.Error:
        return {}
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in tables}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "artifact_type": "eval_source_window_performance",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "run_count": 0,
            "groups": 0,
            "pass_count": 0,
            "pass_rate": 0.0,
            "average_final_score": None,
        },
        "groups": [],
        "items": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _eval_rows(
    conn: sqlite3.Connection,
    *,
    lookback_start: datetime,
    lookback_end: datetime,
) -> list[dict[str, Any]]:
    query = (
        "SELECT id, content_type, threshold, source_window_hours, prompt_count, "
        "commit_count, candidate_count, final_score, rejection_reason, created_at "
        "FROM eval_results "
        "WHERE datetime(created_at) >= datetime(?) "
        "AND datetime(created_at) <= datetime(?) "
        "ORDER BY datetime(created_at) DESC, id ASC"
    )
    cursor = conn.execute(query, (lookback_start.isoformat(), lookback_end.isoformat()))
    names = [description[0] for description in cursor.description]
    return [_row_to_dict(row, names) for row in cursor.fetchall()]


def _groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for row in rows:
        key = (str(row.get("content_type") or "unknown"), int(row.get("source_window_hours") or 0))
        buckets.setdefault(key, []).append(row)

    groups = []
    for (content_type, source_window_hours), group_rows in buckets.items():
        pass_count = sum(1 for row in group_rows if _passed(row))
        groups.append(
            {
                "content_type": content_type,
                "source_window_hours": source_window_hours,
                "run_count": len(group_rows),
                "pass_count": pass_count,
                "pass_rate": round(pass_count / len(group_rows), 4) if group_rows else 0.0,
                "average_prompt_count": _average(row.get("prompt_count") for row in group_rows),
                "average_commit_count": _average(row.get("commit_count") for row in group_rows),
                "average_candidate_count": _average(row.get("candidate_count") for row in group_rows),
                "average_final_score": _average(row.get("final_score") for row in group_rows),
                "rejection_reason_counts": _rejection_reason_counts(group_rows),
                "representative_result_ids": [
                    int(row["id"])
                    for row in group_rows[:5]
                    if row.get("id") is not None
                ],
                "latest_created_at": group_rows[0].get("created_at") if group_rows else None,
            }
        )
    groups.sort(
        key=lambda group: (
            -group["pass_rate"],
            -(group["average_final_score"] or 0),
            -group["run_count"],
            str(group["content_type"]),
            group["source_window_hours"],
        )
    )
    return groups


def _totals(groups: list[dict[str, Any]]) -> dict[str, Any]:
    run_count = sum(int(group["run_count"]) for group in groups)
    pass_count = sum(int(group["pass_count"]) for group in groups)
    scores: list[float] = []
    for group in groups:
        average = group["average_final_score"]
        if average is not None:
            scores.extend([float(average)] * int(group["run_count"]))
    return {
        "run_count": run_count,
        "groups": len(groups),
        "pass_count": pass_count,
        "pass_rate": round(pass_count / run_count, 4) if run_count else 0.0,
        "average_final_score": _average(scores),
    }


def _passed(row: dict[str, Any]) -> bool:
    score = _float_or_none(row.get("final_score"))
    threshold = _float_or_none(row.get("threshold"))
    if score is None or threshold is None:
        return False
    return score >= threshold and not str(row.get("rejection_reason") or "").strip()


def _rejection_reason_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counter = Counter(
        str(row.get("rejection_reason")).strip()
        for row in rows
        if str(row.get("rejection_reason") or "").strip()
    )
    return {reason: counter[reason] for reason in sorted(counter)}


def _average(values: Any) -> float | None:
    numbers = [_float_or_none(value) for value in values]
    numbers = [value for value in numbers if value is not None]
    if not numbers:
        return None
    return round(sum(numbers) / len(numbers), 4)


def _row_to_dict(row: Any, names: list[str]) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    return dict(zip(names, row))


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


def _fmt_pct(value: Any) -> str:
    if value is None:
        return "-"
    return f"{float(value) * 100:.1f}%"
