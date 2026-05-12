"""Summarize pipeline run outcomes over rolling windows."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_WINDOW_DAYS = 30
DEFAULT_LIMIT = 20
REQUIRED_COLUMNS = {"batch_id", "content_type", "outcome", "created_at"}
OPTIONAL_COLUMNS = {
    "rejection_reason",
    "final_score",
    "refinement_picked",
    "best_score_before_refine",
    "best_score_after_refine",
}


def build_pipeline_run_outcome_windows_report(
    db_or_conn: Any,
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return grouped pipeline outcomes and representative recent runs."""

    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    window_start = generated_at - timedelta(days=window_days)
    filters = {
        "window_days": window_days,
        "limit": limit,
        "window_start": window_start.isoformat(),
        "window_end": generated_at.isoformat(),
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "pipeline_runs" not in schema:
        return _empty_report(generated_at, filters, ["pipeline_runs"], {}, [])

    pipeline_columns = schema["pipeline_runs"]
    missing_required = sorted(REQUIRED_COLUMNS - pipeline_columns)
    if missing_required:
        return _empty_report(
            generated_at,
            filters,
            [],
            {"pipeline_runs": missing_required},
            [],
        )
    missing_optional = sorted(OPTIONAL_COLUMNS - pipeline_columns)

    rows = _pipeline_rows(
        conn,
        pipeline_columns,
        window_start=window_start,
        window_end=generated_at,
    )
    items = [_item_from_row(row) for row in rows]
    groups = _group_items(items)

    return {
        "artifact_type": "pipeline_run_outcome_windows",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _totals(items),
        "groups": groups,
        "items": items[:limit],
        "missing_tables": [],
        "missing_columns": {},
        "missing_optional_columns": {"pipeline_runs": missing_optional}
        if missing_optional
        else {},
    }


def format_pipeline_run_outcome_windows_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_pipeline_run_outcome_windows_text(report: dict[str, Any]) -> str:
    """Render a compact operational summary."""

    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Pipeline Run Outcome Windows",
        f"Generated: {report['generated_at']}",
        f"Filters: window_days={filters['window_days']} limit={filters['limit']}",
        (
            f"Totals: runs={totals['runs']} content_types={totals['content_types']} "
            f"outcomes={totals['outcomes']} avg_final_score={_fmt(totals['average_final_score'])} "
            f"avg_refinement_delta={_fmt(totals['average_refinement_delta'])}"
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
    if report.get("missing_optional_columns"):
        formatted = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report["missing_optional_columns"].items())
        ]
        lines.append("Missing optional columns: " + "; ".join(formatted))
    if not report["groups"]:
        lines.append("No pipeline runs matched the window.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Outcome groups:")
    for group in report["groups"]:
        lines.append(
            f"- {group['content_type']} outcome={group['outcome']} "
            f"rejection={group['rejection_reason'] or '-'} "
            f"refinement={group['refinement_picked'] or '-'} count={group['count']} "
            f"avg_final_score={_fmt(group['average_final_score'])} "
            f"avg_refinement_delta={_fmt(group['average_refinement_delta'])}"
        )

    if report["items"]:
        lines.append("")
        lines.append("Recent runs:")
        for item in report["items"]:
            lines.append(
                f"- {item['batch_id']} {item['content_type']} outcome={item['outcome']} "
                f"rejection={item['rejection_reason'] or '-'} "
                f"final_score={_fmt(item['final_score'])} "
                f"refinement={item['refinement_picked'] or '-'} created={item['created_at']}"
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
    missing_optional_columns: list[str],
) -> dict[str, Any]:
    return {
        "artifact_type": "pipeline_run_outcome_windows",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "runs": 0,
            "content_types": 0,
            "outcomes": 0,
            "average_final_score": None,
            "average_refinement_delta": None,
        },
        "groups": [],
        "items": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
        "missing_optional_columns": {"pipeline_runs": missing_optional_columns}
        if missing_optional_columns
        else {},
    }


def _pipeline_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    window_start: datetime,
    window_end: datetime,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "batch_id"),
        _column_expr(columns, "content_type"),
        _column_expr(columns, "outcome"),
        _column_expr(columns, "rejection_reason"),
        _column_expr(columns, "final_score"),
        _column_expr(columns, "refinement_picked"),
        _column_expr(columns, "best_score_before_refine"),
        _column_expr(columns, "best_score_after_refine"),
        _column_expr(columns, "created_at"),
    ]
    query = (
        f"SELECT {', '.join(select_columns)} "
        "FROM pipeline_runs "
        "WHERE datetime(created_at) >= datetime(?) "
        "AND datetime(created_at) <= datetime(?) "
        "ORDER BY datetime(created_at) DESC, batch_id ASC"
    )
    cursor = conn.execute(query, (window_start.isoformat(), window_end.isoformat()))
    names = [description[0] for description in cursor.description]
    return [_row_to_dict(row, names) for row in cursor.fetchall()]


def _item_from_row(row: dict[str, Any]) -> dict[str, Any]:
    before = _float_or_none(row.get("best_score_before_refine"))
    after = _float_or_none(row.get("best_score_after_refine"))
    delta = round(after - before, 4) if before is not None and after is not None else None
    return {
        "batch_id": row.get("batch_id"),
        "content_type": row.get("content_type") or "unknown",
        "outcome": row.get("outcome") or "unknown",
        "rejection_reason": row.get("rejection_reason"),
        "final_score": _float_or_none(row.get("final_score")),
        "refinement_picked": row.get("refinement_picked"),
        "best_score_before_refine": before,
        "best_score_after_refine": after,
        "refinement_delta": delta,
        "created_at": row.get("created_at"),
    }


def _group_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for item in items:
        key = (
            item["content_type"],
            item["outcome"],
            item["rejection_reason"],
            item["refinement_picked"],
        )
        groups.setdefault(key, []).append(item)

    grouped = []
    for (content_type, outcome, rejection_reason, refinement_picked), group_items in groups.items():
        grouped.append(
            {
                "content_type": content_type,
                "outcome": outcome,
                "rejection_reason": rejection_reason,
                "refinement_picked": refinement_picked,
                "count": len(group_items),
                "average_final_score": _average(
                    item["final_score"] for item in group_items
                ),
                "average_refinement_delta": _average(
                    item["refinement_delta"] for item in group_items
                ),
                "representative_batch_ids": [
                    str(item["batch_id"])
                    for item in group_items[:5]
                    if item["batch_id"] is not None
                ],
            }
        )
    grouped.sort(
        key=lambda group: (
            -group["count"],
            str(group["content_type"]),
            str(group["outcome"]),
            str(group["rejection_reason"] or ""),
            str(group["refinement_picked"] or ""),
        )
    )
    return grouped


def _totals(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "runs": len(items),
        "content_types": len({item["content_type"] for item in items}),
        "outcomes": len({item["outcome"] for item in items}),
        "average_final_score": _average(item["final_score"] for item in items),
        "average_refinement_delta": _average(item["refinement_delta"] for item in items),
    }


def _average(values: Any) -> float | None:
    numbers = [float(value) for value in values if value is not None]
    if not numbers:
        return None
    return round(sum(numbers) / len(numbers), 4)


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    if column in columns:
        return column
    return f"{default} AS {column}"


def _row_to_dict(row: Any, names: list[str]) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    return dict(zip(names, row))


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
