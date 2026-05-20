"""Summarize unresolved content feedback load by reviewer."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_LIMIT = 100
DEFAULT_UNRESOLVED_STATUSES = ("default",)
RESOLVED_STATUSES = {"closed", "complete", "completed", "done", "fixed", "resolved", "shipped"}
REVIEWER_COLUMNS = ("reviewer", "reviewer_id", "assigned_to", "assignee", "owner")


def build_content_feedback_reviewer_load_report(
    feedback_rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    unresolved_statuses: str | Iterable[str] = DEFAULT_UNRESOLVED_STATUSES,
    now: datetime | None = None,
    missing_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a deterministic reviewer load report from already-loaded rows."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    status_filter = _normalize_status_filter(unresolved_statuses)
    unresolved = []
    for row in feedback_rows:
        status = _status(row.get("status"))
        if not _is_unresolved(status, status_filter):
            continue
        created_at = _parse_dt(row.get("created_at"))
        age_days = _age_days(created_at, generated_at)
        unresolved.append(
            {
                "feedback_id": _value(row, "feedback_id", "id"),
                "content_id": row.get("content_id"),
                "reviewer": _reviewer(row),
                "status": status,
                "severity": _clean(row.get("severity")) or "unspecified",
                "feedback_type": _clean(row.get("feedback_type")) or "unknown",
                "created_at": _iso_or_none(created_at),
                "age_days": age_days,
                "age_bucket": _age_bucket(age_days),
                "notes_preview": _snippet(row.get("notes")),
            }
        )

    reviewer_groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    aging_buckets: Counter[str] = Counter()
    for item in unresolved:
        aging_buckets[item["age_bucket"]] += 1
        key = (item["reviewer"], item["status"], item["severity"])
        group = reviewer_groups.setdefault(
            key,
            {
                "reviewer": item["reviewer"],
                "status": item["status"],
                "severity": item["severity"],
                "unresolved_count": 0,
                "oldest_age_days": None,
                "example_feedback_ids": [],
            },
        )
        group["unresolved_count"] += 1
        if item["age_days"] is not None:
            group["oldest_age_days"] = max(group["oldest_age_days"] or 0, item["age_days"])
        if len(group["example_feedback_ids"]) < 5:
            group["example_feedback_ids"].append(item["feedback_id"])

    reviewer_summary = list(reviewer_groups.values())
    reviewer_summary.sort(
        key=lambda item: (
            -item["unresolved_count"],
            -(item["oldest_age_days"] or -1),
            item["reviewer"],
            item["status"],
            item["severity"],
        )
    )
    unresolved.sort(key=lambda item: (_example_age_sort(item), str(item["feedback_id"] or "")))
    shown_examples = unresolved[:limit]
    return {
        "artifact_type": "content_feedback_reviewer_load",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit, "unresolved_statuses": list(status_filter)},
        "totals": {
            "rows_scanned": len(feedback_rows),
            "unresolved_count": len(unresolved),
            "shown_examples": len(shown_examples),
            "reviewer_count": len({item["reviewer"] for item in unresolved}),
            "counts_by_status": dict(sorted(Counter(item["status"] for item in unresolved).items())),
            "counts_by_severity": dict(sorted(Counter(item["severity"] for item in unresolved).items())),
        },
        "reviewer_summary": reviewer_summary,
        "aging_buckets": {bucket: aging_buckets.get(bucket, 0) for bucket in ("0-1d", "2-3d", "4-7d", "8-14d", "15-30d", "31d+", "unknown")},
        "unresolved_examples": shown_examples,
        "missing_schema": missing_schema or {"missing_tables": [], "missing_columns": {}},
    }


def build_content_feedback_reviewer_load_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_schema = _missing_schema(schema)
    rows = _load_feedback_rows(conn, schema) if "content_feedback" in schema else []
    return build_content_feedback_reviewer_load_report(rows, missing_schema=missing_schema, **kwargs)


def format_content_feedback_reviewer_load_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_feedback_reviewer_load_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Content Feedback Reviewer Load",
        f"Generated: {report['generated_at']}",
        f"Unresolved statuses: {', '.join(report['filters']['unresolved_statuses'])}",
        (
            "Totals: "
            f"rows={totals['rows_scanned']} "
            f"unresolved={totals['unresolved_count']} "
            f"reviewers={totals['reviewer_count']} "
            f"shown={totals['shown_examples']}"
        ),
    ]
    missing = report["missing_schema"]
    if missing["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(missing["missing_tables"]))
    if missing["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(missing["missing_columns"]))
    if not report["reviewer_summary"]:
        lines.extend(["", "No unresolved content feedback found."])
        return "\n".join(lines)

    lines.extend(["", "Reviewer summary:"])
    for item in report["reviewer_summary"]:
        lines.append(
            f"  - reviewer={item['reviewer']} status={item['status']} severity={item['severity']} "
            f"count={item['unresolved_count']} oldest_age_days={item['oldest_age_days'] if item['oldest_age_days'] is not None else '-'}"
        )
    lines.extend(["", "Aging buckets:"])
    for bucket, count in report["aging_buckets"].items():
        lines.append(f"  - {bucket}: {count}")
    if report["unresolved_examples"]:
        lines.extend(["", "Oldest examples:"])
        for item in report["unresolved_examples"][:5]:
            lines.append(
                f"  - feedback_id={item['feedback_id'] or '-'} content_id={item['content_id'] or '-'} "
                f"reviewer={item['reviewer']} status={item['status']} severity={item['severity']} "
                f"age_days={item['age_days'] if item['age_days'] is not None else '-'}"
            )
    return "\n".join(lines)


def _load_feedback_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema["content_feedback"]
    select = [
        _expr(columns, "id", "cf", "feedback_id", default="cf.rowid"),
        _expr(columns, "content_id", "cf", "content_id", default="NULL"),
        _expr(columns, "status", "cf", "status", default="'unknown'"),
        _expr(columns, "severity", "cf", "severity", default="'unspecified'"),
        _expr(columns, "feedback_type", "cf", "feedback_type", default="'unknown'"),
        _expr(columns, "notes", "cf", "notes", default="NULL"),
        _expr(columns, "created_at", "cf", "created_at", default="NULL"),
        _reviewer_expr(columns),
    ]
    order = "datetime(cf.created_at) ASC, cf.id ASC" if {"created_at", "id"}.issubset(columns) else "cf.rowid ASC"
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM content_feedback cf ORDER BY {order}").fetchall()]


def _missing_schema(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "content_feedback" not in schema:
        return {"missing_tables": ["content_feedback"], "missing_columns": {}}
    optional = {"content_id", "created_at", "status", "severity", *REVIEWER_COLUMNS}
    missing = sorted(optional - schema["content_feedback"])
    return {"missing_tables": [], "missing_columns": {"content_feedback": missing} if missing else {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
    return {table: {column[1] for column in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _expr(columns: set[str], column: str, alias: str, output: str, *, default: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"{default} AS {output}"


def _reviewer_expr(columns: set[str]) -> str:
    for column in REVIEWER_COLUMNS:
        if column in columns:
            return f"cf.{column} AS reviewer"
    return "'unassigned' AS reviewer"


def _normalize_status_filter(value: str | Iterable[str]) -> tuple[str, ...]:
    if isinstance(value, str):
        parts = [part.strip().lower() for part in value.split(",")]
    else:
        parts = [str(part).strip().lower() for part in value]
    statuses = tuple(part for part in parts if part)
    return statuses or DEFAULT_UNRESOLVED_STATUSES


def _is_unresolved(status: str, status_filter: tuple[str, ...]) -> bool:
    if status_filter == DEFAULT_UNRESOLVED_STATUSES:
        return status not in RESOLVED_STATUSES
    return status in status_filter


def _status(value: Any) -> str:
    return _clean(value).lower() or "unknown"


def _reviewer(row: dict[str, Any]) -> str:
    return _clean(row.get("reviewer")) or "unassigned"


def _parse_dt(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    try:
        return _utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        try:
            return _utc(datetime.strptime(text, "%Y-%m-%d %H:%M:%S"))
        except ValueError:
            return None


def _age_days(value: datetime | None, now: datetime) -> int | None:
    if value is None:
        return None
    return int((_utc(now) - _utc(value)).total_seconds() // 86400)


def _age_bucket(age_days: int | None) -> str:
    if age_days is None:
        return "unknown"
    if age_days <= 1:
        return "0-1d"
    if age_days <= 3:
        return "2-3d"
    if age_days <= 7:
        return "4-7d"
    if age_days <= 14:
        return "8-14d"
    if age_days <= 30:
        return "15-30d"
    return "31d+"


def _example_age_sort(item: dict[str, Any]) -> tuple[int, str]:
    age = item["age_days"] if item["age_days"] is not None else -1
    return (-int(age), item["created_at"] or "")


def _iso_or_none(value: datetime | None) -> str | None:
    return None if value is None else _utc(value).isoformat()


def _value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    return None


def _snippet(value: Any, limit: int = 120) -> str:
    text = " ".join(_clean(value).split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "..."


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _format_missing(missing_columns: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}={','.join(columns)}" for table, columns in sorted(missing_columns.items()))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
