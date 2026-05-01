"""Operator reporting for durable manual content feedback."""

from __future__ import annotations

import json
import sqlite3
import string
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any


VALID_FEEDBACK_TYPES = {"all", "reject", "revise", "prefer"}
REPEATED_FEEDBACK_TYPES = {"reject", "revise"}
REPRESENTATIVE_CONTENT_LIMIT = 5


def build_content_feedback_trends_report(
    db_or_conn: Any,
    *,
    days: int = 30,
    feedback_type: str = "all",
    limit: int = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only trend report from content_feedback rows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if feedback_type not in VALID_FEEDBACK_TYPES:
        raise ValueError(f"invalid feedback_type: {feedback_type}")
    if limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db_or_conn)
    now = _aware(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)
    schema = _schema(conn)
    rows = _feedback_rows(conn, schema, feedback_type, cutoff, now)

    grouped = _group_feedback(rows, limit)
    weekly_trends = _weekly_trends(rows)
    repeated_reasons = _repeated_reject_revise_reasons(rows, limit)
    totals = _totals(rows)

    return {
        "generated_at": now.isoformat(),
        "window_days": days,
        "feedback_type": feedback_type,
        "limit": limit,
        "totals": totals,
        "grouped_rows": grouped,
        "weekly_trends": weekly_trends,
        "repeated_reject_revise_reasons": repeated_reasons,
        "empty_state": {
            "is_empty": not rows,
            "schema_present": "content_feedback" in schema,
            "message": (
                "No content feedback found for the selected filters."
                if not rows
                else None
            ),
        },
    }


def format_content_feedback_trends_json(report: dict[str, Any]) -> str:
    """Render a content feedback trends report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_feedback_trends_text(report: dict[str, Any]) -> str:
    """Render a stable human-readable content feedback trends report."""
    lines = [
        "Content feedback trends report",
        f"Generated: {report['generated_at']}",
        f"Window: {report['window_days']} days",
        f"Feedback type: {report['feedback_type']}",
        f"Total feedback: {report['totals']['total']}",
        "",
    ]

    if report["empty_state"]["is_empty"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.append("Totals by feedback type:")
    for feedback_type, count in report["totals"]["by_feedback_type"].items():
        lines.append(f"- {feedback_type}: {count}")

    lines.extend(["", "Top feedback reasons:"])
    if not report["repeated_reject_revise_reasons"]:
        lines.append("No repeated reject/revise reasons found.")
    else:
        for reason in report["repeated_reject_revise_reasons"]:
            lines.append(
                "- "
                f"{reason['reason']} total={reason['count']} "
                f"types={_format_counts(reason['feedback_type_counts'])} "
                "content_ids="
                f"{', '.join(str(item) for item in reason['representative_content_ids'])}"
            )

    lines.extend(["", "Weekly trend counts:"])
    for trend in report["weekly_trends"]:
        lines.append(
            "- "
            f"{trend['week_start']} total={trend['total']} "
            f"types={_format_counts(trend['feedback_type_counts'])}"
        )

    lines.extend(["", "Grouped feedback rows:"])
    for row in report["grouped_rows"]:
        lines.append(
            "- "
            f"{row['feedback_type']} | {row['content_type']} | {row['week_start']} | "
            f"{row['reason']} count={row['count']} "
            "content_ids="
            f"{', '.join(str(item) for item in row['representative_content_ids'])}"
        )

    return "\n".join(lines)


def _feedback_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    feedback_type: str,
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    if "content_feedback" not in schema:
        return []

    feedback_columns = schema["content_feedback"]
    content_columns = schema.get("generated_content", set())
    has_content_table = bool(content_columns)
    select_content_type = (
        "gc.content_type" if has_content_table and "content_type" in content_columns else "NULL"
    )
    join_clause = (
        "LEFT JOIN generated_content gc ON gc.id = cf.content_id"
        if has_content_table
        else ""
    )
    notes_expr = "cf.notes" if "notes" in feedback_columns else "NULL"
    replacement_expr = (
        "cf.replacement_text" if "replacement_text" in feedback_columns else "NULL"
    )
    created_expr = "cf.created_at" if "created_at" in feedback_columns else "NULL"

    filters: list[str] = []
    params: list[Any] = []
    if feedback_type != "all":
        filters.append("cf.feedback_type = ?")
        params.append(feedback_type)
    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    raw_rows = conn.execute(
        f"""SELECT cf.id,
                  cf.content_id,
                  cf.feedback_type,
                  {notes_expr} AS notes,
                  {replacement_expr} AS replacement_text,
                  {created_expr} AS created_at,
                  {select_content_type} AS content_type
           FROM content_feedback cf
           {join_clause}
           {where}
           ORDER BY created_at ASC, cf.id ASC""",
        params,
    ).fetchall()

    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        created_at = _parse_timestamp(row["created_at"]) or now
        if created_at < cutoff or created_at > now:
            continue
        reason = _reason(row["notes"], row["replacement_text"])
        rows.append(
            {
                "id": int(row["id"]),
                "content_id": int(row["content_id"]),
                "feedback_type": row["feedback_type"],
                "reason": reason,
                "content_type": row["content_type"] or "unknown",
                "created_at": created_at.isoformat(),
                "week_start": _week_start(created_at),
            }
        )
    return rows


def _group_feedback(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            row["feedback_type"],
            row["reason"],
            row["content_type"],
            row["week_start"],
        )
        group = groups.setdefault(
            key,
            {
                "feedback_type": row["feedback_type"],
                "reason": row["reason"],
                "content_type": row["content_type"],
                "week_start": row["week_start"],
                "count": 0,
                "representative_content_ids": [],
            },
        )
        group["count"] += 1
        _append_representative_id(group["representative_content_ids"], row["content_id"])

    return sorted(
        groups.values(),
        key=lambda item: (
            -item["count"],
            item["feedback_type"],
            item["reason"],
            item["content_type"],
            item["week_start"],
        ),
    )[:limit]


def _weekly_trends(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    weeks: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        weeks[row["week_start"]][row["feedback_type"]] += 1

    trends = []
    for week_start in sorted(weeks):
        counts = dict(sorted(weeks[week_start].items()))
        trends.append(
            {
                "week_start": week_start,
                "total": sum(counts.values()),
                "feedback_type_counts": counts,
            }
        )
    return trends


def _repeated_reject_revise_reasons(
    rows: list[dict[str, Any]], limit: int
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row["feedback_type"] not in REPEATED_FEEDBACK_TYPES:
            continue
        reason = row["reason"]
        group = groups.setdefault(
            reason,
            {
                "reason": reason,
                "count": 0,
                "feedback_type_counts": Counter(),
                "content_type_counts": Counter(),
                "representative_content_ids": [],
            },
        )
        group["count"] += 1
        group["feedback_type_counts"][row["feedback_type"]] += 1
        group["content_type_counts"][row["content_type"]] += 1
        _append_representative_id(group["representative_content_ids"], row["content_id"])

    repeated = [item for item in groups.values() if item["count"] > 1]
    for item in repeated:
        item["feedback_type_counts"] = dict(sorted(item["feedback_type_counts"].items()))
        item["content_type_counts"] = dict(sorted(item["content_type_counts"].items()))

    return sorted(
        repeated,
        key=lambda item: (-item["count"], item["reason"]),
    )[:limit]


def _totals(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_feedback_type = Counter(row["feedback_type"] for row in rows)
    by_content_type = Counter(row["content_type"] for row in rows)
    return {
        "total": len(rows),
        "by_feedback_type": dict(sorted(by_feedback_type.items())),
        "by_content_type": dict(sorted(by_content_type.items())),
    }


def _append_representative_id(ids: list[int], content_id: int) -> None:
    if content_id not in ids and len(ids) < REPRESENTATIVE_CONTENT_LIMIT:
        ids.append(content_id)


def _reason(notes: str | None, replacement_text: str | None) -> str:
    text = " ".join(str(notes or "").split())
    if not text and replacement_text:
        text = "replacement provided"
    if not text:
        return "(no reason provided)"
    normalized = text.lower().strip()
    normalized = normalized.strip(string.whitespace + ".!?;:")
    return normalized or "(no reason provided)"


def _week_start(value: datetime) -> str:
    monday = value.date() - timedelta(days=value.weekday())
    return monday.isoformat()


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _format_counts(counts: dict[str, int]) -> str:
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))
