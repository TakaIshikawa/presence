"""Read-only digest for content ideas whose snoozes need review."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS_AHEAD = 7
DEFAULT_LIMIT = 50

_PRIORITY_RANK = {"high": 0, "normal": 1, "low": 2}


def build_content_idea_snooze_digest(
    db_or_conn: Any,
    *,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    include_unsnoozed: bool = False,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return open content ideas whose snooze window has expired or is near expiry."""
    if days_ahead < 0:
        raise ValueError("days_ahead must be non-negative")
    if limit < 0:
        raise ValueError("limit must be non-negative")

    conn = getattr(db_or_conn, "conn", db_or_conn)
    schema = _schema(conn)
    now = _ensure_utc(now or datetime.now(timezone.utc))
    due_before = now + timedelta(days=days_ahead)
    filters = {
        "days_ahead": days_ahead,
        "include_unsnoozed": include_unsnoozed,
        "limit": limit,
        "due_before": due_before.isoformat(),
    }
    if limit == 0 or "content_ideas" not in schema:
        return _empty_report(now, filters, due_before)

    rows = _load_open_ideas(
        conn,
        schema,
        due_before=due_before,
        include_unsnoozed=include_unsnoozed,
    )
    items = [_build_item(row, now=now) for row in rows]
    items.sort(key=_item_sort_key)
    items = items[:limit]

    recommendation_counts = Counter(item["recommendation"] for item in items)
    status_counts = Counter(item["snooze_status"] for item in items)
    return {
        "generated_at": now.isoformat(),
        "filters": filters,
        "summary": {
            "idea_count": len(items),
            "expired_count": status_counts.get("expired", 0),
            "upcoming_count": status_counts.get("upcoming", 0),
            "unsnoozed_count": status_counts.get("unsnoozed", 0),
            "recommendation_counts": dict(sorted(recommendation_counts.items())),
        },
        "groups": {
            "priority": _group_counts(items, "priority"),
            "topic": _group_counts(items, "topic"),
            "source": _group_counts(items, "source"),
            "overdue_age": _group_counts(items, "overdue_age_bucket"),
        },
        "ideas": items,
    }


def format_content_idea_snooze_digest_json(report: dict[str, Any]) -> str:
    """Render the snooze digest as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_idea_snooze_digest_text(report: dict[str, Any]) -> str:
    """Render a compact operator-facing snooze digest."""
    filters = report["filters"]
    summary = report["summary"]
    lines = [
        "Content idea snooze digest",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days_ahead={filters['days_ahead']} "
            f"include_unsnoozed={filters['include_unsnoozed']} "
            f"limit={filters['limit']}"
        ),
        (
            "Totals: "
            f"ideas={summary['idea_count']} "
            f"expired={summary['expired_count']} "
            f"upcoming={summary['upcoming_count']} "
            f"unsnoozed={summary['unsnoozed_count']}"
        ),
        "",
    ]
    if not report["ideas"]:
        lines.append("No snoozed content ideas due for review.")
        return "\n".join(lines)

    lines.append("Groups")
    for group_name in ("priority", "topic", "source", "overdue_age"):
        values = report["groups"][group_name]
        rendered = ", ".join(f"{item['value']}={item['count']}" for item in values)
        lines.append(f"  {group_name}: {rendered or '-'}")
    lines.append("")
    lines.append("Ideas")
    lines.append("  ID    Pri     Status      Age        Rec             Topic / Note")
    for idea in report["ideas"]:
        age = _format_age(idea["days_overdue"], idea["days_until_due"])
        lines.append(
            f"  {idea['id']:<5} "
            f"{idea['priority']:<7} "
            f"{idea['snooze_status']:<11} "
            f"{age:<10} "
            f"{idea['recommendation']:<15} "
            f"{_clip(idea['topic'] or '-', 18)} / {_clip(idea['note'], 58)}"
        )
        if idea["recommendation_reasons"]:
            lines.append(f"        reasons: {', '.join(idea['recommendation_reasons'])}")
    return "\n".join(lines)


def _load_open_ideas(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    due_before: datetime,
    include_unsnoozed: bool,
) -> list[dict[str, Any]]:
    columns = schema["content_ideas"]
    required = {"id", "note", "status"}
    if not required.issubset(columns):
        return []

    select = {
        "id": "ci.id",
        "note": "ci.note",
        "topic": _column_expr(columns, "topic", alias="ci"),
        "priority": _column_expr(columns, "priority", "'normal'", alias="ci"),
        "source": _column_expr(columns, "source", alias="ci"),
        "source_metadata": _column_expr(columns, "source_metadata", alias="ci"),
        "snoozed_until": _column_expr(columns, "snoozed_until", alias="ci"),
        "snooze_reason": _column_expr(columns, "snooze_reason", alias="ci"),
        "created_at": _column_expr(columns, "created_at", alias="ci"),
        "updated_at": _column_expr(columns, "updated_at", alias="ci"),
    }
    filters = ["ci.status = 'open'"]
    params: list[Any] = [due_before.isoformat()]
    if include_unsnoozed:
        filters.append(
            "(ci.snoozed_until IS NULL OR datetime(ci.snoozed_until) <= datetime(?))"
        )
    else:
        filters.append(
            "ci.snoozed_until IS NOT NULL AND datetime(ci.snoozed_until) <= datetime(?)"
        )
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   {select['id']} AS id,
                   {select['note']} AS note,
                   {select['topic']} AS topic,
                   {select['priority']} AS priority,
                   {select['source']} AS source,
                   {select['source_metadata']} AS source_metadata,
                   {select['snoozed_until']} AS snoozed_until,
                   {select['snooze_reason']} AS snooze_reason,
                   {select['created_at']} AS created_at,
                   {select['updated_at']} AS updated_at
               FROM content_ideas ci
               WHERE {' AND '.join(filters)}
               ORDER BY ci.id ASC""",
            params,
        ).fetchall()
    ]


def _build_item(row: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    snoozed_until = _parse_datetime(row.get("snoozed_until"))
    source_metadata = _parse_source_metadata(row.get("source_metadata"))
    days_overdue = None
    days_until_due = None
    if snoozed_until is None:
        snooze_status = "unsnoozed"
        bucket = "unsnoozed"
    elif snoozed_until <= now:
        snooze_status = "expired"
        days_overdue = _days_between(snoozed_until, now)
        bucket = _overdue_bucket(days_overdue)
    else:
        snooze_status = "upcoming"
        days_until_due = _days_between(now, snoozed_until)
        bucket = "upcoming"

    recommendation, reasons = _recommendation(
        priority=str(row.get("priority") or "normal"),
        source=row.get("source"),
        source_metadata=source_metadata,
        snooze_status=snooze_status,
        days_overdue=days_overdue,
    )
    return {
        "id": int(row["id"]),
        "note": str(row.get("note") or ""),
        "topic": _value(row.get("topic")),
        "priority": _value(row.get("priority"), "normal"),
        "source": _value(row.get("source")),
        "source_metadata": source_metadata,
        "snoozed_until": row.get("snoozed_until"),
        "snooze_reason": row.get("snooze_reason"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "snooze_status": snooze_status,
        "days_overdue": days_overdue,
        "days_until_due": days_until_due,
        "overdue_age_bucket": bucket,
        "recommendation": recommendation,
        "recommendation_reasons": reasons,
    }


def _recommendation(
    *,
    priority: str,
    source: Any,
    source_metadata: dict[str, Any],
    snooze_status: str,
    days_overdue: float | None,
) -> tuple[str, list[str]]:
    if snooze_status == "upcoming":
        return "keep_snoozed", ["snooze window is still active"]

    metadata_strength = _source_metadata_strength(source_metadata)
    source_present = bool(str(source or "").strip())
    overdue = days_overdue or 0.0
    if priority == "high" and (metadata_strength >= 2 or source_present):
        return "promote", ["high priority", "source metadata available"]
    if priority == "low" and overdue >= 30 and metadata_strength == 0:
        return (
            "dismiss_review",
            [
                "low priority",
                "snooze expired more than 30 days ago",
                "weak source metadata",
            ],
        )
    if snooze_status == "unsnoozed" and priority == "low" and metadata_strength == 0:
        return (
            "dismiss_review",
            ["unsnoozed low-priority idea has weak source metadata"],
        )
    if priority == "high":
        return "promote", ["high priority"]
    if snooze_status == "expired":
        return "unsnooze", ["snooze has expired"]
    return "unsnooze", ["ready for review"]


def _source_metadata_strength(metadata: dict[str, Any]) -> int:
    if not metadata:
        return 0
    score = 0
    if any(key in metadata for key in ("source_count", "content_count", "signals")):
        try:
            signal_count = max(
                int(metadata.get(key) or 0)
                for key in ("source_count", "content_count", "signals")
            )
        except (TypeError, ValueError):
            pass
        else:
            score += 1 if signal_count > 0 else 0
    if any(key in metadata for key in ("latest_source_at", "created_at", "source_url", "url")):
        score += 1
    if any(key.endswith("_id") or key.endswith("_ids") for key in metadata):
        score += 1
    return score


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _empty_report(
    now: datetime,
    filters: dict[str, Any],
    due_before: datetime,
) -> dict[str, Any]:
    filters = {**filters, "due_before": due_before.isoformat()}
    return {
        "generated_at": now.isoformat(),
        "filters": filters,
        "summary": {
            "idea_count": 0,
            "expired_count": 0,
            "upcoming_count": 0,
            "unsnoozed_count": 0,
            "recommendation_counts": {},
        },
        "groups": {"priority": [], "topic": [], "source": [], "overdue_age": []},
        "ideas": [],
    }


def _group_counts(items: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    counts = Counter(_value(item.get(key)) for item in items)
    return [
        {"value": value, "count": count}
        for value, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _item_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    status_rank = {"expired": 0, "upcoming": 1, "unsnoozed": 2}.get(
        item["snooze_status"], 3
    )
    due_sort = item["snoozed_until"] or ""
    return (
        status_rank,
        _PRIORITY_RANK.get(item["priority"], 3),
        due_sort,
        item["topic"],
        item["id"],
    )


def _column_expr(
    columns: set[str],
    column: str,
    default: str = "NULL",
    *,
    alias: str,
) -> str:
    if column in columns:
        return f"{alias}.{column}"
    return default


def _parse_source_metadata(raw_value: Any) -> dict[str, Any]:
    if isinstance(raw_value, dict):
        return raw_value
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except (TypeError, json.JSONDecodeError):
        return {"_malformed": str(raw_value)}
    return parsed if isinstance(parsed, dict) else {}


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return _ensure_utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def _days_between(start: datetime, end: datetime) -> float:
    return round((end - start).total_seconds() / 86400, 1)


def _overdue_bucket(days_overdue: float | None) -> str:
    days = days_overdue or 0.0
    if days < 1:
        return "due_today"
    if days <= 7:
        return "overdue_1_7d"
    if days <= 30:
        return "overdue_8_30d"
    return "overdue_31d_plus"


def _value(value: Any, default: str = "unknown") -> str:
    text = str(value or "").strip()
    return text if text else default


def _format_age(days_overdue: float | None, days_until_due: float | None) -> str:
    if days_overdue is not None:
        return f"{days_overdue:.1f}d late"
    if days_until_due is not None:
        return f"{days_until_due:.1f}d left"
    return "-"


def _clip(value: Any, width: int) -> str:
    text = str(value or "")
    if len(text) <= width:
        return text
    return text[: max(width - 3, 0)] + "..."
