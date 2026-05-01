"""Read-only revalidation planner for pending publish queue rows."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_STATUSES = ("queued", "held")
VALID_STATUSES = ("all", "queued", "held", "failed", "published", "cancelled")
VALID_PLATFORMS = ("all", "x", "bluesky")
RECOMMENDATIONS = ("publish", "re_evaluate", "regenerate", "cancel")
DEFAULT_LOW_EVAL_SCORE = 6.0
DEFAULT_STALE_AFTER_HOURS = 72.0
DEFAULT_REPEATED_FAILURES = 3


@dataclass(frozen=True)
class RevalidationReason:
    """One machine-readable reason for a queue recommendation."""

    code: str
    detail: str
    severity: str
    evidence: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def plan_publish_queue_revalidation(
    db_or_conn: Any,
    *,
    status: str = "all",
    platform: str = "all",
    min_age_hours: float = 0.0,
    limit: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return recommendations for queued or held publish queue items.

    The planner only reads from the database. Optional quality and publication
    tables are used when present and ignored when absent.
    """
    if status not in VALID_STATUSES:
        raise ValueError(f"invalid status: {status}")
    if platform not in VALID_PLATFORMS:
        raise ValueError(f"invalid platform: {platform}")
    if min_age_hours < 0:
        raise ValueError("min_age_hours must be non-negative")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    conn = getattr(db_or_conn, "conn", db_or_conn)
    now = _ensure_aware(now or datetime.now(timezone.utc))
    schema = _schema(conn)
    optional_tables = [
        "content_claim_checks",
        "content_persona_guard",
        "content_topics",
        "publication_attempts",
        "content_publications",
    ]
    if "publish_queue" not in schema or "generated_content" not in schema:
        return _report(now, status, platform, min_age_hours, limit, [], schema, optional_tables)

    rows = _queue_rows(
        conn,
        schema,
        status=status,
        platform=platform,
        min_age_hours=min_age_hours,
        limit=limit,
        now=now,
    )
    items = [_plan_item(conn, schema, row, now) for row in rows]
    items.sort(key=lambda item: (item["scheduled_at"] or "", item["queue_id"]))
    return _report(now, status, platform, min_age_hours, limit, items, schema, optional_tables)


def format_publish_queue_revalidation_json(report: dict[str, Any]) -> str:
    """Render the revalidation plan as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publish_queue_revalidation_text(report: dict[str, Any]) -> str:
    """Render a stable operator-facing revalidation plan."""
    lines = [
        "Publish queue revalidation plan",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: status={report['filters']['status']} "
            f"platform={report['filters']['platform']} "
            f"min_age_hours={report['filters']['min_age_hours']} "
            f"limit={report['filters']['limit'] if report['filters']['limit'] is not None else '-'}"
        ),
        f"Scanned: {report['scanned_count']}",
        (
            "Recommendations: "
            + ", ".join(
                f"{name}={report['recommendation_counts'].get(name, 0)}"
                for name in RECOMMENDATIONS
            )
        ),
        "",
    ]
    if not report["items"]:
        lines.append("No queued or held publish queue items matched the filters.")
        return "\n".join(lines)

    columns = [
        ("queue_id", "QUEUE", 6),
        ("content_id", "CID", 6),
        ("platform", "PLATFORM", 8),
        ("queue_status", "STATUS", 7),
        ("age_hours", "AGE_H", 7),
        ("recommendation", "RECOMMEND", 11),
        ("reason_codes", "REASONS", 48),
    ]
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for item in report["items"]:
        rendered = dict(item)
        rendered["reason_codes"] = ",".join(reason["code"] for reason in item["reasons"]) or "-"
        lines.append(
            "  ".join(
                _clip(rendered.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)


def _queue_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    status: str,
    platform: str,
    min_age_hours: float,
    limit: int | None,
    now: datetime,
) -> list[dict[str, Any]]:
    pq = schema["publish_queue"]
    gc = schema["generated_content"]
    if not {"id", "content_id"}.issubset(pq) or "id" not in gc:
        return []

    select = {
        "queue_id": "pq.id",
        "content_id": "pq.content_id",
        "scheduled_at": _column_expr(pq, "scheduled_at"),
        "platform": _column_expr(pq, "platform", "'all'"),
        "queue_status": _column_expr(pq, "status", "'queued'"),
        "queue_error": _column_expr(pq, "error"),
        "error_category": _column_expr(pq, "error_category"),
        "hold_reason": _column_expr(pq, "hold_reason"),
        "created_at": _column_expr(pq, "created_at"),
        "content_type": _column_expr(gc, "content_type", alias="gc"),
        "content": _column_expr(gc, "content", alias="gc"),
        "eval_score": _column_expr(gc, "eval_score", alias="gc"),
        "eval_feedback": _column_expr(gc, "eval_feedback", alias="gc"),
    }

    filters: list[str] = []
    params: list[Any] = []
    if "status" in pq:
        if status == "all":
            filters.append("pq.status IN (?, ?)")
            params.extend(DEFAULT_STATUSES)
        else:
            filters.append("pq.status = ?")
            params.append(status)
    if platform != "all" and "platform" in pq:
        filters.append("pq.platform = ?")
        params.append(platform)
    if min_age_hours > 0 and "scheduled_at" in pq:
        filters.append("pq.scheduled_at <= ?")
        params.append((now - timedelta(hours=min_age_hours)).isoformat())
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    limit_clause = "LIMIT ?" if limit is not None else ""
    if limit is not None:
        params.append(limit)

    rows = conn.execute(
        f"""SELECT
               {select['queue_id']} AS queue_id,
               {select['content_id']} AS content_id,
               {select['scheduled_at']} AS scheduled_at,
               {select['platform']} AS platform,
               {select['queue_status']} AS queue_status,
               {select['queue_error']} AS queue_error,
               {select['error_category']} AS error_category,
               {select['hold_reason']} AS hold_reason,
               {select['created_at']} AS created_at,
               {select['content_type']} AS content_type,
               {select['content']} AS content,
               {select['eval_score']} AS eval_score,
               {select['eval_feedback']} AS eval_feedback
           FROM publish_queue pq
           INNER JOIN generated_content gc ON gc.id = pq.content_id
           {where_clause}
           ORDER BY {select['scheduled_at']} ASC, pq.id ASC
           {limit_clause}""",
        params,
    ).fetchall()
    planned_rows = []
    for row in rows:
        item = dict(row)
        item["age_hours"] = _age_hours(item.get("scheduled_at"), now)
        if item["age_hours"] is None or item["age_hours"] >= min_age_hours:
            planned_rows.append(item)
    return planned_rows


def _plan_item(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    row: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    reasons: list[RevalidationReason] = []
    content_id = int(row["content_id"])
    platforms = _target_platforms(str(row.get("platform") or "all"))

    eval_score = row.get("eval_score")
    if eval_score is None:
        reasons.append(
            RevalidationReason(
                code="missing_eval_score",
                detail="generated content has no eval_score",
                severity="review",
                evidence={},
            )
        )
    elif float(eval_score) < DEFAULT_LOW_EVAL_SCORE:
        reasons.append(
            RevalidationReason(
                code="low_eval_score",
                detail=f"eval_score is below {DEFAULT_LOW_EVAL_SCORE}",
                severity="block",
                evidence={"eval_score": float(eval_score), "threshold": DEFAULT_LOW_EVAL_SCORE},
            )
        )

    age = row.get("age_hours")
    if age is not None and age >= DEFAULT_STALE_AFTER_HOURS:
        reasons.append(
            RevalidationReason(
                code="excessive_age",
                detail=f"queue item is at least {DEFAULT_STALE_AFTER_HOURS} hours old",
                severity="block",
                evidence={"age_hours": round(age, 2), "threshold_hours": DEFAULT_STALE_AFTER_HOURS},
            )
        )

    reasons.extend(_claim_check_reasons(conn, schema, content_id))
    reasons.extend(_persona_guard_reasons(conn, schema, content_id))
    topics = _topics(conn, schema, content_id)
    publication_state = _publication_state(conn, schema, content_id, platforms)
    attempts = _publication_attempts(conn, schema, row, content_id, platforms)

    failure_count = sum(1 for attempt in attempts if not attempt.get("success"))
    max_state_attempts = max(
        [int(state.get("attempt_count") or 0) for state in publication_state.values()] or [0]
    )
    if max(failure_count, max_state_attempts) >= DEFAULT_REPEATED_FAILURES:
        reasons.append(
            RevalidationReason(
                code="repeated_publish_failures",
                detail=f"publication has failed at least {DEFAULT_REPEATED_FAILURES} times",
                severity="block",
                evidence={
                    "attempt_failures": failure_count,
                    "max_publication_attempt_count": max_state_attempts,
                    "threshold": DEFAULT_REPEATED_FAILURES,
                },
            )
        )

    if any(state.get("status") == "published" for state in publication_state.values()):
        reasons.append(
            RevalidationReason(
                code="already_published",
                detail="content already has a published platform state",
                severity="block",
                evidence={
                    platform: state
                    for platform, state in publication_state.items()
                    if state.get("status") == "published"
                },
            )
        )

    recommendation = _recommendation(reasons)
    return {
        "queue_id": int(row["queue_id"]),
        "content_id": content_id,
        "platform": row.get("platform"),
        "target_platforms": platforms,
        "queue_status": row.get("queue_status"),
        "scheduled_at": row.get("scheduled_at"),
        "created_at": row.get("created_at"),
        "age_hours": round(age, 2) if age is not None else None,
        "content_type": row.get("content_type"),
        "eval_score": float(eval_score) if eval_score is not None else None,
        "topics": topics,
        "publication_state": publication_state,
        "publication_attempts": attempts,
        "recommendation": recommendation,
        "reasons": [reason.to_dict() for reason in reasons],
    }


def _claim_check_reasons(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
) -> list[RevalidationReason]:
    columns = schema.get("content_claim_checks")
    if not columns or "content_id" not in columns:
        return []
    row = conn.execute(
        "SELECT * FROM content_claim_checks WHERE content_id = ?",
        (content_id,),
    ).fetchone()
    if row is None:
        return [
            RevalidationReason(
                code="missing_claim_check",
                detail="content has no claim-check summary",
                severity="review",
                evidence={},
            )
        ]
    data = dict(row)
    unsupported = int(data.get("unsupported_count") or 0)
    if unsupported <= 0:
        return []
    return [
        RevalidationReason(
            code="unsupported_claims",
            detail="claim check found unsupported claims",
            severity="block",
            evidence={
                "unsupported_count": unsupported,
                "supported_count": int(data.get("supported_count") or 0),
            },
        )
    ]


def _persona_guard_reasons(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
) -> list[RevalidationReason]:
    columns = schema.get("content_persona_guard")
    if not columns or "content_id" not in columns:
        return []
    row = conn.execute(
        "SELECT * FROM content_persona_guard WHERE content_id = ?",
        (content_id,),
    ).fetchone()
    if row is None:
        return [
            RevalidationReason(
                code="missing_persona_guard",
                detail="content has no persona guard summary",
                severity="review",
                evidence={},
            )
        ]
    data = dict(row)
    if "checked" in data and not int(data.get("checked") or 0):
        return [
            RevalidationReason(
                code="persona_guard_unchecked",
                detail="persona guard row exists but was not checked",
                severity="review",
                evidence={"status": data.get("status")},
            )
        ]
    if int(data.get("passed") if data.get("passed") is not None else 1):
        return []
    return [
        RevalidationReason(
            code="persona_guard_failed",
            detail="persona guard did not pass",
            severity="block",
            evidence={
                "status": data.get("status"),
                "score": data.get("score"),
                "reasons": _parse_json_list(data.get("reasons")),
            },
        )
    ]


def _topics(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
) -> list[dict[str, Any]]:
    columns = schema.get("content_topics")
    if not columns or not {"content_id", "topic"}.issubset(columns):
        return []
    selected = [
        f"{column}"
        for column in ("topic", "subtopic", "confidence")
        if column in columns
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM content_topics
            WHERE content_id = ?
            ORDER BY topic ASC, subtopic ASC""",
        (content_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _publication_state(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
    platforms: list[str],
) -> dict[str, dict[str, Any]]:
    columns = schema.get("content_publications")
    if not columns or not {"content_id", "platform"}.issubset(columns):
        return {}
    selected = [
        column
        for column in (
            "platform",
            "status",
            "attempt_count",
            "error",
            "error_category",
            "next_retry_at",
            "last_error_at",
            "published_at",
            "platform_url",
        )
        if column in columns
    ]
    placeholders = ",".join("?" for _ in platforms)
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM content_publications
            WHERE content_id = ? AND platform IN ({placeholders})
            ORDER BY platform ASC""",
        [content_id, *platforms],
    ).fetchall()
    return {str(row["platform"]): dict(row) for row in rows}


def _publication_attempts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    row: dict[str, Any],
    content_id: int,
    platforms: list[str],
) -> list[dict[str, Any]]:
    columns = schema.get("publication_attempts")
    required = {"content_id", "platform", "success"}
    if not columns or not required.issubset(columns):
        return []
    selected = [
        column
        for column in (
            "id",
            "queue_id",
            "content_id",
            "platform",
            "attempted_at",
            "success",
            "error",
            "error_category",
        )
        if column in columns
    ]
    filters = ["content_id = ?"]
    params: list[Any] = [content_id]
    placeholders = ",".join("?" for _ in platforms)
    filters.append(f"platform IN ({placeholders})")
    params.extend(platforms)
    if "queue_id" in columns:
        filters.append("(queue_id = ? OR queue_id IS NULL)")
        params.append(row["queue_id"])
    order_columns = []
    if "attempted_at" in columns:
        order_columns.append("attempted_at ASC")
    if "id" in columns:
        order_columns.append("id ASC")
    order_clause = ", ".join(order_columns) or "platform ASC"
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM publication_attempts
            WHERE {' AND '.join(filters)}
            ORDER BY {order_clause}""",
        params,
    ).fetchall()
    attempts = [dict(item) for item in rows]
    for attempt in attempts:
        if "success" in attempt:
            attempt["success"] = bool(attempt["success"])
    return attempts


def _recommendation(reasons: list[RevalidationReason]) -> str:
    codes = {reason.code for reason in reasons}
    if "already_published" in codes or "repeated_publish_failures" in codes:
        return "cancel"
    if {"persona_guard_failed", "unsupported_claims", "low_eval_score", "excessive_age"} & codes:
        return "regenerate"
    if {
        "missing_eval_score",
        "missing_claim_check",
        "missing_persona_guard",
        "persona_guard_unchecked",
    } & codes:
        return "re_evaluate"
    return "publish"


def _report(
    now: datetime,
    status: str,
    platform: str,
    min_age_hours: float,
    limit: int | None,
    items: list[dict[str, Any]],
    schema: dict[str, set[str]],
    optional_tables: list[str],
) -> dict[str, Any]:
    counts = {name: 0 for name in RECOMMENDATIONS}
    for item in items:
        counts[item["recommendation"]] += 1
    return {
        "generated_at": now.isoformat(),
        "filters": {
            "status": status,
            "platform": platform,
            "min_age_hours": min_age_hours,
            "limit": limit,
        },
        "policy": {
            "low_eval_score": DEFAULT_LOW_EVAL_SCORE,
            "stale_after_hours": DEFAULT_STALE_AFTER_HOURS,
            "repeated_failure_threshold": DEFAULT_REPEATED_FAILURES,
        },
        "available_optional_tables": [
            table for table in optional_tables if table in schema
        ],
        "missing_optional_tables": [
            table for table in optional_tables if table not in schema
        ],
        "scanned_count": len(items),
        "recommendation_counts": counts,
        "items": items,
    }


def _target_platforms(platform: str) -> list[str]:
    if platform == "all":
        return ["bluesky", "x"]
    return [platform]


def _age_hours(value: str | None, now: datetime) -> float | None:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return None
    return (now - parsed).total_seconds() / 3600


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return _ensure_aware(parsed)


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _parse_json_list(value: Any) -> list[Any]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


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


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str = "pq",
) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _clip(value: Any, width: int) -> str:
    if value is None:
        text = "-"
    else:
        text = str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."
