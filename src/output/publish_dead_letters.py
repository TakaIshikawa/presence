"""Read-only export of failed publication dead-letter candidates."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import csv
from io import StringIO
import json
import sqlite3
from typing import Any

from .publish_errors import classify_publish_error, normalize_error_category


DEFAULT_MIN_ATTEMPTS = 3
DEFAULT_STALE_HOURS = 24.0
DEFAULT_LIMIT = 50
DEFAULT_DAYS = 30
SUPPORTED_PLATFORMS = ("all", "x", "bluesky")
CSV_HEADERS = [
    "publication_id",
    "content_id",
    "platform",
    "error_category",
    "stuck_reason",
    "operator_action",
    "attempt_count",
    "next_retry_at",
    "last_error_at",
    "platform_post_id",
    "platform_url",
    "content_excerpt",
    "error",
]


@dataclass(frozen=True)
class PublishDeadLetterRow:
    publication_id: int
    content_id: int
    platform: str
    error_category: str
    stuck_reason: str
    operator_action: str
    attempt_count: int
    next_retry_at: str | None
    last_error_at: str | None
    platform_post_id: str | None
    platform_url: str | None
    content_excerpt: str
    error: str | None


def build_publish_dead_letter_report(
    db_or_conn: Any,
    *,
    min_attempts: int = DEFAULT_MIN_ATTEMPTS,
    stale_hours: float = DEFAULT_STALE_HOURS,
    platform: str = "all",
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
    days: int | None = None,
    include_held: bool | None = None,
    max_attempts: int | None = None,
) -> dict[str, Any]:
    """Return failed content_publications rows that are no longer progressing."""
    del days, include_held
    if max_attempts is not None:
        min_attempts = max_attempts
    if min_attempts <= 0:
        raise ValueError("min_attempts must be positive")
    if stale_hours <= 0:
        raise ValueError("stale_hours must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")
    if platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"platform must be one of: {', '.join(SUPPORTED_PLATFORMS)}")

    conn = _connection(db_or_conn)
    generated_at = _aware(now or datetime.now(timezone.utc))
    stale_cutoff = generated_at - timedelta(hours=stale_hours)
    filters = {
        "min_attempts": min_attempts,
        "stale_hours": float(stale_hours),
        "platform": platform,
        "limit": limit,
        "stale_cutoff": stale_cutoff.isoformat(),
    }
    schema = _schema(conn)
    missing = _missing_required(schema)
    if missing:
        return _report(generated_at, filters, [], missing)

    rows = [
        _dead_letter_item(row, min_attempts=min_attempts, stale_cutoff=stale_cutoff)
        for row in _publication_rows(conn, schema, platform=platform)
    ]
    items = [row for row in rows if row is not None]
    items.sort(
        key=lambda item: (
            item.platform,
            item.error_category,
            item.next_retry_at or "",
            item.last_error_at or "",
            item.content_id,
            item.publication_id,
        )
    )
    if limit is not None:
        items = items[:limit]
    return _report(generated_at, filters, items, missing)


def format_json_report(report: dict[str, Any]) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_csv_report(report: dict[str, Any]) -> str:
    """Render dead-letter rows as RFC 4180 CSV with stable headers."""
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=CSV_HEADERS, lineterminator="\n")
    writer.writeheader()
    for row in report["items"]:
        writer.writerow({header: row.get(header) for header in CSV_HEADERS})
    return output.getvalue().rstrip("\n")


def format_text_report(report: dict[str, Any]) -> str:
    """Render a concise operator-readable triage table."""
    filters = report["filters"]
    lines = [
        "Publish Dead-Letter Export",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"min_attempts={filters['min_attempts']} "
            f"stale_hours={filters['stale_hours']:g} "
            f"platform={filters['platform']} "
            f"limit={filters['limit'] if filters['limit'] is not None else 'none'}"
        ),
        f"Total: {report['totals']['items']}",
    ]
    if report.get("missing_required"):
        lines.append("Missing required schema: " + ", ".join(report["missing_required"]))
    if not report["items"]:
        lines.extend(["", "No publish dead-letter candidates found."])
        return "\n".join(lines)

    lines.extend(
        [
            "",
            "Groups:",
            "  Platform  Category     Count",
            "  --------  -----------  -----",
        ]
    )
    for group in report["groups"]:
        lines.append(
            f"  {group['platform']:<8}  {group['error_category']:<11}  {group['count']:>5}"
        )

    lines.extend(
        [
            "",
            "Items:",
            "  ID  Platform  Category     Attempts  Next retry                 Action",
            "  --  --------  -----------  --------  -------------------------  -----------------------",
        ]
    )
    for item in report["items"]:
        lines.append(
            f"  {item['content_id']:<2}  "
            f"{item['platform']:<8}  "
            f"{item['error_category']:<11}  "
            f"{item['attempt_count']:>8}  "
            f"{(item['next_retry_at'] or '-'):<25}  "
            f"{item['operator_action']}"
        )
    return "\n".join(lines)


def _publication_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    platform: str,
) -> list[dict[str, Any]]:
    cp_columns = schema["content_publications"]
    gc_columns = schema.get("generated_content", set())
    select = {
        "publication_id": _column_expr("cp", cp_columns, "id"),
        "content_id": _column_expr("cp", cp_columns, "content_id"),
        "platform": _column_expr("cp", cp_columns, "platform"),
        "status": _column_expr("cp", cp_columns, "status"),
        "platform_post_id": _column_expr("cp", cp_columns, "platform_post_id"),
        "platform_url": _column_expr("cp", cp_columns, "platform_url"),
        "error": _column_expr("cp", cp_columns, "error"),
        "error_category": _column_expr("cp", cp_columns, "error_category"),
        "attempt_count": _column_expr("cp", cp_columns, "attempt_count", "0"),
        "next_retry_at": _column_expr("cp", cp_columns, "next_retry_at"),
        "last_error_at": _column_expr("cp", cp_columns, "last_error_at"),
        "content": _column_expr("gc", gc_columns, "content"),
    }
    joins = ""
    if "generated_content" in schema and {"id", "content"}.issubset(gc_columns):
        joins = "LEFT JOIN generated_content gc ON gc.id = cp.content_id"
    filters = ["LOWER(cp.status) = 'failed'"]
    params: list[Any] = []
    if platform != "all":
        filters.append("cp.platform = ?")
        params.append(platform)
    query = f"""SELECT {', '.join(f'{expr} AS {alias}' for alias, expr in select.items())}
                FROM content_publications cp
                {joins}
                WHERE {' AND '.join(filters)}
                ORDER BY cp.platform ASC, cp.content_id ASC, cp.id ASC"""
    cursor = conn.execute(query, params)
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _dead_letter_item(
    row: dict[str, Any],
    *,
    min_attempts: int,
    stale_cutoff: datetime,
) -> PublishDeadLetterRow | None:
    attempt_count = _int(row.get("attempt_count"))
    next_retry_at = _parse_datetime(row.get("next_retry_at"))
    over_attempts = attempt_count >= min_attempts
    stale_retry = next_retry_at is not None and next_retry_at <= stale_cutoff
    if not over_attempts and not stale_retry:
        return None

    category = (
        normalize_error_category(row.get("error_category"))
        if row.get("error_category") is not None
        else classify_publish_error(row.get("error"), platform=row.get("platform"))
    )
    return PublishDeadLetterRow(
        publication_id=int(row["publication_id"]),
        content_id=int(row["content_id"]),
        platform=str(row["platform"]),
        error_category=category,
        stuck_reason=_stuck_reason(over_attempts, stale_retry),
        operator_action=_operator_action(category, over_attempts, stale_retry),
        attempt_count=attempt_count,
        next_retry_at=row.get("next_retry_at"),
        last_error_at=row.get("last_error_at"),
        platform_post_id=_optional_str(row.get("platform_post_id")),
        platform_url=_optional_str(row.get("platform_url")),
        content_excerpt=_content_excerpt(row.get("content")),
        error=_optional_str(row.get("error")),
    )


def _report(
    generated_at: datetime,
    filters: dict[str, Any],
    items: list[PublishDeadLetterRow],
    missing_required: list[str],
) -> dict[str, Any]:
    rows = [asdict(item) for item in items]
    by_platform = {name: 0 for name in SUPPORTED_PLATFORMS if name != "all"}
    by_category: dict[str, int] = {}
    groups_by_key: dict[tuple[str, str], int] = {}
    for row in rows:
        by_platform[row["platform"]] = by_platform.get(row["platform"], 0) + 1
        by_category[row["error_category"]] = by_category.get(row["error_category"], 0) + 1
        key = (row["platform"], row["error_category"])
        groups_by_key[key] = groups_by_key.get(key, 0) + 1
    groups = [
        {"platform": key[0], "error_category": key[1], "count": count}
        for key, count in sorted(groups_by_key.items())
    ]
    return {
        "artifact_type": "publish_dead_letters",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "items": len(rows),
            "by_platform": dict(sorted(by_platform.items())),
            "by_error_category": dict(sorted(by_category.items())),
        },
        "groups": groups,
        "missing_required": missing_required,
        "items": rows,
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row[0]
        schema[table] = {
            column[1]
            for column in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
    return schema


def _missing_required(schema: dict[str, set[str]]) -> list[str]:
    if "content_publications" not in schema:
        return ["content_publications"]
    required = {"id", "content_id", "platform", "status"}
    missing = sorted(required - schema["content_publications"])
    return [f"content_publications.{column}" for column in missing]


def _column_expr(
    alias: str,
    columns: set[str],
    column: str,
    fallback: str = "NULL",
) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _stuck_reason(over_attempts: bool, stale_retry: bool) -> str:
    if over_attempts and stale_retry:
        return "min_attempts_and_stale_retry"
    if over_attempts:
        return "min_attempts"
    return "stale_retry"


def _operator_action(category: str, over_attempts: bool, stale_retry: bool) -> str:
    if category == "auth":
        return "fix_credentials"
    if category == "duplicate":
        return "cancel_duplicate"
    if category == "media":
        return "fix_media"
    if category == "validation":
        return "fix_content"
    if stale_retry and not over_attempts:
        return "reschedule_or_replay"
    return "manual_replay_or_cancel"


def _content_excerpt(content: Any, width: int = 96) -> str:
    text = " ".join(str(content or "").split())
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None
