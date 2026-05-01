"""Read-only export of publish queue dead-letter candidates."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from io import StringIO
import json
import sqlite3
from typing import Any

from .publish_errors import classify_publish_error, normalize_error_category


DEFAULT_DAYS = 30
DEFAULT_MAX_ATTEMPTS = 3
SUPPORTED_PLATFORMS = ("all", "x", "bluesky")
TERMINAL_ERROR_CATEGORIES = {"auth", "duplicate", "media", "validation"}
CSV_HEADERS = [
    "content_id",
    "queue_id",
    "publication_id",
    "platform",
    "terminal_reason",
    "last_error",
    "failed_at",
    "retry_count",
    "content_preview",
    "operator_action",
]


@dataclass(frozen=True)
class PublishDeadLetterRow:
    content_id: int
    queue_id: int | None
    publication_id: int | None
    platform: str
    terminal_reason: str
    last_error: str | None
    failed_at: str | None
    retry_count: int
    content_preview: str
    operator_action: str


def build_publish_dead_letter_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str = "all",
    include_held: bool = False,
    now: datetime | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> dict[str, Any]:
    """Return terminal failed publications and optionally held queue rows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"platform must be one of: {', '.join(SUPPORTED_PLATFORMS)}")
    if max_attempts <= 0:
        raise ValueError("max_attempts must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _aware(now or datetime.now(timezone.utc))
    filters = {
        "days": days,
        "platform": platform,
        "include_held": include_held,
        "max_attempts": max_attempts,
    }
    missing_required = [
        table
        for table in ("content_publications", "publish_queue")
        if table not in schema
    ]
    if missing_required:
        return _report(generated_at, filters, [], missing_required, _unknown_optional(schema))

    cutoff = (generated_at - timedelta(days=days)).isoformat()
    rows_by_key = _failed_publication_rows(conn, schema, cutoff=cutoff, platform=platform)
    _merge_failed_queue_rows(conn, schema, rows_by_key, cutoff=cutoff, platform=platform)
    if include_held:
        _merge_held_queue_rows(conn, schema, rows_by_key, cutoff=cutoff, platform=platform)

    attempt_counts = _failed_attempt_counts(conn, schema, rows_by_key)
    items = [
        item
        for row in rows_by_key.values()
        if (
            item := _dead_letter_item(
                row,
                attempt_count=attempt_counts.get(
                    (int(row["content_id"]), row["platform"], row.get("queue_id")),
                    0,
                ),
                max_attempts=max_attempts,
            )
        )
        is not None
    ]
    items.sort(
        key=lambda item: (
            item.terminal_reason,
            item.platform,
            item.failed_at or "",
            item.content_id,
            item.queue_id or 0,
            item.publication_id or 0,
        )
    )
    return _report(generated_at, filters, items, [], _unknown_optional(schema))


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
    """Render a compact terminal report."""
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Publish Dead-Letter Export",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"days={filters['days']} "
            f"platform={filters['platform']} "
            f"include_held={_yes_no(filters['include_held'])}"
        ),
        f"Totals: items={totals['items']} failed={totals['failed']} held={totals['held']}",
    ]
    if report.get("missing_required_tables"):
        lines.append("Missing required tables: " + ", ".join(report["missing_required_tables"]))
    if report.get("unknown_optional_signals"):
        lines.append("Unknown optional signals: " + ", ".join(report["unknown_optional_signals"]))
    if not report["items"]:
        lines.extend(["", "No publish dead-letter candidates found."])
        return "\n".join(lines)

    lines.extend(["", "Items"])
    for item in report["items"]:
        identifiers = [f"content={item['content_id']}", f"platform={item['platform']}"]
        if item.get("queue_id") is not None:
            identifiers.append(f"queue={item['queue_id']}")
        if item.get("publication_id") is not None:
            identifiers.append(f"publication={item['publication_id']}")
        lines.append(
            "  - "
            + " ".join(identifiers)
            + f" reason={item['terminal_reason']}"
            + f" retries={item['retry_count']}"
            + f" failed_at={item['failed_at'] or '-'}"
            + f" action={item['operator_action']}"
        )
    return "\n".join(lines)


def _failed_publication_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: str,
    platform: str,
) -> dict[tuple[int, str, str, int | None], dict[str, Any]]:
    columns = schema["content_publications"]
    required = {"id", "content_id", "platform", "status"}
    if not required.issubset(columns):
        return {}
    timestamp = _coalesce_expr(
        "cp",
        columns,
        ["last_error_at", "updated_at"],
        fallback="NULL",
    )
    select = {
        "publication_id": _column_expr("cp", columns, "id"),
        "content_id": _column_expr("cp", columns, "content_id"),
        "platform": _column_expr("cp", columns, "platform"),
        "publication_status": _column_expr("cp", columns, "status"),
        "publication_error": _column_expr("cp", columns, "error"),
        "publication_error_category": _column_expr("cp", columns, "error_category"),
        "attempt_count": _column_expr("cp", columns, "attempt_count", "0"),
        "failed_at": timestamp,
        "content": _content_expr(schema),
        "content_retry_count": _generated_expr(schema, "retry_count", "0"),
    }
    filters = ["LOWER(cp.status) = 'failed'"]
    params: list[Any] = []
    if timestamp != "NULL":
        filters.append(f"{timestamp} >= ?")
        params.append(cutoff)
    if platform != "all":
        filters.append("cp.platform = ?")
        params.append(platform)
    join = _content_join(schema, "cp")
    rows = conn.execute(
        f"""SELECT {', '.join(f'{expr} AS {alias}' for alias, expr in select.items())}
            FROM content_publications cp
            {join}
            WHERE {' AND '.join(filters)}
            ORDER BY cp.platform ASC, cp.content_id ASC, cp.id ASC""",
        params,
    ).fetchall()
    return {
        (int(row["content_id"]), row["platform"], "failed", None): dict(row)
        for row in rows
    }


def _merge_failed_queue_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    rows_by_key: dict[tuple[int, str, str, int | None], dict[str, Any]],
    *,
    cutoff: str,
    platform: str,
) -> None:
    for row in _queue_rows(conn, schema, status="failed", cutoff=cutoff, platform=platform):
        key = (int(row["content_id"]), row["platform"], "failed", None)
        existing = rows_by_key.get(key)
        if existing is None:
            rows_by_key[key] = row
            continue
        existing["queue_id"] = row.get("queue_id")
        existing["queue_error"] = row.get("queue_error")
        existing["queue_error_category"] = row.get("queue_error_category")
        existing["queue_failed_at"] = row.get("queue_failed_at")
        existing["queue_status"] = row.get("queue_status")


def _merge_held_queue_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    rows_by_key: dict[tuple[int, str, str, int | None], dict[str, Any]],
    *,
    cutoff: str,
    platform: str,
) -> None:
    for row in _queue_rows(conn, schema, status="held", cutoff=cutoff, platform=platform):
        key = (int(row["content_id"]), row["platform"], "held", int(row["queue_id"]))
        rows_by_key[key] = row


def _queue_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    status: str,
    cutoff: str,
    platform: str,
) -> list[dict[str, Any]]:
    columns = schema["publish_queue"]
    required = {"id", "content_id", "platform", "status"}
    if not required.issubset(columns):
        return []
    timestamp = _coalesce_expr(
        "pq",
        columns,
        ["published_at", "created_at", "scheduled_at"],
        fallback="NULL",
    )
    select = {
        "queue_id": _column_expr("pq", columns, "id"),
        "content_id": _column_expr("pq", columns, "content_id"),
        "queue_platform": _column_expr("pq", columns, "platform"),
        "queue_status": _column_expr("pq", columns, "status"),
        "queue_error": _column_expr("pq", columns, "error"),
        "queue_error_category": _column_expr("pq", columns, "error_category"),
        "hold_reason": _column_expr("pq", columns, "hold_reason"),
        "queue_failed_at": timestamp,
        "content": _content_expr(schema),
        "content_retry_count": _generated_expr(schema, "retry_count", "0"),
    }
    filters = ["LOWER(pq.status) = ?"]
    params: list[Any] = [status]
    if timestamp != "NULL":
        filters.append(f"{timestamp} >= ?")
        params.append(cutoff)
    join = _content_join(schema, "pq")
    rows = conn.execute(
        f"""SELECT {', '.join(f'{expr} AS {alias}' for alias, expr in select.items())}
            FROM publish_queue pq
            {join}
            WHERE {' AND '.join(filters)}
            ORDER BY pq.platform ASC, pq.id ASC""",
        params,
    ).fetchall()
    expanded: list[dict[str, Any]] = []
    for raw in rows:
        data = dict(raw)
        for target in _target_platforms(data.get("queue_platform")):
            if platform != "all" and target != platform:
                continue
            item = dict(data)
            item["platform"] = target
            expanded.append(item)
    return expanded


def _failed_attempt_counts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    rows_by_key: dict[tuple[int, str, str, int | None], dict[str, Any]],
) -> dict[tuple[int, str, int | None], int]:
    columns = schema.get("publication_attempts")
    if not rows_by_key or not columns:
        return {}
    required = {"content_id", "platform", "success"}
    if not required.issubset(columns):
        return {}
    content_ids = sorted({int(row["content_id"]) for row in rows_by_key.values()})
    placeholders = ",".join("?" for _ in content_ids)
    queue_expr = _column_expr("pa", columns, "queue_id")
    rows = conn.execute(
        f"""SELECT pa.content_id, pa.platform, {queue_expr} AS queue_id, COUNT(*) AS attempts
            FROM publication_attempts pa
            WHERE pa.success = 0
              AND pa.content_id IN ({placeholders})
            GROUP BY pa.content_id, pa.platform, {queue_expr}""",
        content_ids,
    ).fetchall()
    counts: dict[tuple[int, str, int | None], int] = {}
    for row in rows:
        queue_id = row["queue_id"]
        key = (
            int(row["content_id"]),
            row["platform"],
            int(queue_id) if queue_id is not None else None,
        )
        counts[key] = int(row["attempts"] or 0)
        aggregate_key = (key[0], key[1], None)
        counts[aggregate_key] = max(counts.get(aggregate_key, 0), counts[key])
    return counts


def _dead_letter_item(
    row: dict[str, Any],
    *,
    attempt_count: int,
    max_attempts: int,
) -> PublishDeadLetterRow | None:
    status = (row.get("queue_status") or row.get("publication_status") or "").lower()
    retry_count = max(
        _int(row.get("attempt_count")),
        _int(row.get("content_retry_count")),
        attempt_count,
    )
    if status == "held":
        terminal_reason = "held"
        last_error = row.get("hold_reason")
        failed_at = row.get("queue_failed_at")
        action = "review_hold"
    else:
        last_error = row.get("publication_error") or row.get("queue_error")
        raw_category = row.get("publication_error_category") or row.get("queue_error_category")
        category = (
            normalize_error_category(raw_category)
            if raw_category is not None
            else classify_publish_error(last_error, platform=row.get("platform"))
        )
        if category in TERMINAL_ERROR_CATEGORIES:
            terminal_reason = f"{category}_error"
        elif retry_count >= max_attempts:
            terminal_reason = "max_retries"
        else:
            return None
        failed_at = row.get("failed_at") or row.get("queue_failed_at")
        action = _operator_action(terminal_reason)

    return PublishDeadLetterRow(
        content_id=int(row["content_id"]),
        queue_id=_optional_int(row.get("queue_id")),
        publication_id=_optional_int(row.get("publication_id")),
        platform=str(row["platform"]),
        terminal_reason=terminal_reason,
        last_error=last_error,
        failed_at=failed_at,
        retry_count=retry_count,
        content_preview=_content_preview(row.get("content")),
        operator_action=action,
    )


def _report(
    generated_at: datetime,
    filters: dict[str, Any],
    items: list[PublishDeadLetterRow],
    missing_required_tables: list[str],
    unknown_optional_signals: list[str],
) -> dict[str, Any]:
    rows = [asdict(item) for item in items]
    by_platform = {platform: 0 for platform in SUPPORTED_PLATFORMS if platform != "all"}
    by_reason: dict[str, int] = {}
    for row in rows:
        by_platform.setdefault(row["platform"], 0)
        by_platform[row["platform"]] += 1
        by_reason[row["terminal_reason"]] = by_reason.get(row["terminal_reason"], 0) + 1
    return {
        "artifact_type": "publish_dead_letters",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "items": len(rows),
            "failed": sum(1 for row in rows if row["terminal_reason"] != "held"),
            "held": sum(1 for row in rows if row["terminal_reason"] == "held"),
            "by_platform": dict(sorted(by_platform.items())),
            "by_terminal_reason": dict(sorted(by_reason.items())),
        },
        "missing_required_tables": missing_required_tables,
        "unknown_optional_signals": unknown_optional_signals,
        "items": rows,
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row[0]
        schema[table] = {
            column[1]
            for column in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
    return schema


def _unknown_optional(schema: dict[str, set[str]]) -> list[str]:
    optional = []
    if "generated_content" not in schema:
        optional.append("generated_content")
    if "publication_attempts" not in schema:
        optional.append("publication_attempts")
    elif not {"content_id", "platform", "success"}.issubset(schema["publication_attempts"]):
        optional.append("publication_attempts.success")
    if "publish_queue" in schema and "hold_reason" not in schema["publish_queue"]:
        optional.append("publish_queue.hold_reason")
    return optional


def _content_join(schema: dict[str, set[str]], alias: str) -> str:
    if "generated_content" not in schema or "id" not in schema["generated_content"]:
        return ""
    return f"LEFT JOIN generated_content gc ON gc.id = {alias}.content_id"


def _content_expr(schema: dict[str, set[str]]) -> str:
    return _generated_expr(schema, "content")


def _generated_expr(schema: dict[str, set[str]], column: str, fallback: str = "NULL") -> str:
    columns = schema.get("generated_content", set())
    if "id" in columns and column in columns:
        return f"gc.{column}"
    return fallback


def _column_expr(
    alias: str,
    columns: set[str],
    column: str,
    fallback: str = "NULL",
) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _coalesce_expr(
    alias: str,
    columns: set[str],
    names: list[str],
    *,
    fallback: str,
) -> str:
    exprs = [_column_expr(alias, columns, name) for name in names if name in columns]
    if not exprs:
        return fallback
    if len(exprs) == 1:
        return exprs[0]
    return f"COALESCE({', '.join(exprs)})"


def _target_platforms(queue_platform: Any) -> tuple[str, ...]:
    if queue_platform == "all":
        return ("x", "bluesky")
    if queue_platform in {"x", "bluesky"}:
        return (str(queue_platform),)
    return ("unknown",)


def _operator_action(terminal_reason: str) -> str:
    if terminal_reason == "auth_error":
        return "fix_credentials"
    if terminal_reason == "duplicate_error":
        return "cancel_duplicate"
    if terminal_reason == "media_error":
        return "fix_media"
    if terminal_reason == "validation_error":
        return "fix_content"
    if terminal_reason == "max_retries":
        return "manual_replay_or_cancel"
    return "inspect_error"


def _content_preview(content: Any, width: int = 96) -> str:
    text = " ".join(str(content or "").split())
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return _int(value)


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"
