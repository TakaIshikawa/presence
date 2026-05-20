"""Report duplicate successful publication attempts by content and platform."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_PLATFORM = "all"
SUCCESS_STATUSES = {"ok", "published", "sent", "success", "succeeded"}
REQUIRED_COLUMNS = {"content_id", "platform"}
SUCCESS_COLUMNS = {"success", "status", "outcome", "result"}


def build_publication_attempt_duplicate_success_report(
    attempt_rows: list[dict[str, Any]],
    *,
    platform: str = DEFAULT_PLATFORM,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a duplicate successful publication attempts report from row dicts."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    normalized_platform = _clean(platform).lower() or DEFAULT_PLATFORM
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    scanned = 0

    for row in attempt_rows:
        row_platform = _clean(row.get("platform")).lower() or "unknown"
        if normalized_platform != DEFAULT_PLATFORM and row_platform != normalized_platform:
            continue
        scanned += 1
        if not _is_success(row):
            continue
        groups[(_clean(row.get("content_id")), row_platform)].append(row)

    findings: list[dict[str, Any]] = []
    platform_counts: Counter[str] = Counter()
    for (content_id, row_platform), rows in groups.items():
        if len(rows) < 2:
            continue
        post_ids = _distinct_values(rows, "platform_post_id")
        urls = _distinct_values(rows, "platform_url")
        finding = {
            "content_id": content_id,
            "platform": row_platform,
            "successful_attempt_count": len(rows),
            "post_id_conflict": len(post_ids) > 1,
            "url_conflict": len(urls) > 1,
            "platform_post_ids": post_ids,
            "platform_urls": urls,
            "attempts": [_attempt_summary(row) for row in sorted(rows, key=_attempt_sort_key)],
        }
        findings.append(finding)
        platform_counts[row_platform] += 1

    findings.sort(key=_finding_sort_key)
    shown = findings[:limit]
    conflict_count = sum(1 for finding in findings if finding["post_id_conflict"] or finding["url_conflict"])
    return {
        "artifact_type": "publication_attempt_duplicate_success",
        "generated_at": generated_at.isoformat(),
        "filters": {"platform": normalized_platform, "limit": limit},
        "summary": {
            "attempt_count": scanned,
            "duplicate_group_count": len(findings),
            "conflict_group_count": conflict_count,
            "shown_count": len(shown),
            "by_platform": dict(sorted(platform_counts.items())),
        },
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
        },
        "findings": shown,
        "empty_state": {
            "is_empty": not findings,
            "message": "No duplicate successful publication attempts found." if not findings else None,
        },
    }


def build_publication_attempt_duplicate_success_report_from_db(
    db_or_conn: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    """Load successful publication attempts from SQLite and build the report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get("publication_attempts")
    if columns is None:
        return build_publication_attempt_duplicate_success_report(
            [],
            missing_tables=["publication_attempts"],
            **kwargs,
        )

    missing = sorted(REQUIRED_COLUMNS - columns)
    if not columns.intersection(SUCCESS_COLUMNS):
        missing.append("success/status")
    if missing:
        return build_publication_attempt_duplicate_success_report(
            [],
            missing_columns={"publication_attempts": missing},
            **kwargs,
        )

    return build_publication_attempt_duplicate_success_report(
        _load_successful_attempts(conn, columns),
        **kwargs,
    )


def format_publication_attempt_duplicate_success_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_duplicate_success_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Publication Attempt Duplicate Success",
        f"Generated: {report['generated_at']}",
        f"Platform: {report['filters']['platform']}",
        f"Limit: {report['filters']['limit']}",
        (
            "Totals: "
            f"attempts={summary['attempt_count']} "
            f"duplicate_groups={summary['duplicate_group_count']} "
            f"conflict_groups={summary['conflict_group_count']} "
            f"shown={summary['shown_count']}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in report["missing_columns"].items()
        ]
        lines.append("Missing columns: " + "; ".join(missing))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "content_id | platform | successes | post_id_conflict | url_conflict | post_ids | urls"])
    for finding in report["findings"]:
        lines.append(
            f"{finding['content_id']} | {finding['platform']} | "
            f"{finding['successful_attempt_count']} | "
            f"{finding['post_id_conflict']} | {finding['url_conflict']} | "
            f"{', '.join(finding['platform_post_ids']) or '-'} | "
            f"{', '.join(finding['platform_urls']) or '-'}"
        )
    return "\n".join(lines)


def _load_successful_attempts(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    id_expr = _column_expr(columns, "id", fallback="NULL")
    attempted_at_expr = _column_expr(columns, "attempted_at", "created_at", "published_at", fallback="NULL")
    post_id_expr = _column_expr(columns, "platform_post_id", "post_id", fallback="NULL")
    url_expr = _column_expr(columns, "platform_url", "url", fallback="NULL")
    status_expr = _column_expr(columns, "status", "outcome", "result", fallback="NULL")
    success_filter = _success_filter(columns)
    rows = conn.execute(
        f"""SELECT
               {id_expr} AS attempt_id,
               content_id,
               platform,
               {attempted_at_expr} AS attempted_at,
               {post_id_expr} AS platform_post_id,
               {url_expr} AS platform_url,
               {status_expr} AS status,
               {_column_expr(columns, "success", fallback="NULL")} AS success
           FROM publication_attempts
           WHERE {success_filter}
           ORDER BY platform ASC, content_id ASC, {attempted_at_expr} ASC, {id_expr} ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _success_filter(columns: set[str]) -> str:
    filters: list[str] = []
    if "success" in columns:
        filters.append("(success = 1 OR lower(CAST(success AS TEXT)) IN ('true', 'yes'))")
    for column in ("status", "outcome", "result"):
        if column in columns:
            values = ", ".join(f"'{status}'" for status in sorted(SUCCESS_STATUSES))
            filters.append(f"lower(CAST({column} AS TEXT)) IN ({values})")
    return "(" + " OR ".join(filters) + ")"


def _is_success(row: dict[str, Any]) -> bool:
    success = row.get("success")
    if success is not None and _clean(success).lower() in {"1", "true", "yes"}:
        return True
    for key in ("status", "outcome", "result"):
        if _clean(row.get(key)).lower() in SUCCESS_STATUSES:
            return True
    return False


def _attempt_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "attempt_id": row.get("attempt_id", row.get("id")),
        "attempted_at": row.get("attempted_at"),
        "platform_post_id": row.get("platform_post_id"),
        "platform_url": row.get("platform_url"),
    }


def _distinct_values(rows: list[dict[str, Any]], key: str) -> list[str]:
    values = {_clean(row.get(key)) for row in rows if _clean(row.get(key))}
    return sorted(values)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[0]): {
            str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")
        }
        for row in rows
    }


def _column_expr(columns: set[str], *columns_to_try: str, fallback: str) -> str:
    for column in columns_to_try:
        if column in columns:
            return column
    return fallback


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    return (
        0 if finding["post_id_conflict"] or finding["url_conflict"] else 1,
        finding["platform"],
        _int_or_text(finding["content_id"]),
    )


def _attempt_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (_clean(row.get("attempted_at")), _int_or_text(row.get("attempt_id", row.get("id"))))


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_text(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, "" if value is None else str(value))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
