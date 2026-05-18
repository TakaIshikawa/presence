"""Audit publication attempt links back to publish queue rows."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
DEFAULT_PLATFORM = "all"
ISSUE_TYPES = (
    "missing_queue",
    "content_id_mismatch",
    "successful_attempt_unpublished_queue",
)
OPEN_QUEUE_STATUSES = {"queued", "held", "failed"}
SUCCESS_STATUSES = {"success", "succeeded", "published", "sent", "ok"}


def build_publication_attempt_queue_link_gaps_report(
    attempt_rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    platform: str = DEFAULT_PLATFORM,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    normalized_platform = _clean(platform).lower() or DEFAULT_PLATFORM
    findings: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    group_counts: dict[str, Counter[str]] = defaultdict(Counter)
    scanned = 0

    for row in attempt_rows:
        row_platform = _clean(_first(row, "platform", "queue_platform")).lower() or "unknown"
        if normalized_platform != "all" and row_platform != normalized_platform:
            continue
        scanned += 1
        issues = _issues_for_row(row)
        for issue_type in issues:
            finding = {
                "attempt_id": _first(row, "attempt_id", "id"),
                "queue_id": row.get("queue_id"),
                "content_id": row.get("content_id"),
                "queue_content_id": row.get("queue_content_id"),
                "queue_status": row.get("queue_status"),
                "platform": row_platform,
                "attempt_status": row.get("attempt_status"),
                "issue_type": issue_type,
            }
            findings.append(finding)
            counts[issue_type] += 1
            group_counts[row_platform][issue_type] += 1

    findings.sort(key=_finding_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": "publication_attempt_queue_link_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "platform": normalized_platform, "limit": limit},
        "summary": {
            "attempt_count": scanned,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES},
            "by_platform_issue_type": {
                platform_key: {issue_type: counter[issue_type] for issue_type in ISSUE_TYPES}
                for platform_key, counter in sorted(group_counts.items())
            },
        },
        "missing_tables": sorted(missing_tables or []),
        "findings": shown,
        "empty_state": {
            "is_empty": not findings,
            "message": "No publication attempt queue link gaps found." if not findings else None,
        },
    }


def build_publication_attempt_queue_link_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {"publication_attempts", "publish_queue", "generated_content"}
    missing = sorted(required - set(schema))
    if missing:
        return build_publication_attempt_queue_link_gaps_report([], missing_tables=missing, **kwargs)
    days = int(kwargs.get("days", DEFAULT_DAYS))
    now = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)
    return build_publication_attempt_queue_link_gaps_report(
        _load_attempts(conn, schema, cutoff=cutoff),
        **kwargs,
    )


def format_publication_attempt_queue_link_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_queue_link_gaps_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Publication Attempt Queue Link Gaps",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['days']} days",
        f"Platform: {report['filters']['platform']}",
        f"Limit: {report['filters']['limit']}",
        f"Totals: attempts={summary['attempt_count']} findings={summary['finding_count']} shown={summary['shown_count']}",
        "Issue counts: "
        + ", ".join(f"{issue_type}={summary['by_issue_type'].get(issue_type, 0)}" for issue_type in ISSUE_TYPES),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "attempt_id | queue_id | platform | content_id | queue_content_id | queue_status | issue_type"])
    for finding in report["findings"]:
        lines.append(
            f"{finding['attempt_id'] or '-'} | {finding['queue_id'] or '-'} | {finding['platform']} | "
            f"{finding['content_id'] or '-'} | {finding['queue_content_id'] or '-'} | "
            f"{finding['queue_status'] or '-'} | {finding['issue_type']}"
        )
    return "\n".join(lines)


def _issues_for_row(row: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    queue_id = row.get("queue_id")
    queue_exists = row.get("resolved_queue_id") is not None
    if queue_id not in (None, "") and not queue_exists:
        issues.append("missing_queue")
    if queue_exists and _clean(row.get("content_id")) != _clean(row.get("queue_content_id")):
        issues.append("content_id_mismatch")
    if (
        queue_exists
        and _clean(row.get("attempt_status")).lower() in SUCCESS_STATUSES
        and _clean(row.get("queue_status")).lower() in OPEN_QUEUE_STATUSES
    ):
        issues.append("successful_attempt_unpublished_queue")
    return issues


def _load_attempts(conn: sqlite3.Connection, schema: dict[str, set[str]], *, cutoff: datetime) -> list[dict[str, Any]]:
    pa_cols = schema["publication_attempts"]
    pq_cols = schema["publish_queue"]
    if not {"id", "queue_id", "content_id"}.issubset(pa_cols):
        return []
    attempted_at = _column_expr(pa_cols, "attempted_at", "created_at", "published_at", "updated_at", fallback="NULL", alias="pa")
    filters = ["pa.queue_id IS NOT NULL"]
    params: list[Any] = []
    if attempted_at != "NULL":
        filters.append(f"({attempted_at} IS NULL OR datetime({attempted_at}) >= datetime(?))")
        params.append(cutoff.isoformat())
    status = _column_expr(pa_cols, "status", "outcome", "result", fallback="NULL", alias="pa")
    platform = _column_expr(pa_cols, "platform", "channel", fallback=_column_expr(pq_cols, "platform", "target_platform", fallback="'unknown'", alias="pq"), alias="pa")
    rows = conn.execute(
        f"""SELECT
               pa.id AS attempt_id,
               pa.queue_id AS queue_id,
               pq.id AS resolved_queue_id,
               pa.content_id AS content_id,
               pq.content_id AS queue_content_id,
               pq.status AS queue_status,
               {status} AS attempt_status,
               {platform} AS platform,
               gc.id AS resolved_content_id,
               {attempted_at} AS attempted_at
           FROM publication_attempts pa
           LEFT JOIN publish_queue pq ON pq.id = pa.queue_id
           LEFT JOIN generated_content gc ON gc.id = pa.content_id
           WHERE {' AND '.join(filters)}
           ORDER BY {attempted_at} DESC, pa.id DESC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], *columns_to_try: str, fallback: str, alias: str) -> str:
    for column in columns_to_try:
        if column in columns:
            return f"{alias}.{column}"
    return fallback


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    return (
        finding["platform"],
        ISSUE_TYPES.index(finding["issue_type"]) if finding["issue_type"] in ISSUE_TYPES else len(ISSUE_TYPES),
        _int_or_text(finding.get("queue_id")),
        _int_or_text(finding.get("attempt_id")),
    )


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    return None


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
