"""Report queued content without a usable persona guard result."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_LIMIT = 100
DEFAULT_MIN_AGE_HOURS = 0
DEFAULT_STATUS = "queued"
PASSING_STATUSES = {"pass", "passed", "ok", "approved", "clear", "cleared"}


def build_content_persona_guard_queue_gaps_report(
    queue_rows: list[dict[str, Any]],
    *,
    status: str | Iterable[str] = DEFAULT_STATUS,
    min_age_hours: int = DEFAULT_MIN_AGE_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic report from loaded queue/persona-guard rows."""
    if min_age_hours < 0:
        raise ValueError("min_age_hours must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    statuses = _normalize_filter(status, default=DEFAULT_STATUS)
    stale_before = generated_at - timedelta(hours=min_age_hours)
    gap_items: list[dict[str, Any]] = []
    scanned = 0

    for row in queue_rows:
        queue_status = _clean(row.get("queue_status")).lower() or "unknown"
        if statuses != ("all",) and queue_status not in statuses:
            continue
        queued_at = _queued_at(row)
        if min_age_hours and queued_at is not None and queued_at > stale_before:
            continue
        scanned += 1
        issues = _guard_issues(row)
        if not issues:
            continue
        gap_items.append(
            {
                "source": _clean(row.get("source"), "unknown"),
                "queue_id": _int_or_none(row.get("queue_id")),
                "publication_id": _int_or_none(row.get("publication_id")),
                "content_id": _int_or_none(row.get("content_id")),
                "content_type": _clean(row.get("content_type"), "unknown"),
                "platform": _normalize_platform(row.get("platform")),
                "queue_status": queue_status,
                "scheduled_at": row.get("scheduled_at"),
                "queued_at": queued_at.isoformat() if queued_at else None,
                "guard_status": _guard_status(row),
                "guard_checked": _bool_or_none(row.get("guard_checked")),
                "guard_passed": _bool_or_none(row.get("guard_passed")),
                "guard_score": _float_or_none(row.get("guard_score")),
                "guard_updated_at": row.get("guard_updated_at") or row.get("guard_created_at"),
                "issue_types": issues,
                "content_excerpt": _excerpt(row.get("content")),
            }
        )

    gap_items.sort(key=_gap_sort_key)
    shown = gap_items[:limit]
    grouped_counts = _grouped_counts(gap_items)
    issue_counts = dict(sorted(Counter(issue for item in gap_items for issue in item["issue_types"]).items()))
    return {
        "artifact_type": "content_persona_guard_queue_gaps",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "status": list(statuses),
            "min_age_hours": min_age_hours,
            "stale_before": stale_before.isoformat(),
            "limit": limit,
        },
        "summary": {
            "rows_scanned": scanned,
            "gap_count": len(gap_items),
            "shown_count": len(shown),
            "by_issue_type": issue_counts,
        },
        "gap_items": shown,
        "grouped_counts": grouped_counts,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items())},
    }


def build_content_persona_guard_queue_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    rows: list[dict[str, Any]] = []
    rows.extend(_load_publish_queue_rows(conn, schema, missing_tables, missing_columns))
    rows.extend(_load_content_publication_rows(conn, schema, missing_tables, missing_columns))
    if "content_persona_guard" not in schema:
        missing_tables.append("content_persona_guard")
    elif "content_id" not in schema["content_persona_guard"]:
        missing_columns["content_persona_guard"] = ["content_id"]
    if "generated_content" not in schema:
        missing_tables.append("generated_content")
    elif "id" not in schema["generated_content"]:
        missing_columns["generated_content"] = ["id"]
    return build_content_persona_guard_queue_gaps_report(
        rows,
        missing_tables=sorted(set(missing_tables)),
        missing_columns=missing_columns,
        **kwargs,
    )


def format_content_persona_guard_queue_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_persona_guard_queue_gaps_text(report: dict[str, Any]) -> str:
    thresholds = report["thresholds"]
    summary = report["summary"]
    lines = [
        "Content Persona Guard Queue Gaps",
        f"Generated: {report['generated_at']}",
        f"Status: {', '.join(thresholds['status'])}",
        f"Min age: {thresholds['min_age_hours']} hours",
        f"Limit: {thresholds['limit']}",
        (
            "Totals: "
            f"scanned={summary['rows_scanned']} gaps={summary['gap_count']} "
            f"shown={summary['shown_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + "; ".join(f"{table}({', '.join(columns)})" for table, columns in report["missing_columns"].items())
        )
    if not report["gap_items"]:
        lines.append("No queued content persona guard gaps found.")
        return "\n".join(lines)

    lines.extend(["", "Grouped counts:"])
    for group in report["grouped_counts"]:
        lines.append(f"- platform={group['platform']} guard_status={group['guard_status']} count={group['count']}")
    lines.extend(["", "source | id | content_id | platform | status | guard | issues | scheduled_at"])
    for item in report["gap_items"]:
        row_id = item["queue_id"] if item["queue_id"] is not None else item["publication_id"]
        lines.append(
            f"{item['source']} | {row_id or '-'} | {item['content_id'] or '-'} | "
            f"{item['platform']} | {item['queue_status']} | {item['guard_status']} | "
            f"{','.join(item['issue_types'])} | {item['scheduled_at'] or '-'}"
        )
    return "\n".join(lines)


def _load_publish_queue_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> list[dict[str, Any]]:
    if "publish_queue" not in schema:
        missing_tables.append("publish_queue")
        return []
    columns = schema["publish_queue"]
    if "content_id" not in columns:
        missing_columns["publish_queue"] = ["content_id"]
        return []
    select = [
        _expr(columns, "id", "pq", "queue_id", "NULL"),
        "NULL AS publication_id",
        "pq.content_id AS content_id",
        _expr(columns, "platform", "pq", "platform", "'unknown'"),
        _expr(columns, "status", "pq", "queue_status", "'queued'"),
        _expr(columns, "scheduled_at", "pq", "scheduled_at", "NULL"),
        _expr(columns, "created_at", "pq", "queue_created_at", "NULL"),
        "'publish_queue' AS source",
        *_content_select(schema),
        *_guard_select(schema),
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(select)}
            FROM publish_queue pq
            {_content_join(schema, 'pq')}
            {_guard_join(schema, 'pq')}
            ORDER BY pq.rowid ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _load_content_publication_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> list[dict[str, Any]]:
    if "content_publications" not in schema:
        missing_tables.append("content_publications")
        return []
    columns = schema["content_publications"]
    if "content_id" not in columns:
        missing_columns["content_publications"] = ["content_id"]
        return []
    select = [
        "NULL AS queue_id",
        _expr(columns, "id", "cp", "publication_id", "NULL"),
        "cp.content_id AS content_id",
        _expr(columns, "platform", "cp", "platform", "'unknown'"),
        _expr(columns, "status", "cp", "queue_status", "'queued'"),
        _expr(columns, "next_retry_at", "cp", "scheduled_at", "NULL"),
        _expr(columns, "updated_at", "cp", "queue_created_at", "NULL"),
        "'content_publications' AS source",
        *_content_select(schema),
        *_guard_select(schema),
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(select)}
            FROM content_publications cp
            {_content_join(schema, 'cp')}
            {_guard_join(schema, 'cp')}
            ORDER BY cp.rowid ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _content_select(schema: dict[str, set[str]]) -> list[str]:
    columns = schema.get("generated_content", set())
    if "generated_content" not in schema or "id" not in columns:
        return ["NULL AS content_type", "NULL AS content"]
    return [
        _expr(columns, "content_type", "gc", "content_type", "'unknown'"),
        _expr(columns, "content", "gc", "content", "NULL"),
    ]


def _guard_select(schema: dict[str, set[str]]) -> list[str]:
    columns = schema.get("content_persona_guard", set())
    if "content_persona_guard" not in schema or "content_id" not in columns:
        return [
            "NULL AS guard_content_id",
            "NULL AS guard_checked",
            "NULL AS guard_passed",
            "NULL AS guard_status",
            "NULL AS guard_score",
            "NULL AS guard_reasons",
            "NULL AS guard_metrics",
            "NULL AS guard_created_at",
            "NULL AS guard_updated_at",
        ]
    return [
        "cpg.content_id AS guard_content_id",
        _expr(columns, "checked", "cpg", "guard_checked", "0"),
        _expr(columns, "passed", "cpg", "guard_passed", "0"),
        _expr(columns, "status", "cpg", "guard_status", "'unknown'"),
        _expr(columns, "score", "cpg", "guard_score", "NULL"),
        _expr(columns, "reasons", "cpg", "guard_reasons", "NULL"),
        _expr(columns, "metrics", "cpg", "guard_metrics", "NULL"),
        _expr(columns, "created_at", "cpg", "guard_created_at", "NULL"),
        _expr(columns, "updated_at", "cpg", "guard_updated_at", "NULL"),
    ]


def _content_join(schema: dict[str, set[str]], source_alias: str) -> str:
    if "generated_content" in schema and "id" in schema["generated_content"]:
        return f"LEFT JOIN generated_content gc ON gc.id = {source_alias}.content_id"
    return ""


def _guard_join(schema: dict[str, set[str]], source_alias: str) -> str:
    if "content_persona_guard" in schema and "content_id" in schema["content_persona_guard"]:
        return f"LEFT JOIN content_persona_guard cpg ON cpg.content_id = {source_alias}.content_id"
    return ""


def _guard_issues(row: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if row.get("guard_content_id") is None:
        return ["missing_guard"]
    if not _truthy(row.get("guard_checked")):
        issues.append("unchecked_guard")
    passed = _truthy(row.get("guard_passed"))
    status = _guard_status(row)
    if not passed or status not in PASSING_STATUSES:
        issues.append("failed_guard")
    if not _valid_json(row.get("guard_reasons"), expected=(list, dict), allow_empty=True):
        issues.append("malformed_reasons_json")
    if not _valid_json(row.get("guard_metrics"), expected=(dict,), allow_empty=True):
        issues.append("malformed_metrics_json")
    return issues


def _grouped_counts(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str]] = Counter()
    for item in items:
        for issue in item["issue_types"]:
            counts[(item["platform"], issue)] += 1
    return [
        {"platform": platform, "guard_status": guard_status, "count": count}
        for (platform, guard_status), count in sorted(counts.items(), key=lambda item: (item[0][0], item[0][1]))
    ]


def _queued_at(row: dict[str, Any]) -> datetime | None:
    return _parse_dt(row.get("scheduled_at")) or _parse_dt(row.get("queue_created_at"))


def _guard_status(row: dict[str, Any]) -> str:
    if row.get("guard_content_id") is None:
        return "missing"
    return _clean(row.get("guard_status"), "unknown").lower()


def _gap_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    queued_at = _parse_dt(item.get("queued_at"))
    ts = queued_at.timestamp() if queued_at else float("inf")
    return (item["platform"], ts, item["source"], item["content_id"] or 0, item["queue_id"] or item["publication_id"] or 0)


def _normalize_filter(value: str | Iterable[str], *, default: str) -> tuple[str, ...]:
    if isinstance(value, str):
        raw = value or default
        parts = [part.strip().lower() for part in raw.split(",")]
    else:
        parts = [str(part).strip().lower() for part in value]
    normalized = tuple(sorted({part for part in parts if part}))
    return normalized or (default,)


def _valid_json(value: Any, *, expected: tuple[type, ...], allow_empty: bool) -> bool:
    if value in (None, ""):
        return allow_empty
    if isinstance(value, expected):
        return True
    try:
        decoded = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return False
    return isinstance(decoded, expected)


def _expr(columns: set[str], column: str, alias: str, output: str, default: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    for candidate in (text, text.replace(" ", "T", 1)):
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        return _utc(parsed)
    return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _normalize_platform(value: Any) -> str:
    return _clean(value, "unknown").lower()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "pass", "passed"}


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    return _truthy(value)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return round(float(value), 4)
    except (TypeError, ValueError):
        return None


def _excerpt(value: Any, limit: int = 120) -> str:
    text = " ".join(_clean(value).split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "..."
