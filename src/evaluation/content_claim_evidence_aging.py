"""Find generated content with stale claim-check evidence."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_DAYS = 30
DEFAULT_LIMIT = 50


def build_content_claim_evidence_aging_report(
    db_or_conn: Any,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return old content-claim checks ranked by publication risk."""
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {"stale_days": stale_days, "limit": limit}
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or _missing_join_columns(missing_columns):
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    rows = _load_rows(conn, schema)
    issues = []
    for row in rows:
        checked_at = _parse_dt(row.get("checked_at"))
        age_days = _age_days(checked_at, generated_at)
        if checked_at is not None and age_days < stale_days:
            continue
        issues.append(_issue(row, checked_at, age_days, stale_days))

    issues.sort(key=_sort_key)
    issues = issues[:limit]
    return {
        "artifact_type": "content_claim_evidence_aging",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "claim_check_count": len(rows),
            "issue_count": len(issues),
            "by_content_status": dict(sorted(Counter(item["content_status"] for item in issues).items())),
            "by_severity": dict(sorted(Counter(item["severity"] for item in issues).items())),
        },
        "issues": issues,
        "missing_tables": [],
        "missing_columns": {
            table: list(columns) for table, columns in sorted(missing_columns.items())
        },
    }


def format_content_claim_evidence_aging_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_claim_evidence_aging_text(report: dict[str, Any]) -> str:
    lines = [
        "Content Claim Evidence Aging",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: stale_days={report['filters']['stale_days']} "
            f"limit={report['filters']['limit']}"
        ),
        (
            f"Totals: checks={report['totals']['claim_check_count']} "
            f"issues={report['totals']['issue_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append(f"Missing tables: {', '.join(report['missing_tables'])}")
    if report["missing_columns"]:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report["missing_columns"].items())
        ]
        lines.append(f"Missing columns: {'; '.join(missing)}")
    if not report["issues"]:
        lines.extend(["", "No stale content-claim evidence found."])
        return "\n".join(lines)

    lines.extend(["", "Issues:"])
    for item in report["issues"]:
        lines.append(
            f"- claim_check_id={item['claim_check_id']} content_id={item['content_id']} "
            f"type={item['content_type']} status={item['content_status']} "
            f"age_days={item['age_days']} severity={item['severity']}"
        )
        lines.append(f"  recommendation: {item['recommendation']}")
        lines.append(f"  claim: {item['claim'] or '-'}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    gc = schema["generated_content"]
    cc = schema["content_claim_checks"]
    pq = schema.get("publish_queue", set())
    queue_join = ""
    queue_status_expr = "NULL"
    if {"content_id", "status"}.issubset(pq):
        queue_join = (
            "LEFT JOIN publish_queue pq ON pq.content_id = gc.id "
            "AND LOWER(COALESCE(pq.status, 'queued')) IN ('queued', 'scheduled', 'pending', 'held')"
        )
        queue_status_expr = "pq.status"
    checked_expr = (
        f"COALESCE({_column_expr(cc, 'checked_at', alias='cc')}, "
        f"{_column_expr(cc, 'updated_at', alias='cc')}, "
        f"{_column_expr(cc, 'created_at', alias='cc')})"
    )
    rows = conn.execute(
        f"""SELECT
               {_column_expr(cc, "id", "cc.content_id", alias="cc")} AS claim_check_id,
               cc.content_id AS content_id,
               {_column_expr(gc, "content_type", "'unknown'", alias="gc")} AS content_type,
               {_column_expr(gc, "content", "NULL", alias="gc")} AS content,
               {_column_expr(gc, "title", "NULL", alias="gc")} AS title,
               {_column_expr(gc, "metadata", "NULL", alias="gc")} AS metadata,
               {_column_expr(gc, "status", "NULL", alias="gc")} AS status,
               {queue_status_expr} AS queue_status,
               {_column_expr(gc, "published", "0", alias="gc")} AS published,
               {_column_expr(gc, "published_at", "NULL", alias="gc")} AS published_at,
               {_column_expr(cc, "annotation_text", "NULL", alias="cc")} AS annotation_text,
               {_column_expr(cc, "claim", "NULL", alias="cc")} AS claim,
               {_column_expr(cc, "summary", "NULL", alias="cc")} AS summary,
               {_column_expr(cc, "unsupported_count", "0", alias="cc")} AS unsupported_count,
               {checked_expr} AS checked_at
           FROM content_claim_checks cc
           INNER JOIN generated_content gc ON gc.id = cc.content_id
           {queue_join}
           ORDER BY cc.content_id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _issue(
    row: dict[str, Any],
    checked_at: datetime | None,
    age_days: int,
    stale_days: int,
) -> dict[str, Any]:
    status = _content_status(row)
    severity = _severity(age_days, stale_days, status, _int(row.get("unsupported_count")))
    return {
        "claim_check_id": _text(row.get("claim_check_id")) or _text(row.get("content_id")),
        "content_id": int(row["content_id"]),
        "content_type": _text(row.get("content_type")) or "unknown",
        "claim": _claim_text(row),
        "checked_at": checked_at.isoformat() if checked_at else None,
        "age_days": age_days,
        "content_status": status,
        "severity": severity,
        "recommendation": _recommendation(severity, status),
    }


def _content_status(row: dict[str, Any]) -> str:
    queue_status = _text(row.get("queue_status")).lower()
    if queue_status in {"queued", "scheduled", "pending", "held"}:
        return "queued"
    status = _text(row.get("status")).lower()
    if status in {"queued", "queue", "scheduled", "pending", "ready"}:
        return "queued"
    if _truthy(row.get("published")) or row.get("published_at"):
        return "published"
    if status in {"published", "live"}:
        return "published"
    return "unpublished"


def _severity(age_days: int, stale_days: int, content_status: str, unsupported_count: int) -> str:
    if checked_at_missing_age(age_days) or unsupported_count > 0 or (
        content_status in {"queued", "unpublished"} and age_days >= stale_days * 2
    ):
        return "high"
    if content_status in {"queued", "unpublished"}:
        return "medium"
    return "low"


def checked_at_missing_age(age_days: int) -> bool:
    return age_days >= 999999


def _recommendation(severity: str, content_status: str) -> str:
    if content_status == "published" and severity == "low":
        return "ignore until the next substantive update"
    if content_status == "published":
        return "recheck before reusing or promoting the published content"
    if severity == "high":
        return "hold publication and recheck claim evidence"
    return "recheck claim evidence before publication"


def _claim_text(row: dict[str, Any]) -> str:
    metadata = _json_obj(row.get("metadata"))
    for value in (
        row.get("claim"),
        row.get("summary"),
        row.get("annotation_text"),
        metadata.get("claim"),
        metadata.get("claim_summary"),
        row.get("title"),
        row.get("content"),
    ):
        text = _text(value)
        if text:
            return text[:240]
    return ""


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    status_rank = {"queued": 0, "unpublished": 1, "published": 2}
    severity_rank = {"high": 0, "medium": 1, "low": 2}
    return (
        status_rank.get(item["content_status"], 9),
        severity_rank.get(item["severity"], 9),
        -item["age_days"],
        item["content_id"],
    )


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    expected = {
        "generated_content": {"id"},
        "content_claim_checks": {"content_id"},
    }
    missing_tables = [table for table in expected if table not in schema]
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _missing_join_columns(missing_columns: dict[str, list[str]]) -> bool:
    return "id" in missing_columns.get("generated_content", []) or "content_id" in missing_columns.get("content_claim_checks", [])


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "artifact_type": "content_claim_evidence_aging",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "claim_check_count": 0,
            "issue_count": 0,
            "by_content_status": {},
            "by_severity": {},
        },
        "issues": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _column_expr(columns: set[str], column: str, fallback: str = "NULL", *, alias: str) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _age_days(value: datetime | None, now: datetime) -> int:
    if value is None:
        return 999999
    return max(0, int((now - value).total_seconds() // 86400))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "published"}
    return bool(value)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value).strip()


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}
