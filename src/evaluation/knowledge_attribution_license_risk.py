"""Report publication risk from knowledge attribution and licenses."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_MIN_RELEVANCE = 0.5
ARTIFACT_TYPE = "knowledge_attribution_license_risk"
REASONS = ("restricted_license", "missing_attribution", "low_relevance")
RESTRICTED_LICENSES = {
    "restricted",
    "do_not_publish",
    "private",
    "proprietary",
    "all_rights_reserved",
    "noncommercial",
    "non-commercial",
    "cc-by-nc",
    "cc_by_nc",
    "no_reuse",
}
KNOWLEDGE_COLUMNS = ("source_type", "source_id", "author", "source_url", "license", "attribution_required")


def build_knowledge_attribution_license_risk_report(
    rows: list[dict[str, Any]],
    *,
    min_relevance: float = DEFAULT_MIN_RELEVANCE,
    limit: int = DEFAULT_LIMIT,
    now: datetime | str | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic attribution and license risk report from link rows."""
    if min_relevance < 0:
        raise ValueError("min_relevance must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _coerce_ts(now) if now is not None else datetime.now(timezone.utc)
    normalized = [_normalize(row) for row in rows]
    findings = []
    for row in normalized:
        for reason in _risk_reasons(row, min_relevance):
            findings.append({**row, "reason": reason})

    findings.sort(key=_sort_key)
    shown = findings[:limit]
    counts = Counter(item["reason"] for item in findings)
    usage_counts = Counter(row["usage_type"] for row in normalized)
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit, "min_relevance": min_relevance},
        "totals": {
            "usage_count": len(normalized),
            "content_usage_count": usage_counts["content"],
            "reply_usage_count": usage_counts["reply"],
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_reason": {reason: counts[reason] for reason in REASONS},
        },
        "findings": _group_findings(shown),
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
        "empty_state": {
            "is_empty": not findings,
            "message": "No knowledge attribution license risks found." if not findings else None,
        },
    }


def build_knowledge_attribution_license_risk_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load linked content/reply knowledge rows from SQLite and build the report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = []
    missing_columns: dict[str, list[str]] = {}

    knowledge_columns = schema.get("knowledge")
    if knowledge_columns is None:
        missing_tables.append("knowledge")
    else:
        required = {"id"}
        optional = set(KNOWLEDGE_COLUMNS)
        missing = sorted((required | optional) - knowledge_columns)
        if missing:
            missing_columns["knowledge"] = missing

    content_columns = schema.get("content_knowledge_links")
    if content_columns is None:
        missing_tables.append("content_knowledge_links")
    else:
        required = {"content_id", "knowledge_id"}
        optional = {"id", "relevance_score", "created_at"}
        missing = sorted((required | optional) - content_columns)
        if missing:
            missing_columns["content_knowledge_links"] = missing

    reply_columns = schema.get("reply_knowledge_links")
    if reply_columns is None:
        missing_tables.append("reply_knowledge_links")
    else:
        required = {"reply_queue_id", "knowledge_id"}
        optional = {"id", "relevance_score", "created_at"}
        missing = sorted((required | optional) - reply_columns)
        if missing:
            missing_columns["reply_knowledge_links"] = missing

    rows: list[dict[str, Any]] = []
    can_load_knowledge = knowledge_columns is not None and "id" in knowledge_columns
    if can_load_knowledge and content_columns is not None and {"content_id", "knowledge_id"}.issubset(content_columns):
        rows.extend(_load_content_rows(conn, schema, knowledge_columns, content_columns))
    if can_load_knowledge and reply_columns is not None and {"reply_queue_id", "knowledge_id"}.issubset(reply_columns):
        rows.extend(_load_reply_rows(conn, schema, knowledge_columns, reply_columns))

    return build_knowledge_attribution_license_risk_report(
        rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_knowledge_attribution_license_risk_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_knowledge_attribution_license_risk_text(report: dict[str, Any]) -> str:
    """Render the report as terminal-friendly text."""
    totals = report["totals"]
    lines = [
        "Knowledge Attribution License Risk",
        f"Generated: {report['generated_at']}",
        f"Filters: min_relevance={report['filters']['min_relevance']} limit={report['filters']['limit']}",
        (
            "Totals: "
            f"usage={totals['usage_count']} content={totals['content_usage_count']} "
            f"replies={totals['reply_usage_count']} findings={totals['finding_count']} shown={totals['shown_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "reason | usage_type | content_id | reply_queue_id | knowledge_id | source_type | source_id | author | source_url | license | attribution_required | relevance_score"])
    for group in report["findings"]:
        for item in group["items"]:
            lines.append(
                f"{item['reason']} | {item['usage_type']} | {_display(item['content_id'])} | {_display(item['reply_queue_id'])} | "
                f"{item['knowledge_id']} | {_display(item['source_type'])} | {_display(item['source_id'])} | "
                f"{_display(item['author'])} | {_display(item['source_url'])} | {_display(item['license'])} | "
                f"{item['attribution_required']} | {_display(item['relevance_score'])}"
            )
    return "\n".join(lines)


def _load_content_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    knowledge_columns: set[str],
    link_columns: set[str],
) -> list[dict[str, Any]]:
    generated_columns = schema.get("generated_content", set())
    select = _knowledge_select(knowledge_columns)
    selected = [
        "'content' AS usage_type",
        _column_expr(link_columns, "id", alias="ckl", fallback="NULL") + " AS link_id",
        "ckl.content_id AS content_id",
        "NULL AS reply_queue_id",
        "ckl.knowledge_id AS knowledge_id",
        _column_expr(link_columns, "relevance_score", alias="ckl", fallback="NULL") + " AS relevance_score",
        _column_expr(link_columns, "created_at", alias="ckl", fallback="NULL") + " AS linked_at",
        _column_expr(generated_columns, "content_type", alias="gc", fallback="NULL") + " AS content_type",
        "NULL AS reply_status",
        *select,
    ]
    join_generated = "LEFT JOIN generated_content gc ON gc.id = ckl.content_id" if generated_columns else ""
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(selected)}
                FROM content_knowledge_links ckl
                INNER JOIN knowledge k ON k.id = ckl.knowledge_id
                {join_generated}
                ORDER BY ckl.content_id ASC, ckl.knowledge_id ASC"""
        ).fetchall()
    ]


def _load_reply_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    knowledge_columns: set[str],
    link_columns: set[str],
) -> list[dict[str, Any]]:
    reply_columns = schema.get("reply_queue", set())
    selected = [
        "'reply' AS usage_type",
        _column_expr(link_columns, "id", alias="rkl", fallback="NULL") + " AS link_id",
        "NULL AS content_id",
        "rkl.reply_queue_id AS reply_queue_id",
        "rkl.knowledge_id AS knowledge_id",
        _column_expr(link_columns, "relevance_score", alias="rkl", fallback="NULL") + " AS relevance_score",
        _column_expr(link_columns, "created_at", alias="rkl", fallback="NULL") + " AS linked_at",
        "NULL AS content_type",
        _column_expr(reply_columns, "status", alias="rq", fallback="NULL") + " AS reply_status",
        *_knowledge_select(knowledge_columns),
    ]
    join_reply = "LEFT JOIN reply_queue rq ON rq.id = rkl.reply_queue_id" if reply_columns else ""
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(selected)}
                FROM reply_knowledge_links rkl
                INNER JOIN knowledge k ON k.id = rkl.knowledge_id
                {join_reply}
                ORDER BY rkl.reply_queue_id ASC, rkl.knowledge_id ASC"""
        ).fetchall()
    ]


def _knowledge_select(columns: set[str]) -> list[str]:
    return [
        _column_expr(columns, "source_type", alias="k", fallback="NULL") + " AS source_type",
        _column_expr(columns, "source_id", alias="k", fallback="NULL") + " AS source_id",
        _column_expr(columns, "author", alias="k", fallback="NULL") + " AS author",
        _column_expr(columns, "source_url", alias="k", fallback="NULL") + " AS source_url",
        _column_expr(columns, "license", alias="k", fallback="NULL") + " AS license",
        _column_expr(columns, "attribution_required", alias="k", fallback="NULL") + " AS attribution_required",
    ]


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "usage_type": _clean(row.get("usage_type")) or "content",
        "link_id": _int_or_none(row.get("link_id")),
        "content_id": _int_or_none(row.get("content_id")),
        "reply_queue_id": _int_or_none(row.get("reply_queue_id")),
        "knowledge_id": _int_or_none(row.get("knowledge_id")) or 0,
        "content_type": _clean(row.get("content_type")) or None,
        "reply_status": _clean(row.get("reply_status")) or None,
        "source_type": _clean(row.get("source_type")).lower() or None,
        "source_id": _clean(row.get("source_id")) or None,
        "author": _clean(row.get("author")) or None,
        "source_url": _clean(row.get("source_url")) or None,
        "license": _clean(row.get("license")).lower() or None,
        "attribution_required": _truthy(row.get("attribution_required")),
        "relevance_score": _float_or_none(row.get("relevance_score")),
        "linked_at": _clean(row.get("linked_at")) or None,
    }


def _risk_reasons(row: dict[str, Any], min_relevance: float) -> list[str]:
    reasons = []
    if row["license"] in RESTRICTED_LICENSES:
        reasons.append("restricted_license")
    if row["attribution_required"] and not (row["source_url"] or row["author"]):
        reasons.append("missing_attribution")
    if row["relevance_score"] is not None and row["relevance_score"] < min_relevance:
        reasons.append("low_relevance")
    return reasons


def _group_findings(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[item["reason"]].append(item)
    return [{"reason": reason, "count": len(grouped[reason]), "items": grouped[reason]} for reason in REASONS if reason in grouped]


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        REASONS.index(item["reason"]),
        item["usage_type"],
        _int_sort(item.get("content_id")),
        _int_sort(item.get("reply_queue_id")),
        _int_sort(item.get("knowledge_id")),
        _float_sort(item.get("relevance_score")),
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row["name"]): {str(column["name"]) for column in conn.execute(f"PRAGMA table_info({_quote(str(row['name']))})")} for row in rows}


def _column_expr(columns: set[str], column: str, *, alias: str, fallback: str) -> str:
    return f"{alias}.{_quote(column)}" if column in columns else fallback


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _display(value: Any) -> str:
    return "-" if value is None or value == "" else str(value)


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _truthy(value: Any) -> bool:
    return _clean(value).lower() in {"1", "true", "yes", "y", "required", "attribution_required"}


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_sort(value: Any) -> int:
    parsed = _int_or_none(value)
    return parsed if parsed is not None else -1


def _float_sort(value: Any) -> float:
    parsed = _float_or_none(value)
    return parsed if parsed is not None else -1.0


def _parse_ts(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    try:
        return _utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def _coerce_ts(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return _utc(value)
    parsed = _parse_ts(value)
    if parsed is None:
        raise ValueError(f"invalid timestamp: {value}")
    return parsed


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
