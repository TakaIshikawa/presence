"""Report knowledge timestamp normalization issues that affect freshness ranking."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_GRACE_DAYS = 1
DEFAULT_LIMIT = 100
ARTIFACT_TYPE = "knowledge_timestamp_normalization"
CURATED_SOURCE_TYPES = {"curated_article", "curated_newsletter"}
ISSUE_TYPES = (
    "invalid_published_at",
    "invalid_created_at",
    "published_at_in_future",
    "created_at_in_future",
    "published_at_after_created_at",
    "curated_source_url_missing_timestamp",
)


def build_knowledge_timestamp_normalization_report(
    rows: list[dict[str, Any]],
    *,
    now: datetime | str | None = None,
    grace_days: int = DEFAULT_GRACE_DAYS,
    limit: int = DEFAULT_LIMIT,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic timestamp hygiene report from knowledge rows."""
    if grace_days < 0:
        raise ValueError("grace_days must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _coerce_ts(now) if now is not None else datetime.now(timezone.utc)
    grace = timedelta(days=grace_days)
    findings: list[dict[str, Any]] = []
    for raw in rows:
        row = _normalize(raw)
        published = _parse_ts(row["published_at"])
        created = _parse_ts(row["created_at"])

        if row["published_at"] and published is None:
            findings.append(_finding(row, "invalid_published_at"))
        if row["created_at"] and created is None:
            findings.append(_finding(row, "invalid_created_at"))
        if published is not None and published > generated_at:
            findings.append(_finding(row, "published_at_in_future", days_delta=_days_delta(published, generated_at)))
        if created is not None and created > generated_at:
            findings.append(_finding(row, "created_at_in_future", days_delta=_days_delta(created, generated_at)))
        if published is not None and created is not None and published - created > grace:
            findings.append(_finding(row, "published_at_after_created_at", days_delta=_days_delta(published, created)))
        if row["source_type"] in CURATED_SOURCE_TYPES and row["source_url"] and not row["published_at"] and not row["created_at"]:
            findings.append(_finding(row, "curated_source_url_missing_timestamp"))

    findings.sort(key=_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"now": generated_at.isoformat(), "grace_days": grace_days, "limit": limit},
        "summary": {
            "knowledge_count": len(rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": {issue_type: Counter(item["issue_type"] for item in findings)[issue_type] for issue_type in ISSUE_TYPES},
        },
        "findings": _group_findings(shown),
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
        "empty_state": {
            "is_empty": not findings,
            "message": "No knowledge timestamp normalization issues found." if not findings else None,
        },
    }


def build_knowledge_timestamp_normalization_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load knowledge rows from SQLite and build the timestamp normalization report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "knowledge" not in schema:
        return build_knowledge_timestamp_normalization_report([], missing_tables=["knowledge"], **kwargs)
    columns = schema["knowledge"]
    required = {"id", "source_type"}
    missing_required = sorted(required - columns)
    optional_missing = [column for column in ("source_url", "author", "published_at", "created_at") if column not in columns]
    missing_columns = {"knowledge": missing_required + optional_missing} if missing_required or optional_missing else {}
    rows = [] if missing_required else _load_rows(conn, columns)
    return build_knowledge_timestamp_normalization_report(rows, missing_columns=missing_columns, **kwargs)


def format_knowledge_timestamp_normalization_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_knowledge_timestamp_normalization_text(report: dict[str, Any]) -> str:
    """Render the report as terminal-friendly text."""
    summary = report["summary"]
    lines = [
        "Knowledge Timestamp Normalization",
        f"Generated: {report['generated_at']}",
        f"Filters: now={report['filters']['now']} grace_days={report['filters']['grace_days']} limit={report['filters']['limit']}",
        f"Totals: knowledge={summary['knowledge_count']} findings={summary['finding_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "knowledge_id | source_type | issue_type | days_delta | published_at | created_at | source_url"])
    for group in report["findings"]:
        for item in group["items"]:
            lines.append(
                f"{item['knowledge_id']} | {item['source_type']} | {item['issue_type']} | "
                f"{_display(item.get('days_delta'))} | {_display(item['published_at'])} | "
                f"{_display(item['created_at'])} | {_display(item['source_url'])}"
            )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        '"id" AS knowledge_id',
        '"source_type" AS source_type',
        _expr(columns, "source_url", "source_url", "NULL"),
        _expr(columns, "author", "author", "NULL"),
        _expr(columns, "published_at", "published_at", "NULL"),
        _expr(columns, "created_at", "created_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM knowledge ORDER BY id ASC")]


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "knowledge_id": _int_or_none(row.get("knowledge_id") or row.get("id")) or 0,
        "source_type": _clean(row.get("source_type")) or "unknown",
        "source_url": _clean(row.get("source_url")) or None,
        "author": _clean(row.get("author")) or None,
        "published_at": _clean(row.get("published_at")) or None,
        "created_at": _clean(row.get("created_at")) or None,
    }


def _finding(row: dict[str, Any], issue_type: str, **extra: Any) -> dict[str, Any]:
    return {
        "knowledge_id": row["knowledge_id"],
        "source_type": row["source_type"],
        "source_url": row["source_url"],
        "author": row["author"],
        "published_at": row["published_at"],
        "created_at": row["created_at"],
        "issue_type": issue_type,
        "days_delta": extra.pop("days_delta", None),
        **extra,
    }


def _group_findings(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[item["issue_type"]].append(item)
    return [
        {"issue_type": issue_type, "count": len(grouped[issue_type]), "items": grouped[issue_type]}
        for issue_type in ISSUE_TYPES
        if issue_type in grouped
    ]


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (ISSUE_TYPES.index(item["issue_type"]), item["knowledge_id"])


def _parse_ts(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    for parser in (
        lambda raw: datetime.fromisoformat(raw.replace("Z", "+00:00")),
        lambda raw: datetime.strptime(raw, "%Y-%m-%d %H:%M:%S"),
        lambda raw: datetime.strptime(raw, "%Y-%m-%d"),
    ):
        try:
            return _utc(parser(text))
        except ValueError:
            continue
    return None


def _coerce_ts(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return _utc(value)
    parsed = _parse_ts(value)
    if parsed is None:
        raise ValueError(f"invalid timestamp: {value}")
    return parsed


def _days_delta(left: datetime, right: datetime) -> int:
    return int((left - right).total_seconds() // 86400)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({_quote(str(row[0]))})")} for row in rows}


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{_quote(column)} AS {output}" if column in columns else f"{default} AS {output}"


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _display(value: Any) -> str:
    return "-" if value is None or value == "" else str(value)


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
