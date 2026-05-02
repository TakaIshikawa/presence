"""Report coverage of Cultivate relationship context on queued replies."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Mapping, Sequence


DEFAULT_DAYS = 7
DEFAULT_STATUS = ("pending",)
DEFAULT_MIN_STRENGTH = 0.0
COVERAGE_FIELDS = ("stage", "tier", "strength", "last_interaction_at", "notes")


def build_relationship_context_coverage_report(
    db_or_conn: Any,
    *,
    status: str | Sequence[str] = DEFAULT_STATUS,
    platform: str | Sequence[str] | None = None,
    days: int = DEFAULT_DAYS,
    min_strength: float = DEFAULT_MIN_STRENGTH,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return relationship_context coverage gaps for reply_queue rows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_strength < 0:
        raise ValueError("min_strength must be non-negative")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    statuses = _normalise_filter(status)
    platforms = _normalise_filter(platform)
    filters = {
        "status": list(statuses),
        "platform": list(platforms),
        "days": days,
        "cutoff": cutoff.isoformat(),
        "min_strength": min_strength,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    if "reply_queue" not in schema:
        missing_tables.append("reply_queue")
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    required = {"id", "relationship_context"}
    missing = sorted(required - schema["reply_queue"])
    if missing:
        missing_columns["reply_queue"] = missing
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    rows = _load_reply_rows(
        conn,
        schema["reply_queue"],
        statuses=statuses,
        platforms=platforms,
        cutoff=cutoff,
    )
    findings = [
        _coverage_row(row, min_strength=min_strength)
        for row in rows
    ]
    affected = [row for row in findings if row["missing_fields"] or row["malformed_context"]]
    affected.sort(key=lambda row: (row.get("detected_at") or "", row["reply_queue_id"]))

    by_field = Counter(
        field
        for row in affected
        for field in row["missing_fields"]
    )
    ids_by_field = {
        field: [row["reply_queue_id"] for row in affected if field in row["missing_fields"]]
        for field in COVERAGE_FIELDS
    }
    return {
        "artifact_type": "relationship_context_coverage",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "scanned_count": len(rows),
            "complete_count": len(rows) - len(affected),
            "affected_count": len(affected),
            "malformed_count": sum(1 for row in affected if row["malformed_context"]),
            "by_missing_field": {field: by_field.get(field, 0) for field in COVERAGE_FIELDS},
        },
        "missing_fields": [
            {
                "field": field,
                "count": by_field.get(field, 0),
                "reply_queue_ids": ids_by_field[field],
            }
            for field in COVERAGE_FIELDS
        ],
        "affected_reply_queue_ids": [row["reply_queue_id"] for row in affected],
        "rows": affected,
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def format_relationship_context_coverage_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_relationship_context_coverage_text(report: dict[str, Any]) -> str:
    """Render a compact human-readable coverage report."""
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Relationship Context Coverage",
        (
            "Filters: "
            f"status={_display_filter(filters.get('status'))} "
            f"platform={_display_filter(filters.get('platform'))} "
            f"days={filters.get('days')} "
            f"min_strength={filters.get('min_strength')}"
        ),
        (
            f"Scanned: {totals['scanned_count']} "
            f"complete={totals['complete_count']} "
            f"affected={totals['affected_count']} "
            f"malformed={totals['malformed_count']}"
        ),
        "Missing fields: "
        + ", ".join(
            f"{field}={totals['by_missing_field'].get(field, 0)}"
            for field in COVERAGE_FIELDS
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        formatted = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report["missing_columns"].items())
        ]
        lines.append("Missing columns: " + "; ".join(formatted))
    if not report["rows"]:
        lines.append("No relationship context coverage gaps found.")
        return "\n".join(lines)
    lines.append("")
    for row in report["rows"]:
        handle = row.get("inbound_author_handle") or "unknown"
        fields = ",".join(row["missing_fields"]) or "malformed"
        lines.append(
            f"#{row['reply_queue_id']} platform={row.get('platform') or 'unknown'} "
            f"status={row.get('status') or 'unknown'} author={handle} "
            f"missing={fields}"
        )
    return "\n".join(lines)


def _coverage_row(row: Mapping[str, Any], *, min_strength: float) -> dict[str, Any]:
    context, parse_error = _parse_context(row.get("relationship_context"))
    missing = list(COVERAGE_FIELDS) if parse_error else _missing_fields(context, min_strength)
    strength = _float_or_none(_first_value(context, "relationship_strength", "strength"))
    return {
        "reply_queue_id": _int_or_zero(row.get("id")),
        "platform": _clean(row.get("platform")),
        "status": _clean(row.get("status")),
        "detected_at": _clean(row.get("detected_at")),
        "inbound_author_handle": _clean(row.get("inbound_author_handle")),
        "inbound_tweet_id": _clean(row.get("inbound_tweet_id")),
        "missing_fields": missing,
        "malformed_context": parse_error is not None,
        "parse_error": parse_error,
        "relationship_strength": strength,
    }


def _missing_fields(context: Mapping[str, Any], min_strength: float) -> list[str]:
    missing: list[str] = []
    if _first_value(context, "engagement_stage", "stage") is None:
        missing.append("stage")
    if _first_value(context, "dunbar_tier", "tier") is None:
        missing.append("tier")
    strength = _float_or_none(_first_value(context, "relationship_strength", "strength"))
    if strength is None or strength < min_strength:
        missing.append("strength")
    if _first_value(context, "last_interaction_at", "last_interaction", "last_seen_at") is None:
        missing.append("last_interaction_at")
    if not _has_notes_like_context(context):
        missing.append("notes")
    return missing


def _has_notes_like_context(context: Mapping[str, Any]) -> bool:
    if _first_value(
        context,
        "relationship_notes",
        "relationship_note",
        "relationship_summary",
        "context_notes",
        "notes",
    ) is not None:
        return True
    recent = context.get("recent_interactions")
    if not isinstance(recent, list):
        return False
    for item in recent:
        if isinstance(item, Mapping) and _first_value(item, "text", "summary", "body"):
            return True
    return False


def _load_reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    statuses: tuple[str, ...],
    platforms: tuple[str, ...],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    where: list[str] = []
    params: list[Any] = []
    if "status" in columns and statuses:
        where.append(f"LOWER(COALESCE(status, '')) IN ({_placeholders(statuses)})")
        params.extend(statuses)
    if "platform" in columns and platforms:
        where.append(f"LOWER(COALESCE(platform, '')) IN ({_placeholders(platforms)})")
        params.extend(platforms)
    if "detected_at" in columns:
        where.append("(detected_at IS NULL OR datetime(detected_at) >= datetime(?))")
        params.append(cutoff.isoformat())

    select_columns = [
        _column_expr(columns, "id"),
        _column_expr(columns, "relationship_context"),
        _column_expr(columns, "platform"),
        _column_expr(columns, "status"),
        _column_expr(columns, "detected_at"),
        _column_expr(columns, "inbound_author_handle"),
        _column_expr(columns, "inbound_tweet_id"),
    ]
    query = f"SELECT {', '.join(select_columns)} FROM reply_queue"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY " + _order_clause(columns)
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _parse_context(value: Any) -> tuple[dict[str, Any], str | None]:
    if isinstance(value, Mapping):
        return dict(value), None
    if value is None or str(value).strip() == "":
        return {}, None
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError as exc:
        return {}, f"relationship_context is not valid JSON: {exc.msg}"
    if not isinstance(parsed, dict):
        return {}, "relationship_context must be a JSON object"
    return parsed, None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in tables}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
    }


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "artifact_type": "relationship_context_coverage",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "scanned_count": 0,
            "complete_count": 0,
            "affected_count": 0,
            "malformed_count": 0,
            "by_missing_field": {field: 0 for field in COVERAGE_FIELDS},
        },
        "missing_fields": [
            {"field": field, "count": 0, "reply_queue_ids": []}
            for field in COVERAGE_FIELDS
        ],
        "affected_reply_queue_ids": [],
        "rows": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _normalise_filter(value: str | Sequence[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        values = (value,)
    else:
        values = tuple(value)
    return tuple(sorted({item.strip().casefold() for item in values if item.strip()}))


def _first_value(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip() != "":
            return value
    return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_zero(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _display_filter(value: Any) -> str:
    if not value:
        return "all"
    if isinstance(value, (list, tuple)):
        return ",".join(str(item) for item in value)
    return str(value)


def _column_expr(columns: set[str], column: str) -> str:
    if column in columns:
        return _quote_identifier(column)
    return f"NULL AS {_quote_identifier(column)}"


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) ASC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _placeholders(values: Sequence[str]) -> str:
    return ",".join("?" for _ in values)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
