"""Audit knowledge.metadata JSON shape by source type."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
ARTIFACT_TYPE = "knowledge_metadata_schema_drift"
DEFAULT_SOURCE_TYPES = ("all",)
REASONS = (
    "malformed_metadata",
    "non_object_metadata",
    "missing_expected_source_fields",
    "restricted_license_missing_provenance",
)
EXPECTED_FIELD_GROUPS = {
    "curated_x": (("post_id", "tweet_id", "platform_post_id"), ("author", "handle", "username")),
    "curated_article": (("title",), ("author", "byline"), ("published_at", "publishedAt", "date")),
    "curated_newsletter": (("title", "subject"), ("newsletter", "publication"), ("issue_date", "published_at", "date")),
    "own_conversation": (("conversation_id", "thread_id"), ("participant", "participants", "speaker")),
}
RESTRICTED_LICENSES = {"restricted", "attribution_required"}
PROVENANCE_KEYS = ("provenance", "provenance_url", "source_url", "original_url", "attribution", "citation")


def build_knowledge_metadata_schema_drift_report(
    rows: list[dict[str, Any]],
    *,
    source_types: list[str] | tuple[str, ...] | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | str | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic knowledge metadata schema drift report from rows."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _coerce_ts(now) if now is not None else datetime.now(timezone.utc)
    source_filter = _source_filter(source_types)
    matched = [_normalize(row) for row in rows if _matches_source(_clean(row.get("source_type")).lower() or "unknown", source_filter)]
    findings = []
    for row in matched:
        reasons, details = _drift_reasons(row)
        if reasons:
            findings.append({**row, "drift_reasons": reasons, "details": details})
    findings.sort(key=_sort_key)
    shown = findings[:limit]
    reason_counts = Counter(reason for finding in findings for reason in finding["drift_reasons"])
    source_counts = Counter(row["source_type"] for row in matched)
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"source_types": list(source_filter), "limit": limit},
        "totals": {
            "knowledge_count": len(matched),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_source_type": dict(sorted(source_counts.items())),
            "by_drift_reason": {reason: reason_counts[reason] for reason in REASONS},
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
        "empty_state": {
            "is_empty": not findings,
            "message": "No knowledge metadata schema drift found." if not findings else None,
        },
    }


def build_knowledge_metadata_schema_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load knowledge rows from SQLite and build the metadata schema drift report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get("knowledge")
    if columns is None:
        return build_knowledge_metadata_schema_drift_report([], missing_tables=["knowledge"], **kwargs)
    missing_required = sorted({"id", "source_type"} - columns)
    optional_missing = [column for column in ("source_id", "source_url", "license", "metadata") if column not in columns]
    missing_columns = {"knowledge": missing_required + optional_missing} if missing_required or optional_missing else {}
    rows = [] if missing_required else _load_rows(conn, columns)
    return build_knowledge_metadata_schema_drift_report(rows, missing_columns=missing_columns, **kwargs)


def format_knowledge_metadata_schema_drift_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_knowledge_metadata_schema_drift_text(report: dict[str, Any]) -> str:
    """Render the report as terminal-friendly text."""
    totals = report["totals"]
    lines = [
        "Knowledge Metadata Schema Drift",
        f"Generated: {report['generated_at']}",
        f"Filters: source_types={','.join(report['filters']['source_types'])} limit={report['filters']['limit']}",
        f"Totals: knowledge={totals['knowledge_count']} findings={totals['finding_count']} shown={totals['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "knowledge_id | source_type | source_id | source_url | license | drift_reasons"])
    for finding in report["findings"]:
        lines.append(
            f"{finding['knowledge_id']} | {finding['source_type']} | {_display(finding['source_id'])} | "
            f"{_display(finding['source_url'])} | {_display(finding['license'])} | {','.join(finding['drift_reasons'])}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        '"id" AS knowledge_id',
        '"source_type" AS source_type',
        _expr(columns, "source_id", "source_id", "NULL"),
        _expr(columns, "source_url", "source_url", "NULL"),
        _expr(columns, "license", "license", "NULL"),
        _expr(columns, "metadata", "metadata", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM knowledge ORDER BY id ASC").fetchall()]


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "knowledge_id": _int_or_none(row.get("knowledge_id") or row.get("id")) or 0,
        "source_type": _clean(row.get("source_type")).lower() or "unknown",
        "source_id": _clean(row.get("source_id")) or None,
        "source_url": _clean(row.get("source_url")) or None,
        "license": _clean(row.get("license")).lower() or None,
        "metadata": row.get("metadata"),
    }


def _drift_reasons(row: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
    reasons = []
    details: dict[str, Any] = {}
    metadata, error = _metadata_object(row["metadata"])
    if error == "malformed_metadata":
        return ["malformed_metadata"], {"error": "metadata is not valid JSON"}
    if error == "non_object_metadata":
        return ["non_object_metadata"], {"error": "metadata must be a JSON object"}

    missing_groups = _missing_expected_groups(row["source_type"], metadata)
    if missing_groups:
        reasons.append("missing_expected_source_fields")
        details["missing_expected_field_groups"] = missing_groups
    if row["license"] in RESTRICTED_LICENSES and not any(_metadata_value(metadata, key) for key in PROVENANCE_KEYS):
        reasons.append("restricted_license_missing_provenance")
    return reasons, details


def _metadata_object(raw: Any) -> tuple[dict[str, Any], str | None]:
    if raw is None or _clean(raw) == "":
        return {}, None
    if isinstance(raw, dict):
        return raw, None
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError):
        return {}, "malformed_metadata"
    if not isinstance(parsed, dict):
        return {}, "non_object_metadata"
    return parsed, None


def _missing_expected_groups(source_type: str, metadata: dict[str, Any]) -> list[list[str]]:
    groups = EXPECTED_FIELD_GROUPS.get(source_type)
    if not groups:
        return []
    return [list(group) for group in groups if not any(_metadata_value(metadata, key) for key in group)]


def _metadata_value(metadata: dict[str, Any], key: str) -> str:
    value = metadata.get(key)
    if isinstance(value, dict):
        return "1" if any(_clean(child) for child in value.values()) else ""
    if isinstance(value, (list, tuple)):
        return "1" if any(_clean(item) for item in value) else ""
    return _clean(value)


def _source_filter(source_types: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    if not source_types:
        return DEFAULT_SOURCE_TYPES
    normalized = tuple(sorted({_clean(item).lower() for item in source_types if _clean(item)}))
    return normalized or DEFAULT_SOURCE_TYPES


def _matches_source(source_type: str, source_filter: tuple[str, ...]) -> bool:
    return source_filter == DEFAULT_SOURCE_TYPES or source_type in source_filter


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    first_reason = item["drift_reasons"][0]
    return (REASONS.index(first_reason), item["source_type"], item["knowledge_id"])


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
