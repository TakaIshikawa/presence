"""Report metadata hygiene issues for generated content variants."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
PROVENANCE_FIELDS = (
    "source_id",
    "source_url",
    "source",
    "source_title",
    "author",
    "author_id",
    "citation",
    "reference_id",
    "knowledge_source_id",
)


def build_content_variant_metadata_hygiene_report(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
) -> dict[str, Any]:
    """Build a deterministic content variant metadata hygiene report."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    issues: list[dict[str, Any]] = []
    for row in rows:
        metadata, metadata_error = _parse_metadata(row.get("metadata"))
        if metadata_error:
            issues.append(_issue(row, metadata_error))
            continue
        if not isinstance(metadata, dict):
            issues.append(_issue(row, "non_object_metadata"))
            continue
        if _selected(row) and not _has_provenance(metadata):
            issues.append(_issue(row, "missing_selected_provenance"))
        row_platform = _norm(row.get("platform"))
        metadata_platform = _norm(_first_present(metadata, "platform", "target_platform", "channel"))
        if row_platform and metadata_platform and row_platform != metadata_platform:
            issues.append(_issue(row, "platform_conflict", declared_value=metadata_platform))
        row_variant_type = _norm(row.get("variant_type"))
        metadata_variant_type = _norm(_first_present(metadata, "variant_type", "content_type", "type"))
        if row_variant_type and metadata_variant_type and row_variant_type != metadata_variant_type:
            issues.append(_issue(row, "variant_type_conflict", declared_value=metadata_variant_type))

    issues.sort(key=lambda item: (_issue_order(item["issue_type"]), str(item["variant_id"])))
    shown = issues[:limit]
    counts = Counter(issue["issue_type"] for issue in issues)
    return {
        "artifact_type": "content_variant_metadata_hygiene",
        "generated_at": generated_at.isoformat(),
        "thresholds": {"limit": limit, "provenance_fields": list(PROVENANCE_FIELDS)},
        "summary": {
            "row_count": len(rows),
            "issue_count": len(issues),
            "shown_count": len(shown),
            "by_issue_type": dict(sorted(counts.items())),
        },
        "missing_tables": sorted(missing_tables or []),
        "issue_items": shown,
        "empty_state": {"is_empty": not issues, "message": "No content variant metadata hygiene issues found." if not issues else None},
    }


def build_content_variant_metadata_hygiene_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load content variants from SQLite while tolerating older schemas."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "content_variants" not in schema:
        return build_content_variant_metadata_hygiene_report([], missing_tables=["content_variants"], **kwargs)
    return build_content_variant_metadata_hygiene_report(_load_rows(conn, schema["content_variants"]), **kwargs)


def format_content_variant_metadata_hygiene_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_variant_metadata_hygiene_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Content Variant Metadata Hygiene",
        f"Generated: {report['generated_at']}",
        f"Thresholds: limit={report['thresholds']['limit']}",
        f"Totals: rows={summary['row_count']} issues={summary['issue_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["issue_items"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("Issues:")
    for item in report["issue_items"]:
        lines.append(
            "  - "
            f"{item['issue_type']} variant={item['variant_id']} "
            f"platform={item['platform']} variant_type={item['variant_type']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    aliases = {
        "variant_id": ("id", "variant_id"),
        "content_id": ("content_id", "generated_content_id"),
        "metadata": ("metadata",),
        "selected": ("selected", "is_selected", "winner", "selected_flag"),
        "platform": ("platform", "channel"),
        "variant_type": ("variant_type", "content_type", "type"),
    }
    select = ", ".join(f"{_coalesce(columns, names)} AS {alias}" for alias, names in aliases.items())
    order = _coalesce(columns, ("id", "variant_id", "created_at"))
    rows = conn.execute(f"SELECT {select} FROM content_variants ORDER BY {order} ASC").fetchall()
    return [dict(row) for row in rows]


def _issue(row: dict[str, Any], issue_type: str, *, declared_value: str | None = None) -> dict[str, Any]:
    item = {
        "issue_type": issue_type,
        "variant_id": row.get("variant_id"),
        "content_id": row.get("content_id"),
        "platform": _norm(row.get("platform")) or "unknown",
        "variant_type": _norm(row.get("variant_type")) or "unknown",
        "selected": _selected(row),
    }
    if declared_value is not None:
        item["declared_value"] = declared_value
    return item


def _parse_metadata(value: Any) -> tuple[Any, str | None]:
    if value is None:
        return {}, None
    if isinstance(value, (dict, list)):
        return value, None
    text = str(value).strip()
    if not text:
        return {}, None
    try:
        return json.loads(text), None
    except json.JSONDecodeError:
        return None, "malformed_metadata"


def _has_provenance(metadata: dict[str, Any]) -> bool:
    return any(metadata.get(field) not in (None, "") for field in PROVENANCE_FIELDS)


def _selected(row: dict[str, Any]) -> bool:
    value = row.get("selected")
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "selected", "winner"}


def _first_present(metadata: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in metadata:
            return metadata[name]
    return None


def _norm(value: Any) -> str | None:
    text = "" if value is None else str(value).strip().lower()
    return text or None


def _coalesce(columns: set[str], names: tuple[str, ...]) -> str:
    available = [name for name in names if name in columns]
    return "COALESCE(" + ", ".join(available) + ")" if len(available) > 1 else (available[0] if available else "NULL")


def _issue_order(issue_type: str) -> int:
    return {
        "malformed_metadata": 0,
        "non_object_metadata": 1,
        "missing_selected_provenance": 2,
        "platform_conflict": 3,
        "variant_type_conflict": 4,
    }.get(issue_type, 99)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
