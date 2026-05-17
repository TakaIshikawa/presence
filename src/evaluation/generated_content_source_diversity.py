"""Report source type diversity for generated content."""

from __future__ import annotations

from collections import Counter
import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any


DEFAULT_LIMIT = 50
SOURCE_TYPES = ("activity", "commit", "url", "note")
URL_RE = re.compile(r"https?://[^\s)>\"]+")


def build_generated_content_source_diversity_report(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    records = []
    totals = Counter()
    for row in rows:
        source_counts = _source_counts(row)
        populated_types = [name for name in SOURCE_TYPES if source_counts[name] > 0]
        classification = "multi_source_type" if len(populated_types) > 1 else "single_source_type" if len(populated_types) == 1 else "no_source_type"
        totals[classification] += 1
        for source_type, count in source_counts.items():
            totals[f"{source_type}_source_count"] += count
        records.append(
            {
                "content_id": _text(_first(row, "content_id", "id")) or "unknown",
                "content_type": _text(_first(row, "content_type", "format", "type")) or None,
                "title": _text(_first(row, "title", "subject")) or None,
                "source_type_counts": dict(source_counts),
                "source_type_count": len(populated_types),
                "source_classification": classification,
            }
        )

    records.sort(key=lambda item: (-item["source_type_count"], item["content_id"]))
    shown = records[:limit]
    content_count = len(records)
    multi = totals["multi_source_type"]
    single = totals["single_source_type"]
    return {
        "artifact_type": "generated_content_source_diversity",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "totals": {
            "content_count": content_count,
            "shown_count": len(shown),
            "single_source_type": single,
            "multi_source_type": multi,
            "no_source_type": totals["no_source_type"],
            "single_source_type_rate": round(single / content_count, 4) if content_count else 0.0,
            "multi_source_type_rate": round(multi / content_count, 4) if content_count else 0.0,
            "source_reference_counts": {name: totals[f"{name}_source_count"] for name in SOURCE_TYPES},
        },
        "contents": shown,
        "empty_state": {"is_empty": not records, "message": "No generated content rows found." if not records else None},
    }


def build_generated_content_source_diversity_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    return build_generated_content_source_diversity_report(_load_rows(conn, _schema(conn)), **kwargs)


def format_generated_content_source_diversity_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_generated_content_source_diversity_text(report: dict[str, Any]) -> str:
    lines = [
        "Generated Content Source Diversity",
        f"Generated: {report['generated_at']}",
        f"Limit: {report['filters']['limit']}",
        (
            f"Totals: content={report['totals']['content_count']} "
            f"multi={report['totals']['multi_source_type']} "
            f"single={report['totals']['single_source_type']} "
            f"none={report['totals']['no_source_type']} "
            f"multi_rate={report['totals']['multi_source_type_rate']:.2f}"
        ),
    ]
    if not report["contents"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "content_id | type | source_types | activity | commit | url | note | classification"])
    for content in report["contents"]:
        counts = content["source_type_counts"]
        lines.append(
            f"{content['content_id']} | {content['content_type'] or '-'} | {content['source_type_count']} | "
            f"{counts['activity']} | {counts['commit']} | {counts['url']} | {counts['note']} | {content['source_classification']}"
        )
    return "\n".join(lines)


format_generated_content_source_diversity_table = format_generated_content_source_diversity_text


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    cols = schema["generated_content"]
    selected = [
        _col(cols, "id", "content_id", default="NULL") + " AS content_id",
        _col(cols, "content_type", "format", "type", default="NULL") + " AS content_type",
        _col(cols, "title", "subject", default="NULL") + " AS title",
        _col(cols, "content", "body", "text", "draft", default="NULL") + " AS content",
        _col(cols, "metadata", "raw_metadata", default="NULL") + " AS metadata",
        _col(cols, "source_activity_ids", "activity_ids", default="NULL") + " AS source_activity_ids",
        _col(cols, "source_commits", "commit_shas", default="NULL") + " AS source_commits",
        _col(cols, "source_urls", "urls", default="NULL") + " AS source_urls",
        _col(cols, "source_notes", "notes", default="NULL") + " AS source_notes",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content").fetchall()]


def _source_counts(row: dict[str, Any]) -> Counter[str]:
    metadata = _json_object(_first(row, "metadata"))
    counts: Counter[str] = Counter({name: 0 for name in SOURCE_TYPES})
    counts["activity"] = len(_items(_first(row, "source_activity_ids", "activity_ids") or metadata.get("source_activity_ids") or metadata.get("activity_ids")))
    counts["commit"] = len(_items(_first(row, "source_commits", "commit_shas") or metadata.get("source_commits") or metadata.get("commit_shas")))
    explicit_urls = _items(_first(row, "source_urls", "urls") or metadata.get("source_urls") or metadata.get("urls"))
    embedded_urls = URL_RE.findall(_text(_first(row, "content", "body", "text", "draft")))
    counts["url"] = len(set(explicit_urls + embedded_urls))
    counts["note"] = len(_items(_first(row, "source_notes", "notes") or metadata.get("source_notes") or metadata.get("notes")))
    return counts


def _items(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, (list, tuple, set)):
        return [_text(item) for item in value if _text(item)]
    if isinstance(value, dict):
        return [_text(key) for key in value if _text(key)]
    text = _text(value)
    parsed = _json_value(text)
    if isinstance(parsed, list):
        return [_text(item) for item in parsed if _text(item)]
    if isinstance(parsed, dict):
        return [_text(key) for key in parsed if _text(key)]
    return [part.strip() for part in re.split(r"[,;\n]+", text) if part.strip()]


def _json_object(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, (dict, list)) else {}


def _json_value(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _col(columns: set[str], *names: str, default: str = "NULL") -> str:
    for name in names:
        if name in columns:
            return name
    return default


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
