"""Flag content that relies too heavily on quoted source text."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_MAX_QUOTE_DENSITY = 0.25
QUOTE_RE = re.compile(r'"([^"]+)"|“([^”]+)”|‘([^’]+)’')


def build_source_quote_density_report(
    rows: list[dict[str, Any]],
    *,
    max_quote_density: float = DEFAULT_MAX_QUOTE_DENSITY,
    now: datetime | None = None,
) -> dict[str, Any]:
    if max_quote_density < 0 or max_quote_density > 1:
        raise ValueError("max_quote_density must be between 0 and 1")
    generated_at = _utc(now or datetime.now(timezone.utc))
    items = [_density_item(row) for row in rows]
    flagged = [item for item in items if item["quote_density"] > max_quote_density]
    flagged.sort(key=lambda item: (-item["quote_density"], item["content_id"]))
    return {
        "artifact_type": "source_quote_density",
        "generated_at": generated_at.isoformat(),
        "filters": {"max_quote_density": max_quote_density},
        "totals": {"rows_scanned": len(rows), "item_count": len(items), "flagged_count": len(flagged)},
        "flagged_items": flagged,
        "empty_state": {"is_empty": not flagged, "message": "No content exceeds the quote density limit." if not flagged else None},
    }


def build_source_quote_density_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_source_quote_density_report(_load_rows(conn, schema), **kwargs)


def format_source_quote_density_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_source_quote_density_text(report: dict[str, Any]) -> str:
    lines = [
        "Source Quote Density",
        f"Generated: {report['generated_at']}",
        f"Max quote density: {report['filters']['max_quote_density']}",
        f"Totals: items={report['totals']['item_count']} flagged={report['totals']['flagged_count']}",
    ]
    if not report["flagged_items"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "Flagged items:"])
    for item in report["flagged_items"]:
        lines.append(
            f"- {item['content_id']} type={item['content_type']} density={item['quote_density']} "
            f"spans={item['quoted_span_count']} reason={item['reason']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    columns = schema["generated_content"]
    selected = [
        _col(columns, "id", "content_id") + " AS content_id",
        _col(columns, "content_type", "content_format", default="'unknown'") + " AS content_type",
        _col(columns, "content", "body", "text", default="''") + " AS content",
        _col(columns, "source_excerpt", "source_excerpts", "excerpt", default="NULL") + " AS source_excerpt",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content").fetchall()]


def _density_item(row: dict[str, Any]) -> dict[str, Any]:
    content = _text(_first(row, "content", "body", "text"))
    source_excerpt = _text(_first(row, "source_excerpt", "source_excerpts", "source_text"))
    quote_spans = _quote_spans(content)
    source_matches = _source_excerpt_spans(content, source_excerpt)
    quoted_chars = sum(len(span) for span in quote_spans) + sum(len(span) for span in source_matches)
    total_chars = max(len(content), 1)
    density = round(min(quoted_chars / total_chars, 1), 4)
    return {
        "content_id": _text(_first(row, "content_id", "id")) or "unknown",
        "content_type": _text(_first(row, "content_type", "content_format")) or "unknown",
        "quote_density": density,
        "quoted_span_count": len(quote_spans) + len(source_matches),
        "reason": f"Quote-like spans account for {density:.0%} of the content.",
    }


def _quote_spans(content: str) -> list[str]:
    spans = []
    for match in QUOTE_RE.finditer(content):
        span = next((group for group in match.groups() if group), "")
        if span.strip():
            spans.append(span.strip())
    return spans


def _source_excerpt_spans(content: str, source_excerpt: str) -> list[str]:
    if not source_excerpt:
        return []
    normalized_content = content.lower()
    spans = []
    for part in re.split(r"\n+|\s{2,}", source_excerpt):
        cleaned = part.strip()
        if len(cleaned) >= 24 and cleaned.lower() in normalized_content:
            spans.append(cleaned)
    return spans


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
