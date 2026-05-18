"""Identify blog posts or drafts with weak source support."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_MIN_SOURCES = 2
DEFAULT_LIMIT = 100


def build_blog_post_source_gap_report(
    blog_rows: list[dict[str, Any]],
    *,
    min_sources: int = DEFAULT_MIN_SOURCES,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return source-gap findings for blog drafts/posts."""
    if min_sources < 0:
        raise ValueError("min_sources must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    findings = []
    gap_counts: Counter[str] = Counter()
    severity_counts: Counter[str] = Counter()
    for row in blog_rows:
        item = _item(row, min_sources=min_sources)
        if not item["gap_types"]:
            continue
        findings.append(item)
        severity_counts[item["severity"]] += 1
        gap_counts.update(item["gap_types"])

    findings.sort(key=lambda item: (_severity_rank(item["severity"]), -len(item["gap_types"]), item["item_id"]))
    return {
        "artifact_type": "blog_post_source_gap",
        "generated_at": generated_at.isoformat(),
        "filters": {"min_sources": min_sources, "limit": limit},
        "totals": {
            "items_scanned": len(blog_rows),
            "gap_item_count": len(findings),
            "gap_counts": dict(sorted(gap_counts.items())),
            "severity_counts": dict(sorted(severity_counts.items())),
        },
        "findings": findings[:limit],
        "empty_state": {
            "is_empty": not findings,
            "message": "No blog source gaps found." if not findings else None,
        },
    }


def build_blog_post_source_gap_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_blog_rows(conn, schema)
    report = build_blog_post_source_gap_report(rows, **kwargs)
    report["missing_tables"] = [] if rows or _has_blog_shape(schema) else ["blog_posts"]
    return report


def format_blog_post_source_gap_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_post_source_gap_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Blog Post Source Gap",
        f"Generated: {report['generated_at']}",
        f"Filters: min_sources={report['filters']['min_sources']} limit={report['filters']['limit']}",
        f"Totals: scanned={totals['items_scanned']} gaps={totals['gap_item_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "Gaps:"])
    for item in report["findings"]:
        lines.append(
            f"- {item['item_id']} severity={item['severity']} sources={item['source_count']} "
            f"missing={', '.join(item['gap_types'])} summary={item['missing_evidence_summary']}"
        )
    return "\n".join(lines)


format_blog_post_source_gap_table = format_blog_post_source_gap_text


def _item(row: dict[str, Any], *, min_sources: int) -> dict[str, Any]:
    source_count = _source_count(row)
    unsupported_sections = _unsupported_sections(row)
    gap_types = []
    if source_count == 0:
        gap_types.append("zero_sources")
    elif source_count < min_sources:
        gap_types.append("below_threshold_sources")
    if unsupported_sections:
        gap_types.append("unsupported_claim_sections")

    severity = "none"
    if "zero_sources" in gap_types and unsupported_sections:
        severity = "critical"
    elif "zero_sources" in gap_types:
        severity = "high"
    elif unsupported_sections and source_count < min_sources:
        severity = "high"
    elif unsupported_sections or "below_threshold_sources" in gap_types:
        severity = "medium"

    return {
        "item_id": _text(row.get("item_id") or row.get("blog_id") or row.get("draft_id") or row.get("post_id") or row.get("id")),
        "title": _text(row.get("title") or row.get("headline")),
        "status": _text(row.get("status") or row.get("state") or row.get("publication_status") or "unknown"),
        "channel": _text(row.get("channel") or row.get("platform") or "blog"),
        "content_type": _text(row.get("content_type") or row.get("type") or "blog"),
        "source_count": source_count,
        "required_source_count": min_sources,
        "unsupported_sections": unsupported_sections,
        "gap_types": gap_types,
        "severity": severity,
        "missing_evidence_summary": _summary(source_count, min_sources, unsupported_sections),
    }


def _summary(source_count: int, min_sources: int, unsupported_sections: list[dict[str, Any]]) -> str:
    parts = []
    if source_count == 0:
        parts.append("no source references")
    elif source_count < min_sources:
        parts.append(f"{source_count}/{min_sources} required sources")
    if unsupported_sections:
        labels = ", ".join(section["section_id"] for section in unsupported_sections)
        parts.append(f"unsupported claim-heavy sections: {labels}")
    return "; ".join(parts)


def _source_count(row: dict[str, Any]) -> int:
    explicit = row.get("source_count") or row.get("sources_count") or row.get("reference_count")
    if explicit not in (None, ""):
        try:
            return max(0, int(explicit))
        except (TypeError, ValueError):
            pass
    refs = _list(row.get("sources") or row.get("source_refs") or row.get("references") or row.get("citations"))
    return len([ref for ref in refs if _text(ref)])


def _unsupported_sections(row: dict[str, Any]) -> list[dict[str, Any]]:
    sections = _sections(row)
    unsupported = []
    for index, section in enumerate(sections, start=1):
        if not _claim_heavy(section):
            continue
        evidence = _list(
            section.get("evidence")
            or section.get("sources")
            or section.get("source_refs")
            or section.get("citations")
            or section.get("supporting_sources")
        )
        if evidence:
            continue
        unsupported.append(
            {
                "section_id": _text(section.get("section_id") or section.get("id") or section.get("heading") or f"section-{index}"),
                "heading": _text(section.get("heading") or section.get("title")),
                "reason": "claim-heavy section lacks evidence references",
            }
        )
    return unsupported


def _sections(row: dict[str, Any]) -> list[dict[str, Any]]:
    raw = row.get("sections") or row.get("claim_sections")
    parsed = _json(raw)
    if isinstance(parsed, list):
        return [item if isinstance(item, dict) else {"text": item} for item in parsed]
    if isinstance(parsed, dict):
        return [parsed]
    text = _text(row.get("body") or row.get("content") or row.get("draft_text"))
    return [{"section_id": "body", "text": text}] if text else []


def _claim_heavy(section: dict[str, Any]) -> bool:
    if bool(section.get("claim_heavy")):
        return True
    try:
        if int(section.get("claim_count") or 0) > 0:
            return True
    except (TypeError, ValueError):
        pass
    kind = _text(section.get("section_type") or section.get("type")).lower()
    if kind in {"claim", "analysis", "evidence", "argument"}:
        return True
    text = _text(section.get("text") or section.get("content"))
    return bool(re.search(r"\b\d+%|\b\d+x\b|\bstudy\b|\bresearch\b|\bdata\b|\bshows\b|\bproves\b|\bfaster\b|\bslower\b", text, re.I))


def _load_blog_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("blog_posts", "blog_drafts", "blog_items"):
        if table in schema:
            return _rows_from_blog_table(conn, schema, table)
    if "generated_content" in schema:
        return _rows_from_generated_content(conn, schema)
    return []


def _rows_from_blog_table(conn: sqlite3.Connection, schema: dict[str, set[str]], table: str) -> list[dict[str, Any]]:
    cols = schema[table]
    selected = [
        _select(cols, ("id", "blog_id", "draft_id", "post_id"), "item_id"),
        _select(cols, ("title", "headline"), "title"),
        _select(cols, ("status", "state", "publication_status"), "status"),
        _select(cols, ("channel", "platform"), "channel"),
        _select(cols, ("content_type", "type"), "content_type"),
        _select(cols, ("source_count", "sources_count", "reference_count"), "source_count"),
        _select(cols, ("sources", "source_refs", "references", "citations"), "sources"),
        _select(cols, ("sections", "claim_sections"), "sections"),
        _select(cols, ("body", "content", "draft_text"), "body"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]


def _rows_from_generated_content(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cols = schema["generated_content"]
    selected = [
        _select(cols, ("id",), "item_id"),
        _select(cols, ("title", "headline"), "title"),
        _select(cols, ("status", "state"), "status"),
        _select(cols, ("channel", "platform"), "channel"),
        _select(cols, ("content_type", "type"), "content_type"),
        _select(cols, ("source_count", "sources_count", "reference_count"), "source_count"),
        _select(cols, ("sources", "source_refs", "references", "citations"), "sources"),
        _select(cols, ("sections", "claim_sections"), "sections"),
        _select(cols, ("body", "content", "draft_text"), "body"),
    ]
    where = "WHERE LOWER(COALESCE(content_type, type, '')) LIKE '%blog%'" if {"content_type", "type"} & cols else ""
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content {where}").fetchall()]


def _has_blog_shape(schema: dict[str, set[str]]) -> bool:
    return any(table in schema for table in ("blog_posts", "blog_drafts", "blog_items", "generated_content"))


def _select(columns: set[str], names: tuple[str, ...], alias: str) -> str:
    for name in names:
        if name in columns:
            return f"{name} AS {alias}"
    return f"NULL AS {alias}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _list(value: Any) -> list[Any]:
    parsed = _json(value)
    if isinstance(parsed, list):
        return [item for item in parsed if item not in (None, "")]
    if isinstance(parsed, dict):
        return list(parsed.values())
    if value in (None, ""):
        return []
    return [part.strip() for part in str(value).split(",") if part.strip()]


def _json(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _severity_rank(severity: str) -> int:
    return {"critical": 0, "high": 1, "medium": 2, "low": 3, "none": 4}.get(severity, 9)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()
