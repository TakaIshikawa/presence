"""Detect blog posts with overly similar excerpts."""

from __future__ import annotations

from datetime import datetime, timezone
from difflib import SequenceMatcher
import json
import re
import sqlite3
from typing import Any


DEFAULT_THRESHOLD = 0.82


def build_blog_excerpt_similarity_report(
    rows: list[dict[str, Any]],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    now: datetime | None = None,
) -> dict[str, Any]:
    if threshold < 0 or threshold > 1:
        raise ValueError("threshold must be between 0 and 1")
    generated_at = _utc(now or datetime.now(timezone.utc))
    normalized = [_normalize_row(row) for row in rows if _normalize_text(_first(row, "excerpt", "summary", "description"))]
    pairs = []
    for index, left in enumerate(normalized):
        for right in normalized[index + 1 :]:
            score = round(SequenceMatcher(None, left["normalized_excerpt"], right["normalized_excerpt"]).ratio(), 4)
            if score >= threshold:
                pairs.append(
                    {
                        "left_id": left["content_id"],
                        "right_id": right["content_id"],
                        "left_excerpt": left["normalized_excerpt"],
                        "right_excerpt": right["normalized_excerpt"],
                        "similarity": score,
                        "reason": f"Excerpts are {score:.0%} similar, above the {threshold:.0%} threshold.",
                    }
                )
    pairs.sort(key=lambda pair: (-pair["similarity"], pair["left_id"], pair["right_id"]))
    return {
        "artifact_type": "blog_excerpt_similarity",
        "generated_at": generated_at.isoformat(),
        "filters": {"threshold": threshold},
        "totals": {"rows_scanned": len(rows), "excerpt_count": len(normalized), "flagged_pair_count": len(pairs)},
        "pairs": pairs,
        "empty_state": {"is_empty": not pairs, "message": "No overly similar blog excerpts found." if not pairs else None},
    }


def build_blog_excerpt_similarity_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_blog_excerpt_similarity_report(_load_rows(conn, schema), **kwargs)


def format_blog_excerpt_similarity_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_excerpt_similarity_text(report: dict[str, Any]) -> str:
    lines = [
        "Blog Excerpt Similarity",
        f"Generated: {report['generated_at']}",
        f"Threshold: {report['filters']['threshold']}",
        f"Totals: excerpts={report['totals']['excerpt_count']} pairs={report['totals']['flagged_pair_count']}",
    ]
    if not report["pairs"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "Similar pairs:"])
    for pair in report["pairs"]:
        lines.append(f"- {pair['left_id']} <-> {pair['right_id']} similarity={pair['similarity']} reason={pair['reason']}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "blog_posts" if "blog_posts" in schema else "generated_content" if "generated_content" in schema else None
    if table is None:
        return []
    columns = schema[table]
    selected = [
        _col(columns, "id", "content_id", "slug") + " AS content_id",
        _col(columns, "excerpt", "summary", "description", "content", default="NULL") + " AS excerpt",
        _col(columns, "status", "state", default="'unknown'") + " AS status",
    ]
    where = ""
    if table == "generated_content" and "content_type" in columns:
        where = " WHERE LOWER(COALESCE(content_type, '')) LIKE '%blog%'"
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}{where}").fetchall()]


def _normalize_row(row: dict[str, Any]) -> dict[str, str]:
    return {
        "content_id": _text(_first(row, "content_id", "id", "slug")) or "unknown",
        "normalized_excerpt": _normalize_text(_first(row, "excerpt", "summary", "description")),
    }


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", " ", _text(value).lower())).strip()


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
