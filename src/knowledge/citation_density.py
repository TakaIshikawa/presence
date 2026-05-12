"""Measure citation density in generated content."""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_PER_100 = 0.5
DEFAULT_MAX_PER_100 = 8.0
LONG_FORM_TYPES = {"blog_post", "newsletter", "newsletter_section", "long_form"}
SHORT_X_TYPES = {"x_post", "tweet"}
URL_RE = re.compile(r"https?://[^\s)>\]\"']+")


def build_citation_density_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    content_type: str | None = None,
    min_per_100: float = DEFAULT_MIN_PER_100,
    max_per_100: float = DEFAULT_MAX_PER_100,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only report of citation density by generated content item."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_per_100 < 0:
        raise ValueError("min_per_100 must be non-negative")
    if max_per_100 <= 0:
        raise ValueError("max_per_100 must be positive")
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _aware(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    rows = _content_rows(conn, schema, cutoff, content_type)
    link_counts = _knowledge_link_counts(conn, schema)
    items = []
    for row in rows:
        text = str(row.get("content") or "")
        word_count = _word_count(text)
        inline_urls = _inline_urls(text)
        explicit_count = link_counts.get(int(row["id"]), 0)
        citation_count = explicit_count + len(inline_urls)
        per_100 = round((citation_count / max(word_count, 1)) * 100, 3)
        expected = _expected_range(row["content_type"], min_per_100, max_per_100)
        issue = _issue_type(row["content_type"], per_100, citation_count, expected)
        items.append(
            {
                "content_id": int(row["id"]),
                "content_type": row["content_type"],
                "created_at": row["created_at"],
                "citation_count": citation_count,
                "explicit_source_link_count": explicit_count,
                "inline_url_count": len(inline_urls),
                "approximate_word_count": word_count,
                "citations_per_100_words": per_100,
                "expected_range": expected,
                "issue_type": issue,
            }
        )
    flagged = [item for item in items if item["issue_type"]]
    flagged.sort(key=lambda item: (item["issue_type"], item["content_type"], item["content_id"]))
    return {
        "artifact_type": "knowledge_citation_density",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "content_type": content_type,
            "min_per_100": min_per_100,
            "max_per_100": max_per_100,
            "lookback_start": cutoff.isoformat(),
        },
        "totals": {
            "content_scanned": len(items),
            "flagged_count": len(flagged),
            "too_few_count": sum(1 for item in flagged if item["issue_type"] == "too_few_citations"),
            "too_dense_count": sum(1 for item in flagged if item["issue_type"] == "too_dense_citations"),
        },
        "items": flagged,
        "empty_state": {
            "is_empty": not flagged,
            "schema_present": "generated_content" in schema,
            "message": "No citation density issues found." if not flagged else None,
        },
    }


def format_citation_density_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_citation_density_text(report: dict[str, Any]) -> str:
    lines = [
        "Knowledge Citation Density",
        f"Generated: {report['generated_at']}",
        (
            f"Window: {report['filters']['days']} days "
            f"content_type={report['filters']['content_type'] or 'all'} "
            f"min={report['filters']['min_per_100']} max={report['filters']['max_per_100']}"
        ),
        (
            "Totals: "
            f"scanned={report['totals']['content_scanned']} "
            f"flagged={report['totals']['flagged_count']}"
        ),
    ]
    if not report["items"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)
    lines.extend(["", "Flagged content:"])
    for item in report["items"]:
        lines.append(
            f"- content_id={item['content_id']} type={item['content_type']} "
            f"issue={item['issue_type']} citations={item['citation_count']} "
            f"words={item['approximate_word_count']} per100={item['citations_per_100_words']}"
        )
    return "\n".join(lines)


def _content_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    content_type: str | None,
) -> list[dict[str, Any]]:
    columns = schema.get("generated_content")
    if not columns or not {"id", "content", "content_type"}.issubset(columns):
        return []
    where = []
    params: list[Any] = []
    if "created_at" in columns:
        where.append("created_at >= ?")
        params.append(cutoff.isoformat())
    if content_type:
        where.append("content_type = ?")
        params.append(content_type)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    created_expr = "created_at" if "created_at" in columns else "NULL AS created_at"
    rows = conn.execute(
        f"""SELECT id, content_type, content, {created_expr}
           FROM generated_content
           {where_sql}
           ORDER BY created_at DESC, id DESC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _knowledge_link_counts(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, int]:
    ckl = schema.get("content_knowledge_links")
    knowledge = schema.get("knowledge")
    if not ckl or not {"content_id", "knowledge_id"}.issubset(ckl):
        return {}
    join = ""
    where = ""
    if knowledge and {"id", "source_type"}.issubset(knowledge):
        join = "JOIN knowledge k ON k.id = ckl.knowledge_id"
        where = "WHERE k.source_type LIKE 'curated_%'"
    rows = conn.execute(
        f"""SELECT ckl.content_id, COUNT(DISTINCT ckl.knowledge_id) AS citation_count
            FROM content_knowledge_links ckl
            {join}
            {where}
            GROUP BY ckl.content_id"""
    ).fetchall()
    return {int(row["content_id"]): int(row["citation_count"]) for row in rows}


def _inline_urls(text: str) -> set[str]:
    return {match.group(0).rstrip(".,;:") for match in URL_RE.finditer(text)}


def _word_count(text: str) -> int:
    return len(re.findall(r"\b[\w'-]+\b", text))


def _expected_range(content_type: str, min_per_100: float, max_per_100: float) -> dict[str, float | None]:
    if content_type in LONG_FORM_TYPES:
        return {"min_per_100": min_per_100, "max_per_100": None}
    if content_type in SHORT_X_TYPES:
        return {"min_per_100": None, "max_per_100": max_per_100}
    return {"min_per_100": None, "max_per_100": None}


def _issue_type(
    content_type: str,
    per_100: float,
    citation_count: int,
    expected: dict[str, float | None],
) -> str | None:
    min_expected = expected["min_per_100"]
    max_expected = expected["max_per_100"]
    if min_expected is not None and (citation_count == 0 or per_100 < min_expected):
        return "too_few_citations"
    if max_expected is not None and per_100 > max_expected:
        return "too_dense_citations"
    return None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        row["name"]: {column["name"] for column in conn.execute(f"PRAGMA table_info({row['name']})")}
        for row in rows
    }


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
