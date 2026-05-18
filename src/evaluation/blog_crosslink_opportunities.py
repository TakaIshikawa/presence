"""Find published blog posts that should link to older related posts."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_MIN_SHARED_TOKENS = 3
DEFAULT_LIMIT = 100
BLOG_TYPE_RE = re.compile(r"\b(blog|article|post)\b", re.IGNORECASE)
STOPWORDS = {
    "about",
    "after",
    "again",
    "also",
    "blog",
    "from",
    "have",
    "into",
    "post",
    "that",
    "the",
    "this",
    "with",
    "your",
}


def build_blog_crosslink_opportunities_report(
    db_or_conn: Any,
    *,
    min_shared_tokens: int = DEFAULT_MIN_SHARED_TOKENS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return candidate blog-to-blog internal crosslinks."""
    if min_shared_tokens <= 0:
        raise ValueError("min_shared_tokens must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {"min_shared_tokens": min_shared_tokens, "limit": limit}
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    posts = [_normalize(row) for row in _load_rows(conn, schema)]
    candidates: list[dict[str, Any]] = []
    for source in posts:
        if not source["published_at"]:
            continue
        for target in posts:
            if source["id"] == target["id"] or not target["published_at"]:
                continue
            if target["published_at"] >= source["published_at"]:
                continue
            if target["url"] and _contains(source["link_text"], target["url"]):
                continue
            candidate = _candidate(source, target, min_shared_tokens)
            if candidate:
                candidates.append(candidate)

    candidates.sort(key=_sort_key)
    ranked = candidates[:limit]
    return {
        "artifact_type": "blog_crosslink_opportunities",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "published_blog_count": len(posts),
            "candidate_count": len(candidates),
            "record_count": len(ranked),
            "by_reason": dict(sorted(Counter(item["reason"] for item in candidates).items())),
        },
        "opportunities": ranked,
        "missing_tables": [],
        "missing_columns": {},
    }


def format_blog_crosslink_opportunities_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_crosslink_opportunities_text(report: dict[str, Any]) -> str:
    lines = [
        "Blog Crosslink Opportunities",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: min_shared_tokens={report['filters']['min_shared_tokens']} "
            f"limit={report['filters']['limit']}"
        ),
        (
            f"Totals: blogs={report['totals']['published_blog_count']} "
            f"candidates={report['totals']['candidate_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append(f"Missing tables: {', '.join(report['missing_tables'])}")
    if report["missing_columns"]:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report["missing_columns"].items())
        ]
        lines.append(f"Missing columns: {'; '.join(missing)}")
    if not report["opportunities"]:
        lines.extend(["", "No blog crosslink opportunities found."])
        return "\n".join(lines)

    lines.extend(["", "Opportunities:"])
    for item in report["opportunities"]:
        lines.append(
            f"- source_content_id={item['source_content_id']} "
            f"target_content_id={item['target_content_id']} "
            f"confidence={item['confidence']:.2f} reason={item['reason']}"
        )
        lines.append(f"  evidence: {item['evidence']}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cols = schema["generated_content"]
    selected = [
        "id",
        _select(cols, "content_type", "'unknown'"),
        _select(cols, "content", "NULL"),
        _select(cols, "title", "NULL"),
        _select(cols, "metadata", "NULL"),
        _select(cols, "published_url", "NULL"),
        _select(cols, "published_at", "NULL"),
        _select(cols, "created_at", "NULL"),
        _select(cols, "content_format", "NULL"),
    ]
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(selected)}
               FROM generated_content
               WHERE (
                   LOWER(COALESCE(content_type, '')) LIKE '%blog%'
                   OR LOWER(COALESCE(content_type, '')) LIKE '%article%'
                   OR LOWER(COALESCE(content_type, '')) LIKE '%post%'
               )
                 AND COALESCE(published, 0) = 1
               ORDER BY COALESCE(published_at, created_at) DESC, id ASC"""
        ).fetchall()
    ]


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _json_obj(row.get("metadata"))
    title = _text(row.get("title") or metadata.get("title") or _first_line(row.get("content")))
    body = " ".join(_texts(row.get("content"), metadata, row.get("content_format")))
    topics = _topic_values(row, metadata)
    return {
        "id": int(row["id"]),
        "content_type": _text(row.get("content_type")) or "unknown",
        "title": title,
        "url": _text(row.get("published_url") or metadata.get("published_url") or metadata.get("url")),
        "published_at": _parse_dt(row.get("published_at") or row.get("created_at")),
        "metadata_topics": topics,
        "tokens": _tokens(" ".join([title, body])),
        "link_text": " ".join(_texts(row.get("content"), row.get("metadata"))).lower(),
    }


def _candidate(
    source: dict[str, Any],
    target: dict[str, Any],
    min_shared_tokens: int,
) -> dict[str, Any] | None:
    shared_topics = sorted(source["metadata_topics"] & target["metadata_topics"])
    if shared_topics:
        confidence = min(0.98, 0.72 + 0.08 * min(len(shared_topics), 3))
        return _candidate_row(source, target, "shared_topic_metadata", confidence, shared_topics)

    shared_tokens = sorted(source["tokens"] & target["tokens"])
    if len(shared_tokens) >= min_shared_tokens:
        confidence = min(0.7, 0.35 + 0.07 * min(len(shared_tokens), 5))
        return _candidate_row(source, target, "token_overlap", confidence, shared_tokens[:8])
    return None


def _candidate_row(
    source: dict[str, Any],
    target: dict[str, Any],
    reason: str,
    confidence: float,
    evidence_values: list[str],
) -> dict[str, Any]:
    return {
        "source_content_id": source["id"],
        "target_content_id": target["id"],
        "source_title": source["title"],
        "target_title": target["title"],
        "target_published_url": target["url"],
        "reason": reason,
        "confidence": round(confidence, 2),
        "evidence": ", ".join(evidence_values),
    }


def _topic_values(row: dict[str, Any], metadata: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    for key in (
        "topic",
        "topics",
        "theme",
        "themes",
        "category",
        "categories",
        "tag",
        "tags",
    ):
        for value in _parse_list(metadata.get(key)):
            text = _clean_topic(value)
            if text:
                values.add(text)
    for value in _parse_list(row.get("content_format")):
        text = _clean_topic(value)
        if text:
            values.add(text)
    return values


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    reason_rank = {"shared_topic_metadata": 0, "token_overlap": 1}
    return (
        reason_rank.get(item["reason"], 9),
        -item["confidence"],
        item["source_content_id"],
        item["target_content_id"],
    )


def _contains(haystack: str, needle: str) -> bool:
    return bool(needle) and needle.lower() in haystack.lower()


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    if "generated_content" not in schema:
        return ["generated_content"], {}
    required = {"id", "content_type", "published"}
    missing = sorted(required - schema["generated_content"])
    return [], {"generated_content": missing} if missing else {}


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "artifact_type": "blog_crosslink_opportunities",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "published_blog_count": 0,
            "candidate_count": 0,
            "record_count": 0,
            "by_reason": {},
        },
        "opportunities": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _select(columns: set[str], column: str, fallback: str) -> str:
    return column if column in columns else f"{fallback} AS {column}"


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _tokens(value: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9]{3,}", value.lower()) if token not in STOPWORDS}


def _clean_topic(value: Any) -> str:
    return re.sub(r"\s+", " ", _text(value).lower()).strip()


def _first_line(value: Any) -> str:
    return _text(value).splitlines()[0][:120] if _text(value) else ""


def _texts(*values: Any) -> list[str]:
    return [_text(value) for value in values if _text(value)]


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value).strip()


def _parse_list(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = [part.strip() for part in value.split(",")]
        return parsed if isinstance(parsed, list) else [parsed]
    return [value]


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}
