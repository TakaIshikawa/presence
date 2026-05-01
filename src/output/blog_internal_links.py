"""Suggest deterministic internal links for generated blog drafts."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import re
import sqlite3
from pathlib import Path
from typing import Any


DEFAULT_LIMIT = 10
DEFAULT_MIN_SCORE = 2.0
BLOG_CONTENT_TYPES = {"blog", "blog_post", "long_post"}

_STOPWORDS = {
    "a",
    "about",
    "after",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "been",
    "but",
    "by",
    "can",
    "do",
    "for",
    "from",
    "has",
    "have",
    "how",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "our",
    "so",
    "than",
    "that",
    "the",
    "their",
    "this",
    "to",
    "was",
    "we",
    "when",
    "with",
    "you",
    "your",
}


def build_blog_internal_link_suggestions(
    db_or_conn: Any,
    *,
    draft_path: str | Path | None = None,
    content_id: int | None = None,
    limit: int = DEFAULT_LIMIT,
    min_score: float = DEFAULT_MIN_SCORE,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return ranked internal-link suggestions for a markdown draft or content row."""
    if (draft_path is None) == (content_id is None):
        raise ValueError("provide exactly one of draft_path or content_id")
    if limit < 0:
        raise ValueError("limit must be non-negative")
    if min_score < 0:
        raise ValueError("min_score must be non-negative")

    conn = getattr(db_or_conn, "conn", db_or_conn)
    schema = _schema(conn)
    now = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {
        "draft_path": str(draft_path) if draft_path is not None else None,
        "content_id": content_id,
        "limit": limit,
        "min_score": min_score,
    }
    if limit == 0 or "generated_content" not in schema:
        return _empty_report(now, filters, ["generated_content"])

    source = (
        _source_from_markdown(Path(draft_path))
        if draft_path is not None
        else _source_from_content_id(conn, schema, int(content_id))
    )
    candidates = _load_published_blog_candidates(conn, schema)
    suggestions = []
    for candidate in candidates:
        if source["content_id"] is not None and candidate["content_id"] == source["content_id"]:
            continue
        suggestion = _score_candidate(source, candidate)
        if suggestion["score"] >= min_score:
            suggestions.append(suggestion)

    suggestions.sort(
        key=lambda item: (
            -item["score"],
            item["title"].lower(),
            item["target_content_id"],
        )
    )
    suggestions = suggestions[:limit]
    return {
        "generated_at": now.isoformat(),
        "filters": filters,
        "summary": {
            "candidate_count": len(candidates),
            "suggestion_count": len(suggestions),
            "source_content_id": source["content_id"],
            "source_title": source["title"],
        },
        "suggestions": suggestions,
        "missing_required_tables": [],
    }


def format_blog_internal_links_json(report: dict[str, Any]) -> str:
    """Render suggestions as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_internal_links_text(report: dict[str, Any]) -> str:
    """Render suggestions for review in terminal or cron logs."""
    filters = report["filters"]
    summary = report["summary"]
    source = (
        f"content_id={filters['content_id']}"
        if filters.get("content_id") is not None
        else f"draft={filters.get('draft_path')}"
    )
    lines = [
        "Blog internal link suggestions",
        f"Generated: {report['generated_at']}",
        f"Source: {source}",
        (
            f"Filters: limit={filters['limit']} "
            f"min_score={filters['min_score']}"
        ),
        (
            "Totals: "
            f"candidates={summary['candidate_count']} "
            f"suggestions={summary['suggestion_count']}"
        ),
        "",
    ]
    if not report["suggestions"]:
        lines.append("No internal link suggestions found.")
        return "\n".join(lines)

    lines.append("Suggestions")
    lines.append("  Score  Conf    ID     URL  Anchor / reason")
    for item in report["suggestions"]:
        url = item["url"] or "-"
        lines.append(
            f"  {item['score']:<5.1f}  "
            f"{item['confidence']:<6}  "
            f"{item['target_content_id']:<5}  "
            f"{_clip(url, 28):<28}  "
            f"{_clip(item['anchor_text'], 36)}"
        )
        lines.append(f"         topics: {', '.join(item['matched_topics']) or '-'}")
        lines.append(f"         reason: {_clip(item['reason'], 96)}")
    return "\n".join(lines)


def _source_from_markdown(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ValueError(f"draft file not found: {path}")
    text = path.read_text(encoding="utf-8")
    parsed = _parse_markdown(text)
    return {
        "content_id": None,
        "title": parsed["title"] or path.stem.replace("-", " ").replace("_", " ").title(),
        "body": parsed["body"],
        "topics": parsed["topics"],
        "terms": _term_counter(f"{parsed['title']} {parsed['body']}"),
        "title_terms": set(_terms(parsed["title"])),
    }


def _source_from_content_id(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM generated_content WHERE id = ?",
        (content_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"generated_content id {content_id} not found")
    data = dict(row)
    if str(data.get("content_type") or "") not in BLOG_CONTENT_TYPES:
        raise ValueError(f"generated_content id {content_id} is not a blog/long_post")
    parsed = _parse_markdown(str(data.get("content") or ""))
    topics = _topics_for_content(conn, schema, content_id)
    if parsed["topics"]:
        topics = _dedupe_strings([*topics, *parsed["topics"]])
    return {
        "content_id": content_id,
        "title": parsed["title"] or f"Blog Post {content_id}",
        "body": parsed["body"],
        "topics": topics,
        "terms": _term_counter(f"{parsed['title']} {parsed['body']}"),
        "title_terms": set(_terms(parsed["title"])),
    }


def _load_published_blog_candidates(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    required = {"id", "content_type", "content"}
    if not required.issubset(columns):
        return []
    rows = conn.execute(
        """SELECT *
           FROM generated_content
           WHERE content_type IN ('blog', 'blog_post', 'long_post')
           ORDER BY id ASC"""
    ).fetchall()
    publication_urls = _publication_urls(conn, schema)
    candidates = []
    for row in rows:
        data = dict(row)
        content_id = int(data["id"])
        url = _first_text(data.get("published_url"), publication_urls.get(content_id))
        is_published = _truthy(data.get("published")) or bool(publication_urls.get(content_id))
        if not is_published:
            continue
        parsed = _parse_markdown(str(data.get("content") or ""))
        title = parsed["title"] or f"Blog Post {content_id}"
        topics = _topics_for_content(conn, schema, content_id)
        candidates.append(
            {
                "content_id": content_id,
                "url": url,
                "title": title,
                "body": parsed["body"],
                "topics": topics,
                "terms": _term_counter(f"{title} {parsed['body']}"),
                "title_terms": set(_terms(title)),
            }
        )
    return candidates


def _score_candidate(source: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    matched_topics = sorted(
        set(_normalize_topic(topic) for topic in source["topics"])
        & set(_normalize_topic(topic) for topic in candidate["topics"])
    )
    source_terms = set(source["terms"])
    candidate_terms = set(candidate["terms"])
    body_matches = sorted(source_terms & candidate_terms)
    title_matches = sorted(set(source["title_terms"]) & set(candidate["title_terms"]))

    topic_score = len(matched_topics) * 4.0
    title_score = len(title_matches) * 1.5
    body_score = min(sum(min(source["terms"][term], candidate["terms"][term]) for term in body_matches) * 0.35, 6.0)
    score = round(topic_score + title_score + body_score, 2)
    confidence = "high" if score >= 8 else "medium" if score >= 4 else "low"
    reason_parts = []
    if matched_topics:
        reason_parts.append(f"topic match: {', '.join(matched_topics)}")
    if title_matches:
        reason_parts.append(f"title overlap: {', '.join(title_matches[:4])}")
    if body_matches:
        reason_parts.append(f"shared terms: {', '.join(body_matches[:6])}")
    reason = "; ".join(reason_parts) or "weak lexical overlap"
    return {
        "target_content_id": candidate["content_id"],
        "url": candidate["url"],
        "title": candidate["title"],
        "anchor_text": candidate["title"],
        "matched_topics": matched_topics,
        "matched_terms": body_matches[:12],
        "score": score,
        "confidence": confidence,
        "reason": reason,
    }


def _parse_markdown(text: str) -> dict[str, Any]:
    frontmatter: dict[str, Any] = {}
    body = text.strip()
    match = re.match(r"\A---\s*\n(.*?)\n---\s*\n?", text, flags=re.DOTALL)
    if match:
        frontmatter = _parse_frontmatter(match.group(1))
        body = text[match.end() :].strip()

    title = str(frontmatter.get("title") or "").strip()
    if not title:
        title_match = re.search(r"(?im)^title:\s*(.+?)\s*$", body)
        if title_match:
            title = title_match.group(1).strip().strip("\"'")
            body = (body[: title_match.start()] + body[title_match.end() :]).strip()
    if not title:
        heading_match = re.search(r"(?m)^#\s+(.+?)\s*$", body)
        if heading_match:
            title = heading_match.group(1).strip()
            body = (body[: heading_match.start()] + body[heading_match.end() :]).strip()

    raw_topics = frontmatter.get("topics") or frontmatter.get("tags") or []
    return {
        "title": title,
        "body": _strip_markdown(body),
        "topics": _parse_topic_list(raw_topics),
    }


def _parse_frontmatter(raw: str) -> dict[str, Any]:
    data: dict[str, Any] = {}
    current_key: str | None = None
    for line in raw.splitlines():
        if not line.strip():
            continue
        list_item = re.match(r"^\s*-\s*(.+?)\s*$", line)
        if list_item and current_key:
            data.setdefault(current_key, []).append(list_item.group(1).strip().strip("\"'"))
            continue
        match = re.match(r"^([A-Za-z0-9_-]+):\s*(.*?)\s*$", line)
        if not match:
            continue
        key, value = match.group(1).lower(), match.group(2).strip()
        current_key = key
        if value == "":
            data[key] = []
        else:
            data[key] = value.strip().strip("\"'")
    return data


def _parse_topic_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return _dedupe_strings(str(item) for item in value)
    text = str(value or "").strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    return _dedupe_strings(part.strip().strip("\"'") for part in re.split(r"[,;]", text))


def _strip_markdown(text: str) -> str:
    text = re.sub(r"```.*?```", " ", text, flags=re.DOTALL)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"!\[[^\]]*]\([^)]+\)", " ", text)
    text = re.sub(r"\[([^\]]+)]\([^)]+\)", r"\1", text)
    text = re.sub(r"(?m)^#{1,6}\s+", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _topics_for_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
) -> list[str]:
    if "content_topics" not in schema or not {"content_id", "topic"}.issubset(schema["content_topics"]):
        return []
    rows = conn.execute(
        """SELECT topic
           FROM content_topics
           WHERE content_id = ?
           ORDER BY confidence DESC, id ASC""",
        (content_id,),
    ).fetchall()
    return _dedupe_strings(row["topic"] for row in rows if row["topic"])


def _publication_urls(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, str]:
    columns = schema.get("content_publications")
    if not columns or not {"content_id", "status", "platform_url"}.issubset(columns):
        return {}
    urls: dict[int, str] = {}
    rows = conn.execute(
        """SELECT content_id, platform, platform_url
           FROM content_publications
           WHERE status = 'published'
           ORDER BY CASE WHEN platform = 'blog' THEN 0 ELSE 1 END,
                    published_at DESC,
                    id DESC"""
    ).fetchall()
    for row in rows:
        content_id = int(row["content_id"])
        url = str(row["platform_url"] or "").strip()
        if url and content_id not in urls:
            urls[content_id] = url
    return urls


def _term_counter(text: str) -> Counter[str]:
    return Counter(_terms(text))


def _terms(text: str) -> list[str]:
    terms = []
    for term in re.findall(r"[A-Za-z][A-Za-z0-9_-]{2,}", text.lower()):
        normalized = term.replace("_", "-").strip("-")
        if normalized and normalized not in _STOPWORDS and not normalized.isdigit():
            terms.append(_singularize(normalized))
    return terms


def _singularize(term: str) -> str:
    if len(term) > 4 and term.endswith("ies"):
        return term[:-3] + "y"
    if len(term) > 4 and term.endswith("s") and not term.endswith("ss"):
        return term[:-1]
    return term


def _normalize_topic(topic: Any) -> str:
    return re.sub(r"\s+", "-", str(topic or "").strip().lower().replace("_", "-"))


def _dedupe_strings(values: Any) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def _first_text(*values: Any) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def _empty_report(
    now: datetime,
    filters: dict[str, Any],
    missing_required_tables: list[str],
) -> dict[str, Any]:
    return {
        "generated_at": now.isoformat(),
        "filters": filters,
        "summary": {
            "candidate_count": 0,
            "suggestion_count": 0,
            "source_content_id": filters.get("content_id"),
            "source_title": "",
        },
        "suggestions": [],
        "missing_required_tables": missing_required_tables,
    }


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clip(value: Any, width: int) -> str:
    text = str(value or "")
    if len(text) <= width:
        return text
    return text[: max(width - 3, 0)] + "..."
