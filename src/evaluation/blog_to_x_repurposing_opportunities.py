"""Find published blog posts that still need X repurposing."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_WINDOW_DAYS = 30
DEFAULT_MIN_TITLE_TOKEN_OVERLAP = 2
DEFAULT_LIMIT = 50
URL_RE = re.compile(r"https?://[^\s<>)\"']+")
TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9-]{2,}")
STOP_WORDS = {
    "about",
    "after",
    "again",
    "from",
    "into",
    "that",
    "the",
    "this",
    "with",
    "your",
}


def build_blog_to_x_repurposing_opportunities_report(
    blog_rows: list[dict[str, Any]],
    social_rows: list[dict[str, Any]],
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    min_title_token_overlap: int = DEFAULT_MIN_TITLE_TOKEN_OVERLAP,
    limit: int = DEFAULT_LIMIT,
    require_thread: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if min_title_token_overlap <= 0:
        raise ValueError("min_title_token_overlap must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    blogs = [_normalize_blog(row) for row in blog_rows if _is_published_blog(row)]
    social = [_normalize_social(row) for row in social_rows if _is_x_content(row)]
    opportunities = []
    coverage_counts = Counter()
    required_formats = {"x_post", "x_thread"} if require_thread else {"x_any"}

    for blog in blogs:
        matches = [_match(blog, item, window_days, min_title_token_overlap) for item in social]
        matches = [item for item in matches if item["matched"]]
        format_counts = Counter(match["format"] for match in matches)
        has_post = format_counts["x_post"] > 0
        has_thread = format_counts["x_thread"] > 0
        fully_covered = has_post and has_thread if require_thread else has_post or has_thread
        coverage = "full" if fully_covered else "partial" if matches else "missing"
        coverage_counts[coverage] += 1
        if fully_covered:
            continue

        reason_codes = []
        if not has_post:
            reason_codes.append("missing_x_post")
        if require_thread and not has_thread:
            reason_codes.append("missing_x_thread")
        blog_age_days = _age_days(generated_at, blog["published_at"])
        if blog_age_days is not None and blog_age_days > window_days:
            reason_codes.append("stale_blog")
        if matches and not any(match["strong_evidence"] for match in matches):
            reason_codes.append("weak_match_evidence")

        opportunities.append(
            {
                "blog_id": blog["id"],
                "title": blog["title"],
                "url": blog["url"],
                "published_at": _iso(blog["published_at"]),
                "blog_age_days": blog_age_days,
                "matched_x_post_count": format_counts["x_post"],
                "matched_x_thread_count": format_counts["x_thread"],
                "reason_codes": reason_codes,
                "severity_score": _severity_score(reason_codes, blog_age_days, len(matches)),
                "match_evidence": [
                    {
                        "content_id": match["content_id"],
                        "format": match["format"],
                        "published_at": match["published_at"],
                        "evidence": match["evidence"],
                        "title_token_overlap": match["title_token_overlap"],
                    }
                    for match in sorted(matches, key=lambda item: (item["format"], item["content_id"]))
                ],
            }
        )

    opportunities.sort(key=lambda item: (-item["severity_score"], item["published_at"] or "", item["blog_id"]))
    shown = opportunities[:limit]
    return {
        "artifact_type": "blog_to_x_repurposing_opportunities",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "window_days": window_days,
            "min_title_token_overlap": min_title_token_overlap,
            "limit": limit,
            "require_thread": require_thread,
        },
        "totals": {
            "blog_count": len(blogs),
            "social_content_count": len(social),
            "opportunity_count": len(opportunities),
            "shown_count": len(shown),
            "fully_repurposed_count": coverage_counts["full"],
            "partial_repurposed_count": coverage_counts["partial"],
            "missing_repurposed_count": coverage_counts["missing"],
        },
        "opportunities": shown,
        "empty_state": {
            "is_empty": not opportunities,
            "message": "No blog-to-X repurposing opportunities found." if not opportunities else None,
        },
    }


def build_blog_to_x_repurposing_opportunities_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_blog_to_x_repurposing_opportunities_report(_load_blogs(conn, schema), _load_social(conn, schema), **kwargs)


def format_blog_to_x_repurposing_opportunities_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_to_x_repurposing_opportunities_text(report: dict[str, Any]) -> str:
    lines = [
        "Blog To X Repurposing Opportunities",
        f"Generated: {report['generated_at']}",
        (
            f"Window: {report['filters']['window_days']}d "
            f"title_overlap>={report['filters']['min_title_token_overlap']} "
            f"require_thread={report['filters']['require_thread']}"
        ),
        (
            f"Totals: blogs={report['totals']['blog_count']} opportunities={report['totals']['opportunity_count']} "
            f"full={report['totals']['fully_repurposed_count']} partial={report['totals']['partial_repurposed_count']} "
            f"missing={report['totals']['missing_repurposed_count']}"
        ),
    ]
    if not report["opportunities"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "blog_id | age_days | x_posts | x_threads | reasons | title"])
    for row in report["opportunities"]:
        lines.append(
            f"{row['blog_id']} | {row['blog_age_days'] if row['blog_age_days'] is not None else '-'} | "
            f"{row['matched_x_post_count']} | {row['matched_x_thread_count']} | "
            f"{','.join(row['reason_codes']) or '-'} | {row['title'] or '-'}"
        )
    return "\n".join(lines)


format_blog_to_x_repurposing_opportunities_table = format_blog_to_x_repurposing_opportunities_text


def _normalize_blog(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _json_object(_first(row, "metadata", "raw_metadata"))
    return {
        "id": _text(_first(row, "blog_id", "post_id", "content_id", "id", "slug")) or "unknown",
        "title": _text(_first(row, "title", "subject", "content") or metadata.get("title")),
        "url": _normalize_url(_first(row, "url", "published_url", "canonical_url", "blog_url") or metadata.get("url") or metadata.get("published_url")),
        "published_at": _parse_ts(_first(row, "published_at", "created_at", "date")),
        "source_ids": set(_items(_first(row, "source_content_ids", "source_ids", "source_activity_ids") or metadata.get("source_content_ids") or metadata.get("source_ids"))),
        "source_urls": {_normalize_url(url) for url in _items(_first(row, "source_urls", "urls") or metadata.get("source_urls") or metadata.get("urls")) if _normalize_url(url)},
    }


def _normalize_social(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _json_object(_first(row, "metadata", "raw_metadata"))
    text = _text(_first(row, "content", "body", "text", "draft", "title"))
    explicit_urls = _items(_first(row, "source_urls", "urls") or metadata.get("source_urls") or metadata.get("urls"))
    embedded_urls = URL_RE.findall(text)
    return {
        "id": _text(_first(row, "content_id", "id")) or "unknown",
        "format": _x_format(_first(row, "content_type", "format", "type")),
        "title": _text(_first(row, "title", "subject") or text),
        "text": text,
        "published_at": _parse_ts(_first(row, "published_at", "created_at", "scheduled_at")),
        "source_ids": set(_items(_first(row, "source_content_ids", "source_ids", "source_activity_ids") or metadata.get("source_content_ids") or metadata.get("source_ids"))),
        "urls": {_normalize_url(url) for url in explicit_urls + embedded_urls if _normalize_url(url)},
    }


def _match(blog: dict[str, Any], social: dict[str, Any], window_days: int, min_overlap: int) -> dict[str, Any]:
    evidence = []
    if blog["id"] and blog["id"] in social["source_ids"]:
        evidence.append("source_id")
    if blog["source_ids"] and blog["source_ids"] & social["source_ids"]:
        evidence.append("shared_source_id")
    if blog["url"] and blog["url"] in social["urls"]:
        evidence.append("blog_url")
    if blog["source_urls"] and blog["source_urls"] & social["urls"]:
        evidence.append("source_url")
    title_overlap = len(_tokens(blog["title"]) & _tokens(f"{social['title']} {social['text']}"))
    if title_overlap >= min_overlap:
        evidence.append("title_tokens")
    if _within_window(blog["published_at"], social["published_at"], window_days):
        evidence.append("date_proximity")

    has_primary_evidence = any(item in evidence for item in ("source_id", "shared_source_id", "blog_url", "source_url"))
    matched = has_primary_evidence or ("title_tokens" in evidence and "date_proximity" in evidence)
    return {
        "matched": matched,
        "content_id": social["id"],
        "format": social["format"],
        "published_at": _iso(social["published_at"]),
        "evidence": evidence,
        "strong_evidence": has_primary_evidence,
        "title_token_overlap": title_overlap,
    }


def _load_blogs(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "blog_posts" in schema:
        cols = schema["blog_posts"]
        selected = [
            _col(cols, "id", "post_id", "blog_id", default="NULL") + " AS id",
            _col(cols, "slug", default="NULL") + " AS slug",
            _col(cols, "title", "subject", default="NULL") + " AS title",
            _col(cols, "url", "published_url", "canonical_url", "blog_url", default="NULL") + " AS published_url",
            _col(cols, "published_at", "created_at", default="NULL") + " AS published_at",
            _col(cols, "status", default="'published'") + " AS status",
            _col(cols, "source_content_ids", "source_ids", default="NULL") + " AS source_content_ids",
            _col(cols, "source_urls", "urls", default="NULL") + " AS source_urls",
            _col(cols, "metadata", "raw_metadata", default="NULL") + " AS metadata",
        ]
        return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM blog_posts").fetchall()]
    if "generated_content" not in schema:
        return []
    cols = schema["generated_content"]
    selected = _generated_content_select(cols)
    where = "WHERE LOWER(COALESCE(content_type, '')) LIKE '%blog%'"
    if "published" in cols:
        where += " AND COALESCE(published, 0) = 1"
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content {where}").fetchall()]


def _load_social(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    cols = schema["generated_content"]
    selected = _generated_content_select(cols)
    where = "WHERE LOWER(COALESCE(content_type, '')) IN ('x_post', 'x_thread', 'tweet', 'twitter_post', 'thread')"
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content {where}").fetchall()]


def _generated_content_select(cols: set[str]) -> list[str]:
    return [
        _col(cols, "id", "content_id", default="NULL") + " AS id",
        _col(cols, "content_type", "format", "type", default="NULL") + " AS content_type",
        _col(cols, "title", "subject", default="NULL") + " AS title",
        _col(cols, "content", "body", "text", "draft", default="NULL") + " AS content",
        _col(cols, "published_url", "url", "canonical_url", default="NULL") + " AS published_url",
        _col(cols, "published_at", "created_at", default="NULL") + " AS published_at",
        _col(cols, "source_content_ids", "source_ids", default="NULL") + " AS source_content_ids",
        _col(cols, "source_urls", "urls", default="NULL") + " AS source_urls",
        _col(cols, "metadata", "raw_metadata", default="NULL") + " AS metadata",
    ]


def _is_published_blog(row: dict[str, Any]) -> bool:
    content_type = _text(_first(row, "content_type", "format", "type")).lower()
    if content_type and "blog" not in content_type:
        return False
    status = _text(_first(row, "status", "publication_status")).lower()
    if status and status not in {"published", "sent", "live"}:
        return False
    published = _first(row, "published", "is_published")
    return published not in (False, 0, "0", "false", "False")


def _is_x_content(row: dict[str, Any]) -> bool:
    return _x_format(_first(row, "content_type", "format", "type")) in {"x_post", "x_thread"}


def _x_format(value: Any) -> str:
    text = _text(value).lower()
    if text in {"x_thread", "twitter_thread", "thread"}:
        return "x_thread"
    if text in {"x_post", "tweet", "twitter_post", "post"}:
        return "x_post"
    return text


def _within_window(blog_date: datetime | None, social_date: datetime | None, window_days: int) -> bool:
    if not blog_date or not social_date:
        return False
    delta = (social_date - blog_date).total_seconds() / 86400
    return 0 <= delta <= window_days


def _severity_score(reason_codes: list[str], age_days: float | None, match_count: int) -> float:
    score = len([code for code in reason_codes if code.startswith("missing_")]) * 100
    if "stale_blog" in reason_codes:
        score += min(age_days or 0, 365) / 10
    if "weak_match_evidence" in reason_codes:
        score += 5
    return round(score - match_count, 2)


def _tokens(value: str) -> set[str]:
    return {token for token in TOKEN_RE.findall(value.lower()) if token not in STOP_WORDS}


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


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    parsed = _json_value(value)
    return parsed if isinstance(parsed, dict) else {}


def _json_value(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _normalize_url(value: Any) -> str:
    text = _text(value)
    return text.rstrip("/").lower()


def _age_days(now: datetime, value: datetime | None) -> float | None:
    return round(max((now - value).total_seconds() / 86400, 0), 2) if value else None


def _parse_ts(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if value in (None, ""):
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


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
