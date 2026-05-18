"""Rank blog posts and topics that are good continuation candidates."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 50
DEFAULT_HIGH_ENGAGEMENT_SCORE = 100.0
DEFAULT_STALE_SERIES_GAP_DAYS = 45
NEXT_STEP_RE = re.compile(r"\b(next|follow[- ]?up|part\s+2|in a future|we[' ]?ll cover|coming next)\b", re.I)
TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9-]{2,}")
STOP_WORDS = {
    "about",
    "after",
    "again",
    "blog",
    "from",
    "into",
    "that",
    "the",
    "this",
    "with",
    "your",
}


def build_blog_series_continuation_opportunities_report(
    blog_rows: list[dict[str, Any]],
    engagement_rows: list[dict[str, Any]] | None = None,
    followup_rows: list[dict[str, Any]] | None = None,
    *,
    high_engagement_score: float = DEFAULT_HIGH_ENGAGEMENT_SCORE,
    stale_series_gap_days: int = DEFAULT_STALE_SERIES_GAP_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if high_engagement_score < 0:
        raise ValueError("high_engagement_score must be non-negative")
    if stale_series_gap_days <= 0:
        raise ValueError("stale_series_gap_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    blogs = [_normalize_blog(row) for row in blog_rows if _is_published_blog(row)]
    engagement = _engagement_by_blog(engagement_rows or [])
    followups = _followups_by_blog(followup_rows or [])
    topic_counts = Counter(blog["topic_or_series_key"] for blog in blogs)
    latest_by_key: dict[str, datetime | None] = {}
    for blog in blogs:
        current = latest_by_key.get(blog["topic_or_series_key"])
        if current is None or (blog["published_at"] and blog["published_at"] > current):
            latest_by_key[blog["topic_or_series_key"]] = blog["published_at"]

    opportunities = []
    for blog in blogs:
        score = engagement.get(blog["blog_id"], _embedded_engagement_score(blog["metadata"]))
        followup_items = followups.get(blog["blog_id"], [])
        followup_count = len(followup_items)
        last_followup_at = max((_parse_ts(_first(item, "published_at", "created_at", "scheduled_at")) for item in followup_items), default=None)
        reasons: list[str] = []
        if score >= high_engagement_score and followup_count == 0:
            reasons.append("high_engagement_no_followup")
        if _is_stale_series(blog, latest_by_key.get(blog["topic_or_series_key"]), generated_at, stale_series_gap_days) and followup_count == 0:
            reasons.append("stale_series_gap")
        if NEXT_STEP_RE.search(f"{blog['title']} {blog['body']}"):
            reasons.append("dangling_next_step_language")
        if not blog["explicit_series"] and topic_counts[blog["topic_or_series_key"]] > 1:
            reasons.append("recurring_topic_without_series")
        if not reasons:
            continue
        opportunity_score = _opportunity_score(
            engagement_score=score,
            followup_count=followup_count,
            published_at=blog["published_at"],
            generated_at=generated_at,
            reasons=reasons,
        )
        opportunities.append(
            {
                "blog_id": blog["blog_id"],
                "title": blog["title"],
                "topic_or_series_key": blog["topic_or_series_key"],
                "published_at": _iso(blog["published_at"]),
                "engagement_score": score,
                "followup_count": followup_count,
                "last_followup_at": _iso(last_followup_at),
                "opportunity_score": opportunity_score,
                "reasons": reasons,
            }
        )

    opportunities.sort(key=lambda item: (-item["opportunity_score"], item["published_at"] or "", item["blog_id"]))
    shown = opportunities[:limit]
    return {
        "artifact_type": "blog_series_continuation_opportunities",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "high_engagement_score": high_engagement_score,
            "stale_series_gap_days": stale_series_gap_days,
            "limit": limit,
        },
        "totals": {
            "blog_count": len(blogs),
            "opportunity_count": len(opportunities),
            "shown_count": len(shown),
        },
        "opportunities": shown,
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
        "empty_state": {
            "is_empty": not opportunities,
            "message": "No blog series continuation opportunities found." if not opportunities else None,
        },
    }


def build_blog_series_continuation_opportunities_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    blogs = _load_blogs(conn, schema)
    engagement = _load_engagement(conn, schema)
    followups = _load_followups(conn, schema)
    return build_blog_series_continuation_opportunities_report(blogs, engagement, followups, schema_gaps=gaps, **kwargs)


def format_blog_series_continuation_opportunities_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_series_continuation_opportunities_text(report: dict[str, Any]) -> str:
    lines = [
        "Blog Series Continuation Opportunities",
        f"Generated: {report['generated_at']}",
        f"Totals: blogs={report['totals']['blog_count']} opportunities={report['totals']['opportunity_count']}",
    ]
    if not report["opportunities"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "blog_id | engagement | followups | last_followup | score | reasons | title"])
    for row in report["opportunities"]:
        lines.append(
            f"{row['blog_id']} | {row['engagement_score']:.2f} | {row['followup_count']} | {row['last_followup_at'] or '-'} | "
            f"{row['opportunity_score']:.2f} | {','.join(row['reasons'])} | {row['title'] or '-'}"
        )
    return "\n".join(lines)


format_blog_series_continuation_opportunities_table = format_blog_series_continuation_opportunities_text


def _normalize_blog(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _json_object(_first(row, "metadata", "raw_metadata"))
    title = _text(_first(row, "title", "subject") or metadata.get("title"))
    tags = _items(_first(row, "tags", "topics") or metadata.get("tags") or metadata.get("topics"))
    series = _text(_first(row, "series", "series_key", "series_id") or metadata.get("series") or metadata.get("series_key"))
    topic = _text(_first(row, "topic", "topic_key") or metadata.get("topic") or metadata.get("topic_key"))
    return {
        "blog_id": _text(_first(row, "blog_id", "post_id", "content_id", "id", "slug")) or "unknown",
        "title": title,
        "body": _text(_first(row, "body", "content", "excerpt", "summary") or metadata.get("body") or metadata.get("summary")),
        "published_at": _parse_ts(_first(row, "published_at", "created_at", "date")),
        "status": _text(_first(row, "status", "state")).lower(),
        "metadata": metadata,
        "explicit_series": bool(series),
        "topic_or_series_key": _slug(series or topic or (tags[0] if tags else "") or _topic_from_title(title)),
    }


def _engagement_by_blog(rows: list[dict[str, Any]]) -> dict[str, float]:
    scores: dict[str, float] = {}
    for row in rows:
        blog_id = _text(_first(row, "blog_id", "post_id", "content_id", "id"))
        if not blog_id:
            continue
        scores[blog_id] = _engagement_score(row)
    return scores


def _followups_by_blog(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        parent = _text(_first(row, "source_blog_id", "parent_blog_id", "origin_blog_id", "blog_id"))
        source_ids = _items(_first(row, "source_content_ids", "source_ids"))
        for candidate in [parent] + [_text(item) for item in source_ids]:
            if candidate:
                grouped[candidate].append(row)
    return grouped


def _is_published_blog(row: dict[str, Any]) -> bool:
    status = _text(_first(row, "status", "state")).lower()
    content_type = _text(_first(row, "content_type", "type", "format")).lower()
    return (not content_type or "blog" in content_type) and status not in {"draft", "archived", "deleted"} and bool(_first(row, "published_at", "created_at", "date"))


def _is_stale_series(blog: dict[str, Any], latest_at: datetime | None, generated_at: datetime, stale_days: int) -> bool:
    if not blog["explicit_series"]:
        return False
    reference = latest_at or blog["published_at"]
    return bool(reference and (generated_at - reference).days >= stale_days)


def _opportunity_score(
    *,
    engagement_score: float,
    followup_count: int,
    published_at: datetime | None,
    generated_at: datetime,
    reasons: list[str],
) -> float:
    score = engagement_score
    score += 30 * len(reasons)
    score -= followup_count * 15
    if published_at:
        score += min((generated_at - published_at).days, 90) / 3
    return round(max(score, 0.0), 2)


def _engagement_score(row: dict[str, Any]) -> float:
    explicit = _float(_first(row, "engagement_score", "score"))
    if explicit:
        return explicit
    return (
        _float(_first(row, "views", "impressions")) * 0.01
        + _float(_first(row, "clicks")) * 1.0
        + _float(_first(row, "likes", "reactions")) * 2.0
        + _float(_first(row, "shares")) * 3.0
        + _float(_first(row, "comments", "replies")) * 4.0
    )


def _embedded_engagement_score(metadata: dict[str, Any]) -> float:
    return _engagement_score(metadata) if metadata else 0.0


def _topic_from_title(title: str) -> str:
    tokens = [token for token in TOKEN_RE.findall(title.lower()) if token not in STOP_WORDS]
    return "-".join(tokens[:3]) or "unknown"


def _load_blogs(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "blog_posts" if "blog_posts" in schema else "generated_content" if "generated_content" in schema else ""
    if not table:
        return []
    cols = schema[table]
    select = [
        _select(cols, ("id", "post_id", "blog_id", "content_id", "slug"), "id"),
        _select(cols, ("content_type", "type", "format"), "content_type"),
        _select(cols, ("title", "subject"), "title"),
        _select(cols, ("body", "content", "excerpt", "summary"), "body"),
        _select(cols, ("published_at", "created_at", "date"), "published_at"),
        _select(cols, ("status", "state"), "status"),
        _select(cols, ("series", "series_key", "series_id"), "series"),
        _select(cols, ("topic", "topic_key", "tags"), "topic"),
        _select(cols, ("metadata", "raw_metadata"), "metadata"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}").fetchall()]


def _load_engagement(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = next((name for name in ("blog_engagement", "content_engagement", "engagement_metrics") if name in schema), "")
    if not table:
        return []
    cols = schema[table]
    select = [
        _select(cols, ("blog_id", "post_id", "content_id", "id"), "blog_id"),
        _select(cols, ("engagement_score", "score"), "engagement_score"),
        _select(cols, ("views", "impressions"), "views"),
        _select(cols, ("clicks",), "clicks"),
        _select(cols, ("likes", "reactions"), "likes"),
        _select(cols, ("shares",), "shares"),
        _select(cols, ("comments", "replies"), "comments"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}").fetchall()]


def _load_followups(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    cols = schema["generated_content"]
    select = [
        _select(cols, ("id", "content_id"), "id"),
        _select(cols, ("source_blog_id", "parent_blog_id", "origin_blog_id"), "source_blog_id"),
        _select(cols, ("source_content_ids", "source_ids"), "source_content_ids"),
        _select(cols, ("published_at", "created_at", "scheduled_at"), "published_at"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM generated_content").fetchall()]


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "blog_posts" in schema or "generated_content" in schema:
        return {"missing_tables": [], "missing_columns": {}}
    return {"missing_tables": ["blog_posts_or_generated_content"], "missing_columns": {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _first(row: dict[str, Any], *keys: str) -> Any:
    return next((row[key] for key in keys if key in row and row[key] not in (None, "")), None)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple | set):
        return list(value)
    if isinstance(value, str):
        parsed = _json_object_or_list(value)
        if isinstance(parsed, list):
            return parsed
        return [part.strip() for part in value.split(",") if part.strip()]
    return [value]


def _json_object(value: Any) -> dict[str, Any]:
    parsed = _json_object_or_list(value)
    return parsed if isinstance(parsed, dict) else {}


def _json_object_or_list(value: Any) -> Any:
    if isinstance(value, dict | list):
        return value
    if not value:
        return {}
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return {}


def _slug(value: Any) -> str:
    text = _text(value).lower()
    tokens = TOKEN_RE.findall(text)
    return "-".join(tokens) or "unknown"


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _utc(value)
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None
