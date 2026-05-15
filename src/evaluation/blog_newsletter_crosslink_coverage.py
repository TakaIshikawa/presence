"""Find blog posts and newsletter issues that should cross-link."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_WINDOW_DAYS = 14
DEFAULT_MIN_SHARED_TOKENS = 2
DEFAULT_LIMIT = 100
STOPWORDS = {
    "about",
    "after",
    "again",
    "from",
    "have",
    "into",
    "that",
    "the",
    "this",
    "with",
    "your",
}


def build_blog_newsletter_crosslink_coverage_report(
    blog_rows: list[dict[str, Any]],
    newsletter_rows: list[dict[str, Any]],
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    min_shared_tokens: int = DEFAULT_MIN_SHARED_TOKENS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return candidate blog/newsletter pairs and missing cross-link directions."""
    if window_days < 0:
        raise ValueError("window_days must be non-negative")
    if min_shared_tokens <= 0:
        raise ValueError("min_shared_tokens must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    blogs = [_normalize_blog(row) for row in blog_rows]
    newsletters = [_normalize_newsletter(row) for row in newsletter_rows]
    candidates = []
    for blog in blogs:
        if not blog["published_at"]:
            continue
        for newsletter in newsletters:
            if not newsletter["sent_at"]:
                continue
            reasons = _match_reasons(blog, newsletter, window_days, min_shared_tokens)
            if not reasons:
                continue
            gap = abs((blog["published_at"] - newsletter["sent_at"]).total_seconds() / 86400)
            newsletter_links_blog = _links_to_blog(newsletter, blog)
            blog_links_newsletter = _links_to_newsletter(blog, newsletter)
            missing = []
            if not newsletter_links_blog:
                missing.append("newsletter_to_blog")
            if not blog_links_newsletter:
                missing.append("blog_to_newsletter")
            candidates.append(
                {
                    "blog_id": blog["id"],
                    "blog_title": blog["title"],
                    "blog_url": blog["url"],
                    "blog_published_at": blog["published_at"].isoformat(),
                    "newsletter_send_id": newsletter["id"],
                    "issue_id": newsletter["issue_id"],
                    "newsletter_subject": newsletter["subject"],
                    "newsletter_url": newsletter["url"],
                    "newsletter_sent_at": newsletter["sent_at"].isoformat(),
                    "publication_date_gap_days": round(gap, 2),
                    "match_reason": ", ".join(reasons),
                    "match_reasons": reasons,
                    "newsletter_links_blog": newsletter_links_blog,
                    "blog_links_newsletter": blog_links_newsletter,
                    "missing_directions": missing,
                }
            )

    candidates.sort(key=_candidate_sort_key)
    ranked = candidates[:limit]
    totals = {
        "candidate_pair_count": len(candidates),
        "record_count": len(ranked),
        "newsletter_to_blog_linked": sum(1 for item in candidates if item["newsletter_links_blog"]),
        "blog_to_newsletter_linked": sum(1 for item in candidates if item["blog_links_newsletter"]),
        "missing_newsletter_to_blog": sum(1 for item in candidates if not item["newsletter_links_blog"]),
        "missing_blog_to_newsletter": sum(1 for item in candidates if not item["blog_links_newsletter"]),
    }
    totals["newsletter_to_blog_coverage_rate"] = _rate(totals["newsletter_to_blog_linked"], len(candidates))
    totals["blog_to_newsletter_coverage_rate"] = _rate(totals["blog_to_newsletter_linked"], len(candidates))
    return {
        "artifact_type": "blog_newsletter_crosslink_coverage",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "window_days": window_days,
            "min_shared_tokens": min_shared_tokens,
            "limit": limit,
        },
        "totals": totals,
        "pairs": ranked,
        "missing_pairs": [item for item in ranked if item["missing_directions"]],
        "empty_state": {
            "is_empty": not candidates,
            "message": "No candidate blog/newsletter pairs found." if not candidates else None,
        },
    }


def build_blog_newsletter_crosslink_coverage_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_blog_newsletter_crosslink_coverage_report(
        _load_blogs(conn, schema),
        _load_newsletters(conn, schema),
        **kwargs,
    )


def format_blog_newsletter_crosslink_coverage_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_newsletter_crosslink_coverage_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Blog Newsletter Crosslink Coverage",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: window_days={report['filters']['window_days']} "
            f"min_shared_tokens={report['filters']['min_shared_tokens']} limit={report['filters']['limit']}"
        ),
        (
            "Totals: "
            f"pairs={totals['candidate_pair_count']} "
            f"newsletter_to_blog={totals['newsletter_to_blog_coverage_rate']:.2%} "
            f"blog_to_newsletter={totals['blog_to_newsletter_coverage_rate']:.2%} "
            f"missing_n2b={totals['missing_newsletter_to_blog']} "
            f"missing_b2n={totals['missing_blog_to_newsletter']}"
        ),
    ]
    if not report["pairs"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)
    lines.extend(["", "Pairs:", "gap_d  n2b  b2n  missing                 reason        blog / newsletter"])
    for item in report["pairs"]:
        missing = ",".join(item["missing_directions"]) or "-"
        lines.append(
            f"{item['publication_date_gap_days']:<6.2f} "
            f"{str(item['newsletter_links_blog']):<4} "
            f"{str(item['blog_links_newsletter']):<4} "
            f"{missing[:23]:<23} "
            f"{item['match_reason'][:13]:<13} "
            f"{item['blog_title'] or item['blog_id']} / {item['newsletter_subject'] or item['issue_id']}"
        )
    return "\n".join(lines)


def _load_blogs(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "blog_posts" in schema:
        columns = schema["blog_posts"]
        selected = [
            _select(columns, ("id", "post_id", "slug"), "id"),
            _select(columns, ("title",), "title"),
            _select(columns, ("slug",), "slug"),
            _select(columns, ("url", "published_url", "canonical_url"), "url"),
            _select(columns, ("content", "body", "html"), "content"),
            _select(columns, ("topic", "topics", "tags"), "topics"),
            _select(columns, ("source_content_ids", "source_ids"), "source_content_ids"),
            _select(columns, ("published_at", "created_at"), "published_at"),
            _select(columns, ("status", "publication_status"), "status"),
        ]
        where = "WHERE LOWER(COALESCE(status, 'published')) = 'published'" if "status" in columns else ""
        return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM blog_posts {where}").fetchall()]
    columns = schema.get("generated_content", set())
    if not {"id", "content_type"}.issubset(columns):
        return []
    selected = [
        "id",
        "content AS title" if "content" in columns else "NULL AS title",
        "content" if "content" in columns else "NULL AS content",
        "published_url" if "published_url" in columns else "NULL AS url",
        "source_commits" if "source_commits" in columns else "NULL AS source_commits",
        "source_messages" if "source_messages" in columns else "NULL AS source_messages",
        "source_activity_ids" if "source_activity_ids" in columns else "NULL AS source_activity_ids",
        "content_format" if "content_format" in columns else "NULL AS topics",
        "published_at" if "published_at" in columns else "created_at AS published_at",
        "published" if "published" in columns else "1 AS published",
    ]
    where = "WHERE content_type LIKE '%blog%' AND COALESCE(published, 0) = 1" if "published" in columns else "WHERE content_type LIKE '%blog%'"
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content {where}").fetchall()]


def _load_newsletters(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema.get("newsletter_sends", set())
    if not {"id", "sent_at"}.issubset(columns):
        return []
    selected = [
        "id",
        "issue_id" if "issue_id" in columns else "NULL AS issue_id",
        "subject" if "subject" in columns else "NULL AS subject",
        "source_content_ids" if "source_content_ids" in columns else "NULL AS source_content_ids",
        "metadata" if "metadata" in columns else "NULL AS metadata",
        "sent_at",
        "status" if "status" in columns else "'sent' AS status",
    ]
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(selected)}
               FROM newsletter_sends
               WHERE LOWER(COALESCE(status, 'sent')) = 'sent'"""
        ).fetchall()
    ]


def _normalize_blog(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _json_obj(row.get("metadata"))
    return {
        "id": _text(_first(row, "blog_id", "post_id", "id", "slug")),
        "title": _text(_first(row, "title", "subject")),
        "slug": _text(row.get("slug")),
        "url": _text(_first(row, "url", "published_url", "canonical_url") or metadata.get("url")),
        "published_at": _parse_dt(_first(row, "published_at", "created_at")),
        "tokens": _tokens(" ".join(_texts(_first(row, "title", "subject"), row.get("topic"), row.get("topics"), row.get("tags")))),
        "source_ids": _source_ids(row, include_id=True),
        "link_text": _link_text(row, metadata),
    }


def _normalize_newsletter(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _json_obj(row.get("metadata"))
    return {
        "id": _text(_first(row, "newsletter_send_id", "send_id", "id")),
        "issue_id": _text(_first(row, "issue_id", "newsletter_issue_id") or metadata.get("issue_id")),
        "subject": _text(_first(row, "subject", "title")),
        "url": _text(_first(row, "url", "issue_url", "archive_url") or metadata.get("url") or metadata.get("archive_url")),
        "sent_at": _parse_dt(_first(row, "sent_at", "published_at", "created_at")),
        "tokens": _tokens(" ".join(_texts(_first(row, "subject", "title"), row.get("topic"), row.get("topics"), row.get("tags")))),
        "source_ids": _source_ids(row, include_id=False),
        "link_text": _link_text(row, metadata),
    }


def _match_reasons(
    blog: dict[str, Any],
    newsletter: dict[str, Any],
    window_days: int,
    min_shared_tokens: int,
) -> list[str]:
    reasons = []
    gap = abs((blog["published_at"] - newsletter["sent_at"]).total_seconds() / 86400)
    if gap <= window_days:
        reasons.append("publication_window")
    shared_tokens = sorted(blog["tokens"] & newsletter["tokens"])
    if len(shared_tokens) >= min_shared_tokens:
        reasons.append("shared_tokens:" + "|".join(shared_tokens[:5]))
    shared_sources = sorted(blog["source_ids"] & newsletter["source_ids"])
    if shared_sources:
        reasons.append("shared_source_ids:" + "|".join(shared_sources[:5]))
    return reasons


def _links_to_blog(newsletter: dict[str, Any], blog: dict[str, Any]) -> bool:
    return _contains_reference(newsletter["link_text"], blog["url"], blog["slug"], blog["id"])


def _links_to_newsletter(blog: dict[str, Any], newsletter: dict[str, Any]) -> bool:
    return _contains_reference(blog["link_text"], newsletter["url"], newsletter["issue_id"], newsletter["id"])


def _contains_reference(haystack: str, *needles: str) -> bool:
    text = haystack.lower()
    for needle in needles:
        value = _text(needle).lower()
        if value and value in text:
            return True
    return False


def _source_ids(row: dict[str, Any], *, include_id: bool) -> set[str]:
    values: set[str] = set()
    if include_id:
        own_id = _text(_first(row, "content_id", "blog_id", "post_id", "id"))
        if own_id:
            values.add(f"content:{own_id}")
    for key in (
        "source_content_ids",
        "content_ids",
        "generated_content_ids",
        "source_ids",
        "source_commits",
        "source_messages",
        "source_activity_ids",
    ):
        for value in _parse_list(row.get(key)):
            text = _text(value)
            if text:
                prefix = "content" if "content" in key else key.removeprefix("source_").removesuffix("s")
                values.add(f"{prefix}:{text}")
                if key in {"source_content_ids", "content_ids", "generated_content_ids"}:
                    values.add(f"content:{text}")
    return values


def _link_text(row: dict[str, Any], metadata: dict[str, Any]) -> str:
    chunks = _texts(
        row.get("content"),
        row.get("body"),
        row.get("html"),
        row.get("markdown"),
        row.get("links"),
        row.get("url"),
        row.get("published_url"),
        metadata.get("links"),
        metadata.get("body"),
        metadata.get("html"),
        metadata.get("url"),
        metadata.get("archive_url"),
    )
    return " ".join(chunks)


def _tokens(value: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9]{3,}", value.lower()) if token not in STOPWORDS}


def _candidate_sort_key(item: dict[str, Any]) -> tuple[int, float, str, str]:
    return (
        len(item["missing_directions"]),
        item["publication_date_gap_days"],
        str(item["blog_id"]),
        str(item["newsletter_send_id"]),
    )


def _rate(count: int, total: int) -> float:
    return round(count / total, 4) if total else 0.0


def _select(columns: set[str], names: tuple[str, ...], alias: str) -> str:
    for name in names:
        if name in columns:
            return f"{name} AS {alias}"
    return f"NULL AS {alias}"


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
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if row.get(key) not in (None, ""):
            return row.get(key)
    return None


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
        if isinstance(parsed, list):
            return parsed
        return [parsed]
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


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}
