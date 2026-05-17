"""Compare blog publication timing with related newsletter sends."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_EARLY_TOLERANCE_DAYS = 0
DEFAULT_LATE_AFTER_DAYS = 14
DEFAULT_LIMIT = 100
_TOKEN_RE = re.compile(r"[a-z0-9]+")


def build_blog_newsletter_timing_alignment_report(
    blog_rows: list[dict[str, Any]],
    newsletter_rows: list[dict[str, Any]],
    *,
    early_tolerance_days: int = DEFAULT_EARLY_TOLERANCE_DAYS,
    late_after_days: int = DEFAULT_LATE_AFTER_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if early_tolerance_days < 0:
        raise ValueError("early_tolerance_days must be non-negative")
    if late_after_days < 0:
        raise ValueError("late_after_days must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    blogs = [_normalize_blog(row) for row in blog_rows]
    newsletters = [_normalize_newsletter(row) for row in newsletter_rows]
    findings = []
    for newsletter in newsletters:
        match = _best_blog_match(newsletter, blogs)
        if not match or not newsletter["sent_at"] or not match["published_at"]:
            findings.append(_finding(newsletter, match, "unmatched", None, []))
            continue
        gap_days = (newsletter["sent_at"] - match["published_at"]).total_seconds() / 86400
        if gap_days < -early_tolerance_days:
            status = "early"
        elif gap_days > late_after_days:
            status = "late"
        else:
            status = "aligned"
        findings.append(_finding(newsletter, match, status, round(gap_days, 2), _match_reasons(newsletter, match)))

    findings.sort(key=lambda item: (_status_rank(item["status"]), item["newsletter_sent_at"] or "", item["newsletter_id"]))
    return {
        "artifact_type": "blog_newsletter_timing_alignment",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "early_tolerance_days": early_tolerance_days,
            "late_after_days": late_after_days,
            "limit": limit,
        },
        "totals": {
            "newsletter_count": len(newsletters),
            "matched_count": sum(1 for item in findings if item["status"] != "unmatched"),
            "early_count": sum(1 for item in findings if item["status"] == "early"),
            "aligned_count": sum(1 for item in findings if item["status"] == "aligned"),
            "late_count": sum(1 for item in findings if item["status"] == "late"),
            "unmatched_count": sum(1 for item in findings if item["status"] == "unmatched"),
        },
        "findings": findings[:limit],
        "attention_items": [item for item in findings if item["status"] in {"early", "late", "unmatched"}][:limit],
        "empty_state": {
            "is_empty": not newsletters,
            "message": "No newsletter sends found." if not newsletters else None,
        },
    }


def build_blog_newsletter_timing_alignment_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_blog_newsletter_timing_alignment_report(_load_blogs(conn, schema), _load_newsletters(conn, schema), **kwargs)


def format_blog_newsletter_timing_alignment_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_newsletter_timing_alignment_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Blog Newsletter Timing Alignment",
        f"Generated: {report['generated_at']}",
        f"Filters: late_after_days={report['filters']['late_after_days']} limit={report['filters']['limit']}",
        (
            "Totals: "
            f"newsletters={totals['newsletter_count']} matched={totals['matched_count']} "
            f"early={totals['early_count']} aligned={totals['aligned_count']} "
            f"late={totals['late_count']} unmatched={totals['unmatched_count']}"
        ),
    ]
    if not report["findings"]:
        lines.extend(["", report["empty_state"]["message"] or "No timing findings."])
        return "\n".join(lines)
    lines.extend(["", "Findings:", "status     gap_d   newsletter -> blog"])
    for item in report["findings"]:
        gap = "-" if item["gap_days"] is None else f"{item['gap_days']:.2f}"
        lines.append(
            f"{item['status']:<9}  {gap:>6}  "
            f"{item['newsletter_subject'] or item['newsletter_id']} -> {item['blog_title'] or item['blog_id'] or '-'}"
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
            _select(columns, ("topic", "topics", "tags"), "topic"),
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
        "published_url AS url" if "published_url" in columns else "NULL AS url",
        "content_format AS topic" if "content_format" in columns else "NULL AS topic",
        "published_at" if "published_at" in columns else "created_at AS published_at",
        "published" if "published" in columns else "1 AS published",
    ]
    where = "WHERE LOWER(content_type) LIKE '%blog%'"
    if "published" in columns:
        where += " AND COALESCE(published, 0) = 1"
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content {where}").fetchall()]


def _load_newsletters(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema.get("newsletter_sends", set())
    if not {"id", "sent_at"}.issubset(columns):
        return []
    selected = [
        "id",
        "issue_id" if "issue_id" in columns else "NULL AS issue_id",
        "subject" if "subject" in columns else "NULL AS subject",
        "metadata" if "metadata" in columns else "NULL AS metadata",
        "source_content_ids" if "source_content_ids" in columns else "NULL AS source_content_ids",
        "sent_at",
        "status" if "status" in columns else "'sent' AS status",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM newsletter_sends WHERE LOWER(COALESCE(status, 'sent')) = 'sent'").fetchall()]


def _normalize_blog(row: dict[str, Any]) -> dict[str, Any]:
    title = _text(row.get("title"))
    url = _text(row.get("url") or row.get("published_url"))
    slug = _text(row.get("slug")) or _slug_from_url(url)
    return {
        "id": _text(row.get("id") or row.get("post_id") or slug),
        "title": title,
        "slug": slug,
        "url": url,
        "topic": _text(row.get("topic") or row.get("topics") or row.get("tags")),
        "published_at": _parse_dt(row.get("published_at") or row.get("created_at")),
        "tokens": _tokens(" ".join([title, slug, _text(row.get("topic") or row.get("topics"))])),
    }


def _normalize_newsletter(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _json_obj(row.get("metadata"))
    body = _text(row.get("body") or metadata.get("body") or metadata.get("content"))
    url = _text(row.get("url") or metadata.get("url") or metadata.get("blog_url"))
    subject = _text(row.get("subject") or metadata.get("subject"))
    return {
        "id": _text(row.get("id") or row.get("issue_id")),
        "issue_id": _text(row.get("issue_id")),
        "subject": subject,
        "url": url,
        "body": body,
        "source_content_ids": _ids(row.get("source_content_ids")),
        "sent_at": _parse_dt(row.get("sent_at") or row.get("created_at")),
        "tokens": _tokens(" ".join([subject, body, url, _text(metadata.get("topic"))])),
    }


def _best_blog_match(newsletter: dict[str, Any], blogs: list[dict[str, Any]]) -> dict[str, Any] | None:
    scored = []
    for blog in blogs:
        reasons = _match_reasons(newsletter, blog)
        if reasons:
            scored.append((len(reasons), len(newsletter["tokens"] & blog["tokens"]), blog))
    if not scored:
        return None
    scored.sort(key=lambda item: (-item[0], -item[1], item[2]["published_at"] or datetime.min.replace(tzinfo=timezone.utc)))
    return scored[0][2]


def _match_reasons(newsletter: dict[str, Any], blog: dict[str, Any]) -> list[str]:
    reasons = []
    if blog["url"] and (blog["url"] == newsletter["url"] or blog["url"] in newsletter["body"]):
        reasons.append("url")
    if blog["slug"] and (blog["slug"] in newsletter["url"] or blog["slug"] in newsletter["body"].lower()):
        reasons.append("slug")
    if blog["id"] and blog["id"] in newsletter["source_content_ids"]:
        reasons.append("source_content_id")
    if len(newsletter["tokens"] & blog["tokens"]) >= 2:
        reasons.append("shared_title_tokens")
    return reasons


def _finding(newsletter: dict[str, Any], blog: dict[str, Any] | None, status: str, gap_days: float | None, reasons: list[str]) -> dict[str, Any]:
    return {
        "newsletter_id": newsletter["id"],
        "issue_id": newsletter["issue_id"],
        "newsletter_subject": newsletter["subject"],
        "newsletter_sent_at": newsletter["sent_at"].isoformat() if newsletter["sent_at"] else None,
        "blog_id": blog["id"] if blog else None,
        "blog_title": blog["title"] if blog else None,
        "blog_url": blog["url"] if blog else None,
        "blog_published_at": blog["published_at"].isoformat() if blog and blog["published_at"] else None,
        "gap_days": gap_days,
        "status": status,
        "match_reasons": reasons,
    }


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


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _ids(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, (list, tuple, set)):
        return {_text(item) for item in value if _text(item)}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return {_text(value)}
    return {_text(item) for item in decoded} if isinstance(decoded, list) else {_text(decoded)}


def _tokens(value: str) -> set[str]:
    return {token for token in _TOKEN_RE.findall(value.lower()) if len(token) > 2}


def _slug_from_url(value: str) -> str:
    return value.rstrip("/").rsplit("/", 1)[-1].lower() if value else ""


def _status_rank(status: str) -> int:
    return {"early": 0, "late": 1, "unmatched": 2, "aligned": 3}.get(status, 4)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _utc(value)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return _utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()
