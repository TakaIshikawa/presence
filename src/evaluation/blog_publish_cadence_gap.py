"""Report gaps in published blog cadence."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_WARNING_DAYS = 14
DEFAULT_CRITICAL_DAYS = 30
DEFAULT_LIMIT = 25


def build_blog_publish_cadence_gap_report(
    post_rows: list[dict[str, Any]],
    *,
    warning_days: int = DEFAULT_WARNING_DAYS,
    critical_days: int = DEFAULT_CRITICAL_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return inter-post gap metrics for published blog posts."""
    if warning_days <= 0:
        raise ValueError("warning_days must be positive")
    if critical_days < warning_days:
        raise ValueError("critical_days must be greater than or equal to warning_days")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    posts = sorted(
        (post for post in (_normalize_post(row) for row in post_rows) if post["published_at"]),
        key=lambda post: post["published_at"],
    )
    gaps = []
    for previous, current in zip(posts, posts[1:]):
        gap_days = (current["published_at"] - previous["published_at"]).total_seconds() / 86400
        status = _gap_status(gap_days, warning_days, critical_days)
        gaps.append(
            {
                "previous_post_id": previous["id"],
                "previous_title": previous["title"],
                "previous_published_at": previous["published_at"].isoformat(),
                "next_post_id": current["id"],
                "next_title": current["title"],
                "next_published_at": current["published_at"].isoformat(),
                "gap_days": round(gap_days, 2),
                "status": status,
            }
        )

    latest_post = posts[-1] if posts else None
    days_since_latest = (
        round((generated_at - latest_post["published_at"]).total_seconds() / 86400, 2)
        if latest_post
        else None
    )
    flagged = [gap for gap in gaps if gap["status"] != "ok"]
    flagged.sort(key=lambda gap: (-gap["gap_days"], gap["next_published_at"]))

    return {
        "artifact_type": "blog_publish_cadence_gap",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "warning_days": warning_days,
            "critical_days": critical_days,
            "limit": limit,
        },
        "totals": {
            "published_post_count": len(posts),
            "gap_count": len(gaps),
            "warning_gap_count": sum(1 for gap in gaps if gap["status"] == "warning"),
            "critical_gap_count": sum(1 for gap in gaps if gap["status"] == "critical"),
            "longest_gap_days": round(max((gap["gap_days"] for gap in gaps), default=0.0), 2),
            "average_gap_days": round(sum(gap["gap_days"] for gap in gaps) / len(gaps), 2) if gaps else 0.0,
            "days_since_latest_post": days_since_latest,
            "overdue_status": _overdue_status(days_since_latest, warning_days, critical_days),
        },
        "latest_post": _post_payload(latest_post),
        "gaps": gaps,
        "flagged_gaps": flagged[:limit],
        "empty_state": {
            "is_empty": not posts,
            "message": "No published blog posts found." if not posts else None,
        },
    }


def build_blog_publish_cadence_gap_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    return build_blog_publish_cadence_gap_report(_load_posts(conn, _schema(conn)), **kwargs)


def format_blog_publish_cadence_gap_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_publish_cadence_gap_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Blog Publish Cadence Gap",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: warning_days={report['filters']['warning_days']} "
            f"critical_days={report['filters']['critical_days']} limit={report['filters']['limit']}"
        ),
        (
            "Totals: "
            f"posts={totals['published_post_count']} gaps={totals['gap_count']} "
            f"longest={totals['longest_gap_days']:.2f}d average={totals['average_gap_days']:.2f}d "
            f"overdue={totals['overdue_status']}"
        ),
    ]
    if not report["gaps"]:
        lines.extend(["", report["empty_state"]["message"] or "Not enough published posts to compute gaps."])
        return "\n".join(lines)
    lines.extend(["", "Flagged gaps:", "status    days    previous -> next"])
    if not report["flagged_gaps"]:
        lines.append("No gaps exceeded the configured thresholds.")
        return "\n".join(lines)
    for gap in report["flagged_gaps"]:
        lines.append(
            f"{gap['status']:<8}  {gap['gap_days']:>6.2f}  "
            f"{gap['previous_title'] or gap['previous_post_id']} -> {gap['next_title'] or gap['next_post_id']}"
        )
    return "\n".join(lines)


def _load_posts(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "blog_posts" in schema:
        columns = schema["blog_posts"]
        selected = [
            _select(columns, ("id", "post_id", "slug"), "id"),
            _select(columns, ("title",), "title"),
            _select(columns, ("slug",), "slug"),
            _select(columns, ("url", "published_url", "canonical_url"), "url"),
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
        "published_at" if "published_at" in columns else "created_at AS published_at",
        "published" if "published" in columns else "1 AS published",
    ]
    where = "WHERE LOWER(content_type) LIKE '%blog%'"
    if "published" in columns:
        where += " AND COALESCE(published, 0) = 1"
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content {where}").fetchall()]


def _normalize_post(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _text(row.get("id") or row.get("post_id") or row.get("slug")),
        "title": _text(row.get("title")),
        "url": _text(row.get("url") or row.get("published_url")),
        "published_at": _parse_dt(row.get("published_at") or row.get("created_at")),
    }


def _post_payload(post: dict[str, Any] | None) -> dict[str, Any] | None:
    if not post:
        return None
    return {
        "id": post["id"],
        "title": post["title"],
        "url": post["url"],
        "published_at": post["published_at"].isoformat(),
    }


def _gap_status(days: float, warning_days: int, critical_days: int) -> str:
    if days >= critical_days:
        return "critical"
    if days >= warning_days:
        return "warning"
    return "ok"


def _overdue_status(days: float | None, warning_days: int, critical_days: int) -> str:
    if days is None:
        return "no_posts"
    return _gap_status(days, warning_days, critical_days)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _utc(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return _utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()
