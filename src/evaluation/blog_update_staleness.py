"""Rank published blog posts that need source or metadata refresh."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_MONITOR_DAYS = 30
DEFAULT_REFRESH_DAYS = 90
DEFAULT_URGENT_DAYS = 180
DEFAULT_LIMIT = 50


def build_blog_update_staleness_report(
    post_rows: list[dict[str, Any]],
    *,
    monitor_days: int = DEFAULT_MONITOR_DAYS,
    refresh_days: int = DEFAULT_REFRESH_DAYS,
    urgent_days: int = DEFAULT_URGENT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return ranked staleness records from in-memory blog post rows."""
    if not (0 < monitor_days <= refresh_days <= urgent_days):
        raise ValueError("thresholds must satisfy 0 < monitor_days <= refresh_days <= urgent_days")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    records = []
    for row in post_rows:
        if _text(row.get("status") or row.get("publication_status") or "published").lower() != "published":
            continue
        published_at = _parse_dt(row.get("published_at") or row.get("created_at"))
        last_updated = _parse_dt(row.get("last_updated") or row.get("updated_at"))
        newest_source_at = _parse_dt(row.get("newest_source_at") or row.get("source_updated_at"))
        reference = last_updated or published_at or newest_source_at
        age_days = _age_days(generated_at, reference)
        source_age_days = _age_days(generated_at, newest_source_at)
        publication_age_days = _age_days(generated_at, published_at)
        missing_update_timestamp = last_updated is None
        severity_days = max(
            age_days if age_days is not None else urgent_days + 1,
            source_age_days or 0,
            publication_age_days or 0,
        )
        classification = _classify(severity_days, monitor_days, refresh_days, urgent_days, missing_update_timestamp)
        records.append(
            {
                "post_id": _text(row.get("post_id") or row.get("id") or row.get("slug")),
                "slug": _text(row.get("slug")),
                "title": _text(row.get("title")),
                "status": "published",
                "published_at": _iso(published_at),
                "last_updated": _iso(last_updated),
                "newest_source_at": _iso(newest_source_at),
                "age_days": age_days,
                "source_age_days": source_age_days,
                "publication_age_days": publication_age_days,
                "missing_update_timestamp": missing_update_timestamp,
                "classification": classification,
                "severity_score": round(severity_days, 2),
                "recommended_action": _action(classification, missing_update_timestamp),
            }
        )

    records.sort(key=_sort_key)
    ranked = records[:limit]
    totals = {
        "post_count": len(records),
        "ranked_count": len(ranked),
        "fresh": sum(1 for item in records if item["classification"] == "fresh"),
        "monitor": sum(1 for item in records if item["classification"] == "monitor"),
        "refresh_due": sum(1 for item in records if item["classification"] == "refresh_due"),
        "urgent": sum(1 for item in records if item["classification"] == "urgent"),
        "missing_update_timestamps": sum(1 for item in records if item["missing_update_timestamp"]),
    }
    return {
        "artifact_type": "blog_update_staleness",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "monitor_days": monitor_days,
            "refresh_days": refresh_days,
            "urgent_days": urgent_days,
            "limit": limit,
        },
        "totals": totals,
        "posts": ranked,
        "empty_state": {
            "is_empty": not records,
            "message": "No published blog posts found." if not records else None,
        },
    }


def build_blog_update_staleness_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_blog_update_staleness_report(_load_posts(conn, schema), **kwargs)


def format_blog_update_staleness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_update_staleness_text(report: dict[str, Any]) -> str:
    lines = [
        "Blog Update Staleness",
        f"Generated: {report['generated_at']}",
        (
            f"Thresholds: monitor={report['filters']['monitor_days']}d "
            f"refresh={report['filters']['refresh_days']}d urgent={report['filters']['urgent_days']}d"
        ),
        (
            "Totals: "
            f"posts={report['totals']['post_count']} urgent={report['totals']['urgent']} "
            f"refresh_due={report['totals']['refresh_due']} monitor={report['totals']['monitor']}"
        ),
    ]
    if not report["posts"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)
    lines.extend(["", "Posts:", "class        score   updated  source   title"])
    for item in report["posts"]:
        updated = "-" if item["age_days"] is None else f"{item['age_days']:.1f}"
        source = "-" if item["source_age_days"] is None else f"{item['source_age_days']:.1f}"
        lines.append(
            f"{item['classification']:<12} {item['severity_score']:<7.1f} "
            f"{updated:<8} {source:<8} {item['title'] or item['slug'] or item['post_id']}"
        )
    return "\n".join(lines)


def _load_posts(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "blog_posts" if "blog_posts" in schema else "generated_content" if "generated_content" in schema else None
    if table is None:
        return []
    columns = schema[table]
    if table == "generated_content":
        selected = [
            "id",
            "content AS title" if "content" in columns else "NULL AS title",
            "content_type" if "content_type" in columns else "'blog_post' AS content_type",
            "published_at" if "published_at" in columns else "NULL AS published_at",
            "updated_at" if "updated_at" in columns else "NULL AS updated_at",
            "created_at" if "created_at" in columns else "NULL AS created_at",
            "'published' AS status",
        ]
        where = "WHERE content_type LIKE '%blog%' AND COALESCE(published, 0) = 1" if "published" in columns else "WHERE content_type LIKE '%blog%'"
    else:
        selected = [
            "id",
            "slug" if "slug" in columns else "NULL AS slug",
            "title" if "title" in columns else "NULL AS title",
            "status" if "status" in columns else "'published' AS status",
            "published_at" if "published_at" in columns else "NULL AS published_at",
            "last_updated" if "last_updated" in columns else "NULL AS last_updated",
            "updated_at" if "updated_at" in columns else "NULL AS updated_at",
            "newest_source_at" if "newest_source_at" in columns else "NULL AS newest_source_at",
        ]
        where = "WHERE LOWER(COALESCE(status, 'published')) = 'published'" if "status" in columns else ""
    rows = conn.execute(f"SELECT {', '.join(selected)} FROM {table} {where}").fetchall()
    return [dict(row) for row in rows]


def _classify(days: float, monitor: int, refresh: int, urgent: int, missing_update: bool) -> str:
    if days >= urgent:
        return "urgent"
    if days >= refresh:
        return "refresh_due"
    if days >= monitor or missing_update:
        return "monitor"
    return "fresh"


def _action(classification: str, missing_update: bool) -> str:
    if missing_update:
        return "add last_updated metadata and review source freshness"
    return {
        "fresh": "no refresh needed",
        "monitor": "schedule source review",
        "refresh_due": "refresh cited evidence and metadata",
        "urgent": "prioritize rewrite or evidence audit",
    }[classification]


def _sort_key(item: dict[str, Any]) -> tuple[int, float, str]:
    rank = {"urgent": 3, "refresh_due": 2, "monitor": 1, "fresh": 0}
    return (-rank[item["classification"]], -item["severity_score"], item["post_id"])


def _age_days(now: datetime, value: datetime | None) -> float | None:
    return round(max((now - value).total_seconds() / 86400, 0), 2) if value else None


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return _utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in tables}
