"""Flag newsletter links that point to old or undated source material."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_FRESH_DAYS = 30
DEFAULT_STALE_DAYS = 90
DEFAULT_LIMIT = 100
URL_RE = re.compile(r"https?://[^\s<>)\"']+")
BUCKETS = ("fresh", "aging", "stale", "unknown_date")


def build_newsletter_link_recency_report(
    newsletter_rows: list[dict[str, Any]],
    source_rows: list[dict[str, Any]] | None = None,
    *,
    fresh_days: int = DEFAULT_FRESH_DAYS,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if fresh_days < 0 or stale_days <= 0 or fresh_days > stale_days:
        raise ValueError("fresh_days must be non-negative and no greater than stale_days")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    sources = {_text(_first(row, "url", "source_url", "link_url")): row for row in source_rows or []}
    links = []
    counts = Counter({bucket: 0 for bucket in BUCKETS})
    for newsletter in newsletter_rows:
        newsletter_id = _text(_first(newsletter, "newsletter_id", "id", "send_id")) or "unknown"
        sent_at = _parse_ts(_first(newsletter, "sent_at", "created_at", "published_at")) or generated_at
        for url in sorted(set(URL_RE.findall(_text(_first(newsletter, "body", "html", "content", "text"))))):
            source = sources.get(url, {})
            source_date = _parse_ts(_first(source, "source_date", "published_at", "created_at", "date"))
            age_days = round((sent_at - source_date).total_seconds() / 86400, 2) if source_date else None
            bucket = _bucket(age_days, fresh_days, stale_days)
            counts[bucket] += 1
            links.append(
                {
                    "newsletter_id": newsletter_id,
                    "url": url,
                    "source_date": source_date.isoformat() if source_date else None,
                    "age_days": age_days,
                    "recency_bucket": bucket,
                }
            )
    links.sort(key=lambda item: (BUCKETS.index(item["recency_bucket"]), -(item["age_days"] or -1), item["newsletter_id"], item["url"]))
    total = len(links)
    return {
        "artifact_type": "newsletter_link_recency",
        "generated_at": generated_at.isoformat(),
        "filters": {"fresh_days": fresh_days, "stale_days": stale_days, "limit": limit},
        "totals": {
            "link_count": total,
            "fresh": counts["fresh"],
            "aging": counts["aging"],
            "stale": counts["stale"],
            "unknown_date": counts["unknown_date"],
            "stale_rate": round(counts["stale"] / total, 4) if total else 0.0,
        },
        "links": links[:limit],
        "empty_state": {"is_empty": not links, "message": "No newsletter links found." if not links else None},
    }


def build_newsletter_link_recency_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_newsletter_link_recency_report(_load_newsletters(conn, schema), _load_sources(conn, schema), **kwargs)


def format_newsletter_link_recency_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_link_recency_text(report: dict[str, Any]) -> str:
    lines = [
        "Newsletter Link Recency",
        f"Generated: {report['generated_at']}",
        f"Buckets: fresh<={report['filters']['fresh_days']}d stale>{report['filters']['stale_days']}d",
        (
            f"Totals: links={report['totals']['link_count']} fresh={report['totals']['fresh']} aging={report['totals']['aging']} "
            f"stale={report['totals']['stale']} unknown={report['totals']['unknown_date']} stale_rate={report['totals']['stale_rate']:.2f}"
        ),
    ]
    if not report["links"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "newsletter_id | bucket | age_days | source_date | url"])
    for row in report["links"]:
        lines.append(f"{row['newsletter_id']} | {row['recency_bucket']} | {row['age_days'] if row['age_days'] is not None else '-'} | {row['source_date'] or '-'} | {row['url']}")
    return "\n".join(lines)


format_newsletter_link_recency_table = format_newsletter_link_recency_text


def _load_newsletters(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "newsletter_sends" not in schema:
        return []
    cols = schema["newsletter_sends"]
    selected = [
        _col(cols, "id", "newsletter_id", default="NULL") + " AS newsletter_id",
        _col(cols, "body", "html", "content", "text", default="NULL") + " AS body",
        _col(cols, "sent_at", "created_at", "published_at", default="NULL") + " AS sent_at",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM newsletter_sends").fetchall()]


def _load_sources(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("newsletter_links", "source_metadata", "knowledge_sources", "generated_content"):
        if table not in schema:
            continue
        cols = schema[table]
        selected = [
            _col(cols, "url", "source_url", "link_url", default="NULL") + " AS url",
            _col(cols, "source_date", "published_at", "created_at", "date", default="NULL") + " AS source_date",
        ]
        return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]
    return []


def _bucket(age_days: float | None, fresh_days: int, stale_days: int) -> str:
    if age_days is None:
        return "unknown_date"
    if age_days <= fresh_days:
        return "fresh"
    if age_days <= stale_days:
        return "aging"
    return "stale"


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


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
