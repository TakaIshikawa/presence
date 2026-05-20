"""Audit engagement metrics against publication state."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_MINIMUM_AGE_HOURS = 24
METRIC_TABLES = {
    "x": "post_engagement",
    "bluesky": "bluesky_engagement",
    "linkedin": "linkedin_engagement",
    "mastodon": "mastodon_engagement",
}


def build_engagement_metric_publication_gaps_report(
    metric_rows: list[dict[str, Any]],
    content_rows: list[dict[str, Any]],
    publication_rows: list[dict[str, Any]],
    *,
    minimum_age_hours: int = DEFAULT_MINIMUM_AGE_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if minimum_age_hours <= 0:
        raise ValueError("minimum_age_hours must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(hours=minimum_age_hours)
    content_by_id = {
        content_id: row
        for row in content_rows
        if (content_id := _int_or_none(row.get("content_id") or row.get("id"))) is not None
    }
    publications_by_key: dict[tuple[int, str], dict[str, Any]] = {}
    for pub in publication_rows:
        content_id = _int_or_none(pub.get("content_id"))
        if content_id is None:
            continue
        platform = _platform(pub)
        existing = publications_by_key.get((content_id, platform))
        if existing is None or _parse_dt(pub.get("published_at")) or _status(pub) == "published":
            publications_by_key[(content_id, platform)] = pub

    metric_keys: set[tuple[int, str]] = set()
    gap_items: list[dict[str, Any]] = []
    for row in metric_rows:
        platform = _platform(row)
        content_id = _int_or_none(row.get("content_id"))
        if content_id is None:
            gap_items.append(_item(row, None, "engagement_missing_content_id"))
            continue
        metric_keys.add((content_id, platform))
        content = content_by_id.get(content_id)
        if content is None and content_rows:
            gap_items.append(_item(row, None, "engagement_content_missing"))
            continue
        publication = publications_by_key.get((content_id, platform))
        if publication is None and content is not None and not _content_published(content):
            gap_items.append(_item(row, None, "content_without_published_marker"))
        if publication is not None:
            metric_id = _clean(row.get("metric_post_id"))
            metric_url = _clean(row.get("metric_url"))
            pub_id = _clean(publication.get("platform_post_id"))
            pub_url = _clean(publication.get("platform_url"))
            if metric_id and pub_id and metric_id != pub_id:
                gap_items.append(_item(row, publication, "platform_post_id_mismatch"))
            if metric_url and pub_url and metric_url != pub_url:
                gap_items.append(_item(row, publication, "platform_url_mismatch"))

    for pub in publication_rows:
        content_id = _int_or_none(pub.get("content_id"))
        platform = _platform(pub)
        published_at = _parse_dt(pub.get("published_at"))
        if content_id is None or _status(pub) != "published" or not published_at or published_at > cutoff:
            continue
        if (content_id, platform) not in metric_keys:
            gap_items.append(_publication_item(pub, "published_content_missing_engagement", generated_at))

    gap_items.sort(key=_sort_key)
    shown = gap_items[:limit]
    return {
        "artifact_type": "engagement_metric_publication_gaps",
        "generated_at": generated_at.isoformat(),
        "thresholds": {"minimum_age_hours": minimum_age_hours, "limit": limit},
        "summary": {
            "metric_count": len(metric_rows),
            "publication_count": len(publication_rows),
            "gap_count": len(gap_items),
            "shown_count": len(shown),
            "by_platform": dict(sorted(Counter(item["platform"] for item in gap_items).items())),
            "by_issue_type": dict(sorted(Counter(item["issue_type"] for item in gap_items).items())),
        },
        "groups": _groups(gap_items),
        "gap_items": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items())},
    }


def build_engagement_metric_publication_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [table for table in ("content_publications", "generated_content", *METRIC_TABLES.values()) if table not in schema]
    missing_columns: dict[str, list[str]] = {}
    content_rows = _load_content(conn, schema, missing_columns) if "generated_content" in schema else []
    publication_rows = _load_publications(conn, schema, missing_columns) if "content_publications" in schema else []
    metric_rows: list[dict[str, Any]] = []
    for platform, table in METRIC_TABLES.items():
        if table in schema:
            metric_rows.extend(_load_metrics(conn, schema, table, platform, missing_columns))
    return build_engagement_metric_publication_gaps_report(
        metric_rows,
        content_rows,
        publication_rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_engagement_metric_publication_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_engagement_metric_publication_gaps_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Engagement Metric Publication Gaps",
        f"Generated: {report['generated_at']}",
        f"Minimum age: {report['thresholds']['minimum_age_hours']} hours",
        f"Totals: metrics={summary['metric_count']} publications={summary['publication_count']} gaps={summary['gap_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + "; ".join(f"{t}({', '.join(c)})" for t, c in report["missing_columns"].items()))
    if not report["gap_items"]:
        lines.append("No engagement metric publication gaps found.")
        return "\n".join(lines)
    lines.extend(["", "platform | issue_type | count"])
    for group in report["groups"]:
        lines.append(f"{group['platform']} | {group['issue_type']} | {group['count']}")
    lines.extend(["", "content_id | platform | issue_type | metric_id | publication_id"])
    for item in report["gap_items"]:
        lines.append(
            f"{item['content_id'] or '-'} | {item['platform']} | {item['issue_type']} | "
            f"{item['metric_id'] or '-'} | {item['publication_id'] or '-'}"
        )
    return "\n".join(lines)


def _load_content(conn: sqlite3.Connection, schema: dict[str, set[str]], missing_columns: dict[str, list[str]]) -> list[dict[str, Any]]:
    cols = schema["generated_content"]
    if "id" not in cols:
        missing_columns["generated_content"] = ["id"]
        return []
    select = [
        "id AS content_id",
        _expr(cols, "status", "status", "'unknown'"),
        _expr(cols, "published", "published", "0"),
        _expr(cols, "published_at", "published_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM generated_content ORDER BY id ASC")]


def _load_publications(conn: sqlite3.Connection, schema: dict[str, set[str]], missing_columns: dict[str, list[str]]) -> list[dict[str, Any]]:
    cols = schema["content_publications"]
    if "content_id" not in cols:
        missing_columns["content_publications"] = ["content_id"]
        return []
    select = [
        _expr(cols, "id", "publication_id", "NULL"),
        "content_id AS content_id",
        _expr(cols, "platform", "platform", "'unknown'"),
        _expr(cols, "status", "status", "'unknown'"),
        _expr(cols, "platform_post_id", "platform_post_id", "NULL"),
        _expr(cols, "platform_url", "platform_url", "NULL"),
        _expr(cols, "published_at", "published_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM content_publications ORDER BY rowid ASC")]


def _load_metrics(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    table: str,
    platform: str,
    missing_columns: dict[str, list[str]],
) -> list[dict[str, Any]]:
    cols = schema[table]
    if "content_id" not in cols:
        missing_columns[table] = ["content_id"]
    post_id_expr = _first_expr(cols, ("platform_post_id", "tweet_id", "post_id", "status_id", "uri", "post_uri"), "metric_post_id", "NULL")
    url_expr = _first_expr(cols, ("platform_url", "post_url", "url"), "metric_url", "NULL")
    fetched_expr = _first_expr(cols, ("fetched_at", "collected_at", "created_at", "updated_at"), "metric_at", "NULL")
    select = [
        f"'{platform}' AS platform",
        _expr(cols, "content_id", "content_id", "NULL"),
        post_id_expr,
        url_expr,
        fetched_expr,
        "rowid AS metric_id",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY rowid ASC")]


def _item(metric: dict[str, Any], publication: dict[str, Any] | None, issue_type: str) -> dict[str, Any]:
    return {
        "content_id": _int_or_none(metric.get("content_id")),
        "platform": _platform(metric),
        "issue_type": issue_type,
        "metric_id": _int_or_none(metric.get("metric_id")),
        "publication_id": _int_or_none((publication or {}).get("publication_id") or (publication or {}).get("id")),
        "metric_post_id": _clean(metric.get("metric_post_id")) or None,
        "publication_platform_post_id": _clean((publication or {}).get("platform_post_id")) or None,
        "metric_url": _clean(metric.get("metric_url")) or None,
        "publication_platform_url": _clean((publication or {}).get("platform_url")) or None,
    }


def _publication_item(pub: dict[str, Any], issue_type: str, now: datetime) -> dict[str, Any]:
    published_at = _parse_dt(pub.get("published_at"))
    return {
        "content_id": _int_or_none(pub.get("content_id")),
        "platform": _platform(pub),
        "issue_type": issue_type,
        "metric_id": None,
        "publication_id": _int_or_none(pub.get("publication_id") or pub.get("id")),
        "metric_post_id": None,
        "publication_platform_post_id": _clean(pub.get("platform_post_id")) or None,
        "metric_url": None,
        "publication_platform_url": _clean(pub.get("platform_url")) or None,
        "age_hours": round((now - published_at).total_seconds() / 3600, 2) if published_at else None,
    }


def _groups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((item["platform"], item["issue_type"]) for item in items)
    return [{"platform": platform, "issue_type": issue_type, "count": count} for (platform, issue_type), count in sorted(counts.items())]


def _content_published(content: dict[str, Any]) -> bool:
    return _clean(content.get("status")).lower() in {"published", "sent"} or _truthy(content.get("published")) or _parse_dt(content.get("published_at")) is not None


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (item["platform"], item["content_id"] or 0, item["issue_type"], item["metric_id"] or 0)


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


def _first_expr(columns: set[str], candidates: tuple[str, ...], output: str, default: str) -> str:
    for column in candidates:
        if column in columns:
            return f"{column} AS {output}"
    return f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _status(row: dict[str, Any]) -> str:
    return _clean(row.get("status"), "unknown").lower()


def _platform(row: dict[str, Any]) -> str:
    return _clean(row.get("platform"), "unknown").lower()


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    for candidate in (text, text.replace(" ", "T", 1)):
        try:
            return _utc(datetime.fromisoformat(candidate))
        except ValueError:
            continue
    return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _truthy(value: Any) -> bool:
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes"}


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
