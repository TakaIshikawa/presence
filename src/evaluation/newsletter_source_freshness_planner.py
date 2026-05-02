"""Plan source refresh work for newsletter drafts and newsletter-ready content."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_DAYS = 30
DEFAULT_MAX_SOURCE_AGE_DAYS = 14
NEWSLETTER_READY_STATUSES = {
    "draft",
    "ready",
    "review",
    "pending",
    "pending_review",
    "queued",
    "scheduled",
}
PUBLISHED_STATUSES = {"sent", "published", "posted"}


def build_newsletter_source_freshness_plan(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    max_source_age_days: int = DEFAULT_MAX_SOURCE_AGE_DAYS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return stale newsletter-source groups with deterministic refresh prompts."""
    if days <= 0:
        raise ValueError("days must be positive")
    if max_source_age_days < 0:
        raise ValueError("max_source_age_days must be non-negative")

    conn = _connection(db_or_conn)
    conn.row_factory = sqlite3.Row
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "max_source_age_days": max_source_age_days,
    }
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if "generated_content" in missing_tables and "newsletter_sends" in missing_tables:
        return _empty_report(generated_at, filters, missing_tables, missing_columns)
    if missing_columns.get("newsletter_sends") or missing_columns.get("generated_content"):
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    source_rows = _load_source_content(conn, schema)
    items = [
        *_load_newsletter_send_items(conn, schema, cutoff=cutoff),
        *_load_generated_newsletter_items(conn, schema, cutoff=cutoff),
    ]
    items.sort(
        key=lambda item: (
            item.get("item_timestamp") or "",
            item["group_type"],
            int(item["group_id"]),
        ),
        reverse=True,
    )

    groups: list[dict[str, Any]] = []
    totals = Counter(
        {
            "items_scanned": len(items),
            "source_count": 0,
            "stale_source_count": 0,
            "missing_source_count": 0,
            "stale_item_count": 0,
            "suggestions_count": 0,
        }
    )
    all_suggestions: list[dict[str, Any]] = []
    for item in items:
        group = _group_for_item(
            item,
            source_rows=source_rows,
            now=generated_at,
            max_source_age_days=max_source_age_days,
        )
        totals["source_count"] += group["source_count"]
        totals["stale_source_count"] += group["stale_source_count"]
        totals["missing_source_count"] += group["missing_source_count"]
        if group["stale_source_count"]:
            totals["stale_item_count"] += 1
            totals["suggestions_count"] += len(group["suggestions"])
            all_suggestions.extend(group["suggestions"])
            groups.append(group)

    groups.sort(key=lambda group: (-group["stale_source_count"], group["group_type"], group["group_id"]))
    all_suggestions.sort(key=lambda item: (item["group_type"], item["group_id"], item["source_content_id"]))
    return {
        "artifact_type": "newsletter_source_freshness_planner",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": dict(totals),
        "groups": groups,
        "items": groups,
        "suggestions": all_suggestions,
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def format_newsletter_source_freshness_plan_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_source_freshness_plan_text(report: dict[str, Any]) -> str:
    """Render a compact source refresh plan."""
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Newsletter Source Freshness Plan",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={filters['days']} "
            f"max_source_age_days={filters['max_source_age_days']}"
        ),
        (
            "Totals: "
            f"items={totals['items_scanned']} "
            f"stale_items={totals['stale_item_count']} "
            f"sources={totals['source_count']} "
            f"stale_sources={totals['stale_source_count']} "
            f"missing_sources={totals['missing_source_count']} "
            f"suggestions={totals['suggestions_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report["missing_columns"].items())
        ]
        lines.append("Missing columns: " + "; ".join(missing))
    if not report["groups"]:
        lines.append("")
        lines.append("No stale newsletter sources found.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Stale source groups:")
    for group in report["groups"]:
        item = group["item"]
        label = item.get("subject") or item.get("title") or item.get("issue_id") or "-"
        lines.append(
            f"- {group['group_type']}={group['group_id']} "
            f"label={_shorten(label, 72)} stale_sources={group['stale_source_count']}"
        )
        for source in group["sources"]:
            if not source["is_stale"]:
                continue
            age = "n/a" if source["source_age_days"] is None else f"{source['source_age_days']:.1f}d"
            title = source["source_title"] or source["source_url"] or f"content {source['source_content_id']}"
            lines.append(f"  - source={source['source_content_id']} age={age} title={_shorten(title, 90)}")
        for suggestion in group["suggestions"]:
            lines.append(f"    query: {suggestion['query']}")
    return "\n".join(lines)


def _group_for_item(
    item: dict[str, Any],
    *,
    source_rows: dict[int, dict[str, Any]],
    now: datetime,
    max_source_age_days: int,
) -> dict[str, Any]:
    sources: list[dict[str, Any]] = []
    suggestions: list[dict[str, Any]] = []
    missing_source_count = 0
    stale_source_count = 0
    for source_id in item["source_content_ids"]:
        source = source_rows.get(source_id)
        if source is None:
            missing_source_count += 1
            sources.append(
                {
                    "source_content_id": source_id,
                    "source_created_at": None,
                    "source_age_days": None,
                    "source_url": None,
                    "source_title": None,
                    "content_type": None,
                    "is_stale": False,
                    "is_missing": True,
                }
            )
            continue
        source_created_at = _parse_datetime(source.get("created_at"))
        source_age_days = _age_days(source_created_at, now) if source_created_at else None
        is_stale = source_age_days is not None and source_age_days > max_source_age_days
        source_title = _source_title(source)
        source_url = _clean(source.get("source_url") or source.get("published_url") or source.get("url"))
        source_item = {
            "source_content_id": source_id,
            "source_created_at": source_created_at.isoformat() if source_created_at else None,
            "source_age_days": source_age_days,
            "source_url": source_url,
            "source_title": source_title,
            "content_type": _clean(source.get("content_type")),
            "is_stale": is_stale,
            "is_missing": False,
        }
        sources.append(source_item)
        if is_stale:
            stale_source_count += 1
            suggestions.append(_suggestion(item, source_item))
    sources.sort(key=lambda row: (not row["is_stale"], -(row["source_age_days"] or -1), row["source_content_id"]))
    suggestions.sort(key=lambda row: row["source_content_id"])
    return {
        "group_id": item["group_id"],
        "group_type": item["group_type"],
        "item": {
            key: item.get(key)
            for key in (
                "newsletter_send_id",
                "content_id",
                "issue_id",
                "subject",
                "title",
                "status",
                "item_timestamp",
                "source_content_ids",
            )
            if key in item
        },
        "source_count": len(sources),
        "stale_source_count": stale_source_count,
        "missing_source_count": missing_source_count,
        "sources": sources,
        "suggestions": suggestions,
    }


def _suggestion(item: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    topic = _clean(item.get("subject") or item.get("title") or item.get("issue_id")) or "newsletter draft"
    source_label = _clean(source.get("source_title") or source.get("source_url")) or f"source {source['source_content_id']}"
    query = f"Find current sources for {topic} that update or replace {source_label}"
    return {
        "group_id": item["group_id"],
        "group_type": item["group_type"],
        "source_content_id": source["source_content_id"],
        "query": query,
    }


def _load_newsletter_send_items(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    columns = schema.get("newsletter_sends")
    if not columns or not {"id", "source_content_ids"}.issubset(columns):
        return []
    select = {
        "id": "ns.id",
        "issue_id": _column_expr(columns, "issue_id", "''", alias="ns"),
        "subject": _column_expr(columns, "subject", "''", alias="ns"),
        "status": _column_expr(columns, "status", "''", alias="ns"),
        "created_at": _column_expr(columns, "created_at", "NULL", alias="ns"),
        "sent_at": _column_expr(columns, "sent_at", "NULL", alias="ns"),
        "source_content_ids": "ns.source_content_ids",
    }
    rows = conn.execute(
        f"""SELECT {select['id']} AS id,
                  {select['issue_id']} AS issue_id,
                  {select['subject']} AS subject,
                  {select['status']} AS status,
                  {select['created_at']} AS created_at,
                  {select['sent_at']} AS sent_at,
                  {select['source_content_ids']} AS source_content_ids
           FROM newsletter_sends ns
           ORDER BY {select['created_at']} DESC, ns.id DESC"""
    ).fetchall()
    items = []
    for row in rows:
        data = dict(row)
        timestamp = _parse_datetime(data.get("created_at") or data.get("sent_at"))
        status = _normalise_status(data.get("status"))
        is_draft = not _clean(data.get("sent_at")) or not status or status in NEWSLETTER_READY_STATUSES
        if status in PUBLISHED_STATUSES:
            is_draft = False
        if not is_draft or (timestamp is not None and timestamp < cutoff):
            continue
        source_ids, malformed = _parse_source_ids(data.get("source_content_ids"))
        if malformed or not source_ids:
            continue
        items.append(
            {
                "group_id": int(data["id"]),
                "group_type": "newsletter_send",
                "newsletter_send_id": int(data["id"]),
                "issue_id": _clean(data.get("issue_id")),
                "subject": _clean(data.get("subject")),
                "status": _clean(data.get("status")),
                "item_timestamp": timestamp.isoformat() if timestamp else None,
                "source_content_ids": source_ids,
            }
        )
    return items


def _load_generated_newsletter_items(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    columns = schema.get("generated_content")
    if not columns or not {"id", "created_at", "source_content_ids"}.issubset(columns):
        return []
    select = {
        "id": "gc.id",
        "content_type": _column_expr(columns, "content_type", "''", alias="gc"),
        "title": _column_expr(columns, "title", "''", alias="gc"),
        "content": _column_expr(columns, "content", "''", alias="gc"),
        "status": _column_expr(columns, "status", "''", alias="gc"),
        "curation_quality": _column_expr(columns, "curation_quality", "''", alias="gc"),
        "created_at": "gc.created_at",
        "source_content_ids": "gc.source_content_ids",
    }
    rows = conn.execute(
        f"""SELECT {select['id']} AS id,
                  {select['content_type']} AS content_type,
                  {select['title']} AS title,
                  {select['content']} AS content,
                  {select['status']} AS status,
                  {select['curation_quality']} AS curation_quality,
                  {select['created_at']} AS created_at,
                  {select['source_content_ids']} AS source_content_ids
           FROM generated_content gc
           ORDER BY gc.created_at DESC, gc.id DESC"""
    ).fetchall()
    items = []
    for row in rows:
        data = dict(row)
        timestamp = _parse_datetime(data.get("created_at"))
        if timestamp is not None and timestamp < cutoff:
            continue
        content_type = (_clean(data.get("content_type")) or "").casefold()
        status = _normalise_status(data.get("status") or data.get("curation_quality"))
        if "newsletter" not in content_type and status not in NEWSLETTER_READY_STATUSES:
            continue
        if status in PUBLISHED_STATUSES:
            continue
        source_ids, malformed = _parse_source_ids(data.get("source_content_ids"))
        if malformed or not source_ids:
            continue
        title = _clean(data.get("title")) or _first_line(data.get("content"))
        items.append(
            {
                "group_id": int(data["id"]),
                "group_type": "generated_content",
                "content_id": int(data["id"]),
                "title": title,
                "status": _clean(data.get("status") or data.get("curation_quality")),
                "item_timestamp": timestamp.isoformat() if timestamp else None,
                "source_content_ids": source_ids,
            }
        )
    return items


def _load_source_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, dict[str, Any]]:
    columns = schema.get("generated_content")
    if not columns or not {"id", "created_at"}.issubset(columns):
        return {}
    select = {
        "id": "gc.id",
        "content_type": _column_expr(columns, "content_type", "NULL", alias="gc"),
        "created_at": "gc.created_at",
        "title": _column_expr(columns, "title", "NULL", alias="gc"),
        "content": _column_expr(columns, "content", "NULL", alias="gc"),
        "source_url": _column_expr(columns, "source_url", "NULL", alias="gc"),
        "published_url": _column_expr(columns, "published_url", "NULL", alias="gc"),
        "url": _column_expr(columns, "url", "NULL", alias="gc"),
    }
    rows = conn.execute(
        f"""SELECT {select['id']} AS id,
                  {select['content_type']} AS content_type,
                  {select['created_at']} AS created_at,
                  {select['title']} AS title,
                  {select['content']} AS content,
                  {select['source_url']} AS source_url,
                  {select['published_url']} AS published_url,
                  {select['url']} AS url
           FROM generated_content gc
           ORDER BY gc.id ASC"""
    ).fetchall()
    return {int(row["id"]): dict(row) for row in rows}


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    missing_tables = [
        table
        for table in ("newsletter_sends", "generated_content")
        if table not in schema
    ]
    required = {
        "newsletter_sends": {"id", "source_content_ids"},
        "generated_content": {"id", "created_at"},
    }
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "artifact_type": "newsletter_source_freshness_planner",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "items_scanned": 0,
            "source_count": 0,
            "stale_source_count": 0,
            "missing_source_count": 0,
            "stale_item_count": 0,
            "suggestions_count": 0,
        },
        "groups": [],
        "items": [],
        "suggestions": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _parse_source_ids(raw_value: Any) -> tuple[list[int], bool]:
    if raw_value in (None, ""):
        return [], False
    try:
        parsed = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (TypeError, json.JSONDecodeError):
        return [], True
    if not isinstance(parsed, list):
        return [], True
    ids: list[int] = []
    malformed = False
    for value in parsed:
        if isinstance(value, bool):
            malformed = True
            continue
        try:
            content_id = int(value)
        except (TypeError, ValueError):
            malformed = True
            continue
        if content_id <= 0:
            malformed = True
            continue
        ids.append(content_id)
    return ids, malformed


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[0]): {
            str(info["name"] if isinstance(info, sqlite3.Row) else info[1])
            for info in conn.execute(
                f"PRAGMA table_info({_quote_identifier(str(row['name'] if isinstance(row, sqlite3.Row) else row[0]))})"
            )
        }
        for row in rows
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3 connection or database wrapper with .conn")
    return conn


def _column_expr(columns: set[str], column: str, fallback: str = "NULL", *, alias: str) -> str:
    return f"{alias}.{_quote_identifier(column)}" if column in columns else fallback


def _parse_datetime(value: Any) -> datetime | None:
    cleaned = _clean(value)
    if not cleaned:
        return None
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        try:
            parsed = datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _age_days(start: datetime | None, end: datetime) -> float | None:
    if start is None:
        return None
    return round(max(0.0, (end - start).total_seconds() / 86400), 2)


def _source_title(row: dict[str, Any]) -> str | None:
    return _clean(row.get("title")) or _first_line(row.get("content"))


def _first_line(value: Any) -> str | None:
    cleaned = _clean(value)
    if not cleaned:
        return None
    return cleaned.splitlines()[0].strip() or None


def _normalise_status(value: Any) -> str:
    return (_clean(value) or "").casefold().replace("-", "_").replace(" ", "_")


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _shorten(value: Any, limit: int) -> str:
    text = _clean(value) or "-"
    return text if len(text) <= limit else text[: limit - 3].rstrip() + "..."


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
