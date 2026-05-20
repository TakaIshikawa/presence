"""Detect conflicting newsletter link click attribution."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


DEFAULT_LIMIT = 100
TRACKING_PREFIXES = ("utm_",)
TRACKING_PARAMS = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "source", "utm"}


def build_newsletter_link_click_conflicts_report(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    items = [_item(row) for row in rows]
    attribution_conflicts = _attribution_conflicts(items)
    destination_conflicts = _destination_conflicts(items)
    issue_summary = _issue_summary(items, attribution_conflicts, destination_conflicts)
    return {
        "artifact_type": "newsletter_link_click_conflicts",
        "generated_at": generated_at.isoformat(),
        "thresholds": {"limit": limit},
        "attribution_conflicts": attribution_conflicts[:limit],
        "destination_conflicts": destination_conflicts[:limit],
        "issue_summary": issue_summary,
        "missing_schema": missing_schema or {"missing_tables": [], "missing_columns": {}},
        "normalization": {
            "drops_tracking_parameters": sorted(TRACKING_PARAMS),
            "drops_tracking_prefixes": list(TRACKING_PREFIXES),
            "normalizes": ["scheme", "host_case", "path_trailing_slash", "query_sort"],
        },
    }


def build_newsletter_link_click_conflicts_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_schema = _missing_schema(schema)
    rows = _load(conn, schema["newsletter_link_clicks"]) if "newsletter_link_clicks" in schema else []
    return build_newsletter_link_click_conflicts_report(rows, missing_schema=missing_schema, **kwargs)


def format_newsletter_link_click_conflicts_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_link_click_conflicts_text(report: dict[str, Any]) -> str:
    lines = ["Newsletter Link Click Conflicts", f"Generated: {report['generated_at']}"]
    if report["missing_schema"]["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_schema"]["missing_tables"]))
    lines.append(f"Attribution conflicts: {len(report['attribution_conflicts'])}")
    lines.append(f"Destination conflicts: {len(report['destination_conflicts'])}")
    return "\n".join(lines)


def normalize_newsletter_click_url(url: Any) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    if "://" not in text:
        text = "https://" + text
    split = urlsplit(text)
    scheme = "https"
    host = split.netloc.lower()
    path = split.path or "/"
    if path != "/":
        path = path.rstrip("/")
    query = []
    for key, value in parse_qsl(split.query, keep_blank_values=True):
        lower = key.lower()
        if lower in TRACKING_PARAMS or any(lower.startswith(prefix) for prefix in TRACKING_PREFIXES):
            continue
        query.append((lower, value))
    return urlunsplit((scheme, host, path, urlencode(sorted(query)), ""))


def _item(row: dict[str, Any]) -> dict[str, Any]:
    url = _clean(_first(row, "link_url", "url", "destination_url", "raw_url"))
    content_id = _clean(_first(row, "content_id", "generated_content_id", "source_content_id")) or "unknown"
    return {
        "click_id": _first(row, "click_id", "id"),
        "newsletter_send_id": _clean(_first(row, "newsletter_send_id", "send_id")) or None,
        "issue_id": _clean(_first(row, "issue_id", "newsletter_issue_id")) or None,
        "content_id": content_id,
        "link_url": url,
        "normalized_url": normalize_newsletter_click_url(url),
        "clicks": _int(_first(row, "clicks", "click_count")),
        "unique_clicks": _optional_int(_first(row, "unique_clicks", "unique_count")),
    }


def _issue_key(item: dict[str, Any]) -> tuple[str, str]:
    return (item["newsletter_send_id"] or "-", item["issue_id"] or "-")


def _attribution_conflicts(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[(*_issue_key(item), item["normalized_url"])].append(item)
    conflicts = []
    for (send_id, issue_id, url), rows in grouped.items():
        content_ids = sorted({row["content_id"] for row in rows})
        if len(content_ids) <= 1:
            continue
        conflicts.append(
            {
                "newsletter_send_id": None if send_id == "-" else send_id,
                "issue_id": None if issue_id == "-" else issue_id,
                "normalized_url": url,
                "content_ids": content_ids,
                "click_total": sum(row["clicks"] for row in rows),
                "rows": rows,
            }
        )
    conflicts.sort(key=lambda item: (-item["click_total"], str(item["newsletter_send_id"]), item["normalized_url"]))
    return conflicts


def _destination_conflicts(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[(*_issue_key(item), item["content_id"])].append(item)
    conflicts = []
    for (send_id, issue_id, content_id), rows in grouped.items():
        urls = sorted({row["normalized_url"] for row in rows if row["normalized_url"]})
        if len(urls) <= 1:
            continue
        conflicts.append(
            {
                "newsletter_send_id": None if send_id == "-" else send_id,
                "issue_id": None if issue_id == "-" else issue_id,
                "content_id": content_id,
                "normalized_urls": urls,
                "click_total": sum(row["clicks"] for row in rows),
                "rows": rows,
            }
        )
    conflicts.sort(key=lambda item: (-item["click_total"], str(item["newsletter_send_id"]), item["content_id"]))
    return conflicts


def _issue_summary(items: list[dict[str, Any]], attribution: list[dict[str, Any]], destination: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped = Counter(_issue_key(item) for item in items)
    attr_counts = Counter((item["newsletter_send_id"] or "-", item["issue_id"] or "-") for item in attribution)
    dest_counts = Counter((item["newsletter_send_id"] or "-", item["issue_id"] or "-") for item in destination)
    summaries = []
    for key, row_count in sorted(grouped.items()):
        send_id, issue_id = key
        issue_items = [item for item in items if _issue_key(item) == key]
        summaries.append(
            {
                "newsletter_send_id": None if send_id == "-" else send_id,
                "issue_id": None if issue_id == "-" else issue_id,
                "row_count": row_count,
                "click_total": sum(item["clicks"] for item in issue_items),
                "attribution_conflict_count": attr_counts[key],
                "destination_conflict_count": dest_counts[key],
            }
        )
    return summaries


def _load(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = ", ".join(f"{column} AS {column}" for column in sorted(columns)) or "rowid"
    order = "id" if "id" in columns else "rowid"
    return [dict(row) for row in conn.execute(f"SELECT {select} FROM newsletter_link_clicks ORDER BY {order}").fetchall()]


def _missing_schema(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "newsletter_link_clicks" not in schema:
        return {"missing_tables": ["newsletter_link_clicks"], "missing_columns": {}}
    required_any = {"link_url", "url", "destination_url", "raw_url"}
    missing_columns = {}
    if not required_any & schema["newsletter_link_clicks"]:
        missing_columns["newsletter_link_clicks"] = sorted(required_any)
    return {"missing_tables": [], "missing_columns": missing_columns}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
    return {table: {column[1] for column in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return _int(value)
