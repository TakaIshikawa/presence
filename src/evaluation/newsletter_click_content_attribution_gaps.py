"""Report newsletter click rows with generated-content attribution gaps."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse

from output.newsletter import normalize_newsletter_link_url


DEFAULT_WINDOW_DAYS = 90
DEFAULT_MIN_CLICKS = 1
DEFAULT_LIMIT = 100
GENERATED_SOURCE_KINDS = {
    "content",
    "generated",
    "generated_content",
    "generated-content",
    "blog",
    "blog_post",
    "x_post",
    "x_thread",
}


def build_newsletter_click_content_attribution_gaps_report(
    click_rows: list[dict[str, Any]],
    *,
    issue_id: str | None = None,
    min_clicks: int = DEFAULT_MIN_CLICKS,
    window_days: int = DEFAULT_WINDOW_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic attribution-gap report from click metric rows."""
    if min_clicks < 0:
        raise ValueError("min_clicks must be non-negative")
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    window_start = generated_at - timedelta(days=window_days)
    scanned = 0
    gap_items: list[dict[str, Any]] = []
    duplicate_groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)

    for row in click_rows:
        if issue_id and _clean(row.get("issue_id")) != issue_id:
            continue
        fetched_at = _parse_dt(row.get("fetched_at")) or _parse_dt(row.get("created_at"))
        if fetched_at is not None and fetched_at < window_start:
            continue
        clicks = _int(row.get("clicks"))
        if clicks < min_clicks:
            continue
        scanned += 1
        normalized_url = _normalize_url(row.get("link_url") or row.get("raw_url"))
        duplicate_groups[
            (
                _clean(row.get("issue_id"), "unknown"),
                normalized_url,
                str(_int_or_none(row.get("content_id")) or "none"),
                _clean(row.get("fetched_at"), "unknown"),
            )
        ].append(row)

        issues = _row_issues(row)
        if issues:
            gap_items.append(_gap_item(row, issues, normalized_url))

    for (_issue, normalized_url, _content_key, fetched_at), rows in duplicate_groups.items():
        if len(rows) <= 1:
            continue
        representative = sorted(rows, key=lambda item: _int(item.get("id")))[0]
        gap_items.append(
            {
                **_gap_item(representative, ["duplicate_attribution_rows"], normalized_url),
                "duplicate_count": len(rows),
                "duplicate_row_ids": [_int_or_none(row.get("id")) for row in sorted(rows, key=lambda item: _int(item.get("id")))],
                "fetched_at": fetched_at,
            }
        )

    gap_items.sort(key=_gap_sort_key)
    shown = gap_items[:limit]
    grouped_counts = [
        {"issue_type": issue_type, "count": count}
        for issue_type, count in sorted(Counter(issue for item in gap_items for issue in item["issue_types"]).items())
    ]
    return {
        "artifact_type": "newsletter_click_content_attribution_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "issue_id": issue_id,
            "min_clicks": min_clicks,
            "window_days": window_days,
            "window_start": window_start.isoformat(),
            "limit": limit,
        },
        "summary": {
            "rows_scanned": scanned,
            "gap_count": len(gap_items),
            "shown_count": len(shown),
            "by_issue_type": {item["issue_type"]: item["count"] for item in grouped_counts},
        },
        "gap_items": shown,
        "grouped_counts": grouped_counts,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items())},
    }


def build_newsletter_click_content_attribution_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    rows = _load_click_rows(conn, schema, missing_tables, missing_columns)
    return build_newsletter_click_content_attribution_gaps_report(
        rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_newsletter_click_content_attribution_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_click_content_attribution_gaps_text(report: dict[str, Any]) -> str:
    filters = report["filters"]
    summary = report["summary"]
    lines = [
        "Newsletter Click Content Attribution Gaps",
        f"Generated: {report['generated_at']}",
        f"Issue: {filters['issue_id'] or 'all'}",
        f"Window: {filters['window_days']} days",
        f"Min clicks: {filters['min_clicks']}",
        f"Limit: {filters['limit']}",
        (
            "Totals: "
            f"scanned={summary['rows_scanned']} gaps={summary['gap_count']} "
            f"shown={summary['shown_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + "; ".join(f"{table}({', '.join(columns)})" for table, columns in report["missing_columns"].items())
        )
    if not report["gap_items"]:
        lines.append("No newsletter click content attribution gaps found.")
        return "\n".join(lines)

    lines.extend(["", "Grouped counts:"])
    for group in report["grouped_counts"]:
        lines.append(f"- issue_type={group['issue_type']} count={group['count']}")
    lines.extend(["", "row_id | issue_id | content_id | clicks | issues | url"])
    for item in report["gap_items"]:
        lines.append(
            f"{item['row_id'] or '-'} | {item['issue_id']} | {item['content_id'] or '-'} | "
            f"{item['clicks']} | {','.join(item['issue_types'])} | {item['normalized_url'] or '-'}"
        )
    return "\n".join(lines)


def _load_click_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> list[dict[str, Any]]:
    if "newsletter_link_clicks" not in schema:
        missing_tables.append("newsletter_link_clicks")
        if "generated_content" not in schema:
            missing_tables.append("generated_content")
        return []
    columns = schema["newsletter_link_clicks"]
    required = {"id", "issue_id", "link_url", "clicks", "fetched_at"}
    missing = sorted(required - columns)
    if missing:
        missing_columns["newsletter_link_clicks"] = missing
        return []
    if "generated_content" not in schema:
        missing_tables.append("generated_content")
    elif "id" not in schema["generated_content"]:
        missing_columns["generated_content"] = ["id"]

    select = [
        "nlc.id AS id",
        _expr(columns, "newsletter_send_id", "nlc", "newsletter_send_id", "NULL"),
        "nlc.issue_id AS issue_id",
        _expr(columns, "content_id", "nlc", "content_id", "NULL"),
        _expr(columns, "source_kind", "nlc", "source_kind", "NULL"),
        "nlc.link_url AS link_url",
        _expr(columns, "raw_url", "nlc", "raw_url", "NULL"),
        "nlc.clicks AS clicks",
        _expr(columns, "unique_clicks", "nlc", "unique_clicks", "NULL"),
        "nlc.fetched_at AS fetched_at",
        _expr(columns, "created_at", "nlc", "created_at", "NULL"),
    ]
    if "generated_content" in schema and "id" in schema["generated_content"] and "content_id" in columns:
        select.append("gc.id AS matched_content_id")
        join = "LEFT JOIN generated_content gc ON gc.id = nlc.content_id"
    else:
        select.append("NULL AS matched_content_id")
        join = ""
    rows = conn.execute(
        f"""SELECT {', '.join(select)}
            FROM newsletter_link_clicks nlc
            {join}
            ORDER BY nlc.issue_id ASC, nlc.link_url ASC, nlc.fetched_at ASC, nlc.id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _row_issues(row: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    content_id = _int_or_none(row.get("content_id"))
    generated_source = _is_generated_source(row.get("source_kind"))
    if generated_source and content_id is None:
        issues.append("missing_content_id")
    if content_id is not None and row.get("matched_content_id") is None:
        issues.append("orphan_content_id")
    if content_id is None and _looks_internal_content_url(row.get("link_url"), row.get("raw_url")):
        issues.append("internal_unattributed_link")
    return issues


def _gap_item(row: dict[str, Any], issues: list[str], normalized_url: str) -> dict[str, Any]:
    return {
        "row_id": _int_or_none(row.get("id")),
        "newsletter_send_id": _int_or_none(row.get("newsletter_send_id")),
        "issue_id": _clean(row.get("issue_id"), "unknown"),
        "content_id": _int_or_none(row.get("content_id")),
        "source_kind": row.get("source_kind"),
        "link_url": row.get("link_url"),
        "raw_url": row.get("raw_url"),
        "normalized_url": normalized_url,
        "clicks": _int(row.get("clicks")),
        "unique_clicks": _int_or_none(row.get("unique_clicks")),
        "fetched_at": row.get("fetched_at"),
        "issue_types": issues,
    }


def _is_generated_source(value: Any) -> bool:
    source = _clean(value).lower()
    return source in GENERATED_SOURCE_KINDS or source.startswith("generated_") or source.startswith("content_")


def _looks_internal_content_url(*values: Any) -> bool:
    for value in values:
        text = _clean(value).lower()
        if not text:
            continue
        parsed = urlparse(text)
        haystack = f"{parsed.netloc}{parsed.path}?{parsed.query}" if parsed.scheme else text
        if re.search(r"(^|[/?&_-])(content|generated-content|generated_content|blog|posts?)(/|=|_|-|$)", haystack):
            return True
        if re.search(r"content_id=\d+", haystack):
            return True
    return False


def _normalize_url(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    return normalize_newsletter_link_url(text)


def _gap_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        item["issue_id"],
        -int(item["clicks"]),
        item["normalized_url"],
        item["content_id"] or 0,
        item["row_id"] or 0,
        ",".join(item["issue_types"]),
    )


def _expr(columns: set[str], column: str, alias: str, output: str, default: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    for candidate in (text, text.replace(" ", "T", 1)):
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        return _utc(parsed)
    return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
