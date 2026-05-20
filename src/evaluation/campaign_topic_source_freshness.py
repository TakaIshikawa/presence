"""Score campaign planned topics by freshness of supporting source material."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS_AHEAD = 30
DEFAULT_STALE_DAYS = 30
DEFAULT_LIMIT = 100
ARTIFACT_TYPE = "campaign_topic_source_freshness"
FRESHNESS_BUCKETS = ("missing_source_material", "no_linked_generated_content", "stale_knowledge_support", "fresh_supported")


def build_campaign_topic_source_freshness_report(
    rows: list[dict[str, Any]],
    *,
    campaign_id: int | None = None,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic planned-topic source freshness report."""
    if campaign_id is not None and campaign_id <= 0:
        raise ValueError("campaign_id must be positive")
    if days_ahead < 0:
        raise ValueError("days_ahead must be non-negative")
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    stale_before = generated_at - timedelta(days=stale_days)
    findings = [_finding(row, generated_at, stale_before) for row in rows]
    findings.sort(key=_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"campaign_id": campaign_id, "days_ahead": days_ahead, "stale_days": stale_days, "limit": limit},
        "freshness_buckets": {bucket: Counter(item["freshness_bucket"] for item in findings)[bucket] for bucket in FRESHNESS_BUCKETS},
        "campaign_summary": _campaign_summary(findings),
        "summary": {
            "topic_count": len(rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "stale_before": stale_before.isoformat(),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
    }


def build_campaign_topic_source_freshness_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load planned topic/source rows from SQLite and build the report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    campaign_id = kwargs.get("campaign_id")
    days_ahead = kwargs.get("days_ahead", DEFAULT_DAYS_AHEAD)
    now = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    missing_tables = [] if "planned_topics" in schema else ["planned_topics"]
    missing_columns = _missing_columns(schema)
    if missing_tables or missing_columns.get("planned_topics"):
        return build_campaign_topic_source_freshness_report([], missing_tables=missing_tables, missing_columns=missing_columns, now=now, **{k: v for k, v in kwargs.items() if k != "now"})
    rows = _load_rows(conn, schema, campaign_id=campaign_id, today=now.date(), days_ahead=days_ahead)
    return build_campaign_topic_source_freshness_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, now=now, **{k: v for k, v in kwargs.items() if k != "now"})


def format_campaign_topic_source_freshness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_campaign_topic_source_freshness_text(report: dict[str, Any]) -> str:
    filters = report["filters"]
    summary = report["summary"]
    lines = [
        "Campaign Topic Source Freshness",
        f"Generated: {report['generated_at']}",
        f"Filters: campaign_id={filters['campaign_id'] or '-'} days_ahead={filters['days_ahead']} stale_days={filters['stale_days']} limit={filters['limit']}",
        f"Totals: topics={summary['topic_count']} findings={summary['finding_count']} shown={summary['shown_count']}",
        "Buckets: " + ", ".join(f"{key}={value}" for key, value in report["freshness_buckets"].items()),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append("No campaign topic source freshness findings found.")
        return "\n".join(lines)
    lines.extend(["", "campaign_id | topic_id | target_date | bucket | content_id | knowledge_count | newest_source_at"])
    for item in report["findings"]:
        lines.append(
            f"{item['campaign_id']} | {item['topic_id']} | {_display(item['target_date'])} | {item['freshness_bucket']} | "
            f"{_display(item['content_id'])} | {item['knowledge_support_count']} | {_display(item['newest_source_at'])}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], *, campaign_id: int | None, today: date, days_ahead: int) -> list[dict[str, Any]]:
    pt = schema["planned_topics"]
    gc = schema.get("generated_content", set())
    ckl = schema.get("content_knowledge_links", set())
    k = schema.get("knowledge", set())
    params: list[Any] = [today.isoformat(), (today + timedelta(days=days_ahead)).isoformat()]
    where = ["(pt.target_date IS NULL OR date(pt.target_date) BETWEEN date(?) AND date(?))"]
    if campaign_id is not None:
        where.append("pt.campaign_id = ?")
        params.append(campaign_id)
    content_expr = _content_expr(pt, gc)
    joins = [_generated_join(schema, content_expr)]
    if {"content_id", "knowledge_id"}.issubset(ckl) and "id" in k:
        joins.append(f"LEFT JOIN content_knowledge_links ckl ON ckl.content_id = {content_expr}")
        joins.append("LEFT JOIN knowledge k ON k.id = ckl.knowledge_id")
        newest_expr = _coalesce("k", k, ("published_at", "source_timestamp", "created_at", "ingested_at"))
        knowledge_id_expr = "k.id"
    else:
        joins.append("LEFT JOIN (SELECT NULL AS content_id, NULL AS knowledge_id) ckl ON 0")
        joins.append("LEFT JOIN (SELECT NULL AS id, NULL AS source_at) k ON 0")
        newest_expr = "NULL"
        knowledge_id_expr = "NULL"
    select = [
        "pt.id AS topic_id",
        _expr(pt, "campaign_id", "pt", "campaign_id", "NULL"),
        _expr(pt, "topic", "pt", "topic", "NULL"),
        _expr(pt, "target_date", "pt", "target_date", "NULL"),
        _expr(pt, "status", "pt", "status", "'planned'"),
        _expr(pt, "source_material", "pt", "source_material", "NULL"),
        f"{content_expr} AS content_id",
        f"COUNT(DISTINCT {knowledge_id_expr}) AS knowledge_support_count",
        f"MAX({newest_expr}) AS newest_source_at",
        f"GROUP_CONCAT(DISTINCT {knowledge_id_expr}) AS knowledge_ids",
    ]
    sql = f"""SELECT {', '.join(select)}
              FROM planned_topics pt
              {' '.join(joins)}
              WHERE {' AND '.join(where)}
              GROUP BY pt.rowid
              ORDER BY pt.target_date ASC NULLS LAST, pt.id ASC"""
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _finding(row: dict[str, Any], now: datetime, stale_before: datetime) -> dict[str, Any]:
    content_id = _int_or_none(row.get("content_id"))
    support_count = _int_or_none(row.get("knowledge_support_count")) or 0
    newest = _parse_dt(row.get("newest_source_at"))
    if not _clean(row.get("source_material")):
        bucket = "missing_source_material"
        detail = "planned topic has no source_material"
    elif content_id is None:
        bucket = "no_linked_generated_content"
        detail = "planned topic has source_material but no linked generated content"
    elif support_count == 0 or newest is None or newest < stale_before:
        bucket = "stale_knowledge_support"
        detail = "linked generated content is only supported by stale or missing knowledge timestamps"
    else:
        bucket = "fresh_supported"
        detail = "planned topic has fresh linked knowledge support"
    return {
        "campaign_id": _int_or_none(row.get("campaign_id")),
        "topic_id": _int_or_none(row.get("topic_id")),
        "topic": _clean(row.get("topic")) or None,
        "target_date": _clean(row.get("target_date")) or None,
        "status": _clean(row.get("status"), "planned"),
        "content_id": content_id,
        "freshness_bucket": bucket,
        "detail": detail,
        "knowledge_support_count": support_count,
        "newest_source_at": _iso(newest),
        "newest_source_age_days": int((now - newest).total_seconds() // 86400) if newest else None,
        "knowledge_ids": _ids(row.get("knowledge_ids"))[:5],
    }


def _campaign_summary(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[int | None, Counter[str]] = {}
    for item in findings:
        grouped.setdefault(item["campaign_id"], Counter())[item["freshness_bucket"]] += 1
    return [
        {"campaign_id": campaign_id, "topic_count": sum(counts.values()), "buckets": {bucket: counts[bucket] for bucket in FRESHNESS_BUCKETS}}
        for campaign_id, counts in sorted(grouped.items(), key=lambda item: item[0] or 0)
    ]


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (FRESHNESS_BUCKETS.index(item["freshness_bucket"]), item["campaign_id"] or 0, item["target_date"] or "", item["topic_id"] or 0)


def _generated_join(schema: dict[str, set[str]], content_expr: str) -> str:
    gc = schema.get("generated_content", set())
    pt = schema["planned_topics"]
    if "generated_content" not in schema:
        return "LEFT JOIN (SELECT NULL AS id) gc ON 0"
    if "content_id" in pt and "id" in gc:
        return "LEFT JOIN generated_content gc ON gc.id = pt.content_id"
    if "planned_topic_id" in gc:
        return "LEFT JOIN generated_content gc ON gc.planned_topic_id = pt.id"
    if "topic_id" in gc:
        return "LEFT JOIN generated_content gc ON gc.topic_id = pt.id"
    return f"LEFT JOIN generated_content gc ON gc.id = {content_expr}" if "id" in gc else "LEFT JOIN (SELECT NULL AS id) gc ON 0"


def _content_expr(pt: set[str], gc: set[str]) -> str:
    if "content_id" in pt and "id" in gc:
        return "COALESCE(pt.content_id, gc.id)"
    if "content_id" in pt:
        return "pt.content_id"
    if "id" in gc:
        return "gc.id"
    return "NULL"


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    missing: dict[str, list[str]] = {}
    required = {"id", "campaign_id", "target_date", "source_material"}
    if "planned_topics" in schema:
        gap = sorted(required - schema["planned_topics"])
        if gap:
            missing["planned_topics"] = gap
    if "content_knowledge_links" in schema and not {"content_id", "knowledge_id"}.issubset(schema["content_knowledge_links"]):
        missing["content_knowledge_links"] = sorted({"content_id", "knowledge_id"} - schema["content_knowledge_links"])
    if "knowledge" in schema and "id" not in schema["knowledge"]:
        missing["knowledge"] = ["id"]
    return missing


def _expr(columns: set[str], column: str, alias: str, output: str, default: str) -> str:
    return f"{alias}.{_quote(column)} AS {output}" if column in columns else f"{default} AS {output}"


def _coalesce(alias: str, columns: set[str], names: tuple[str, ...]) -> str:
    available = [f"{alias}.{_quote(name)}" for name in names if name in columns]
    return "COALESCE(" + ", ".join(available) + ")" if len(available) > 1 else (available[0] if available else "NULL")


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row["name"]): {str(column["name"]) for column in conn.execute(f"PRAGMA table_info({_quote(str(row['name']))})")} for row in rows}


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
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _ids(value: Any) -> list[int]:
    ids = []
    for item in str(value or "").split(","):
        parsed = _int_or_none(item)
        if parsed is not None:
            ids.append(parsed)
    return sorted(set(ids))


def _display(value: Any) -> str:
    return "-" if value is None or value == "" else str(value)


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
