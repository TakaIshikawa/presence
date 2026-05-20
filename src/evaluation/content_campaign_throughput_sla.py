"""Report content campaign throughput SLA gaps."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_OVERDUE_DAYS = 1
DEFAULT_GENERATED_AGE_DAYS = 3
ARTIFACT_TYPE = "content_campaign_throughput_sla"
ACTIVE_STATUSES = {"active"}
THROUGHPUT_REASONS = (
    "daily_limit_breach",
    "weekly_limit_breach",
    "overdue_planned_topic",
    "stale_generated_unpublished",
    "active_campaign_date_boundary",
)


def build_content_campaign_throughput_sla_report(
    rows: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    overdue_days: int = DEFAULT_OVERDUE_DAYS,
    generated_age_days: int = DEFAULT_GENERATED_AGE_DAYS,
    limit: int = DEFAULT_LIMIT,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic campaign throughput SLA report from joined rows."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    if overdue_days < 0:
        raise ValueError("overdue_days must be non-negative")
    if generated_age_days < 0:
        raise ValueError("generated_age_days must be non-negative")

    generated_at = _utc(now or datetime.now(timezone.utc))
    campaigns = _collapse(rows)
    findings: list[dict[str, Any]] = []
    summaries = []
    for campaign in campaigns:
        summaries.append(_campaign_summary(campaign))
        findings.extend(
            _campaign_findings(
                campaign,
                now=generated_at,
                overdue_days=overdue_days,
                generated_age_days=generated_age_days,
            )
        )

    findings.sort(key=_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {
            "generated_age_days": generated_age_days,
            "limit": limit,
            "overdue_days": overdue_days,
        },
        "summary": {
            "campaign_count": len(campaigns),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_campaign": _counts_by_campaign(findings),
            "by_throughput_reason": _counts_by_reason(findings),
        },
        "campaign_summaries": summaries,
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items())},
        "empty_state": {
            "is_empty": not findings,
            "message": "No content campaign throughput SLA gaps found." if not findings else None,
        },
    }


def build_content_campaign_throughput_sla_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load campaign/topic rows from SQLite and build the throughput SLA report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {
        "content_campaigns": {"id"},
        "planned_topics": {"id", "campaign_id"},
    }
    missing_tables = [table for table in required if table not in schema]
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and not columns.issubset(schema[table])
    }
    if missing_tables or missing_columns:
        return build_content_campaign_throughput_sla_report(
            [],
            missing_tables=missing_tables,
            missing_columns=missing_columns,
            **kwargs,
        )

    return build_content_campaign_throughput_sla_report(_load_rows(conn, schema), **kwargs)


def format_content_campaign_throughput_sla_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_campaign_throughput_sla_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Content Campaign Throughput SLA",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"overdue_days={report['filters']['overdue_days']} "
            f"generated_age_days={report['filters']['generated_age_days']} "
            f"limit={report['filters']['limit']}"
        ),
        f"Totals: campaigns={summary['campaign_count']} findings={summary['finding_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "campaign_id | campaign | topic_id | target_date | reason | observed | limit"])
    for item in report["findings"]:
        lines.append(
            f"{item['campaign_id']} | {_display(item.get('campaign_name'))} | {_display(item.get('topic_id'))} | "
            f"{_display(item.get('target_date'))} | {item['throughput_reason']} | "
            f"{_display(item.get('observed_count'))} | {_display(item.get('limit_value'))}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cc = schema["content_campaigns"]
    pt = schema["planned_topics"]
    gc = schema.get("generated_content", set())
    cp = schema.get("content_publications", set())

    select = [
        "cc.id AS campaign_id",
        _expr(cc, "name", "cc", "campaign_name", "NULL"),
        _expr(cc, "status", "cc", "campaign_status", "'active'"),
        _expr(cc, "start_date", "cc", "start_date", "NULL"),
        _expr(cc, "end_date", "cc", "end_date", "NULL"),
        _expr(cc, "daily_limit", "cc", "daily_limit", "NULL"),
        _expr(cc, "weekly_limit", "cc", "weekly_limit", "NULL"),
        "pt.id AS topic_id",
        _expr(pt, "topic", "pt", "topic", "NULL"),
        _expr(pt, "target_date", "pt", "target_date", "NULL"),
        _expr(pt, "status", "pt", "topic_status", "'planned'"),
        _expr(pt, "content_id", "pt", "content_id", "NULL"),
        _expr(gc, "created_at", "gc", "content_created_at", "NULL"),
        _expr(gc, "published", "gc", "generated_published", "0"),
        _expr(gc, "published_at", "gc", "generated_published_at", "NULL"),
        "MAX(CASE WHEN cp.status = 'published' THEN 1 ELSE 0 END) AS publication_published",
    ]
    joins = ["LEFT JOIN planned_topics pt ON pt.campaign_id = cc.id"]
    joins.append("LEFT JOIN generated_content gc ON gc.id = pt.content_id" if "generated_content" in schema and "content_id" in pt and "id" in gc else "LEFT JOIN (SELECT NULL AS id) gc ON 0")
    joins.append("LEFT JOIN content_publications cp ON cp.content_id = pt.content_id" if "content_publications" in schema and "content_id" in pt and "content_id" in cp and "status" in cp else "LEFT JOIN (SELECT NULL AS content_id, NULL AS status) cp ON 0")
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(select)}
                FROM content_campaigns cc
                {' '.join(joins)}
                GROUP BY cc.rowid, pt.rowid
                ORDER BY cc.id ASC, pt.target_date ASC, pt.id ASC"""
        )
    ]


def _collapse(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    campaigns: dict[int, dict[str, Any]] = {}
    for row in rows:
        status = _clean(row.get("campaign_status"), "active").lower()
        if status not in ACTIVE_STATUSES:
            continue
        campaign_id = _int_or_none(row.get("campaign_id"))
        if campaign_id is None:
            continue
        campaign = campaigns.setdefault(
            campaign_id,
            {
                "campaign_id": campaign_id,
                "campaign_name": _clean(row.get("campaign_name")) or None,
                "status": status,
                "start_date": row.get("start_date"),
                "end_date": row.get("end_date"),
                "daily_limit": _int_or_none(row.get("daily_limit")),
                "weekly_limit": _int_or_none(row.get("weekly_limit")),
                "topics": [],
            },
        )
        if row.get("topic_id") is not None:
            campaign["topics"].append(_topic(row))
    return [campaigns[key] for key in sorted(campaigns)]


def _topic(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "topic_id": _int_or_none(row.get("topic_id")),
        "topic": row.get("topic"),
        "target_date": row.get("target_date"),
        "topic_status": _clean(row.get("topic_status"), "planned").lower(),
        "content_id": _int_or_none(row.get("content_id")),
        "content_created_at": row.get("content_created_at"),
        "generated_published": _bool(row.get("generated_published")),
        "generated_published_at": row.get("generated_published_at"),
        "publication_published": _bool(row.get("publication_published")),
    }


def _campaign_summary(campaign: dict[str, Any]) -> dict[str, Any]:
    counts_by_day: dict[str, Counter[str]] = defaultdict(Counter)
    counts_by_week: dict[str, Counter[str]] = defaultdict(Counter)
    for topic in campaign["topics"]:
        target_date = _parse_date(topic.get("target_date"))
        if target_date is None:
            continue
        status_bucket = _status_bucket(topic)
        counts_by_day[target_date.isoformat()][status_bucket] += 1
        year, week, _weekday = target_date.isocalendar()
        counts_by_week[f"{year}-W{week:02d}"][status_bucket] += 1
    return {
        "campaign_id": campaign["campaign_id"],
        "campaign_name": campaign["campaign_name"],
        "status": campaign["status"],
        "start_date": campaign["start_date"],
        "end_date": campaign["end_date"],
        "daily_limit": campaign["daily_limit"],
        "weekly_limit": campaign["weekly_limit"],
        "planned_count": sum(1 for topic in campaign["topics"] if topic["topic_status"] == "planned"),
        "generated_count": sum(1 for topic in campaign["topics"] if topic["content_id"] is not None and topic["topic_status"] != "skipped"),
        "skipped_count": sum(1 for topic in campaign["topics"] if topic["topic_status"] == "skipped"),
        "counts_by_day": {day: dict(counts) for day, counts in sorted(counts_by_day.items())},
        "counts_by_week": {week: dict(counts) for week, counts in sorted(counts_by_week.items())},
    }


def _campaign_findings(
    campaign: dict[str, Any],
    *,
    now: datetime,
    overdue_days: int,
    generated_age_days: int,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    today = now.date()
    start = _parse_date(campaign.get("start_date"))
    end = _parse_date(campaign.get("end_date"))
    if (start and today < start) or (end and today > end):
        findings.append(_finding(campaign, "active_campaign_date_boundary", target_date=today.isoformat()))

    daily_counts: Counter[str] = Counter()
    weekly_counts: Counter[str] = Counter()
    for topic in campaign["topics"]:
        if topic["topic_status"] == "skipped":
            continue
        target = _parse_date(topic.get("target_date"))
        if target is not None:
            daily_counts[target.isoformat()] += 1
            year, week, _weekday = target.isocalendar()
            weekly_counts[f"{year}-W{week:02d}"] += 1

        if topic["content_id"] is None and topic["topic_status"] == "planned" and target is not None and target < today - timedelta(days=overdue_days):
            findings.append(_finding(campaign, "overdue_planned_topic", topic=topic, target_date=target.isoformat()))

        if topic["content_id"] is not None and not _is_topic_published(topic):
            generated_at = _parse_datetime(topic.get("content_created_at"))
            age_days = (now - generated_at).total_seconds() / 86400 if generated_at else None
            if age_days is not None and age_days > generated_age_days:
                findings.append(
                    _finding(
                        campaign,
                        "stale_generated_unpublished",
                        topic=topic,
                        target_date=target.isoformat() if target else None,
                        observed_count=round(age_days, 3),
                        limit_value=generated_age_days,
                    )
                )

    daily_limit = campaign.get("daily_limit")
    if daily_limit is not None:
        for bucket, count in sorted(daily_counts.items()):
            if count > daily_limit:
                findings.append(_finding(campaign, "daily_limit_breach", target_date=bucket, observed_count=count, limit_value=daily_limit))

    weekly_limit = campaign.get("weekly_limit")
    if weekly_limit is not None:
        for bucket, count in sorted(weekly_counts.items()):
            if count > weekly_limit:
                findings.append(_finding(campaign, "weekly_limit_breach", target_date=bucket, observed_count=count, limit_value=weekly_limit))
    return findings


def _finding(
    campaign: dict[str, Any],
    throughput_reason: str,
    *,
    topic: dict[str, Any] | None = None,
    target_date: str | None = None,
    observed_count: int | float | None = None,
    limit_value: int | None = None,
) -> dict[str, Any]:
    return {
        "campaign_id": campaign["campaign_id"],
        "campaign_name": campaign["campaign_name"],
        "topic_id": (topic or {}).get("topic_id"),
        "content_id": (topic or {}).get("content_id"),
        "target_date": target_date or (topic or {}).get("target_date"),
        "daily_limit": campaign.get("daily_limit"),
        "weekly_limit": campaign.get("weekly_limit"),
        "observed_count": observed_count,
        "limit_value": limit_value,
        "throughput_reason": throughput_reason,
    }


def _status_bucket(topic: dict[str, Any]) -> str:
    if topic["topic_status"] == "skipped":
        return "skipped"
    if topic["content_id"] is not None:
        return "generated"
    return "planned"


def _is_topic_published(topic: dict[str, Any]) -> bool:
    return topic["generated_published"] or topic["publication_published"] or bool(_clean(topic.get("generated_published_at")))


def _counts_by_campaign(findings: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(str(finding["campaign_id"]) for finding in findings)
    return dict(sorted(counts.items(), key=lambda item: int(item[0])))


def _counts_by_reason(findings: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(finding["throughput_reason"] for finding in findings)
    return {reason: counts[reason] for reason in THROUGHPUT_REASONS}


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (THROUGHPUT_REASONS.index(item["throughput_reason"]), item["campaign_id"], item.get("target_date") or "", item.get("topic_id") or 0)


def _expr(columns: set[str], column: str, alias: str, output: str, default: str) -> str:
    return f"{alias}.{_quote_identifier(column)} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({_quote_identifier(str(row[0]))})")} for row in rows}


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except ValueError:
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    for candidate in (text, text.replace(" ", "T", 1)):
        try:
            return _utc(datetime.fromisoformat(candidate))
        except ValueError:
            continue
    return None


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _display(value: Any) -> str:
    return "-" if value is None or value == "" else str(value)


def _bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "published"}


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
