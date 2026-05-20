"""Audit campaign planned topic counts against daily and weekly limits."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
_ACTIVE_STATUSES = {"active", "planned"}
_ISSUE_RANK = {
    "over_daily_limit": 0,
    "over_weekly_limit": 1,
    "no_target_dates": 2,
    "campaign_without_planned_topics": 3,
}


def build_campaign_limit_utilization_report(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] | list[str] = (),
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    campaigns = _collapse(rows)
    summaries = [_campaign_summary(campaign) for campaign in campaigns]
    issue_items = [issue for summary in summaries for issue in summary["issues"]]
    issue_items.sort(key=_issue_sort_key)
    shown = issue_items[:limit]
    counts = Counter(issue["issue_type"] for issue in issue_items)
    return {
        "artifact_type": "campaign_limit_utilization",
        "generated_at": generated_at.isoformat(),
        "missing_tables": sorted(missing_tables),
        "thresholds": {"limit": limit, "included_statuses": sorted(_ACTIVE_STATUSES)},
        "summary": {
            "campaigns_scanned": len(campaigns),
            "issue_count": len(issue_items),
            "shown_count": len(shown),
            "by_issue_type": dict(sorted(counts.items())),
        },
        "campaign_summaries": [
            {key: value for key, value in summary.items() if key != "issues"} for summary in summaries
        ],
        "issue_items": shown,
    }


def build_campaign_limit_utilization_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [table for table in ("content_campaigns", "planned_topics") if table not in schema]
    rows = [] if missing_tables else _load_rows(conn, schema)
    return build_campaign_limit_utilization_report(rows, missing_tables=tuple(missing_tables), **kwargs)


def format_campaign_limit_utilization_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_campaign_limit_utilization_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Campaign Limit Utilization",
        f"Generated: {report['generated_at']}",
        f"Totals: campaigns={summary['campaigns_scanned']} issues={summary['issue_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["issue_items"]:
        lines.append("No campaign limit utilization issues found.")
        return "\n".join(lines)
    lines.extend(["", "campaign_id | issue | bucket | count | limit"])
    for item in report["issue_items"]:
        lines.append(
            f"{item['campaign_id']} | {item['issue_type']} | {item.get('bucket') or '-'} | "
            f"{item.get('planned_count') or 0} | {item.get('limit') or '-'}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cc = schema["content_campaigns"]
    pt = schema["planned_topics"]
    select = [
        _expr(cc, "id", "cc", "campaign_id", "cc.rowid"),
        _expr(cc, "name", "cc", "campaign_name", "NULL"),
        _expr(cc, "status", "cc", "campaign_status", "'planned'"),
        _expr(cc, "daily_limit", "cc", "daily_limit", "NULL"),
        _expr(cc, "weekly_limit", "cc", "weekly_limit", "NULL"),
        _expr(pt, "id", "pt", "planned_topic_id", "NULL"),
        _expr(pt, "topic", "pt", "topic", "NULL"),
        _expr(pt, "target_date", "pt", "target_date", "NULL"),
    ]
    join = "pt.campaign_id = cc.id" if "campaign_id" in pt and "id" in cc else "0"
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(select)}
                FROM content_campaigns cc
                LEFT JOIN planned_topics pt ON {join}
                ORDER BY cc.rowid ASC, pt.rowid ASC"""
        )
    ]


def _collapse(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    campaigns: dict[int | None, dict[str, Any]] = {}
    for row in rows:
        status = _clean(row.get("campaign_status"), "planned").lower()
        if status not in _ACTIVE_STATUSES:
            continue
        campaign_id = _int_or_none(row.get("campaign_id"))
        campaign = campaigns.setdefault(
            campaign_id,
            {
                "campaign_id": campaign_id,
                "campaign_name": _clean(row.get("campaign_name")) or None,
                "status": status,
                "daily_limit": _int_or_none(row.get("daily_limit")),
                "weekly_limit": _int_or_none(row.get("weekly_limit")),
                "topics": [],
            },
        )
        if row.get("planned_topic_id") is not None:
            campaign["topics"].append(
                {
                    "planned_topic_id": _int_or_none(row.get("planned_topic_id")),
                    "topic": row.get("topic"),
                    "target_date": row.get("target_date"),
                }
            )
    return [campaigns[key] for key in sorted(campaigns, key=lambda value: value or 0)]


def _campaign_summary(campaign: dict[str, Any]) -> dict[str, Any]:
    daily_counts: Counter[str] = Counter()
    weekly_counts: Counter[str] = Counter()
    malformed_topics = []
    for topic in campaign["topics"]:
        parsed = _parse_date(topic.get("target_date"))
        if parsed is None:
            malformed_topics.append(topic)
            continue
        daily_counts[parsed.isoformat()] += 1
        year, week, _weekday = parsed.isocalendar()
        weekly_counts[f"{year}-W{week:02d}"] += 1
    issues: list[dict[str, Any]] = []
    if not campaign["topics"]:
        issues.append(_issue(campaign, "campaign_without_planned_topics"))
    if malformed_topics:
        issues.append(
            _issue(
                campaign,
                "no_target_dates",
                planned_count=len(malformed_topics),
                planned_topic_ids=[topic["planned_topic_id"] for topic in malformed_topics],
            )
        )
    daily_limit = campaign["daily_limit"]
    if daily_limit is not None:
        for bucket, count in sorted(daily_counts.items()):
            if count > daily_limit:
                issues.append(_issue(campaign, "over_daily_limit", bucket=bucket, planned_count=count, limit=daily_limit))
    weekly_limit = campaign["weekly_limit"]
    if weekly_limit is not None:
        for bucket, count in sorted(weekly_counts.items()):
            if count > weekly_limit:
                issues.append(_issue(campaign, "over_weekly_limit", bucket=bucket, planned_count=count, limit=weekly_limit))
    return {
        "campaign_id": campaign["campaign_id"],
        "campaign_name": campaign["campaign_name"],
        "status": campaign["status"],
        "daily_limit": daily_limit,
        "weekly_limit": weekly_limit,
        "planned_topic_count": len(campaign["topics"]),
        "valid_target_date_count": sum(daily_counts.values()),
        "planned_counts_by_day": dict(sorted(daily_counts.items())),
        "planned_counts_by_week": dict(sorted(weekly_counts.items())),
        "issues": issues,
    }


def _issue(campaign: dict[str, Any], issue_type: str, **extra: Any) -> dict[str, Any]:
    return {
        "campaign_id": campaign["campaign_id"],
        "campaign_name": campaign["campaign_name"],
        "status": campaign["status"],
        "issue_type": issue_type,
        **extra,
    }


def _issue_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    overflow = (item.get("planned_count") or 0) - (item.get("limit") or item.get("planned_count") or 0)
    return (_ISSUE_RANK[item["issue_type"]], -overflow, item["campaign_id"] or 0, item.get("bucket") or "")


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    text = str(value).strip()
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _expr(columns: set[str], column: str, alias: str, output: str, default: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _clean(value: Any, default: str = "") -> str:
    text = str(value).strip() if value is not None else ""
    return text or default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
