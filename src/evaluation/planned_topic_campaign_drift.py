"""Report planned topics whose campaign relationship has drifted."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_DAYS = 30
DEFAULT_LIMIT = 100
LIVE_STATUSES = {"planned", "scheduled", "active", "ready"}
SKIPPED_STATUSES = {"skipped", "cancelled", "canceled", "done", "published"}
ISSUE_TYPES = (
    "missing_campaign",
    "target_before_campaign",
    "target_after_campaign",
    "active_campaign_without_live_topics",
    "stale_planned_topic",
)


def build_planned_topic_campaign_drift_report(
    topic_rows: list[dict[str, Any]],
    campaign_rows: list[dict[str, Any]],
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
) -> dict[str, Any]:
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    today = generated_at.date()
    campaigns = {_clean(row.get("campaign_id")): row for row in campaign_rows if _clean(row.get("campaign_id"))}
    topics_by_campaign: dict[str, list[dict[str, Any]]] = defaultdict(list)
    issues: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()

    for row in topic_rows:
        campaign_id = _clean(row.get("campaign_id"))
        if campaign_id:
            topics_by_campaign[campaign_id].append(row)
        campaign = campaigns.get(campaign_id)
        target_date = _parse_date(row.get("target_date"))
        status = _clean(row.get("status")).lower() or "unknown"
        if campaign_id and campaign is None:
            _add(issues, counts, "missing_campaign", row, None, target_date, today)
            continue
        if campaign is not None and target_date is not None:
            start = _parse_date(campaign.get("start_date"))
            end = _parse_date(campaign.get("end_date"))
            if start and target_date < start:
                _add(issues, counts, "target_before_campaign", row, campaign, target_date, today)
            if end and target_date > end:
                _add(issues, counts, "target_after_campaign", row, campaign, target_date, today)
        if status in LIVE_STATUSES and target_date is not None and (today - target_date).days > stale_days:
            _add(issues, counts, "stale_planned_topic", row, campaign, target_date, today)

    for campaign_id, campaign in campaigns.items():
        status = _clean(campaign.get("status")).lower()
        end = _parse_date(campaign.get("end_date"))
        if status not in {"active", "running", "scheduled"}:
            continue
        if end is not None and end < today:
            continue
        campaign_topics = topics_by_campaign.get(campaign_id, [])
        live_topics = [row for row in campaign_topics if _clean(row.get("status")).lower() in LIVE_STATUSES and not _is_stale(row, today, stale_days)]
        if not live_topics:
            _add(issues, counts, "active_campaign_without_live_topics", {"campaign_id": campaign_id}, campaign, None, today)

    issues.sort(key=lambda item: (ISSUE_TYPES.index(item["issue_type"]), item.get("campaign_id") or "", item.get("planned_topic_id") or 0))
    shown = issues[:limit]
    return {
        "artifact_type": "planned_topic_campaign_drift",
        "generated_at": generated_at.isoformat(),
        "thresholds": {"stale_days": stale_days, "limit": limit},
        "summary": {
            "planned_topic_count": len(topic_rows),
            "campaign_count": len(campaign_rows),
            "issue_count": len(issues),
            "shown_count": len(shown),
            "by_issue_type": {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES},
        },
        "missing_tables": sorted(missing_tables or []),
        "issue_items": shown,
        "empty_state": {"is_empty": not issues, "message": "No planned topic campaign drift issues found." if not issues else None},
    }


def build_planned_topic_campaign_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing = sorted(table for table in ("planned_topics", "content_campaigns") if table not in schema)
    return build_planned_topic_campaign_drift_report(
        _load_topics(conn, schema["planned_topics"]) if "planned_topics" in schema else [],
        _load_campaigns(conn, schema["content_campaigns"]) if "content_campaigns" in schema else [],
        missing_tables=missing,
        **kwargs,
    )


def format_planned_topic_campaign_drift_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_planned_topic_campaign_drift_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Planned Topic Campaign Drift",
        f"Generated: {report['generated_at']}",
        f"Thresholds: stale_days={report['thresholds']['stale_days']} limit={report['thresholds']['limit']}",
        f"Totals: planned_topics={summary['planned_topic_count']} campaigns={summary['campaign_count']} issues={summary['issue_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["issue_items"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("Issues:")
    for item in report["issue_items"]:
        lines.append(f"  - {item['issue_type']} topic={item.get('planned_topic_id') or '-'} campaign={item.get('campaign_id') or '-'} target={item.get('target_date') or '-'}")
    return "\n".join(lines)


def _load_topics(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    campaign_expr = _expr(columns, "campaign_id", "content_campaign_id", fallback="NULL")
    select = [
        _expr(columns, "id", fallback="rowid") + " AS planned_topic_id",
        campaign_expr + " AS campaign_id",
        _expr(columns, "target_date", "scheduled_date", "publish_date", fallback="NULL") + " AS target_date",
        _expr(columns, "status", fallback="'planned'") + " AS status",
    ]
    order = _expr(columns, "id", "target_date", fallback="rowid")
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM planned_topics ORDER BY {order} ASC").fetchall()]


def _load_campaigns(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        _expr(columns, "id", fallback="rowid") + " AS campaign_id",
        _expr(columns, "name", "title", fallback="NULL") + " AS campaign_name",
        _expr(columns, "start_date", "starts_at", fallback="NULL") + " AS start_date",
        _expr(columns, "end_date", "ends_at", fallback="NULL") + " AS end_date",
        _expr(columns, "status", fallback="'active'") + " AS status",
    ]
    order = _expr(columns, "id", fallback="rowid")
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM content_campaigns ORDER BY {order} ASC").fetchall()]


def _add(issues: list[dict[str, Any]], counts: Counter[str], issue_type: str, topic: dict[str, Any], campaign: dict[str, Any] | None, target_date: date | None, today: date) -> None:
    start = _parse_date(campaign.get("start_date")) if campaign else None
    end = _parse_date(campaign.get("end_date")) if campaign else None
    age_days = (today - target_date).days if target_date else None
    issues.append(
        {
            "issue_type": issue_type,
            "planned_topic_id": topic.get("planned_topic_id"),
            "campaign_id": topic.get("campaign_id") or (campaign.get("campaign_id") if campaign else None),
            "campaign_name": campaign.get("campaign_name") if campaign else None,
            "target_date": target_date.isoformat() if target_date else None,
            "campaign_start_date": start.isoformat() if start else None,
            "campaign_end_date": end.isoformat() if end else None,
            "status": topic.get("status") or (campaign.get("status") if campaign else None),
            "age_days": age_days,
        }
    )
    counts[issue_type] += 1


def _is_stale(row: dict[str, Any], today: date, stale_days: int) -> bool:
    target = _parse_date(row.get("target_date"))
    return target is not None and (today - target).days > stale_days


def _expr(columns: set[str], *names: str, fallback: str) -> str:
    for name in names:
        if name in columns:
            return name
    return fallback


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
