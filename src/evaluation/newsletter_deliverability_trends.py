"""Report newsletter deliverability trends by provider and campaign."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import sqlite3
from typing import Any

from ._batch_report_utils import connection, dump_json, first_table, parse_time, pick, schema, text, utc_now


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_BOUNCE_THRESHOLD = 0.05
DEFAULT_COMPLAINT_THRESHOLD = 0.01
DEFAULT_LIMIT = 100
STATUSES = {"delivered", "delivery", "sent", "bounce", "bounced", "complaint", "complained", "deferred", "deferral"}


def build_newsletter_deliverability_trends_report(
    conn: sqlite3.Connection,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    bounce_threshold: float = DEFAULT_BOUNCE_THRESHOLD,
    complaint_threshold: float = DEFAULT_COMPLAINT_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if bounce_threshold < 0 or complaint_threshold < 0:
        raise ValueError("thresholds must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = utc_now(now)
    cutoff = generated_at - timedelta(days=lookback_days)
    sch = schema(connection(conn))
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    table = first_table(sch, ("newsletter_send_events", "newsletter_sends"))
    if table is None:
        missing_tables.append("newsletter_send_events|newsletter_sends")
        rows: list[dict[str, Any]] = []
    else:
        cols = sch[table]
        required = {"status"}
        if not {"occurred_at", "created_at", "sent_at", "updated_at"} & cols:
            missing_columns[table] = ["occurred_at|created_at|sent_at|updated_at"]
        missing = sorted(required - cols)
        if missing:
            missing_columns.setdefault(table, []).extend(missing)
        rows = [] if missing_columns else _load_rows(connection(conn), table, cols, cutoff)

    totals = Counter()
    groups: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    missing_payloads: list[dict[str, Any]] = []
    daily_deferred: dict[tuple[str, str, str], int] = defaultdict(int)
    for row in rows:
        status = _status(row.get("status"))
        provider = text(row.get("provider")) or "missing"
        campaign = text(row.get("campaign")) or "missing"
        totals["events"] += 1
        totals[status] += 1
        groups[(provider, campaign)][status] += 1
        groups[(provider, campaign)]["events"] += 1
        ts = parse_time(row.get("occurred_at"))
        if status == "deferred" and ts:
            daily_deferred[(provider, campaign, ts.date().isoformat())] += 1
        if provider == "missing" or not text(row.get("status")):
            missing_payloads.append({"event_id": row.get("id"), "provider": row.get("provider"), "status": row.get("status"), "campaign": campaign})

    findings = []
    for (provider, campaign), counts in groups.items():
        events = counts["events"] or 1
        bounce_rate = counts["bounce"] / events
        complaint_rate = counts["complaint"] / events
        if bounce_rate >= bounce_threshold and counts["bounce"]:
            findings.append(_finding("bounce_rate_spike", provider, campaign, counts, bounce_rate=bounce_rate))
        if complaint_rate >= complaint_threshold and counts["complaint"]:
            findings.append(_finding("complaint_rate_spike", provider, campaign, counts, complaint_rate=complaint_rate))
        days = sorted((day, count) for (p, c, day), count in daily_deferred.items() if p == provider and c == campaign)
        if len(days) >= 2 and days[-1][1] > days[0][1]:
            item = _finding("deferred_backlog_growth", provider, campaign, counts)
            item["first_deferred_count"] = days[0][1]
            item["latest_deferred_count"] = days[-1][1]
            findings.append(item)
        if provider == "missing":
            findings.append(_finding("missing_provider_payload", provider, campaign, counts))
    for row in missing_payloads[:limit]:
        if not text(row.get("status")):
            findings.append({"finding_type": "missing_status_payload", "severity": "medium", **row})

    findings.sort(key=lambda item: (item["finding_type"], item.get("provider", ""), item.get("campaign", ""), str(item.get("event_id", ""))))
    findings = findings[:limit]
    grouped = [
        {"provider": provider, "campaign": campaign, "totals": dict(sorted(counts.items()))}
        for (provider, campaign), counts in sorted(groups.items())
    ]
    return {
        "artifact_type": "newsletter_deliverability_trends",
        "generated_at": generated_at.isoformat(),
        "filters": {"lookback_days": lookback_days, "bounce_threshold": bounce_threshold, "complaint_threshold": complaint_threshold, "limit": limit},
        "totals": dict(sorted(totals.items())),
        "grouped_findings": grouped,
        "findings": findings,
        "missing_tables": sorted(missing_tables),
        "missing_columns": {k: sorted(v) for k, v in sorted(missing_columns.items())},
        "empty_state": {"is_empty": not rows or not findings, "message": "No newsletter deliverability trend findings found." if rows and not findings else "No newsletter deliverability events found." if not rows and not (missing_tables or missing_columns) else None},
    }


def build_newsletter_deliverability_trends_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    return build_newsletter_deliverability_trends_report(connection(db_or_conn), **kwargs)


def format_newsletter_deliverability_trends_json(report: dict[str, Any]) -> str:
    return dump_json(report)


def format_newsletter_deliverability_trends_text(report: dict[str, Any]) -> str:
    lines = [
        "Newsletter Deliverability Trends",
        f"Generated: {report['generated_at']}",
        f"Filters: lookback_days={report['filters']['lookback_days']} bounce_threshold={report['filters']['bounce_threshold']} complaint_threshold={report['filters']['complaint_threshold']} limit={report['filters']['limit']}",
        f"Totals: events={report['totals'].get('events', 0)} delivered={report['totals'].get('delivered', 0)} bounce={report['totals'].get('bounce', 0)} complaint={report['totals'].get('complaint', 0)} deferred={report['totals'].get('deferred', 0)}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if not report["findings"]:
        if report["empty_state"]["message"]:
            lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("Findings:")
    for f in report["findings"]:
        lines.append(f"  - {f['finding_type']} provider={f.get('provider', '-')} campaign={f.get('campaign', '-')} events={f.get('event_count', '-')}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, table: str, cols: set[str], cutoff: datetime) -> list[dict[str, Any]]:
    ts = pick(cols, "occurred_at", "created_at", "sent_at", "updated_at")
    select = [
        f"{pick(cols, 'id', default='rowid')} AS id",
        f"{pick(cols, 'provider', 'email_provider', 'esp', default='NULL')} AS provider",
        f"{pick(cols, 'campaign_id', 'campaign', 'newsletter_issue_id', 'issue_id', default='NULL')} AS campaign",
        "status",
        f"{ts} AS occurred_at",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} WHERE datetime({ts}) >= datetime(?) ORDER BY datetime({ts}) ASC, id ASC", (cutoff.isoformat(),))]


def _status(value: Any) -> str:
    raw = text(value).lower()
    if raw in {"bounced", "bounce"}:
        return "bounce"
    if raw in {"complained", "complaint"}:
        return "complaint"
    if raw in {"deferred", "deferral"}:
        return "deferred"
    if raw in {"delivered", "delivery", "sent"}:
        return "delivered"
    return raw or "missing"


def _finding(kind: str, provider: str, campaign: str, counts: Counter[str], **extra: Any) -> dict[str, Any]:
    return {"finding_type": kind, "severity": "high" if "spike" in kind else "medium", "provider": provider, "campaign": campaign, "event_count": counts["events"], "bounce_count": counts["bounce"], "complaint_count": counts["complaint"], "deferred_count": counts["deferred"], **extra}
