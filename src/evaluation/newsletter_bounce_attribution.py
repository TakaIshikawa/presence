"""Attribute newsletter delivery failures to campaigns and reasons."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
SIGNALS = ("bounce", "failed_delivery", "delivery_error")
_EMAIL_RE = re.compile(r"[\w.+-]+@([\w.-]+\.[a-zA-Z]{2,})")


def build_newsletter_bounce_attribution_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    total_events = 0
    for raw in rows:
        row = _normalize_row(raw)
        if row["occurred_at"] and row["occurred_at"] < cutoff:
            continue
        signals = _signals(row)
        if not signals:
            continue
        total_events += 1
        key = (row["issue_id"], row["campaign"], row["reason"])
        group = groups.setdefault(
            key,
            {
                "issue_id": row["issue_id"],
                "campaign": row["campaign"],
                "reason": row["reason"],
                "signal_counts": Counter(),
                "affected_recipient_count": 0,
                "affected_domain_count": 0,
                "_recipients": Counter(),
                "_domains": Counter(),
                "latest_at": None,
                "example_message": row["message"],
            },
        )
        group["signal_counts"].update(signals)
        if row["recipient"]:
            group["_recipients"][row["recipient"]] += 1
        if row["domain"]:
            group["_domains"][row["domain"]] += 1
        if row["occurred_at"] and (group["latest_at"] is None or row["occurred_at"] > group["latest_at"]):
            group["latest_at"] = row["occurred_at"]

    findings = []
    for group in groups.values():
        recipients = group.pop("_recipients")
        domains = group.pop("_domains")
        group["affected_recipient_count"] = len(recipients)
        group["affected_domain_count"] = len(domains)
        group["top_recipients"] = _top(recipients)
        group["top_domains"] = _top(domains)
        group["event_count"] = sum(group["signal_counts"].values())
        group["signal_counts"] = dict(sorted(group["signal_counts"].items()))
        group["latest_at"] = group["latest_at"].isoformat() if group["latest_at"] else None
        findings.append(group)
    findings.sort(key=lambda item: (-item["event_count"], item["issue_id"], item["reason"]))

    summary = {
        "rows_scanned": len(rows),
        "delivery_issue_events": total_events,
        "finding_count": len(findings),
        "signal_counts": {signal: sum(item["signal_counts"].get(signal, 0) for item in findings) for signal in SIGNALS},
        "reason_counts": dict(sorted(Counter(item["reason"] for item in findings for _ in range(item["event_count"])).items())),
    }
    return {
        "artifact_type": "newsletter_bounce_attribution",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit, "lookback_start": cutoff.isoformat(), "lookback_end": generated_at.isoformat()},
        "summary": summary,
        "findings": findings[:limit],
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def build_newsletter_bounce_attribution_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    rows = _load_rows(conn, schema) if not gaps["missing_tables"] else []
    return build_newsletter_bounce_attribution_report(rows, schema_gaps=gaps, **kwargs)


def format_newsletter_bounce_attribution_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_bounce_attribution_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Newsletter Bounce Attribution",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['days']} days limit={report['filters']['limit']}",
        f"Totals: rows_scanned={summary['rows_scanned']} delivery_issue_events={summary['delivery_issue_events']} findings={summary['finding_count']}",
    ]
    gaps = report.get("schema_gaps") or {}
    if gaps.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(gaps["missing_tables"]))
    if gaps.get("missing_columns"):
        lines.append("Missing columns: " + "; ".join(f"{table}({', '.join(cols)})" for table, cols in sorted(gaps["missing_columns"].items())))
    if not report["findings"]:
        lines.extend(["", "No newsletter bounce attribution findings."])
        return "\n".join(lines)
    lines.extend(["", "Findings:"])
    for item in report["findings"]:
        lines.append(
            f"  - issue={item['issue_id']} campaign={item['campaign']} reason={item['reason']} "
            f"events={item['event_count']} recipients={item['affected_recipient_count']} domains={item['affected_domain_count']}"
        )
        if item["top_domains"]:
            lines.append("    top_domains=" + ", ".join(f"{entry['value']}:{entry['count']}" for entry in item["top_domains"]))
        if item["top_recipients"]:
            lines.append("    top_recipients=" + ", ".join(f"{entry['value']}:{entry['count']}" for entry in item["top_recipients"]))
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows = []
    for table in ("newsletter_sends", "newsletter_metrics"):
        columns = schema.get(table, set())
        if not columns:
            continue
        select = [
            _select(columns, ("id", "send_id", "metric_id"), "id"),
            _select(columns, ("issue_id", "newsletter_id"), "issue_id"),
            _select(columns, ("campaign", "campaign_id", "campaign_name"), "campaign"),
            _select(columns, ("recipient_email", "recipient", "email"), "recipient"),
            _select(columns, ("event_type", "metric_type", "status"), "event_type"),
            _select(columns, ("reason", "bounce_reason", "error_reason"), "reason"),
            _select(columns, ("metadata", "raw_metrics", "payload"), "metadata"),
            _select(columns, ("raw_metrics", "metrics_json", "payload"), "raw_metrics"),
            _select(columns, ("occurred_at", "sent_at", "created_at", "updated_at"), "occurred_at"),
            _select(columns, ("message", "error", "error_message"), "message"),
        ]
        rows.extend(dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}").fetchall())
    return rows


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _json_obj(row.get("metadata"))
    raw_metrics = _json_obj(row.get("raw_metrics"))
    merged = {**raw_metrics, **metadata}
    recipient = _text(row.get("recipient") or merged.get("recipient_email") or merged.get("recipient") or merged.get("email"))
    message = _text(row.get("message") or merged.get("message") or merged.get("error") or merged.get("error_message"))
    reason = _text(row.get("reason") or merged.get("reason") or merged.get("bounce_reason") or merged.get("error_reason") or message) or "unknown"
    return {
        "issue_id": _text(row.get("issue_id") or merged.get("issue_id") or row.get("id")) or "unknown",
        "campaign": _text(row.get("campaign") or merged.get("campaign") or merged.get("campaign_id")) or "unknown",
        "recipient": recipient,
        "domain": _domain(recipient or message),
        "event_type": _text(row.get("event_type") or merged.get("event_type") or merged.get("status")),
        "reason": reason.lower().replace(" ", "_")[:80],
        "message": message,
        "occurred_at": _parse_dt(row.get("occurred_at") or merged.get("occurred_at") or merged.get("created_at")),
        "blob": " ".join(_flatten([row, merged])).lower(),
    }


def _signals(row: dict[str, Any]) -> list[str]:
    found = []
    haystack = f"{row['event_type']} {row['reason']} {row['message']} {row['blob']}".lower()
    for signal in SIGNALS:
        if signal in haystack:
            found.append(signal)
    return found


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if not any(table in schema for table in ("newsletter_sends", "newsletter_metrics")):
        return {"missing_tables": ["newsletter_sends|newsletter_metrics"], "missing_columns": {}}
    return {"missing_tables": [], "missing_columns": {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _flatten(value: Any) -> list[str]:
    if isinstance(value, dict):
        result = []
        for item in value.values():
            result.extend(_flatten(item))
        return result
    if isinstance(value, list):
        result = []
        for item in value:
            result.extend(_flatten(item))
        return result
    return [_text(value)]


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _domain(value: str) -> str:
    if "@" in value:
        match = _EMAIL_RE.search(value)
        if match:
            return match.group(1).lower()
    return ""


def _top(counter: Counter[str]) -> list[dict[str, Any]]:
    return [{"value": value, "count": count} for value, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:5]]


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
