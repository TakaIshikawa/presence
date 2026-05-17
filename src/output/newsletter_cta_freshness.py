"""Flag newsletter CTAs reused too long without refresh or performance data."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_DAYS = 30
DEFAULT_MIN_REUSE = 2


def build_newsletter_cta_freshness_report(
    db_or_conn: Any,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    min_reuse: int = DEFAULT_MIN_REUSE,
    now: datetime | None = None,
) -> dict[str, Any]:
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if min_reuse <= 0:
        raise ValueError("min_reuse must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {"stale_days": stale_days, "min_reuse": min_reuse}
    if "newsletter_ctas" not in schema:
        return _report(generated_at, filters, [], missing_tables=["newsletter_ctas"])
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in conn.execute("SELECT * FROM newsletter_ctas").fetchall():
        item = dict(row)
        groups[(_clean(item.get("cta_text") or item.get("text")), _clean(item.get("target_url") or item.get("url")))].append(item)
    findings = []
    for (text, target), items in groups.items():
        if len(items) < min_reuse:
            continue
        finding = _finding(text, target, items, generated_at, stale_days)
        if finding["severity"] != "fresh":
            findings.append(finding)
    findings.sort(key=lambda item: (-item["age_days"], -item["issue_count"], item["cta_text"], item["target_url"]))
    return _report(generated_at, filters, findings)


def format_newsletter_cta_freshness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_cta_freshness_text(report: dict[str, Any]) -> str:
    lines = [
        "Newsletter CTA Freshness",
        f"Generated: {report['generated_at']}",
        f"Filters: stale_days={report['filters']['stale_days']} min_reuse={report['filters']['min_reuse']}",
        f"Totals: findings={report['totals']['finding_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append("No stale reused CTAs found.")
        return "\n".join(lines)
    for item in report["findings"]:
        lines.append(
            f"  - severity={item['severity']} reuse={item['issue_count']} age={item['age_days']}d "
            f"text={item['cta_text']} target={item['target_url']}"
        )
    return "\n".join(lines)


def _finding(text: str, target: str, items: list[dict[str, Any]], now: datetime, stale_days: int) -> dict[str, Any]:
    timestamps = [_parse_dt(item.get("updated_at") or item.get("created_at") or item.get("sent_at")) for item in items]
    performance = [_parse_dt(item.get("performance_at") or item.get("latest_performance_at")) for item in items]
    latest_refresh = max((value for value in timestamps if value), default=None)
    latest_performance = max((value for value in performance if value), default=None)
    freshness_anchor = max((value for value in (latest_refresh, latest_performance) if value), default=None)
    age_days = int((now - freshness_anchor).total_seconds() // 86400) if freshness_anchor else 999
    missing_performance = latest_performance is None
    severity = "fresh"
    if missing_performance and len(items) >= 2:
        severity = "missing_performance"
    if age_days >= stale_days:
        severity = "stale"
    return {
        "cta_text": text,
        "target_url": target,
        "issue_count": len(items),
        "newsletter_ids": sorted(_clean(item.get("newsletter_id") or item.get("issue_id")) for item in items),
        "latest_refresh_at": latest_refresh.isoformat() if latest_refresh else None,
        "latest_performance_at": latest_performance.isoformat() if latest_performance else None,
        "age_days": age_days,
        "missing_performance": missing_performance,
        "severity": severity,
    }


def _report(generated_at: datetime, filters: dict[str, Any], findings: list[dict[str, Any]], *, missing_tables: list[str] | None = None) -> dict[str, Any]:
    return {
        "artifact_type": "newsletter_cta_freshness",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"finding_count": len(findings)},
        "findings": findings,
        "missing_tables": missing_tables or [],
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
