"""Prioritize stale or failing newsletter links for repair."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_DAYS = 30
DEFAULT_LIMIT = 100


def build_newsletter_link_rot_priority_report(
    db_or_conn: Any,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Rank newsletter links with stale health checks or failing states."""
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=stale_days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {
        "limit": limit,
        "stale_days": stale_days,
        "stale_before": cutoff.isoformat(),
    }
    if "newsletter_links" not in schema:
        return _report(generated_at, filters, [], missing_tables=["newsletter_links"])

    rows = _load_rows(conn, schema)
    findings = [_finding(row, generated_at, stale_days) for row in rows]
    findings = [item for item in findings if item["issue_reason"] != "healthy"]
    findings.sort(
        key=lambda item: (
            -item["priority_score"],
            -item["importance_signal"],
            item["newsletter_id"],
            item["url"],
        )
    )
    return _report(generated_at, filters, findings[:limit])


def format_newsletter_link_rot_priority_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_link_rot_priority_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Newsletter Link Rot Priority",
        f"Generated: {report['generated_at']}",
        f"Filters: stale_days={report['filters']['stale_days']} limit={report['filters']['limit']}",
        f"Totals: findings={totals['finding_count']} stale={totals['stale_count']} failing={totals['failing_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append("No stale or failing newsletter links found.")
        return "\n".join(lines)
    lines.append("")
    lines.append("Priority queue:")
    for item in report["findings"]:
        lines.append(
            f"  - score={item['priority_score']} newsletter={item['newsletter_id']} "
            f"reason={item['issue_reason']} signal={item['importance_signal']} url={item['url']}"
        )
    return "\n".join(lines)


def _report(
    generated_at: datetime,
    filters: dict[str, Any],
    findings: list[dict[str, Any]],
    *,
    missing_tables: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "artifact_type": "newsletter_link_rot_priority",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "finding_count": len(findings),
            "stale_count": sum("stale" in item["issue_reason"] for item in findings),
            "failing_count": sum(item["issue_reason"] in {"broken", "failing"} for item in findings),
            "missing_engagement_count": sum(item["engagement_missing"] for item in findings),
        },
        "findings": findings,
        "missing_tables": missing_tables or [],
    }


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cols = schema["newsletter_links"]
    selected = [
        _expr(cols, "newsletter_id"),
        _expr(cols, "issue_id"),
        _expr(cols, "url"),
        _expr(cols, "status"),
        _expr(cols, "status_code"),
        _expr(cols, "checked_at"),
        _expr(cols, "last_checked_at"),
        _expr(cols, "clicks"),
        _expr(cols, "placement"),
        _expr(cols, "position"),
        _expr(cols, "section"),
    ]
    rows = conn.execute(f"SELECT {', '.join(selected)} FROM newsletter_links").fetchall()
    return [dict(row) for row in rows]


def _finding(row: dict[str, Any], now: datetime, stale_days: int) -> dict[str, Any]:
    newsletter_id = _clean(row.get("newsletter_id") or row.get("issue_id") or "unknown")
    url = _clean(row.get("url"))
    status = _clean(row.get("status")).lower() or _status_from_code(row.get("status_code"))
    checked_at = _parse_dt(row.get("checked_at") or row.get("last_checked_at"))
    age_days = int((now - checked_at).total_seconds() // 86400) if checked_at else None
    clicks = _int_or_none(row.get("clicks"))
    placement = _int_or_none(row.get("placement") or row.get("position"))
    placement_signal = max(0, 20 - (placement or 20)) if placement is not None else 0
    importance_signal = (clicks if clicks is not None else 0) + placement_signal
    failing = status in {"broken", "failing", "error", "timeout"} or _is_bad_status(row.get("status_code"))
    stale = checked_at is None or (age_days is not None and age_days >= stale_days)
    if failing and stale:
        reason = "failing_stale"
    elif failing:
        reason = "broken"
    elif stale:
        reason = "stale"
    else:
        reason = "healthy"
    priority_score = (150 if failing else 0) + (40 if stale else 0) + min(importance_signal, 100)
    return {
        "newsletter_id": newsletter_id,
        "url": url,
        "issue_reason": reason,
        "status": status or "unknown",
        "status_code": _int_or_none(row.get("status_code")),
        "checked_at": checked_at.isoformat() if checked_at else None,
        "age_days": age_days,
        "clicks": clicks,
        "placement": placement,
        "section": _clean(row.get("section")),
        "engagement_missing": clicks is None and placement is None,
        "importance_signal": importance_signal,
        "priority_score": priority_score,
    }


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _expr(cols: set[str], name: str) -> str:
    return name if name in cols else f"NULL AS {name}"


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_bad_status(value: Any) -> bool:
    code = _int_or_none(value)
    return code is not None and code >= 400


def _status_from_code(value: Any) -> str:
    if _is_bad_status(value):
        return "broken"
    code = _int_or_none(value)
    return "healthy" if code is not None else ""


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
