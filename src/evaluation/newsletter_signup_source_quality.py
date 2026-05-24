"""Evaluate newsletter subscriber signup source quality."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import sqlite3
from typing import Any

from ._report_utils import clean, connection, domain, expr, iso, json_dumps, now_iso, positive, schema


ARTIFACT_TYPE = "newsletter_signup_source_quality"
DEFAULT_LIMIT = 50
DEFAULT_BURST_THRESHOLD = 3
DISPOSABLE_DOMAINS = {"mailinator.com", "10minutemail.com", "tempmail.com", "guerrillamail.com"}


def build_newsletter_signup_source_quality_report(
    subscriber_rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    burst_threshold: int = DEFAULT_BURST_THRESHOLD,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    positive("limit", limit)
    positive("burst_threshold", burst_threshold)
    source_stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"total": 0, "missing_consent": 0, "missing_campaign": 0, "disposable": 0, "domains": Counter(), "latest_signup_at": None})
    findings: list[dict[str, Any]] = []
    for row in subscriber_rows:
        source = clean(row.get("signup_source") or row.get("source") or row.get("channel"), "unknown")
        campaign = clean(row.get("campaign") or row.get("utm_campaign"))
        consented_at = clean(row.get("consented_at") or row.get("consent_timestamp"))
        created_at = iso(row.get("created_at") or row.get("signed_up_at"))
        email_domain = domain(str(row.get("email") or "").split("@")[-1] if "@" in str(row.get("email") or "") else row.get("email_domain"))
        stats = source_stats[source]
        stats["total"] += 1
        stats["latest_signup_at"] = max([v for v in [stats["latest_signup_at"], created_at] if v], default=None)
        if not consented_at:
            stats["missing_consent"] += 1
            findings.append(_finding("high", "missing_consent_timestamp", source, row, created_at))
        if not campaign:
            stats["missing_campaign"] += 1
            findings.append(_finding("medium", "missing_campaign_attribution", source, row, created_at))
        if email_domain:
            stats["domains"][email_domain] += 1
            if email_domain in DISPOSABLE_DOMAINS:
                stats["disposable"] += 1
                findings.append(_finding("high", "disposable_email_domain", source, row, created_at, email_domain=email_domain))
    for source, stats in source_stats.items():
        for email_domain, count in stats["domains"].items():
            if count >= burst_threshold:
                findings.append(
                    {
                        "severity": "medium",
                        "reason": "repeated_email_domain_burst",
                        "source": source,
                        "subscriber_id": None,
                        "email_domain": email_domain,
                        "count": count,
                        "created_at": stats["latest_signup_at"],
                    }
                )
    source_breakdown = [
        {
            "source": source,
            "total": stats["total"],
            "missing_consent_timestamp": stats["missing_consent"],
            "missing_campaign_attribution": stats["missing_campaign"],
            "disposable_domain_count": stats["disposable"],
            "top_email_domains": [{"domain": k, "count": v} for k, v in stats["domains"].most_common(5)],
        }
        for source, stats in source_stats.items()
    ]
    source_breakdown.sort(key=lambda r: (-r["missing_consent_timestamp"] - r["disposable_domain_count"], r["source"]))
    findings.sort(key=lambda r: (_severity_rank(r["severity"]), r.get("created_at") or "", r["source"], r["reason"]), reverse=True)
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": now_iso(now),
        "filters": {"limit": limit, "burst_threshold": burst_threshold},
        "totals": {
            "subscriber_count": len(subscriber_rows),
            "source_count": len(source_breakdown),
            "finding_count": len(findings),
            "shown_count": len(shown),
        },
        "source_breakdown": source_breakdown,
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())},
        "empty_state": {"is_empty": not findings, "message": "No newsletter signup source quality findings found." if not findings else None},
    }


def build_newsletter_signup_source_quality_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    s = schema(conn)
    if "newsletter_subscribers" not in s:
        return build_newsletter_signup_source_quality_report([], missing_tables=["newsletter_subscribers"], **kwargs)
    return build_newsletter_signup_source_quality_report(_load_subscribers(conn, s["newsletter_subscribers"]), **kwargs)


def format_newsletter_signup_source_quality_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_newsletter_signup_source_quality_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Newsletter Signup Source Quality",
        f"Generated: {report['generated_at']}",
        f"Totals: subscribers={totals['subscriber_count']} sources={totals['source_count']} findings={totals['finding_count']} shown={totals['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "severity | reason | source | subscriber_id | email_domain | created_at"])
    for finding in report["findings"]:
        lines.append(f"{finding['severity']} | {finding['reason']} | {finding['source']} | {finding['subscriber_id'] or '-'} | {finding.get('email_domain') or '-'} | {finding.get('created_at') or '-'}")
    return "\n".join(lines)


def _load_subscribers(conn: sqlite3.Connection, cols: set[str]) -> list[dict[str, Any]]:
    select = [
        expr(cols, "id", "subscriber_id", default="rowid", out="subscriber_id"),
        expr(cols, "signup_source", "source", "channel", default="'unknown'", out="signup_source"),
        expr(cols, "campaign", "utm_campaign", default="NULL", out="campaign"),
        expr(cols, "consented_at", "consent_timestamp", default="NULL", out="consented_at"),
        expr(cols, "created_at", "signed_up_at", default="NULL", out="created_at"),
        expr(cols, "email", default="NULL", out="email"),
    ]
    return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM newsletter_subscribers ORDER BY rowid")]


def _finding(severity: str, reason: str, source: str, row: dict[str, Any], created_at: str | None, **extra: Any) -> dict[str, Any]:
    payload = {
        "severity": severity,
        "reason": reason,
        "source": source,
        "subscriber_id": row.get("subscriber_id") or row.get("id"),
        "created_at": created_at,
    }
    payload.update(extra)
    return payload


def _severity_rank(value: str) -> int:
    return {"low": 0, "medium": 1, "high": 2}.get(value, 0)
