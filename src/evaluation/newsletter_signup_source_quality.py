"""Evaluate newsletter signup rows for source attribution quality."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from ._report_utils import clean, connection, expr, json_dumps, lower, now_iso, schema


ARTIFACT_TYPE = "newsletter_signup_source_quality"
DEFAULT_LIMIT = 50
DEFAULT_BURST_THRESHOLD = 3
DISPOSABLE_DOMAINS = {"mailinator.com", "10minutemail.com", "tempmail.com", "guerrillamail.com", "trashmail.com"}


def build_newsletter_signup_source_quality_report(
    subscribers: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    burst_threshold: int = DEFAULT_BURST_THRESHOLD,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    if burst_threshold <= 0:
        raise ValueError("burst_threshold must be positive")

    source_stats: dict[str, dict[str, Any]] = {}
    day_domain_counts: Counter[tuple[str, str, str]] = Counter()
    for row in subscribers:
        source = clean(row.get("signup_source") or row.get("source") or row.get("channel"), "unknown").lower()
        stat = source_stats.setdefault(
            source,
            {"source": source, "total": 0, "missing_consent": 0, "missing_campaign": 0, "disposable_domains": 0, "burst_rows": 0},
        )
        stat["total"] += 1
        if not clean(row.get("consented_at")):
            stat["missing_consent"] += 1
        if not clean(row.get("campaign") or row.get("utm_campaign")):
            stat["missing_campaign"] += 1
        domain = _email_domain(row.get("email"))
        if domain in DISPOSABLE_DOMAINS:
            stat["disposable_domains"] += 1
        created_day = (_parse_dt(row.get("created_at")) or _generated(now)).date().isoformat()
        if domain:
            day_domain_counts[(source, created_day, domain)] += 1

    burst_keys = {key for key, count in day_domain_counts.items() if count >= burst_threshold}
    for row in subscribers:
        source = clean(row.get("signup_source") or row.get("source") or row.get("channel"), "unknown").lower()
        domain = _email_domain(row.get("email"))
        created_day = (_parse_dt(row.get("created_at")) or _generated(now)).date().isoformat()
        if (source, created_day, domain) in burst_keys:
            source_stats[source]["burst_rows"] += 1

    findings: list[dict[str, Any]] = []
    for index, row in enumerate(subscribers):
        source = clean(row.get("signup_source") or row.get("source") or row.get("channel"), "unknown").lower()
        domain = _email_domain(row.get("email"))
        created = _parse_dt(row.get("created_at"))
        reasons: list[str] = []
        if not clean(row.get("consented_at")):
            reasons.append("missing_consent_timestamp")
        if not clean(row.get("campaign") or row.get("utm_campaign")):
            reasons.append("missing_campaign_attribution")
        if domain in DISPOSABLE_DOMAINS:
            reasons.append("disposable_domain")
        day = (created or _generated(now)).date().isoformat()
        if domain and (source, day, domain) in burst_keys:
            reasons.append("email_domain_burst")
        if reasons:
            findings.append(
                {
                    "subscriber_id": row.get("subscriber_id") or row.get("id"),
                    "source": source,
                    "email_domain": domain or None,
                    "created_at": created.isoformat() if created else clean(row.get("created_at")) or None,
                    "reasons": reasons,
                    "severity": _severity(reasons),
                    "_index": index,
                }
            )

    findings.sort(key=lambda item: (-item["severity"], item["created_at"] or "", item["_index"]), reverse=False)
    shown = [{k: v for k, v in item.items() if k != "_index"} for item in findings[:limit]]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": now_iso(now),
        "filters": {"limit": limit, "burst_threshold": burst_threshold},
        "totals": {
            "subscribers": len(subscribers),
            "sources": len(source_stats),
            "findings": len(findings),
            "shown_findings": len(shown),
            "missing_consent": sum(s["missing_consent"] for s in source_stats.values()),
            "missing_campaign": sum(s["missing_campaign"] for s in source_stats.values()),
            "disposable_domains": sum(s["disposable_domains"] for s in source_stats.values()),
            "burst_rows": sum(s["burst_rows"] for s in source_stats.values()),
        },
        "source_breakdown": sorted(source_stats.values(), key=lambda s: (-s["total"], s["source"])),
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items())},
        "empty_state": {
            "is_empty": not subscribers or not findings,
            "message": "No newsletter signup source quality findings found." if subscribers and not findings else "No newsletter subscribers found." if not subscribers and not (missing_tables or missing_columns) else None,
        },
    }


def build_newsletter_signup_source_quality_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    tables = schema(conn)
    if "newsletter_subscribers" not in tables:
        return build_newsletter_signup_source_quality_report([], missing_tables=["newsletter_subscribers"], **kwargs)
    return build_newsletter_signup_source_quality_report(_load_subscribers(conn, tables["newsletter_subscribers"]), **kwargs)


def format_newsletter_signup_source_quality_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_newsletter_signup_source_quality_text(report: dict[str, Any]) -> str:
    lines = [
        "Newsletter Signup Source Quality",
        f"Generated: {report['generated_at']}",
        f"Totals: subscribers={report['totals']['subscribers']} findings={report['totals']['findings']} shown={report['totals']['shown_findings']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"] or "No findings.")
        return "\n".join(lines)
    lines.extend(["", "source | total | missing_consent | missing_campaign | disposable | burst_rows"])
    for row in report["source_breakdown"]:
        lines.append(f"{row['source']} | {row['total']} | {row['missing_consent']} | {row['missing_campaign']} | {row['disposable_domains']} | {row['burst_rows']}")
    lines.extend(["", "subscriber_id | source | email_domain | severity | reasons"])
    for row in report["findings"]:
        lines.append(f"{row['subscriber_id'] or '-'} | {row['source']} | {row['email_domain'] or '-'} | {row['severity']} | {', '.join(row['reasons'])}")
    return "\n".join(lines)


def _load_subscribers(conn: Any, columns: set[str]) -> list[dict[str, Any]]:
    id_expr = expr(columns, "id", "subscriber_id", default="rowid", out="subscriber_id")
    select = [
        id_expr,
        expr(columns, "signup_source", "source", "channel", default="'unknown'", out="signup_source"),
        expr(columns, "campaign", "utm_campaign", default="NULL", out="campaign"),
        expr(columns, "consented_at", "consent_at", default="NULL", out="consented_at"),
        expr(columns, "created_at", "subscribed_at", default="NULL", out="created_at"),
        expr(columns, "email", "email_address", default="NULL", out="email"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM newsletter_subscribers ORDER BY rowid")]


def _email_domain(value: Any) -> str:
    text = lower(value)
    return text.rsplit("@", 1)[-1] if "@" in text else ""


def _parse_dt(value: Any) -> datetime | None:
    text = clean(value).replace("Z", "+00:00")
    if not text:
        return None
    for candidate in (text, text.replace(" ", "T", 1)):
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
    return None


def _generated(now: datetime | None) -> datetime:
    value = now or datetime.now(timezone.utc)
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _severity(reasons: list[str]) -> int:
    weights = {"email_domain_burst": 4, "disposable_domain": 3, "missing_consent_timestamp": 2, "missing_campaign_attribution": 1}
    return sum(weights.get(reason, 1) for reason in reasons)


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(cols)})" for table, cols in sorted(missing.items()))
