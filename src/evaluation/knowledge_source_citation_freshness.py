"""Evaluate freshness and verification status for cited knowledge sources."""
from __future__ import annotations

from collections import Counter
from datetime import timedelta
from typing import Any

from ._batch_report_common import *


ARTIFACT_TYPE = "knowledge_source_citation_freshness"
DEFAULT_LIMIT = 50
DEFAULT_MAX_PUBLISHED_AGE_DAYS = 180
DEFAULT_MAX_VERIFICATION_AGE_DAYS = 45


def build_knowledge_source_citation_freshness_report(
    rows: list[dict[str, Any]],
    *,
    max_published_age_days: int = DEFAULT_MAX_PUBLISHED_AGE_DAYS,
    max_verification_age_days: int = DEFAULT_MAX_VERIFICATION_AGE_DAYS,
    limit: int = DEFAULT_LIMIT,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    now=None,
) -> dict[str, Any]:
    positive("max_published_age_days", max_published_age_days)
    positive("max_verification_age_days", max_verification_age_days)
    positive("limit", limit)
    gen = now_value(now)
    stale_cutoff = gen - timedelta(days=max_published_age_days)
    verify_cutoff = gen - timedelta(days=max_verification_age_days)
    findings: list[dict[str, Any]] = []
    issue_counts: Counter[str] = Counter()
    for row in rows:
        published_at = dt(row.get("published_at") or row.get("source_published_at"))
        ingested_at = dt(row.get("ingested_at") or row.get("created_at"))
        last_checked_at = dt(row.get("last_checked_at") or row.get("verified_at"))
        issues: list[str] = []
        if not published_at and not ingested_at:
            issues.append("undated")
        elif published_at and published_at < stale_cutoff:
            issues.append("stale_published_date")
        if not last_checked_at:
            issues.append("never_verified")
        elif last_checked_at < verify_cutoff:
            issues.append("old_verification")
        if not issues:
            continue
        issue_counts.update(issues)
        age_days = (gen - published_at).days if published_at else None
        verification_age_days = (gen - last_checked_at).days if last_checked_at else None
        findings.append(
            {
                "source_id": clean(row.get("source_id") or row.get("id")),
                "source_url": clean(row.get("source_url") or row.get("url")),
                "source_domain": domain(row.get("source_url") or row.get("url")),
                "published_at": published_at.isoformat() if published_at else None,
                "ingested_at": ingested_at.isoformat() if ingested_at else None,
                "last_checked_at": last_checked_at.isoformat() if last_checked_at else None,
                "published_age_days": age_days,
                "verification_age_days": verification_age_days,
                "issue_codes": issues,
                "severity": "high" if "undated" in issues or "never_verified" in issues else "medium",
                "recommendation": _recommendation(issues),
            }
        )
    findings.sort(key=lambda f: (-len(f["issue_codes"]), -(f["published_age_days"] or 999999), f["source_url"], f["source_id"]))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": gen.isoformat(),
        "filters": {
            "max_published_age_days": max_published_age_days,
            "max_verification_age_days": max_verification_age_days,
            "limit": limit,
        },
        "summary": {
            "source_count": len(rows),
            "finding_count": len(findings),
            "shown": len(shown),
            "issue_counts": dict(sorted(issue_counts.items())),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())},
        "empty_state": empty_state(findings, "No knowledge source citation freshness findings.", schema_gap=bool(missing_tables or missing_columns)),
    }


def build_knowledge_source_citation_freshness_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    sch = schema(conn)
    source_table = next((t for t in ("knowledge_sources", "knowledge_items", "knowledge") if t in sch), None)
    if not source_table:
        return build_knowledge_source_citation_freshness_report([], missing_tables=["knowledge_sources|knowledge_items|knowledge"], **kwargs)
    cols = sch[source_table]
    missing: dict[str, list[str]] = {}
    if not ({"id", "source_id"} & cols):
        missing[source_table] = ["id|source_id"]
    if not ({"url", "source_url"} & cols):
        missing.setdefault(source_table, []).append("url|source_url")
    if missing:
        return build_knowledge_source_citation_freshness_report([], missing_columns=missing, **kwargs)
    rows = load_table(
        conn,
        source_table,
        cols,
        {
            "source_id": ("id", "source_id"),
            "source_url": ("url", "source_url"),
            "published_at": ("published_at", "source_published_at"),
            "ingested_at": ("ingested_at", "created_at"),
            "last_checked_at": ("last_checked_at", "verified_at", "checked_at"),
        },
    )
    return build_knowledge_source_citation_freshness_report(rows, **kwargs)


def format_knowledge_source_citation_freshness_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_knowledge_source_citation_freshness_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Knowledge Source Citation Freshness",
        f"Generated: {report['generated_at']}",
        f"Totals: sources={summary['source_count']} findings={summary['finding_count']} shown={summary['shown']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines += ["", "source_id | domain | published_age | verification_age | issues | url"]
    for item in report["findings"]:
        lines.append(
            f"{item['source_id'] or '-'} | {item['source_domain'] or '-'} | {item['published_age_days']} | "
            f"{item['verification_age_days']} | {','.join(item['issue_codes'])} | {item['source_url']}"
        )
    return "\n".join(lines)


def _recommendation(issues: list[str]) -> str:
    if "undated" in issues:
        return "add a source publication or ingestion date before citing this source"
    if "never_verified" in issues:
        return "verify the source URL and record last_checked_at"
    if "old_verification" in issues:
        return "recheck the source URL before reuse"
    return "refresh or replace the stale cited source"
