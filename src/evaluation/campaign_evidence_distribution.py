"""Report campaign evidence concentration risks."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 25


@dataclass(frozen=True)
class CampaignEvidenceFinding:
    campaign: str
    risk_type: str
    label: str
    count: int
    share: float
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CampaignEvidenceDistributionReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    findings: tuple[CampaignEvidenceFinding, ...]
    schema_warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "campaign_evidence_distribution",
            "filters": dict(self.filters),
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "schema_warnings": list(self.schema_warnings),
            "totals": dict(sorted(self.totals.items())),
        }


def build_campaign_evidence_distribution_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    campaign: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> CampaignEvidenceDistributionReport:
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "campaign": campaign, "limit": limit, "cutoff": cutoff.isoformat()}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    warnings = _schema_warnings(schema)
    if warnings:
        return _report(generated_at, filters, (), warnings, content_count=0)
    rows = _content_rows(conn, schema, cutoff, campaign)
    evidence = _evidence_rows(conn, schema, [int(row["id"]) for row in rows])
    claim_checks = _claim_checks(conn, schema, [int(row["id"]) for row in rows])
    findings = _findings(rows, evidence, claim_checks)[:limit]
    return _report(generated_at, filters, tuple(findings), (), content_count=len(rows))


def format_campaign_evidence_distribution_json(report: CampaignEvidenceDistributionReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_campaign_evidence_distribution_text(report: CampaignEvidenceDistributionReport) -> str:
    lines = [
        "Campaign Evidence Distribution",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['days']} days",
        f"Campaign: {report.filters.get('campaign') or 'all'}",
        f"Totals: content={report.totals['content_count']} findings={report.totals['finding_count']}",
    ]
    if report.schema_warnings:
        lines.append("Schema warnings: " + "; ".join(report.schema_warnings))
    if not report.findings:
        lines.append("No campaign evidence distribution risks found.")
        return "\n".join(lines)
    lines.append("")
    lines.append("Findings:")
    for finding in report.findings:
        lines.append(
            f"- campaign={finding.campaign} type={finding.risk_type} label={finding.label} "
            f"count={finding.count} share={finding.share:.2f} action={finding.recommended_action}"
        )
    return "\n".join(lines)


def _content_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime, campaign: str | None) -> list[dict[str, Any]]:
    created_at = _column_expr(schema["generated_content"], "created_at", "NULL", "gc")
    rows = [
        dict(row)
        for row in conn.execute(
            f"""SELECT gc.id, gc.content_type, gc.content, {created_at} AS created_at
                FROM generated_content gc
                WHERE {created_at} IS NULL OR datetime({created_at}) >= datetime(?)
                ORDER BY {created_at} DESC, gc.id DESC""",
            (cutoff.isoformat(),),
        )
    ]
    campaigns = _campaigns(conn, schema, [int(row["id"]) for row in rows])
    for row in rows:
        row["campaigns"] = sorted(campaigns.get(int(row["id"]), {"uncampaign"}))
    if campaign:
        key = campaign.lower()
        rows = [row for row in rows if key in {item.lower() for item in row["campaigns"]}]
    return rows


def _campaigns(conn: sqlite3.Connection, schema: dict[str, set[str]], ids: list[int]) -> dict[int, set[str]]:
    result: dict[int, set[str]] = defaultdict(set)
    if not ids:
        return result
    placeholders = ",".join("?" for _ in ids)
    if {"planned_topics", "content_campaigns"}.issubset(schema) and {"content_id", "campaign_id"}.issubset(schema["planned_topics"]):
        for row in conn.execute(
            f"""SELECT pt.content_id, cc.name
                FROM planned_topics pt JOIN content_campaigns cc ON cc.id = pt.campaign_id
                WHERE pt.content_id IN ({placeholders})""",
            ids,
        ):
            if row["name"]:
                result[int(row["content_id"])].add(str(row["name"]))
    if "content_campaigns" in schema and {"content_id", "campaign"}.issubset(schema["content_campaigns"]):
        for row in conn.execute(f"SELECT content_id, campaign FROM content_campaigns WHERE content_id IN ({placeholders})", ids):
            if row["campaign"]:
                result[int(row["content_id"])].add(str(row["campaign"]))
    return result


def _evidence_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    if not ids or not {"content_knowledge_links", "knowledge"}.issubset(schema):
        return {}
    if not {"content_id", "knowledge_id"}.issubset(schema["content_knowledge_links"]):
        return {}
    placeholders = ",".join("?" for _ in ids)
    source_url = _column_expr(schema["knowledge"], "source_url", "NULL", "k")
    source_type = _column_expr(schema["knowledge"], "source_type", "NULL", "k")
    result: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in conn.execute(
        f"""SELECT ckl.content_id, {source_url} AS source_url, {source_type} AS source_type
            FROM content_knowledge_links ckl LEFT JOIN knowledge k ON k.id = ckl.knowledge_id
            WHERE ckl.content_id IN ({placeholders})""",
        ids,
    ):
        result[int(row["content_id"])].append(dict(row))
    return result


def _claim_checks(conn: sqlite3.Connection, schema: dict[str, set[str]], ids: list[int]) -> dict[int, str]:
    if not ids or "content_claim_checks" not in schema or "content_id" not in schema["content_claim_checks"]:
        return {}
    placeholders = ",".join("?" for _ in ids)
    result = {}
    for row in conn.execute(
        f"SELECT content_id, supported_count, unsupported_count FROM content_claim_checks WHERE content_id IN ({placeholders})",
        ids,
    ):
        supported = int(row["supported_count"] or 0)
        unsupported = int(row["unsupported_count"] or 0)
        result[int(row["content_id"])] = "unsupported" if unsupported > supported else "supported"
    return result


def _findings(rows: list[dict[str, Any]], evidence: dict[int, list[dict[str, Any]]], claim_checks: dict[int, str]) -> list[CampaignEvidenceFinding]:
    by_campaign: dict[str, list[int]] = defaultdict(list)
    for row in rows:
        for campaign in row["campaigns"]:
            by_campaign[campaign].append(int(row["id"]))
    findings: list[CampaignEvidenceFinding] = []
    for campaign, ids in by_campaign.items():
        total = len(ids)
        missing = [content_id for content_id in ids if not evidence.get(content_id)]
        if missing:
            findings.append(_finding(campaign, "missing_evidence", "no cited evidence", len(missing), total, "add evidence links before publishing"))
        domains = Counter()
        urls = Counter()
        claims = Counter()
        for content_id in ids:
            if claim_checks.get(content_id):
                claims[claim_checks[content_id]] += 1
            for item in evidence.get(content_id, []):
                url = item.get("source_url") or ""
                if url:
                    urls[url] += 1
                    domains[urlparse(url).netloc.lower()] += 1
                if item.get("source_type"):
                    claims[str(item["source_type"])] += 1
        evidence_total = sum(urls.values()) or 1
        for domain, count in domains.items():
            if count / evidence_total >= 0.6 and count >= 2:
                findings.append(_finding(campaign, "over_reused_domain", domain, count, evidence_total, "add sources from additional domains"))
        for url, count in urls.items():
            if count >= 2:
                findings.append(_finding(campaign, "over_reused_url", url, count, evidence_total, "rotate or refresh cited evidence URL"))
        claim_total = sum(claims.values()) or 1
        for claim_type, count in claims.items():
            if count / claim_total >= 0.75 and count >= 2:
                findings.append(_finding(campaign, "claim_type_concentration", claim_type, count, claim_total, "broaden evidence claim types"))
    return sorted(findings, key=lambda item: (item.campaign, -item.share, item.risk_type, item.label))


def _finding(campaign: str, risk_type: str, label: str, count: int, total: int, action: str) -> CampaignEvidenceFinding:
    return CampaignEvidenceFinding(campaign, risk_type, label, count, round(count / max(total, 1), 4), action)


def _report(generated_at: datetime, filters: dict[str, Any], findings: tuple[CampaignEvidenceFinding, ...], warnings: tuple[str, ...], *, content_count: int) -> CampaignEvidenceDistributionReport:
    return CampaignEvidenceDistributionReport(generated_at.isoformat(), filters, {"content_count": content_count, "finding_count": len(findings)}, findings, warnings)


def _schema_warnings(schema: dict[str, set[str]]) -> tuple[str, ...]:
    if "generated_content" not in schema:
        return ("missing table: generated_content",)
    missing = {"id", "content"} - schema["generated_content"]
    return (f"missing columns: generated_content({', '.join(sorted(missing))})",) if missing else ()


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _column_expr(columns: set[str], column: str, fallback: str, alias: str) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
