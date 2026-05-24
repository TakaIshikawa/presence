"""Estimate factual density for blog drafts."""

from __future__ import annotations

from collections import Counter
import re
from typing import Any

from ._report_utils import clean, connection, expr, json_dumps, now_iso, positive, schema


ARTIFACT_TYPE = "blog_draft_fact_density"
DEFAULT_LIMIT = 50
DEFAULT_MIN_FACTS_PER_100_WORDS = 2.0


def build_blog_draft_fact_density_report(
    content_rows: list[dict[str, Any]],
    claim_rows: list[dict[str, Any]] | None = None,
    link_rows: list[dict[str, Any]] | None = None,
    *,
    min_facts_per_100_words: float = DEFAULT_MIN_FACTS_PER_100_WORDS,
    limit: int = DEFAULT_LIMIT,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    now: Any = None,
) -> dict[str, Any]:
    positive("limit", limit)
    positive("min_facts_per_100_words", min_facts_per_100_words)
    claims = Counter(str(r.get("content_id")) for r in (claim_rows or []) if r.get("content_id") is not None)
    citations = Counter(str(r.get("content_id")) for r in (link_rows or []) if r.get("content_id") is not None)
    findings: list[dict[str, Any]] = []
    for row in content_rows:
        cid = str(row.get("content_id"))
        text = " ".join(clean(row.get(k)) for k in ("title", "summary", "body"))
        words = re.findall(r"\b[\w'-]+\b", text)
        word_count = len(words)
        marker_count = len(re.findall(r"\b\d+(?:\.\d+)?%?|\b(?:because|according to|reported|found|shows|data)\b", text, re.I))
        claim_count = claims[cid] or marker_count
        citation_count = citations[cid] + len(re.findall(r"https?://|\[[^\]]+\]\([^)]+\)", text))
        quote_count = text.count('"') // 2 + text.count("'") // 2
        density = round((claim_count + citation_count) * 100 / max(word_count, 1), 2)
        reasons: list[str] = []
        if density < min_facts_per_100_words:
            reasons.append("low_evidence_density")
        if citation_count >= 3 and claim_count <= 1:
            reasons.append("citation_heavy_low_claims")
        if quote_count * 25 > max(word_count, 1):
            reasons.append("quote_heavy")
        if reasons:
            findings.append(
                {
                    "content_id": cid,
                    "title": clean(row.get("title")) or None,
                    "word_count": word_count,
                    "claim_count": claim_count,
                    "citation_count": citation_count,
                    "quote_count": quote_count,
                    "facts_per_100_words": density,
                    "risk_reason": ",".join(reasons),
                }
            )
    findings.sort(key=lambda r: (r["facts_per_100_words"], -r["citation_count"], r["content_id"]))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": now_iso(now),
        "thresholds": {"min_facts_per_100_words": min_facts_per_100_words, "limit": limit},
        "summary": {"content_count": len(content_rows), "finding_count": len(findings), "shown_count": len(shown), "by_risk_reason": dict(sorted(Counter(x for r in findings for x in r["risk_reason"].split(",")).items()))},
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())},
    }


def build_blog_draft_fact_density_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    s = schema(conn)
    missing_tables = [t for t in ("generated_content",) if t not in s]
    missing_columns: dict[str, list[str]] = {}
    content = _load_content(conn, s, missing_columns) if "generated_content" in s else []
    claims = _load_simple(conn, "content_claims", s) if "content_claims" in s else []
    links = _load_simple(conn, "content_knowledge_links", s) if "content_knowledge_links" in s else []
    return build_blog_draft_fact_density_report(content, claims, links, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_blog_draft_fact_density_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_blog_draft_fact_density_text(report: dict[str, Any]) -> str:
    lines = ["Blog Draft Fact Density", f"Generated: {report['generated_at']}", f"Totals: content={report['summary']['content_count']} findings={report['summary']['finding_count']} shown={report['summary']['shown_count']}"]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append("No blog draft fact density risks found.")
        return "\n".join(lines)
    lines.extend(["", "content_id | title | words | claims | citations | quotes | facts_per_100_words | risk_reason"])
    for r in report["findings"]:
        lines.append(f"{r['content_id']} | {r['title'] or '-'} | {r['word_count']} | {r['claim_count']} | {r['citation_count']} | {r['quote_count']} | {r['facts_per_100_words']} | {r['risk_reason']}")
    return "\n".join(lines)


def _load_content(conn: Any, s: dict[str, set[str]], missing: dict[str, list[str]]) -> list[dict[str, Any]]:
    cols = s["generated_content"]
    if "id" not in cols:
        missing["generated_content"] = ["id"]
        return []
    select = ["id AS content_id", expr(cols, "title", default="NULL", out="title"), expr(cols, "summary", default="NULL", out="summary"), expr(cols, "body", "content", default="NULL", out="body"), expr(cols, "content_type", "type", default="'blog'", out="content_type"), expr(cols, "status", default="'draft'", out="status")]
    rows = [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM generated_content ORDER BY id")]
    return [r for r in rows if "blog" in clean(r.get("content_type"), "blog").lower() and clean(r.get("status"), "draft").lower() in {"draft", "generated", "review"}]


def _load_simple(conn: Any, table: str, s: dict[str, set[str]]) -> list[dict[str, Any]]:
    cols = s[table]
    if "content_id" not in cols:
        return []
    return [dict(r) for r in conn.execute(f"SELECT content_id FROM {table} ORDER BY rowid")]

