"""Check content ideas for enough supporting evidence before promotion."""
from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from typing import Any

from ._batch_report_common import *


ARTIFACT_TYPE = "content_idea_evidence_coverage"
DEFAULT_LIMIT = 50
DEFAULT_MIN_SOURCES = 2
DEFAULT_MAX_EVIDENCE_AGE_DAYS = 90


def build_content_idea_evidence_coverage_report(
    rows: list[dict[str, Any]],
    *,
    min_sources: int = DEFAULT_MIN_SOURCES,
    max_evidence_age_days: int = DEFAULT_MAX_EVIDENCE_AGE_DAYS,
    limit: int = DEFAULT_LIMIT,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    now=None,
) -> dict[str, Any]:
    positive("min_sources", min_sources)
    positive("max_evidence_age_days", max_evidence_age_days)
    positive("limit", limit)
    gen = now_value(now)
    cutoff = gen - timedelta(days=max_evidence_age_days)
    grouped: dict[str, dict[str, Any]] = defaultdict(lambda: {"idea": {}, "evidence": []})
    for row in rows:
        idea_id = clean(row.get("idea_id") or row.get("id"))
        grouped[idea_id]["idea"] = row
        if clean(row.get("source_url") or row.get("evidence_url") or row.get("source_domain") or row.get("evidence_type")):
            grouped[idea_id]["evidence"].append(row)
    findings: list[dict[str, Any]] = []
    for idea_id, data in grouped.items():
        idea = data["idea"]
        evidence = data["evidence"]
        domains = {domain(e.get("source_url") or e.get("evidence_url") or e.get("source_domain")) for e in evidence if domain(e.get("source_url") or e.get("evidence_url") or e.get("source_domain"))}
        work_artifacts = [e for e in evidence if lower(e.get("evidence_type") or e.get("source_type")) in {"artifact", "work_artifact", "author_experience", "commit", "session"} or _truthy(e.get("author_experience"))]
        newest = max((dt(e.get("evidence_at") or e.get("published_at") or e.get("created_at")) for e in evidence), default=None)
        newest = max((d for d in [newest] if d), default=None)
        issues: list[str] = []
        if not evidence:
            issues.append("no_linked_sources")
        elif len(evidence) < min_sources:
            issues.append("insufficient_sources")
        if evidence and len(domains) < min(min_sources, 2):
            issues.append("weak_source_diversity")
        if not work_artifacts:
            issues.append("missing_author_experience_artifact")
        if evidence and (not newest or newest < cutoff):
            issues.append("stale_evidence")
        if issues:
            findings.append(
                {
                    "idea_id": idea_id,
                    "idea_text": clean(idea.get("idea_text") or idea.get("title") or idea.get("note")),
                    "evidence_count": len(evidence),
                    "source_diversity": len(domains),
                    "newest_evidence_at": newest.isoformat() if newest else None,
                    "issue_codes": issues,
                    "severity": "high" if "no_linked_sources" in issues else "medium",
                    "recommendation": _recommendation(issues),
                }
            )
    findings.sort(key=lambda f: (-len(f["issue_codes"]), f["idea_id"]))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": gen.isoformat(),
        "filters": {"min_sources": min_sources, "max_evidence_age_days": max_evidence_age_days, "limit": limit},
        "summary": {
            "idea_count": len(grouped),
            "finding_count": len(findings),
            "shown": len(shown),
            "issue_counts": dict(sorted(Counter(code for f in findings for code in f["issue_codes"]).items())),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())},
        "empty_state": empty_state(findings, "No content idea evidence coverage findings.", schema_gap=bool(missing_tables or missing_columns)),
    }


def build_content_idea_evidence_coverage_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    sch = schema(conn)
    if "content_ideas" not in sch:
        return build_content_idea_evidence_coverage_report([], missing_tables=["content_ideas"], **kwargs)
    idea_cols = sch["content_ideas"]
    evidence_table = next((t for t in ("content_idea_evidence", "content_idea_sources") if t in sch), None)
    if not evidence_table:
        return build_content_idea_evidence_coverage_report(
            _idea_rows(conn, idea_cols),
            missing_tables=["content_idea_evidence|content_idea_sources"],
            **kwargs,
        )
    ev_cols = sch[evidence_table]
    if not ({"idea_id", "content_idea_id"} & ev_cols):
        return build_content_idea_evidence_coverage_report([], missing_columns={evidence_table: ["idea_id|content_idea_id"]}, **kwargs)
    idea_id = "id" if "id" in idea_cols else "rowid"
    ev_idea_id = "idea_id" if "idea_id" in ev_cols else "content_idea_id"
    rows = [
        dict(r)
        for r in conn.execute(
            f"SELECT ci.{idea_id} AS idea_id, {pick(idea_cols,'title','note','idea_text',out='idea_text')}, "
            f"{pick(ev_cols,'url','source_url','evidence_url',out='source_url')}, {pick(ev_cols,'domain','source_domain',out='source_domain')}, "
            f"{pick(ev_cols,'evidence_type','source_type','type',out='evidence_type')}, {pick(ev_cols,'evidence_at','published_at','created_at',out='evidence_at')}, "
            f"{pick(ev_cols,'author_experience','is_work_artifact',default='0',out='author_experience')} "
            f"FROM content_ideas ci LEFT JOIN {evidence_table} ev ON ev.{ev_idea_id}=ci.{idea_id}"
        )
    ]
    return build_content_idea_evidence_coverage_report(rows, **kwargs)


def format_content_idea_evidence_coverage_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_content_idea_evidence_coverage_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Content Idea Evidence Coverage",
        f"Generated: {report['generated_at']}",
        f"Totals: ideas={summary['idea_count']} findings={summary['finding_count']} shown={summary['shown']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines += ["", "idea_id | evidence | diversity | issues | idea"]
    for item in report["findings"]:
        lines.append(f"{item['idea_id']} | {item['evidence_count']} | {item['source_diversity']} | {','.join(item['issue_codes'])} | {item['idea_text']}")
    return "\n".join(lines)


def _idea_rows(conn, cols: set[str]) -> list[dict[str, Any]]:
    return load_table(conn, "content_ideas", cols, {"idea_id": ("id",), "idea_text": ("title", "note", "idea_text")})


def _truthy(value: Any) -> bool:
    return lower(value) in {"1", "true", "yes", "y"}


def _recommendation(issues: list[str]) -> str:
    if "no_linked_sources" in issues:
        return "link supporting sources before promotion"
    if "missing_author_experience_artifact" in issues:
        return "attach an author-experience artifact or work sample"
    if "stale_evidence" in issues:
        return "refresh evidence before promotion"
    return "add diverse supporting evidence"
