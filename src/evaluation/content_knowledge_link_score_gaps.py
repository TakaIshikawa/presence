"""Report content knowledge link score and grounding gaps."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

from ._report_helpers import base_report, col, connection, json_format, missing, schema, text_format, to_float, truthy, utc


ARTIFACT_TYPE = "content_knowledge_link_score_gaps"
DEFAULT_WEAK_THRESHOLD = 0.3
DEFAULT_MIN_LINKS = 1
DEFAULT_LIMIT = 100
REASONS = ("missing_content", "missing_knowledge", "invalid_relevance_score", "duplicate_link", "weak_published_grounding")


def build_content_knowledge_link_score_gaps_report(rows: list[dict[str, Any]], *, weak_threshold: float = DEFAULT_WEAK_THRESHOLD, min_links: int = DEFAULT_MIN_LINKS, limit: int = DEFAULT_LIMIT, now: datetime | None = None, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None) -> dict[str, Any]:
    if weak_threshold < 0:
        raise ValueError("weak_threshold must be non-negative")
    if min_links <= 0 or limit <= 0:
        raise ValueError("min_links and limit must be positive")
    generated_at = utc(now)
    findings: list[dict[str, Any]] = []
    seen: dict[tuple[Any, Any], int] = {}
    links_by_content: defaultdict[Any, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (row.get("content_id"), row.get("knowledge_id"))
        score = to_float(row.get("relevance_score"))
        base = {"id": row.get("id"), "content_id": row.get("content_id"), "knowledge_id": row.get("knowledge_id"), "relevance_score": score}
        if row.get("resolved_content_id") is None:
            findings.append({**base, "reason": "missing_content", "detail": "missing generated_content row"})
        if row.get("resolved_knowledge_id") is None:
            findings.append({**base, "reason": "missing_knowledge", "detail": "missing knowledge row"})
        if score is None or score < 0 or score > 1:
            findings.append({**base, "reason": "invalid_relevance_score", "detail": "relevance_score is null or outside 0..1"})
        if key in seen:
            findings.append({**base, "reason": "duplicate_link", "previous_id": seen[key], "detail": "duplicate content_id+knowledge_id link"})
        seen[key] = row.get("id")
        links_by_content[row.get("content_id")].append(row)
    for content_id, links in links_by_content.items():
        if not any(truthy(link.get("content_published")) for link in links):
            continue
        valid_scores = [to_float(link.get("relevance_score")) for link in links if to_float(link.get("relevance_score")) is not None]
        strong = [score for score in valid_scores if score is not None and score >= weak_threshold]
        if len(strong) < min_links:
            findings.append({"reason": "weak_published_grounding", "content_id": content_id, "strong_link_count": len(strong), "detail": "published content has too few strong knowledge links"})
    findings.sort(key=lambda f: (REASONS.index(f["reason"]), str(f.get("content_id")), str(f.get("knowledge_id")), str(f.get("id"))))
    return base_report(artifact_type=ARTIFACT_TYPE, generated_at=generated_at, filters={"weak_threshold": weak_threshold, "min_links": min_links, "limit": limit}, rows_count=len(rows), findings=findings, reasons=REASONS, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def build_content_knowledge_link_score_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    db_schema = schema(conn)
    required = {"content_knowledge_links": {"content_id", "knowledge_id"}, "generated_content": {"id"}, "knowledge": {"id"}}
    missing_tables, missing_columns = missing(db_schema, required)
    if missing_tables or missing_columns:
        return build_content_knowledge_link_score_gaps_report([], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)
    ckl, gc = db_schema["content_knowledge_links"], db_schema["generated_content"]
    select = ["ckl.rowid AS id", "ckl.content_id", "ckl.knowledge_id", "gc.id AS resolved_content_id", "k.id AS resolved_knowledge_id", col(ckl, "relevance_score", "ckl") + " AS relevance_score", col(gc, "published", "gc", "0") + " AS content_published"]
    rows = [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM content_knowledge_links ckl LEFT JOIN generated_content gc ON gc.id = ckl.content_id LEFT JOIN knowledge k ON k.id = ckl.knowledge_id ORDER BY ckl.rowid").fetchall()]
    return build_content_knowledge_link_score_gaps_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_content_knowledge_link_score_gaps_json(report: dict[str, Any]) -> str:
    return json_format(report)


def format_content_knowledge_link_score_gaps_text(report: dict[str, Any]) -> str:
    return text_format("Content Knowledge Link Score Gaps", report)
