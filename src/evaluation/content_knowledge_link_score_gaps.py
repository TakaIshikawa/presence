"""Report content knowledge links with score and grounding gaps."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ._gap_report_utils import (
    DEFAULT_LIMIT,
    base_report,
    column,
    connection,
    format_json,
    format_text,
    require_positive,
    require_probability,
    schema,
    to_float,
    truthy,
    utc,
)


ARTIFACT_TYPE = "content_knowledge_link_score_gaps"
DEFAULT_WEAK_THRESHOLD = 0.35
DEFAULT_MIN_LINKS = 1
REASONS = (
    "missing_content",
    "missing_knowledge",
    "invalid_relevance_score",
    "duplicate_link",
    "weak_published_grounding",
)
REQUIRED = {
    "content_knowledge_links": {"content_id", "knowledge_id"},
    "generated_content": {"id"},
    "knowledge": {"id"},
}


def build_content_knowledge_link_score_gaps_report(
    link_rows: list[dict[str, Any]],
    *,
    content_rows: list[dict[str, Any]] | None = None,
    weak_threshold: float = DEFAULT_WEAK_THRESHOLD,
    min_links: int = DEFAULT_MIN_LINKS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | str | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    require_probability("weak_threshold", weak_threshold)
    require_positive("min_links", min_links)
    require_positive("limit", limit)
    generated_at = utc(now)
    findings: list[dict[str, Any]] = []
    pair_counts: dict[tuple[Any, Any], int] = {}
    published_links: dict[Any, list[dict[str, Any]]] = {}

    for row in content_rows or []:
        if not _content_is_published(row):
            continue
        content_id = row.get("content_id") or row.get("id")
        published_links.setdefault(content_id, [])

    for index, row in enumerate(link_rows):
        content_id = row.get("content_id")
        knowledge_id = row.get("knowledge_id")
        pair = (content_id, knowledge_id)
        pair_counts[pair] = pair_counts.get(pair, 0) + 1
        score = to_float(row.get("relevance_score"))
        base = {
            "id": row.get("link_id") or row.get("id") or index + 1,
            "link_id": row.get("link_id") or row.get("id") or index + 1,
            "content_id": content_id,
            "knowledge_id": knowledge_id,
            "relevance_score": row.get("relevance_score"),
        }
        if row.get("resolved_content_id") is None:
            findings.append({**base, "reason": "missing_content", "detail": "content_knowledge_links.content_id has no generated_content row"})
        if row.get("resolved_knowledge_id") is None:
            findings.append({**base, "reason": "missing_knowledge", "detail": "content_knowledge_links.knowledge_id has no knowledge row"})
        if score is None or score < 0 or score > 1:
            findings.append({**base, "reason": "invalid_relevance_score", "detail": "relevance_score is null, non-numeric, or outside 0..1"})
        if _published(row):
            published_links.setdefault(content_id, []).append({**base, "score": score})

    for row in link_rows:
        pair = (row.get("content_id"), row.get("knowledge_id"))
        if pair_counts.get(pair, 0) > 1:
            findings.append(
                {
                    "id": row.get("link_id") or row.get("id"),
                    "link_id": row.get("link_id") or row.get("id"),
                    "content_id": row.get("content_id"),
                    "knowledge_id": row.get("knowledge_id"),
                    "reason": "duplicate_link",
                    "detail": f"duplicate content_id+knowledge_id appears {pair_counts[pair]} times",
                }
            )

    for content_id, links in published_links.items():
        valid_scores = [link["score"] for link in links if link["score"] is not None and 0 <= link["score"] <= 1]
        if len(valid_scores) < min_links or (valid_scores and max(valid_scores) < weak_threshold):
            findings.append(
                {
                    "id": content_id,
                    "content_id": content_id,
                    "reason": "weak_published_grounding",
                    "link_count": len(valid_scores),
                    "max_relevance_score": max(valid_scores) if valid_scores else None,
                    "detail": "published content has too few valid links or only weak relevance scores",
                }
            )

    return base_report(
        artifact_type=ARTIFACT_TYPE,
        generated_at=generated_at,
        filters={"weak_threshold": weak_threshold, "min_links": min_links, "limit": limit},
        rows_scanned=len(link_rows),
        findings=findings,
        reasons=REASONS,
        limit=limit,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        empty_message="No content knowledge link score gaps found.",
    )


def build_content_knowledge_link_score_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    db_schema = schema(conn)
    missing_tables = [table for table in REQUIRED if table not in db_schema]
    missing_columns = {
        table: sorted(required - db_schema.get(table, set()))
        for table, required in REQUIRED.items()
        if table in db_schema and required - db_schema.get(table, set())
    }
    if missing_tables or missing_columns:
        return build_content_knowledge_link_score_gaps_report(
            [], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs
        )
    return build_content_knowledge_link_score_gaps_report(
        _load_rows(conn, db_schema),
        content_rows=_load_published_content_rows(conn, db_schema),
        **kwargs,
    )


def format_content_knowledge_link_score_gaps_json(report: dict[str, Any]) -> str:
    return format_json(report)


def format_content_knowledge_link_score_gaps_text(report: dict[str, Any]) -> str:
    return format_text("Content Knowledge Link Score Gaps", report)


def _load_rows(conn: Any, db_schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    ckl = db_schema["content_knowledge_links"]
    gc = db_schema["generated_content"]
    score = column(ckl, "relevance_score", "NULL", alias="ckl")
    published = column(gc, "published", "0", alias="gc")
    status = column(gc, "status", "NULL", alias="gc")
    published_at = column(gc, "published_at", "NULL", alias="gc")
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT ckl.rowid AS link_id,
                       ckl.content_id,
                       ckl.knowledge_id,
                       {score} AS relevance_score,
                       gc.id AS resolved_content_id,
                       k.id AS resolved_knowledge_id,
                       {published} AS content_published,
                       {status} AS content_status,
                       {published_at} AS published_at
                  FROM content_knowledge_links ckl
                  LEFT JOIN generated_content gc ON gc.id = ckl.content_id
                  LEFT JOIN knowledge k ON k.id = ckl.knowledge_id
                 ORDER BY ckl.rowid ASC"""
        ).fetchall()
    ]


def _published(row: dict[str, Any]) -> bool:
    return _content_is_published(
        {
            "published": row.get("content_published"),
            "status": row.get("content_status"),
            "published_at": row.get("published_at"),
        }
    )


def _load_published_content_rows(conn: Any, db_schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    gc = db_schema["generated_content"]
    published = column(gc, "published", "0", alias="gc")
    status = column(gc, "status", "NULL", alias="gc")
    published_at = column(gc, "published_at", "NULL", alias="gc")
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT gc.id AS content_id,
                       {published} AS published,
                       {status} AS status,
                       {published_at} AS published_at
                  FROM generated_content gc
                 ORDER BY gc.rowid ASC"""
        ).fetchall()
    ]


def _content_is_published(row: dict[str, Any]) -> bool:
    status = str(row.get("status") or row.get("content_status") or "").strip().lower()
    return (
        truthy(row.get("published"))
        or truthy(row.get("content_published"))
        or status in {"published", "posted", "live", "sent", "success", "succeeded"}
        or bool(row.get("published_at"))
    )
