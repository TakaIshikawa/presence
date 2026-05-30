"""Report whether revision feedback was adopted into downstream content."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from ._report_helpers import base_report, clean, col, connection, json_format, missing, norm_text, parse_time, schema, text_format, utc


ARTIFACT_TYPE = "content_feedback_revision_adoption"
DEFAULT_MIN_AGE_HOURS = 24
DEFAULT_LIMIT = 100
REASONS = ("unadopted_revision", "adopted_in_variant", "adopted_in_source_content", "conflicting_replacements")


def build_content_feedback_revision_adoption_report(
    rows: list[dict[str, Any]],
    *,
    min_age_hours: int = DEFAULT_MIN_AGE_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if min_age_hours < 0:
        raise ValueError("min_age_hours must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = utc(now)
    cutoff = generated_at - timedelta(hours=min_age_hours)
    findings: list[dict[str, Any]] = []
    replacements_by_content: dict[Any, set[str]] = {}

    for row in rows:
        feedback_type = clean(row.get("feedback_type")).lower()
        replacement = clean(row.get("replacement_text"))
        if feedback_type not in {"revise", "prefer"} or not replacement:
            continue
        content_id = row.get("content_id")
        normalized = norm_text(replacement)
        replacements_by_content.setdefault(content_id, set()).add(normalized)
        source_hit = normalized in norm_text(row.get("source_content"))
        variant_hit = normalized in norm_text(row.get("variant_content"))
        publication_hit = normalized in norm_text(row.get("publication_content"))
        base = {
            "feedback_id": row.get("feedback_id") or row.get("id"),
            "content_id": content_id,
            "feedback_type": feedback_type,
            "replacement_text": replacement,
            "created_at": row.get("created_at"),
        }
        if source_hit:
            findings.append({**base, "reason": "adopted_in_source_content", "detail": "replacement_text appears in generated_content.content"})
        if variant_hit or publication_hit:
            findings.append({**base, "reason": "adopted_in_variant", "detail": "replacement_text appears in variant or published content"})
        created_at = parse_time(row.get("created_at"))
        if not (source_hit or variant_hit or publication_hit) and (created_at is None or created_at <= cutoff):
            findings.append({**base, "reason": "unadopted_revision", "detail": "aged revision feedback was not found downstream"})

    for content_id, replacements in replacements_by_content.items():
        if len(replacements) > 1:
            findings.append(
                {
                    "reason": "conflicting_replacements",
                    "content_id": content_id,
                    "replacement_count": len(replacements),
                    "detail": "multiple distinct replacement_text values exist for content_id",
                }
            )
    findings.sort(key=lambda item: (REASONS.index(item["reason"]), str(item.get("content_id")), str(item.get("feedback_id"))))
    return base_report(
        artifact_type=ARTIFACT_TYPE,
        generated_at=generated_at,
        filters={"min_age_hours": min_age_hours, "limit": limit},
        rows_count=len(rows),
        findings=findings,
        reasons=REASONS,
        limit=limit,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def build_content_feedback_revision_adoption_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    db_schema = schema(conn)
    required = {
        "content_feedback": {"id", "content_id", "feedback_type", "replacement_text"},
        "generated_content": {"id", "content"},
    }
    missing_tables, missing_columns = missing(db_schema, required)
    if missing_tables or missing_columns:
        return build_content_feedback_revision_adoption_report([], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)
    cf = db_schema["content_feedback"]
    gc = db_schema["generated_content"]
    joins = []
    select = [
        "cf.id AS feedback_id",
        "cf.content_id",
        "cf.feedback_type",
        "cf.replacement_text",
        col(cf, "created_at", "cf") + " AS created_at",
        col(gc, "content", "gc") + " AS source_content",
        "NULL AS variant_content",
        "NULL AS publication_content",
    ]
    if "content_variants" in db_schema and {"content_id", "content"}.issubset(db_schema["content_variants"]):
        joins.append("LEFT JOIN content_variants cv ON cv.content_id = cf.content_id")
        select[6] = "GROUP_CONCAT(cv.content, ' ') AS variant_content"
    if "content_publications" in db_schema:
        cp = db_schema["content_publications"]
        joins.append("LEFT JOIN content_publications cp ON cp.content_id = cf.content_id")
        pieces = [
            f"COALESCE({col(cp, 'platform_url', 'cp')}, '')",
            f"COALESCE({col(cp, 'platform_post_id', 'cp')}, '')",
            f"COALESCE({col(cp, 'status', 'cp')}, '')",
        ]
        select[7] = "GROUP_CONCAT(" + " || ' ' || ".join(pieces) + ", ' ') AS publication_content"
    rows = [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(select)}
                FROM content_feedback cf
                LEFT JOIN generated_content gc ON gc.id = cf.content_id
                {' '.join(joins)}
                GROUP BY cf.id
                ORDER BY cf.id ASC"""
        ).fetchall()
    ]
    return build_content_feedback_revision_adoption_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_content_feedback_revision_adoption_json(report: dict[str, Any]) -> str:
    return json_format(report)


def format_content_feedback_revision_adoption_text(report: dict[str, Any]) -> str:
    return text_format("Content Feedback Revision Adoption", report)
