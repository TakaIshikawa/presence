"""Report LinkedIn engagement platform identity gaps."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ._report_helpers import base_report, clean, col, connection, json_format, missing, schema, text_format, utc


ARTIFACT_TYPE = "linkedin_engagement_identity_gaps"
DEFAULT_LIMIT = 100
REASONS = ("missing_content", "missing_platform_identity", "publication_identity_conflict", "duplicate_identity")


def build_linkedin_engagement_identity_gaps_report(rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, now: datetime | None = None, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = utc(now)
    findings: list[dict[str, Any]] = []
    identities: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        linkedin_url = clean(row.get("linkedin_url"))
        post_id = clean(row.get("post_id"))
        base = {"id": row.get("id"), "content_id": row.get("content_id"), "linkedin_url": linkedin_url or None, "post_id": post_id or None}
        if row.get("resolved_content_id") is None:
            findings.append({**base, "reason": "missing_content", "detail": "linkedin_engagement content_id has no generated_content row"})
        if not linkedin_url and not post_id:
            findings.append({**base, "reason": "missing_platform_identity", "detail": "row lacks both linkedin_url and post_id"})
        for identity in (f"url:{linkedin_url}" if linkedin_url else "", f"post:{post_id}" if post_id else ""):
            if identity:
                identities.setdefault(identity.lower(), []).append(row)
        pub_url = clean(row.get("publication_url"))
        pub_post = clean(row.get("publication_post_id"))
        if (pub_url and linkedin_url and pub_url != linkedin_url) or (pub_post and post_id and pub_post != post_id):
            findings.append({**base, "reason": "publication_identity_conflict", "publication_url": pub_url or None, "publication_post_id": pub_post or None, "detail": "engagement identity conflicts with content_publications"})
    for identity, items in identities.items():
        content_ids = {item.get("content_id") for item in items}
        if len(items) > 1 and len(content_ids) > 1:
            findings.append({"reason": "duplicate_identity", "identity": identity, "content_ids": sorted(str(v) for v in content_ids), "detail": "platform identity appears on multiple content rows"})
    findings.sort(key=lambda f: (REASONS.index(f["reason"]), str(f.get("content_id")), str(f.get("id"))))
    return base_report(artifact_type=ARTIFACT_TYPE, generated_at=generated_at, filters={"limit": limit}, rows_count=len(rows), findings=findings, reasons=REASONS, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def build_linkedin_engagement_identity_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    db_schema = schema(conn)
    required = {"linkedin_engagement": {"content_id"}, "generated_content": {"id"}}
    missing_tables, missing_columns = missing(db_schema, required)
    if missing_tables or missing_columns:
        return build_linkedin_engagement_identity_gaps_report([], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)
    le = db_schema["linkedin_engagement"]
    select = ["le.rowid AS id", "le.content_id", "gc.id AS resolved_content_id", col(le, "linkedin_url", "le") + " AS linkedin_url", col(le, "post_id", "le") + " AS post_id", "NULL AS publication_url", "NULL AS publication_post_id"]
    join = ""
    if "content_publications" in db_schema and "content_id" in db_schema["content_publications"]:
        cp = db_schema["content_publications"]
        join = "LEFT JOIN content_publications cp ON cp.content_id = le.content_id AND lower(COALESCE(cp.platform, '')) = 'linkedin'"
        select[5] = col(cp, "platform_url", "cp") + " AS publication_url"
        select[6] = col(cp, "platform_post_id", "cp") + " AS publication_post_id"
    rows = [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM linkedin_engagement le LEFT JOIN generated_content gc ON gc.id = le.content_id {join} ORDER BY le.rowid").fetchall()]
    return build_linkedin_engagement_identity_gaps_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_linkedin_engagement_identity_gaps_json(report: dict[str, Any]) -> str:
    return json_format(report)


def format_linkedin_engagement_identity_gaps_text(report: dict[str, Any]) -> str:
    return text_format("LinkedIn Engagement Identity Gaps", report)
