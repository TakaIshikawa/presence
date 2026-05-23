"""Report content published or queued despite persona guard failures."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ._report_helpers import base_report, clean, col, connection, json_format, missing, schema, text_format, to_float, truthy, utc


ARTIFACT_TYPE = "content_persona_guard_publication_overrides"
DEFAULT_LIMIT = 100
REASONS = ("failed_guard_published", "unchecked_guard_published", "failed_guard_queued")


def build_content_persona_guard_publication_overrides_report(rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, now: datetime | None = None, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = utc(now)
    findings: list[dict[str, Any]] = []
    for row in rows:
        status = clean(row.get("publication_status")).lower()
        guard_status = clean(row.get("guard_status")).lower()
        checked = truthy(row.get("guard_checked"))
        passed = truthy(row.get("guard_passed"))
        selected = truthy(row.get("variant_selected"))
        published = status in {"published", "posted", "sent"} or truthy(row.get("content_published"))
        queued = status in {"queued", "scheduled"} or selected
        failed = checked and (not passed or guard_status in {"fail", "failed", "rejected", "blocked"})
        unchecked = not checked or guard_status in {"", "unchecked", "missing"}
        base = {"content_id": row.get("content_id"), "platform": row.get("platform"), "guard_status": guard_status or None, "guard_score": to_float(row.get("guard_score")), "guard_reasons": row.get("guard_reasons")}
        if published and failed:
            findings.append({**base, "reason": "failed_guard_published", "detail": "failed persona guard content was published"})
        if published and unchecked:
            findings.append({**base, "reason": "unchecked_guard_published", "detail": "published content lacks a checked persona guard pass"})
        if queued and failed:
            findings.append({**base, "reason": "failed_guard_queued", "detail": "failed persona guard content is queued or selected"})
    findings.sort(key=lambda f: (REASONS.index(f["reason"]), str(f.get("content_id")), str(f.get("platform"))))
    return base_report(artifact_type=ARTIFACT_TYPE, generated_at=generated_at, filters={"limit": limit}, rows_count=len(rows), findings=findings, reasons=REASONS, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def build_content_persona_guard_publication_overrides_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    db_schema = schema(conn)
    required = {"generated_content": {"id"}, "content_persona_guard": {"content_id"}}
    missing_tables, missing_columns = missing(db_schema, required)
    if missing_tables or missing_columns:
        return build_content_persona_guard_publication_overrides_report([], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)
    gc, pg = db_schema["generated_content"], db_schema["content_persona_guard"]
    joins = ["LEFT JOIN content_persona_guard pg ON pg.content_id = gc.id"]
    select = ["gc.id AS content_id", col(gc, "published", "gc") + " AS content_published", col(pg, "checked", "pg") + " AS guard_checked", col(pg, "passed", "pg") + " AS guard_passed", col(pg, "status", "pg") + " AS guard_status", col(pg, "score", "pg") + " AS guard_score", col(pg, "reasons", "pg") + " AS guard_reasons", "NULL AS publication_status", "NULL AS platform", "NULL AS variant_selected"]
    if "content_publications" in db_schema and "content_id" in db_schema["content_publications"]:
        cp = db_schema["content_publications"]
        joins.append("LEFT JOIN content_publications cp ON cp.content_id = gc.id")
        select[7] = col(cp, "status", "cp") + " AS publication_status"
        select[8] = col(cp, "platform", "cp") + " AS platform"
    if "content_variants" in db_schema and "content_id" in db_schema["content_variants"]:
        cv = db_schema["content_variants"]
        joins.append("LEFT JOIN content_variants cv ON cv.content_id = gc.id")
        select[9] = col(cv, "selected", "cv", "0") + " AS variant_selected"
    rows = [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM generated_content gc {' '.join(joins)} ORDER BY gc.id").fetchall()]
    return build_content_persona_guard_publication_overrides_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_content_persona_guard_publication_overrides_json(report: dict[str, Any]) -> str:
    return json_format(report)


def format_content_persona_guard_publication_overrides_text(report: dict[str, Any]) -> str:
    return text_format("Content Persona Guard Publication Overrides", report)
