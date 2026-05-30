"""Report engagement prediction component completeness gaps."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from ._report_helpers import base_report, clean, col, connection, json_format, missing, parse_time, schema, text_format, to_float, utc


ARTIFACT_TYPE = "engagement_prediction_component_completeness"
DEFAULT_MAX_BACKFILL_AGE_HOURS = 72
DEFAULT_LIMIT = 100
COMPONENTS = ("hook_strength", "specificity", "emotional_resonance", "novelty", "actionability")
REASONS = ("missing_component", "invalid_component_range", "missing_prompt_identity", "stale_actual_backfill", "prediction_error_mismatch")


def build_engagement_prediction_component_completeness_report(rows: list[dict[str, Any]], *, max_backfill_age_hours: int = DEFAULT_MAX_BACKFILL_AGE_HOURS, limit: int = DEFAULT_LIMIT, now: datetime | None = None, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None) -> dict[str, Any]:
    if max_backfill_age_hours < 0:
        raise ValueError("max_backfill_age_hours must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = utc(now)
    cutoff = generated_at - timedelta(hours=max_backfill_age_hours)
    findings: list[dict[str, Any]] = []
    for row in rows:
        base = {"id": row.get("id"), "content_id": row.get("content_id")}
        for component in COMPONENTS:
            value = to_float(row.get(component))
            if value is None:
                findings.append({**base, "reason": "missing_component", "component": component, "detail": "component score is missing"})
            elif value < 0 or value > 1:
                findings.append({**base, "reason": "invalid_component_range", "component": component, "value": value, "detail": "component score is outside 0..1"})
        if not clean(row.get("prompt_version")) or not clean(row.get("prompt_hash")):
            findings.append({**base, "reason": "missing_prompt_identity", "detail": "prompt_version or prompt_hash is missing"})
        published_at = parse_time(row.get("published_at"))
        if row.get("actual_engagement_score") in (None, "") and published_at and published_at <= cutoff:
            findings.append({**base, "reason": "stale_actual_backfill", "detail": "published content is older than backfill age and actual_engagement_score is missing"})
        predicted = to_float(row.get("predicted_score") if row.get("predicted_score") is not None else row.get("predicted_engagement_score"))
        actual = to_float(row.get("actual_engagement_score"))
        error = to_float(row.get("prediction_error"))
        if predicted is not None and actual is not None and error is not None and abs(error - (actual - predicted)) > 0.0001:
            findings.append({**base, "reason": "prediction_error_mismatch", "expected_prediction_error": round(actual - predicted, 6), "detail": "prediction_error does not equal actual minus predicted"})
    findings.sort(key=lambda f: (REASONS.index(f["reason"]), str(f.get("content_id")), str(f.get("id")), str(f.get("component"))))
    return base_report(artifact_type=ARTIFACT_TYPE, generated_at=generated_at, filters={"max_backfill_age_hours": max_backfill_age_hours, "limit": limit}, rows_count=len(rows), findings=findings, reasons=REASONS, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def build_engagement_prediction_component_completeness_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    db_schema = schema(conn)
    required = {"engagement_predictions": {"id", "content_id"}}
    missing_tables, missing_columns = missing(db_schema, required)
    if missing_tables or missing_columns:
        return build_engagement_prediction_component_completeness_report([], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)
    ep = db_schema["engagement_predictions"]
    joins = ""
    published = "NULL AS published_at"
    if "generated_content" in db_schema and "id" in db_schema["generated_content"]:
        joins = "LEFT JOIN generated_content gc ON gc.id = ep.content_id"
        published = col(db_schema["generated_content"], "published_at", "gc") + " AS published_at"
    select = ["ep.id", "ep.content_id", col(ep, "predicted_score", "ep", col(ep, "predicted_engagement_score", "ep")) + " AS predicted_score", col(ep, "actual_engagement_score", "ep") + " AS actual_engagement_score", col(ep, "prediction_error", "ep") + " AS prediction_error", col(ep, "prompt_version", "ep") + " AS prompt_version", col(ep, "prompt_hash", "ep") + " AS prompt_hash", published]
    select.extend(col(ep, component, "ep") + f" AS {component}" for component in COMPONENTS)
    rows = [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM engagement_predictions ep {joins} ORDER BY ep.id").fetchall()]
    return build_engagement_prediction_component_completeness_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_engagement_prediction_component_completeness_json(report: dict[str, Any]) -> str:
    return json_format(report)


def format_engagement_prediction_component_completeness_text(report: dict[str, Any]) -> str:
    return text_format("Engagement Prediction Component Completeness", report)
