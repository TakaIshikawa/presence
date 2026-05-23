"""Catalog publication attempt response metadata JSON shapes."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
import json
from typing import Any

from ._report_helpers import base_report, clean, col, connection, json_format, missing, schema, text_format, truthy, utc


ARTIFACT_TYPE = "publication_attempt_response_shape_catalog"
DEFAULT_MAX_METADATA_BYTES = 4096
DEFAULT_RARE_SHAPE_THRESHOLD = 1
DEFAULT_LIMIT = 100
REASONS = ("malformed_json", "missing_success_response_key", "oversized_metadata", "rare_shape")


def _shape(value: Any) -> tuple[str | None, str | None]:
    text = clean(value)
    if not text:
        return None, None
    try:
        parsed = json.loads(text)
    except ValueError:
        return None, "malformed"
    if not isinstance(parsed, dict):
        return type(parsed).__name__, None
    return ",".join(sorted(parsed.keys())), None


def build_publication_attempt_response_shape_catalog_report(rows: list[dict[str, Any]], *, max_metadata_bytes: int = DEFAULT_MAX_METADATA_BYTES, rare_shape_threshold: int = DEFAULT_RARE_SHAPE_THRESHOLD, limit: int = DEFAULT_LIMIT, now: datetime | None = None, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None) -> dict[str, Any]:
    if max_metadata_bytes < 0:
        raise ValueError("max_metadata_bytes must be non-negative")
    if rare_shape_threshold <= 0 or limit <= 0:
        raise ValueError("rare_shape_threshold and limit must be positive")
    generated_at = utc(now)
    findings: list[dict[str, Any]] = []
    shape_counts: Counter[tuple[str, str, str, str]] = Counter()
    row_shapes: list[tuple[dict[str, Any], str | None]] = []
    for row in rows:
        metadata = clean(row.get("response_metadata"))
        shape, error = _shape(metadata)
        key = (clean(row.get("platform"), "unknown"), str(int(truthy(row.get("success")))), clean(row.get("error_category"), "none"), shape or "empty")
        shape_counts[key] += 1
        row_shapes.append((row, shape))
        base = {"id": row.get("id"), "platform": row.get("platform"), "shape": shape}
        if error:
            findings.append({**base, "reason": "malformed_json", "detail": "response_metadata is malformed JSON"})
        if truthy(row.get("success")) and shape is not None and not ({"id", "post_id", "url", "uri"} & set(shape.split(","))):
            findings.append({**base, "reason": "missing_success_response_key", "detail": "successful attempt metadata lacks id/url key"})
        if len(metadata.encode("utf-8")) > max_metadata_bytes:
            findings.append({**base, "reason": "oversized_metadata", "metadata_bytes": len(metadata.encode("utf-8")), "detail": "response_metadata exceeds byte limit"})
    for row, shape in row_shapes:
        key = (clean(row.get("platform"), "unknown"), str(int(truthy(row.get("success")))), clean(row.get("error_category"), "none"), shape or "empty")
        if shape and shape_counts[key] <= rare_shape_threshold:
            findings.append({"id": row.get("id"), "platform": row.get("platform"), "shape": shape, "reason": "rare_shape", "detail": "metadata shape is rare for platform/success/error group"})
    findings.sort(key=lambda f: (REASONS.index(f["reason"]), str(f.get("platform")), str(f.get("id"))))
    summaries = [{"platform": k[0], "success": k[1] == "1", "error_category": k[2], "shape": k[3], "count": v} for k, v in sorted(shape_counts.items())]
    return base_report(artifact_type=ARTIFACT_TYPE, generated_at=generated_at, filters={"max_metadata_bytes": max_metadata_bytes, "rare_shape_threshold": rare_shape_threshold, "limit": limit}, rows_count=len(rows), findings=findings, reasons=REASONS, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns, extra={"shape_summaries": summaries})


def build_publication_attempt_response_shape_catalog_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    db_schema = schema(conn)
    required = {"publication_attempts": {"id", "platform", "success"}}
    missing_tables, missing_columns = missing(db_schema, required)
    if missing_tables or missing_columns:
        return build_publication_attempt_response_shape_catalog_report([], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)
    pa = db_schema["publication_attempts"]
    select = ["id", "platform", "success", col(pa, "error_category", "publication_attempts") + " AS error_category", col(pa, "response_metadata", "publication_attempts") + " AS response_metadata"]
    rows = [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM publication_attempts ORDER BY id").fetchall()]
    return build_publication_attempt_response_shape_catalog_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_publication_attempt_response_shape_catalog_json(report: dict[str, Any]) -> str:
    return json_format(report)


def format_publication_attempt_response_shape_catalog_text(report: dict[str, Any]) -> str:
    return text_format("Publication Attempt Response Shape Catalog", report)
