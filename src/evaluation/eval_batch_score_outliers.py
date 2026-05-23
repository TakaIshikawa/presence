"""Report eval result score outliers within batches."""

from __future__ import annotations

from datetime import datetime
from statistics import mean, pstdev
from typing import Any

from ._report_helpers import base_report, clean, col, connection, json_format, missing, schema, text_format, to_float, to_int, utc


ARTIFACT_TYPE = "eval_batch_score_outliers"
DEFAULT_Z_THRESHOLD = 2.0
DEFAULT_MIN_BATCH_SIZE = 3
DEFAULT_LIMIT = 100
REASONS = ("low_score_outlier", "high_score_outlier", "missing_final_score", "threshold_mismatch", "count_anomaly")


def build_eval_batch_score_outliers_report(rows: list[dict[str, Any]], *, z_threshold: float = DEFAULT_Z_THRESHOLD, min_batch_size: int = DEFAULT_MIN_BATCH_SIZE, limit: int = DEFAULT_LIMIT, now: datetime | None = None, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None) -> dict[str, Any]:
    if z_threshold < 0:
        raise ValueError("z_threshold must be non-negative")
    if min_batch_size <= 0 or limit <= 0:
        raise ValueError("min_batch_size and limit must be positive")
    generated_at = utc(now)
    findings: list[dict[str, Any]] = []
    batches: dict[Any, list[dict[str, Any]]] = {}
    for row in rows:
        batches.setdefault(row.get("batch_id"), []).append(row)
        base = {"id": row.get("id"), "batch_id": row.get("batch_id")}
        if to_float(row.get("final_score")) is None and clean(row.get("final_content")):
            findings.append({**base, "reason": "missing_final_score", "detail": "accepted output has no final_score"})
        if to_float(row.get("threshold")) is not None and to_float(row.get("batch_threshold")) is not None and to_float(row.get("threshold")) != to_float(row.get("batch_threshold")):
            findings.append({**base, "reason": "threshold_mismatch", "detail": "result threshold differs from batch threshold"})
        if (to_int(row.get("candidate_count")) or 0) <= 0 or (to_int(row.get("prompt_count")) or 0) <= 0:
            findings.append({**base, "reason": "count_anomaly", "detail": "candidate_count or prompt_count is not positive"})
    for batch_id, items in batches.items():
        scored = [(row, to_float(row.get("final_score"))) for row in items if to_float(row.get("final_score")) is not None]
        if len(scored) < min_batch_size:
            continue
        values = [score for _row, score in scored if score is not None]
        avg = mean(values)
        sd = pstdev(values)
        if sd == 0:
            continue
        for row, score in scored:
            assert score is not None
            z = (score - avg) / sd
            if z <= -z_threshold:
                findings.append({"id": row.get("id"), "batch_id": batch_id, "reason": "low_score_outlier", "z_score": round(z, 3), "detail": "final_score is far below batch peers"})
            elif z >= z_threshold:
                findings.append({"id": row.get("id"), "batch_id": batch_id, "reason": "high_score_outlier", "z_score": round(z, 3), "detail": "final_score is far above batch peers"})
    findings.sort(key=lambda f: (str(f.get("batch_id")), REASONS.index(f["reason"]), str(f.get("id"))))
    return base_report(artifact_type=ARTIFACT_TYPE, generated_at=generated_at, filters={"z_threshold": z_threshold, "min_batch_size": min_batch_size, "limit": limit}, rows_count=len(rows), findings=findings, reasons=REASONS, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def build_eval_batch_score_outliers_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    db_schema = schema(conn)
    required = {"eval_batches": {"id", "threshold"}, "eval_results": {"id", "batch_id"}}
    missing_tables, missing_columns = missing(db_schema, required)
    if missing_tables or missing_columns:
        return build_eval_batch_score_outliers_report([], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)
    er, eb = db_schema["eval_results"], db_schema["eval_batches"]
    select = ["er.id", "er.batch_id", col(er, "final_score", "er") + " AS final_score", col(er, "threshold", "er") + " AS threshold", col(eb, "threshold", "eb") + " AS batch_threshold", col(er, "candidate_count", "er") + " AS candidate_count", col(er, "prompt_count", "er") + " AS prompt_count", col(er, "final_content", "er") + " AS final_content"]
    rows = [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM eval_results er LEFT JOIN eval_batches eb ON eb.id = er.batch_id ORDER BY er.batch_id, er.id").fetchall()]
    return build_eval_batch_score_outliers_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_eval_batch_score_outliers_json(report: dict[str, Any]) -> str:
    return json_format(report)


def format_eval_batch_score_outliers_text(report: dict[str, Any]) -> str:
    return text_format("Eval Batch Score Outliers", report)
