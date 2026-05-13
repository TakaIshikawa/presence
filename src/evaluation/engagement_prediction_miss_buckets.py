"""Bucket engagement prediction misses by prompt identity."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_LIMIT = 100
DEFAULT_ABSOLUTE_ERROR_THRESHOLD = 2.0


def build_engagement_prediction_miss_buckets_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    absolute_error_threshold: float = DEFAULT_ABSOLUTE_ERROR_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build prediction miss buckets from ``engagement_predictions``."""
    if days <= 0:
        raise ValueError("days must be positive")
    if absolute_error_threshold < 0:
        raise ValueError("absolute_error_threshold must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {
        "days": days,
        "absolute_error_threshold": absolute_error_threshold,
        "limit": limit,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
    }
    missing_tables = [] if "engagement_predictions" in schema else ["engagement_predictions"]
    required = {
        "id",
        "content_id",
        "predicted_score",
        "prompt_type",
        "prompt_version",
        "prompt_hash",
        "actual_engagement_score",
        "prediction_error",
        "created_at",
    }
    missing_columns = {
        "engagement_predictions": sorted(required - schema.get("engagement_predictions", set()))
    } if "engagement_predictions" in schema and required - schema.get("engagement_predictions", set()) else {}
    if missing_tables or missing_columns:
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    rows: list[dict[str, Any]] = []
    prompt_groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    counts: Counter[str] = Counter()
    for raw in _load_rows(conn, schema, cutoff, generated_at):
        row = dict(raw)
        bucket = _bucket(row.get("actual_engagement_score"), row.get("prediction_error"), absolute_error_threshold)
        counts[bucket] += 1
        item = {
            "prediction_id": int(row["id"]),
            "content_id": row.get("content_id"),
            "content_type": row.get("content_type"),
            "prompt_type": row.get("prompt_type") or "unknown",
            "prompt_version": row.get("prompt_version") or "unknown",
            "prompt_hash": row.get("prompt_hash") or "unknown",
            "predicted_score": _float_or_none(row.get("predicted_score")),
            "actual_engagement_score": _float_or_none(row.get("actual_engagement_score")),
            "prediction_error": _float_or_none(row.get("prediction_error")),
            "bucket": bucket,
        }
        rows.append(item)
        _add_prompt_group(prompt_groups, item)

    rows.sort(key=lambda item: (_bucket_priority(item["bucket"]), item["prompt_type"], item["prediction_id"]))
    return {
        "artifact_type": "engagement_prediction_miss_buckets",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "prediction_count": len(rows),
            "prompt_group_count": len(prompt_groups),
            "rows": min(len(rows), limit),
        },
        "bucket_counts": dict(sorted(counts.items())),
        "prompt_groups": sorted(prompt_groups.values(), key=lambda g: (-g["count"], g["prompt_type"], g["prompt_version"], g["prompt_hash"])),
        "rows": rows[:limit],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def format_engagement_prediction_miss_buckets_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_engagement_prediction_miss_buckets_text(report: dict[str, Any]) -> str:
    lines = [
        "Engagement Prediction Miss Buckets",
        f"Generated: {report['generated_at']}",
        f"Totals: predictions={report['totals']['prediction_count']} prompt_groups={report['totals']['prompt_group_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    lines.append("Buckets: " + ", ".join(f"{k}={v}" for k, v in report["bucket_counts"].items()))
    return "\n".join(lines)


def _empty_report(generated_at: datetime, filters: dict[str, Any], missing_tables: list[str], missing_columns: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "artifact_type": "engagement_prediction_miss_buckets",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"prediction_count": 0, "prompt_group_count": 0, "rows": 0},
        "bucket_counts": {},
        "prompt_groups": [],
        "rows": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime, now: datetime) -> list[sqlite3.Row]:
    has_content = "generated_content" in schema and {"id", "content_type"}.issubset(schema["generated_content"])
    join = "LEFT JOIN generated_content gc ON gc.id = ep.content_id" if has_content else ""
    content_type = "gc.content_type AS content_type" if has_content else "NULL AS content_type"
    return conn.execute(
        f"""SELECT ep.id, ep.content_id, ep.predicted_score, ep.actual_engagement_score,
                  ep.prediction_error, ep.prompt_type, ep.prompt_version, ep.prompt_hash,
                  {content_type}
           FROM engagement_predictions ep
           {join}
           WHERE datetime(ep.created_at) >= datetime(?) AND datetime(ep.created_at) <= datetime(?)
           ORDER BY datetime(ep.created_at) DESC, ep.id DESC""",
        (cutoff.isoformat(), now.isoformat()),
    ).fetchall()


def _bucket(actual: Any, error: Any, threshold: float) -> str:
    if actual is None or error is None:
        return "missing_actual"
    value = _float_or_none(error)
    if value is None:
        return "missing_actual"
    if value >= threshold:
        return "high_underprediction"
    if value <= -threshold:
        return "high_overprediction"
    return "well_calibrated"


def _add_prompt_group(groups: dict[tuple[str, str, str], dict[str, Any]], row: dict[str, Any]) -> None:
    key = (row["prompt_type"], row["prompt_version"], row["prompt_hash"])
    group = groups.setdefault(
        key,
        {
            "prompt_type": row["prompt_type"],
            "prompt_version": row["prompt_version"],
            "prompt_hash": row["prompt_hash"],
            "count": 0,
            "bucket_counts": {},
            "content_types": {},
            "representative_content_ids": [],
            "representative_prediction_ids": [],
        },
    )
    group["count"] += 1
    group["bucket_counts"][row["bucket"]] = group["bucket_counts"].get(row["bucket"], 0) + 1
    content_type = row.get("content_type") or "unknown"
    group["content_types"][content_type] = group["content_types"].get(content_type, 0) + 1
    if row.get("content_id") is not None and len(group["representative_content_ids"]) < 5:
        group["representative_content_ids"].append(row["content_id"])
    if len(group["representative_prediction_ids"]) < 5:
        group["representative_prediction_ids"].append(row["prediction_id"])


def _bucket_priority(bucket: str) -> int:
    return {
        "high_overprediction": 0,
        "high_underprediction": 1,
        "missing_actual": 2,
        "well_calibrated": 3,
    }.get(bucket, 9)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
