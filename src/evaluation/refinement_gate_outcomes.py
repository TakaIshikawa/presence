"""Summarize refinement churn and final gate outcomes."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 50
DEFAULT_HIGH_CHURN_ATTEMPTS = 3


def build_refinement_gate_outcomes_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    high_churn_attempts: int = DEFAULT_HIGH_CHURN_ATTEMPTS,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if high_churn_attempts <= 0:
        raise ValueError("high_churn_attempts must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    items = []
    for row in rows:
        created_at = _parse_dt(_first(row, "created_at", "updated_at", "completed_at"))
        if created_at and created_at < cutoff:
            continue
        items.append(_item(row, created_at))

    aggregate_groups = _aggregate_groups(items)
    high_churn_groups = [
        group
        for group in aggregate_groups
        if group["average_refinement_attempts"] >= high_churn_attempts
        or group["max_refinement_attempts"] >= high_churn_attempts
    ]
    high_churn_groups.sort(
        key=lambda group: (
            -group["average_refinement_attempts"],
            -group["max_refinement_attempts"],
            group["format"],
            group["source_type"],
        )
    )
    improved_but_rejected = [
        item for item in items if item["score_movement"] == "improved" and item["final_gate_outcome"] == "failed"
    ]
    improved_but_rejected.sort(
        key=lambda item: (-item["refinement_delta"], item["format"], item["source_type"], item["candidate_id"])
    )
    reason_counts = Counter(item["failure_reason"] for item in items if item["final_gate_outcome"] == "failed")

    return {
        "artifact_type": "refinement_gate_outcomes",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "limit": limit,
            "high_churn_attempts": high_churn_attempts,
            "lookback_start": cutoff.isoformat(),
        },
        "summary": {
            "rows_scanned": len(items),
            "passed": sum(1 for item in items if item["final_gate_outcome"] == "passed"),
            "failed": sum(1 for item in items if item["final_gate_outcome"] == "failed"),
            "unknown": sum(1 for item in items if item["final_gate_outcome"] == "unknown"),
            "movement_counts": dict(sorted(Counter(item["score_movement"] for item in items).items())),
            "failure_reason_counts": dict(sorted(reason_counts.items())),
        },
        "aggregate_counts": aggregate_groups,
        "high_churn_groups": high_churn_groups[:limit],
        "improved_but_rejected": improved_but_rejected[:limit],
        "common_final_rejection_reasons": [
            {"failure_reason": reason, "count": count}
            for reason, count in sorted(reason_counts.items(), key=lambda pair: (-pair[1], pair[0]))[:limit]
        ],
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def build_refinement_gate_outcomes_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    rows = _load_rows(conn, schema) if not gaps["missing_tables"] else []
    return build_refinement_gate_outcomes_report(rows, schema_gaps=gaps, **kwargs)


def format_refinement_gate_outcomes_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_refinement_gate_outcomes_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Refinement Gate Outcomes",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['days']} days limit={report['filters']['limit']}",
        f"Totals: rows={summary['rows_scanned']} passed={summary['passed']} failed={summary['failed']} unknown={summary['unknown']}",
    ]
    if not report["aggregate_counts"]:
        lines.extend(["", "No refinement gate outcomes found."])
        return "\n".join(lines)
    lines.extend(["", "Aggregate counts:"])
    for group in report["aggregate_counts"]:
        lines.append(
            f"  - format={group['format']} source={group['source_type']} count={group['count']} "
            f"passed={group['passed']} failed={group['failed']} avg_attempts={group['average_refinement_attempts']}"
        )
    if report["high_churn_groups"]:
        lines.extend(["", "High churn groups:"])
        for group in report["high_churn_groups"]:
            lines.append(
                f"  - format={group['format']} source={group['source_type']} "
                f"avg_attempts={group['average_refinement_attempts']} max_attempts={group['max_refinement_attempts']}"
            )
    if report["common_final_rejection_reasons"]:
        lines.extend(["", "Common final rejection reasons:"])
        for reason in report["common_final_rejection_reasons"]:
            lines.append(f"  - {reason['failure_reason']}: {reason['count']}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "refinement_gate_runs" if "refinement_gate_runs" in schema else "pipeline_runs" if "pipeline_runs" in schema else ""
    if not table:
        return []
    columns = schema[table]
    select = [
        _select(columns, ("id", "batch_id"), "candidate_id"),
        _select(columns, ("format", "content_format", "content_type"), "format"),
        _select(columns, ("source_type",), "source_type"),
        _select(columns, ("refinement_attempts", "attempt_count", "candidates_generated"), "refinement_attempts"),
        _select(columns, ("before_score", "best_score_before_refine"), "before_score"),
        _select(columns, ("after_score", "best_score_after_refine", "final_score"), "after_score"),
        _select(columns, ("final_gate_outcome", "outcome"), "final_gate_outcome"),
        _select(columns, ("failure_reason", "rejection_reason"), "failure_reason"),
        _select(columns, ("published",), "published"),
        _select(columns, ("created_at", "updated_at", "completed_at"), "created_at"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}").fetchall()]


def _aggregate_groups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[(item["format"], item["source_type"])].append(item)
    groups = []
    for (fmt, source_type), group_items in grouped.items():
        groups.append(
            {
                "format": fmt,
                "source_type": source_type,
                "count": len(group_items),
                "passed": sum(1 for item in group_items if item["final_gate_outcome"] == "passed"),
                "failed": sum(1 for item in group_items if item["final_gate_outcome"] == "failed"),
                "unknown": sum(1 for item in group_items if item["final_gate_outcome"] == "unknown"),
                "improved": sum(1 for item in group_items if item["score_movement"] == "improved"),
                "regressed": sum(1 for item in group_items if item["score_movement"] == "regressed"),
                "unchanged": sum(1 for item in group_items if item["score_movement"] == "unchanged"),
                "average_refinement_attempts": _average(item["refinement_attempts"] for item in group_items) or 0,
                "max_refinement_attempts": max(item["refinement_attempts"] for item in group_items),
                "failure_reason_counts": dict(
                    sorted(Counter(item["failure_reason"] for item in group_items if item["final_gate_outcome"] == "failed").items())
                ),
            }
        )
    groups.sort(key=lambda group: (-group["count"], group["format"], group["source_type"]))
    return groups


def _item(row: dict[str, Any], created_at: datetime | None) -> dict[str, Any]:
    before = _float_or_none(_first(row, "before_score", "best_score_before_refine"))
    after = _float_or_none(_first(row, "after_score", "best_score_after_refine", "final_score"))
    delta = round(after - before, 4) if before is not None and after is not None else None
    outcome = _gate_outcome(row)
    return {
        "candidate_id": _text(_first(row, "candidate_id", "id", "batch_id")) or "unknown",
        "format": _text(_first(row, "format", "content_format", "content_type")) or "unknown",
        "source_type": _text(row.get("source_type")) or "unknown",
        "refinement_attempts": _int_or_zero(_first(row, "refinement_attempts", "attempt_count", "candidates_generated")),
        "before_score": before,
        "after_score": after,
        "refinement_delta": delta,
        "score_movement": _score_movement(before, after),
        "final_gate_outcome": outcome,
        "failure_reason": _failure_reason(row, outcome),
        "created_at": created_at.isoformat() if created_at else None,
    }


def _gate_outcome(row: dict[str, Any]) -> str:
    raw = _text(_first(row, "final_gate_outcome", "outcome", "status")).lower()
    if raw in {"pass", "passed", "accepted", "approved", "published", "success"}:
        return "passed"
    if raw in {"fail", "failed", "rejected", "blocked"}:
        return "failed"
    published = _first(row, "published", "is_published")
    if published in {1, True, "1", "true", "True", "yes", "published"}:
        return "passed"
    if _text(_first(row, "failure_reason", "rejection_reason")):
        return "failed"
    return "unknown"


def _failure_reason(row: dict[str, Any], outcome: str) -> str | None:
    if outcome != "failed":
        return None
    return _text(_first(row, "failure_reason", "rejection_reason")) or "unspecified"


def _score_movement(before: float | None, after: float | None) -> str:
    if before is None or after is None:
        return "unknown"
    if after > before:
        return "improved"
    if after < before:
        return "regressed"
    return "unchanged"


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "refinement_gate_runs" not in schema and "pipeline_runs" not in schema:
        return {"missing_tables": ["refinement_gate_runs|pipeline_runs"], "missing_columns": {}}
    return {"missing_tables": [], "missing_columns": {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_zero(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _average(values: Any) -> float | None:
    numbers = [float(value) for value in values if value is not None]
    if not numbers:
        return None
    return round(sum(numbers) / len(numbers), 4)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
