"""Summarize pipeline filter dropoff by outcome, content type, and stage."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
DEFAULT_STAGE_EXAMPLE_LIMIT = 5


def build_pipeline_filter_dropoff_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only dropoff report from ``pipeline_runs.filter_stats``."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
    }
    missing_tables = [] if "pipeline_runs" in schema else ["pipeline_runs"]
    required = {"id", "content_type", "outcome", "rejection_reason", "filter_stats", "created_at"}
    missing_columns = {
        "pipeline_runs": sorted(required - schema.get("pipeline_runs", set()))
    } if "pipeline_runs" in schema and required - schema.get("pipeline_runs", set()) else {}
    if missing_tables or missing_columns:
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    rows: list[dict[str, Any]] = []
    malformed = 0
    stage_groups: dict[str, dict[str, Any]] = {}
    outcome_groups: dict[str, dict[str, Any]] = {}
    content_outcomes: Counter[tuple[str, str]] = Counter()

    for row in _load_rows(conn, cutoff, generated_at):
        parsed, bad = _parse_stats(row["filter_stats"])
        malformed += int(bad)
        stages = _stage_counts(parsed)
        dominant_stage = _dominant_stage(stages)
        total_filtered = sum(stages.values())
        item = {
            "pipeline_run_id": int(row["id"]),
            "content_type": row["content_type"] or "unknown",
            "outcome": row["outcome"] or "unknown",
            "rejection_reason": row["rejection_reason"],
            "dominant_filter_stage": dominant_stage,
            "total_filtered": total_filtered,
        }
        rows.append(item)
        content_outcomes[(item["content_type"], item["outcome"])] += 1
        _add_outcome_group(outcome_groups, item)
        if item["outcome"] in {"all_filtered", "below_threshold"}:
            for stage, count in stages.items():
                _add_stage_group(stage_groups, stage, count, item)

    rows.sort(key=lambda item: (-item["total_filtered"], item["pipeline_run_id"]))
    return {
        "artifact_type": "pipeline_filter_dropoff",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "pipeline_runs": len(rows),
            "malformed_filter_stats": malformed,
            "rows": min(len(rows), limit),
            "stage_groups": len(stage_groups),
            "outcome_groups": len(outcome_groups),
        },
        "stage_groups": sorted(stage_groups.values(), key=lambda g: (-g["total_filtered"], g["stage"])),
        "outcome_groups": sorted(outcome_groups.values(), key=lambda g: (-g["count"], g["outcome"])),
        "content_type_outcome_groups": [
            {"content_type": ct, "outcome": outcome, "count": count}
            for (ct, outcome), count in sorted(content_outcomes.items())
        ],
        "rows": rows[:limit],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def format_pipeline_filter_dropoff_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_pipeline_filter_dropoff_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Pipeline Filter Dropoff",
        f"Generated: {report['generated_at']}",
        f"Totals: runs={totals['pipeline_runs']} malformed={totals['malformed_filter_stats']} rows={totals['rows']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("stage_groups"):
        lines.append("")
        lines.append("Filter stages:")
        for group in report["stage_groups"]:
            lines.append(
                f"- {group['stage']}: runs={group['run_count']} filtered={group['total_filtered']} "
                f"examples={','.join(str(i) for i in group['representative_pipeline_run_ids'])}"
            )
    return "\n".join(lines)


def _empty_report(generated_at: datetime, filters: dict[str, Any], missing_tables: list[str], missing_columns: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "artifact_type": "pipeline_filter_dropoff",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "pipeline_runs": 0,
            "malformed_filter_stats": 0,
            "rows": 0,
            "stage_groups": 0,
            "outcome_groups": 0,
        },
        "stage_groups": [],
        "outcome_groups": [],
        "content_type_outcome_groups": [],
        "rows": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _load_rows(conn: sqlite3.Connection, cutoff: datetime, now: datetime) -> list[sqlite3.Row]:
    return conn.execute(
        """SELECT id, content_type, outcome, rejection_reason, filter_stats, created_at
           FROM pipeline_runs
           WHERE datetime(created_at) >= datetime(?) AND datetime(created_at) <= datetime(?)
           ORDER BY datetime(created_at) DESC, id DESC""",
        (cutoff.isoformat(), now.isoformat()),
    ).fetchall()


def _parse_stats(raw: Any) -> tuple[dict[str, Any], bool]:
    if raw in (None, ""):
        return {}, False
    if isinstance(raw, dict):
        return raw, False
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}, True
    if not isinstance(parsed, dict):
        return {}, True
    return parsed, False


def _stage_counts(stats: dict[str, Any]) -> dict[str, int]:
    stages: dict[str, int] = {}
    for key, value in stats.items():
        if isinstance(value, (int, float)) and value > 0 and (
            key.endswith("_rejected") or key.endswith("_filtered") or "filter" in key
        ):
            stage = key.removesuffix("_rejected").removesuffix("_filtered")
            stages[stage] = stages.get(stage, 0) + int(value)
    nested = stats.get("stages")
    if isinstance(nested, dict):
        for key, value in nested.items():
            if isinstance(value, dict):
                count = value.get("filtered") or value.get("rejected") or value.get("count")
            else:
                count = value
            if isinstance(count, (int, float)) and count > 0:
                stages[str(key)] = stages.get(str(key), 0) + int(count)
    return stages


def _dominant_stage(stages: dict[str, int]) -> str | None:
    if not stages:
        return None
    return sorted(stages.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _add_stage_group(groups: dict[str, dict[str, Any]], stage: str, count: int, row: dict[str, Any]) -> None:
    group = groups.setdefault(
        stage,
        {
            "stage": stage,
            "run_count": 0,
            "total_filtered": 0,
            "content_types": {},
            "outcomes": {},
            "representative_pipeline_run_ids": [],
        },
    )
    group["run_count"] += 1
    group["total_filtered"] += count
    group["content_types"][row["content_type"]] = group["content_types"].get(row["content_type"], 0) + 1
    group["outcomes"][row["outcome"]] = group["outcomes"].get(row["outcome"], 0) + 1
    if len(group["representative_pipeline_run_ids"]) < DEFAULT_STAGE_EXAMPLE_LIMIT:
        group["representative_pipeline_run_ids"].append(row["pipeline_run_id"])


def _add_outcome_group(groups: dict[str, dict[str, Any]], row: dict[str, Any]) -> None:
    group = groups.setdefault(
        row["outcome"],
        {"outcome": row["outcome"], "count": 0, "content_types": {}, "total_filtered": 0},
    )
    group["count"] += 1
    group["content_types"][row["content_type"]] = group["content_types"].get(row["content_type"], 0) + 1
    group["total_filtered"] += row["total_filtered"]


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
