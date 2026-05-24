"""Report growth and missing size signals for pipeline run artifacts."""

from __future__ import annotations

from datetime import datetime, timedelta
import sqlite3
from typing import Any

from ._batch_report_utils import connection, dump_json, first_table, parse_time, pick, schema, text, utc_now

DEFAULT_MAX_BYTES = 1_000_000
DEFAULT_GROWTH_RATIO = 2.0
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LIMIT = 100


def build_pipeline_run_artifact_size_growth_report_from_db(db_or_conn: Any, *, max_bytes: int = DEFAULT_MAX_BYTES, growth_ratio: float = DEFAULT_GROWTH_RATIO, lookback_days: int = DEFAULT_LOOKBACK_DAYS, limit: int = DEFAULT_LIMIT, now: datetime | None = None) -> dict[str, Any]:
    if max_bytes <= 0 or growth_ratio <= 1 or lookback_days <= 0 or limit <= 0:
        raise ValueError("max_bytes, lookback_days, and limit must be positive; growth_ratio must be greater than 1")
    generated_at = utc_now(now)
    conn = connection(db_or_conn)
    sch = schema(conn)
    table = first_table(sch, ("pipeline_run_artifacts", "pipeline_artifacts"))
    missing_tables = [] if table else ["pipeline_run_artifacts|pipeline_artifacts"]
    missing_columns: dict[str, list[str]] = {}
    rows: list[dict[str, Any]] = []
    if table:
        cols = sch[table]
        missing = []
        if not {"created_at", "updated_at"} & cols:
            missing.append("created_at|updated_at")
        if missing:
            missing_columns[table] = missing
        else:
            rows = _load(conn, table, cols, generated_at - timedelta(days=lookback_days))
    findings = []
    by_stage: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        size = _size(row)
        stage = text(row.get("stage")) or "unknown"
        by_stage.setdefault(stage, []).append({**row, "size_bytes": size})
        if size is None:
            findings.append(_finding("missing_size_signal", row, None))
        elif size > max_bytes:
            findings.append(_finding("oversized_artifact", row, size))
    for stage, items in by_stage.items():
        sized = sorted([i for i in items if i["size_bytes"] is not None], key=lambda r: parse_time(r.get("created_at")) or datetime.min.replace(tzinfo=generated_at.tzinfo))
        if len(sized) >= 2 and sized[0]["size_bytes"] and sized[-1]["size_bytes"] / sized[0]["size_bytes"] >= growth_ratio:
            f = _finding("rapid_size_growth", sized[-1], sized[-1]["size_bytes"])
            f["stage"] = stage
            f["growth_ratio"] = round(sized[-1]["size_bytes"] / sized[0]["size_bytes"], 2)
            findings.append(f)
    stage_totals = {stage: sum(i["size_bytes"] or 0 for i in items) for stage, items in by_stage.items()}
    total = sum(stage_totals.values())
    for stage, bytes_ in stage_totals.items():
        if total and bytes_ / total >= 0.75 and len(stage_totals) > 1:
            findings.append({"finding_type": "stage_growth_concentration", "stage": stage, "size_bytes": bytes_, "share": round(bytes_ / total, 3)})
    findings.sort(key=lambda f: (f["finding_type"], str(f.get("stage", "")), str(f.get("artifact_id", ""))))
    findings = findings[:limit]
    return {"artifact_type": "pipeline_run_artifact_size_growth", "generated_at": generated_at.isoformat(), "filters": {"max_bytes": max_bytes, "growth_ratio": growth_ratio, "lookback_days": lookback_days, "limit": limit}, "summary": {"artifact_count": len(rows), "finding_count": len(findings), "total_size_bytes": total}, "findings": findings, "missing_tables": missing_tables, "missing_columns": missing_columns, "empty_state": {"is_empty": not rows or not findings, "message": "No pipeline artifact size growth findings found." if rows and not findings else "No pipeline artifacts found." if not rows and not (missing_tables or missing_columns) else None}}


def build_pipeline_run_artifact_size_growth_report(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return build_pipeline_run_artifact_size_growth_report_from_db(*args, **kwargs)


def format_pipeline_run_artifact_size_growth_json(report: dict[str, Any]) -> str:
    return dump_json(report)


def format_pipeline_run_artifact_size_growth_text(report: dict[str, Any]) -> str:
    lines = ["Pipeline Run Artifact Size Growth", f"Generated: {report['generated_at']}", f"Totals: artifacts={report['summary']['artifact_count']} findings={report['summary']['finding_count']} bytes={report['summary']['total_size_bytes']}"]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if not report["findings"] and report["empty_state"]["message"]:
        lines.append(report["empty_state"]["message"])
    for f in report["findings"]:
        lines.append(f"  - {f['finding_type']} artifact={f.get('artifact_id', '-')} stage={f.get('stage', '-')} bytes={f.get('size_bytes', '-')}")
    return "\n".join(lines)


def _load(conn: sqlite3.Connection, table: str, cols: set[str], cutoff: datetime) -> list[dict[str, Any]]:
    ts = pick(cols, "created_at", "updated_at")
    select = [f"{pick(cols, 'id', default='rowid')} AS artifact_id", f"{pick(cols, 'pipeline_run_id', 'run_id', default='NULL')} AS run_id", f"{pick(cols, 'stage', 'stage_name', default='NULL')} AS stage", f"{pick(cols, 'size_bytes', 'byte_size', default='NULL')} AS size_bytes", f"{pick(cols, 'payload', 'content', 'metadata', default='NULL')} AS payload", f"{ts} AS created_at"]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} WHERE datetime({ts}) >= datetime(?) ORDER BY datetime({ts}) ASC, artifact_id ASC", (cutoff.isoformat(),))]


def _size(row: dict[str, Any]) -> int | None:
    if row.get("size_bytes") is not None:
        try:
            return int(row["size_bytes"])
        except (TypeError, ValueError):
            pass
    payload = row.get("payload")
    return len(str(payload).encode("utf-8")) if payload not in (None, "") else None


def _finding(kind: str, row: dict[str, Any], size: int | None) -> dict[str, Any]:
    return {"finding_type": kind, "artifact_id": row.get("artifact_id"), "pipeline_run_id": row.get("run_id"), "stage": text(row.get("stage")) or "unknown", "size_bytes": size}
