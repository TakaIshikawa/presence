"""Report reviewer latency for content claim checks."""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import sqlite3
from statistics import median
from typing import Any

from ._batch_report_utils import connection, dump_json, first_table, parse_time, pick, schema, text, utc_now

DEFAULT_PENDING_THRESHOLD_HOURS = 72.0
DEFAULT_DECISION_THRESHOLD_HOURS = 24.0
DEFAULT_LIMIT = 100


def build_content_claim_reviewer_latency_report_from_db(db_or_conn: Any, *, pending_threshold_hours: float = DEFAULT_PENDING_THRESHOLD_HOURS, decision_threshold_hours: float = DEFAULT_DECISION_THRESHOLD_HOURS, limit: int = DEFAULT_LIMIT, now: datetime | None = None) -> dict[str, Any]:
    if pending_threshold_hours <= 0 or decision_threshold_hours <= 0:
        raise ValueError("thresholds must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = utc_now(now)
    conn = connection(db_or_conn)
    sch = schema(conn)
    table = first_table(sch, ("content_claim_checks", "content_claims"))
    missing_tables = [] if table else ["content_claim_checks|content_claims"]
    missing_columns: dict[str, list[str]] = {}
    rows: list[dict[str, Any]] = []
    if table:
        cols = sch[table]
        missing = []
        if "id" not in cols:
            missing.append("id")
        if not {"created_at", "claim_created_at"} & cols:
            missing.append("created_at|claim_created_at")
        if missing:
            missing_columns[table] = missing
        else:
            rows = _load(conn, table, cols)
    findings = []
    latencies: list[float] = []
    for row in rows:
        created = parse_time(row.get("created_at"))
        decided = parse_time(row.get("decided_at"))
        status = text(row.get("status")).lower()
        reviewer = text(row.get("reviewer"))
        claim_id = row.get("claim_id")
        if not created:
            continue
        age = (generated_at - created).total_seconds() / 3600
        if status in {"pending", "review", "queued", ""} and age >= pending_threshold_hours:
            findings.append(_finding("stale_pending_review", claim_id, status or "pending", age, reviewer))
        if decided:
            latency = (decided - created).total_seconds() / 3600
            latencies.append(latency)
            if latency >= decision_threshold_hours and status in {"approved", "rejected", "unsupported", "supported"}:
                findings.append(_finding("slow_review_decision", claim_id, status, latency, reviewer))
                if status in {"rejected", "unsupported"}:
                    findings.append(_finding("slow_rejection", claim_id, status, latency, reviewer))
                if status in {"approved", "supported"}:
                    findings.append(_finding("slow_approval", claim_id, status, latency, reviewer))
        if decided and not reviewer:
            findings.append(_finding("missing_reviewer_identity", claim_id, status, (decided - created).total_seconds() / 3600, reviewer))
        if row.get("reopened_at") and decided:
            reopened = parse_time(row.get("reopened_at"))
            if reopened and (decided - created).total_seconds() / 3600 >= decision_threshold_hours:
                findings.append(_finding("reopened_after_slow_review", claim_id, status, (decided - created).total_seconds() / 3600, reviewer))
    findings.sort(key=lambda f: (f["finding_type"], str(f["claim_id"])))
    findings = findings[:limit]
    return {
        "artifact_type": "content_claim_reviewer_latency",
        "generated_at": generated_at.isoformat(),
        "filters": {"pending_threshold_hours": pending_threshold_hours, "decision_threshold_hours": decision_threshold_hours, "limit": limit},
        "summary": {"claim_count": len(rows), "finding_count": len(findings), "p50_review_latency_hours": _percentile(latencies, 50), "p90_review_latency_hours": _percentile(latencies, 90)},
        "findings": findings,
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
        "empty_state": {"is_empty": not rows or not findings, "message": "No content claim reviewer latency findings found." if rows and not findings else "No content claim checks found." if not rows and not (missing_tables or missing_columns) else None},
    }


def build_content_claim_reviewer_latency_report(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return build_content_claim_reviewer_latency_report_from_db(*args, **kwargs)


def format_content_claim_reviewer_latency_json(report: dict[str, Any]) -> str:
    return dump_json(report)


def format_content_claim_reviewer_latency_text(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = ["Content Claim Reviewer Latency", f"Generated: {report['generated_at']}", f"Totals: claims={s['claim_count']} findings={s['finding_count']} p50={s['p50_review_latency_hours']} p90={s['p90_review_latency_hours']}"]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if not report["findings"]:
        if report["empty_state"]["message"]:
            lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("Findings:")
    lines.extend(f"  - {f['finding_type']} claim={f['claim_id']} hours={f['latency_hours']}" for f in report["findings"])
    return "\n".join(lines)


def _load(conn: sqlite3.Connection, table: str, cols: set[str]) -> list[dict[str, Any]]:
    status_expr = pick(cols, "status", "review_status", "verdict", default="'pending'")
    select = [
        f"{pick(cols, 'id', default='rowid')} AS claim_id",
        f"{pick(cols, 'created_at', 'claim_created_at')} AS created_at",
        f"{pick(cols, 'decided_at', 'reviewed_at', 'updated_at', default='NULL')} AS decided_at",
        f"{status_expr} AS status",
        f"{pick(cols, 'reviewer_id', 'reviewer', 'reviewed_by', default='NULL')} AS reviewer",
        f"{pick(cols, 'reopened_at', default='NULL')} AS reopened_at",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY claim_id ASC")]


def _finding(kind: str, claim_id: Any, status: str, hours: float, reviewer: str) -> dict[str, Any]:
    return {"finding_type": kind, "claim_id": claim_id, "review_status": status, "latency_hours": round(hours, 2), "reviewer": reviewer or None}


def _percentile(values: list[float], pct: int) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 2)
    idx = round((pct / 100) * (len(ordered) - 1))
    return round(ordered[idx], 2)
