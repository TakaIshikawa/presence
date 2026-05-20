"""Audit pipeline run outcome consistency."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
PUBLISHABLE_OUTCOMES = {"published", "publishable", "ready_to_publish", "selected"}
NON_PUBLISHABLE_WITH_CONTENT = {"all_filtered", "below_threshold", "failed", "rejected", "error"}
IN_PROGRESS_OUTCOMES = {"", "pending", "running", "started", "in_progress", "queued"}
VALID_REFINEMENT_PICKS = {"REFINED", "ORIGINAL"}
AUDIT_COLUMNS = {
    "id",
    "batch_id",
    "content_type",
    "published",
    "outcome",
    "content_id",
    "candidates_generated",
    "best_candidate_index",
    "refinement_picked",
    "final_score",
    "filter_stats",
    "created_at",
}


def build_pipeline_run_outcome_integrity_report(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic integrity report for pipeline run outcomes."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    findings: list[dict[str, Any]] = []
    issue_counts: Counter[str] = Counter()

    for raw in rows:
        row = _normalize_row(raw)
        for issue_type, details in _row_issues(row):
            issue_counts[issue_type] += 1
            findings.append(_finding(row, issue_type, details))

    findings.sort(key=lambda item: (item["pipeline_run_id"], item["issue_type"]))
    shown = findings[:limit]
    empty_state = {
        "is_empty": not rows,
        "reason": "missing_schema" if missing_tables or missing_columns else "no_pipeline_runs" if not rows else None,
    }
    return {
        "artifact_type": "pipeline_run_outcome_integrity",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "summary": {
            "pipeline_run_count": len(rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "malformed_filter_stats_count": issue_counts.get("malformed_filter_stats", 0),
            "by_issue_type": dict(sorted(issue_counts.items())),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
            if columns
        },
        "empty_state": empty_state,
    }


def build_pipeline_run_outcome_integrity_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [] if "pipeline_runs" in schema else ["pipeline_runs"]
    missing_columns: dict[str, list[str]] = {}
    if "pipeline_runs" in schema:
        missing = sorted(AUDIT_COLUMNS - schema["pipeline_runs"] - {"id"})
        if missing:
            missing_columns["pipeline_runs"] = missing
    return build_pipeline_run_outcome_integrity_report(
        _load_rows(conn, schema) if "pipeline_runs" in schema else [],
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_pipeline_run_outcome_integrity_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_pipeline_run_outcome_integrity_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Pipeline Run Outcome Integrity",
        f"Generated: {report['generated_at']}",
        f"Filters: limit={report['filters']['limit']}",
        f"Totals: runs={summary['pipeline_run_count']} findings={summary['finding_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report["missing_columns"].items())
        ]
        lines.append("Missing columns: " + "; ".join(missing))
    if not report["findings"]:
        lines.append("No pipeline run outcome integrity issues found.")
        return "\n".join(lines)
    lines.append("Findings:")
    for finding in report["findings"]:
        lines.append(
            f"  - run={finding['pipeline_run_id']} batch={finding['batch_id'] or '-'} "
            f"type={finding['issue_type']} outcome={finding['outcome'] or '-'} "
            f"published={finding['published']} content_id={finding['content_id'] or '-'} "
            f"details={finding['details']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cols = schema["pipeline_runs"]
    select = [
        _expr(cols, "id", fallback="rowid") + " AS pipeline_run_id",
        _expr(cols, "batch_id", fallback="NULL") + " AS batch_id",
        _expr(cols, "content_type", fallback="NULL") + " AS content_type",
        _expr(cols, "published", fallback="NULL") + " AS published",
        _expr(cols, "outcome", fallback="NULL") + " AS outcome",
        _expr(cols, "content_id", fallback="NULL") + " AS content_id",
        _expr(cols, "candidates_generated", fallback="NULL") + " AS candidates_generated",
        _expr(cols, "best_candidate_index", fallback="NULL") + " AS best_candidate_index",
        _expr(cols, "refinement_picked", fallback="NULL") + " AS refinement_picked",
        _expr(cols, "final_score", fallback="NULL") + " AS final_score",
        _expr(cols, "filter_stats", fallback="NULL") + " AS filter_stats",
        _expr(cols, "created_at", fallback="NULL") + " AS created_at",
    ]
    order = _expr(cols, "created_at", fallback="rowid") + ", " + _expr(cols, "id", fallback="rowid")
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM pipeline_runs ORDER BY {order}").fetchall()]


def _row_issues(row: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    issues: list[tuple[str, dict[str, Any]]] = []
    outcome = row["outcome_normalized"]
    published = row["published_bool"]
    has_content = bool(row["content_id"])
    candidates = row["candidates_generated_int"]
    best_index = row["best_candidate_index_int"]

    mismatches = []
    if published and outcome not in PUBLISHABLE_OUTCOMES:
        mismatches.append("published_true_with_non_publishable_outcome")
    if outcome in PUBLISHABLE_OUTCOMES and not published:
        mismatches.append("publishable_outcome_without_published_flag")
    if (published or outcome in PUBLISHABLE_OUTCOMES) and not has_content:
        mismatches.append("publishable_state_without_content_id")
    if has_content and outcome in NON_PUBLISHABLE_WITH_CONTENT:
        mismatches.append("content_id_with_non_publishable_outcome")
    if mismatches:
        issues.append(("published_outcome_content_mismatch", {"mismatches": sorted(mismatches)}))

    if outcome not in IN_PROGRESS_OUTCOMES and (candidates is None or candidates <= 0):
        issues.append(("invalid_candidates_generated", {"candidates_generated": row["candidates_generated"]}))
    if candidates is not None and candidates > 0 and best_index is not None and (best_index < 0 or best_index >= candidates):
        issues.append(
            (
                "invalid_best_candidate_index",
                {"best_candidate_index": best_index, "candidates_generated": candidates},
            )
        )
    if row["refinement_picked"] and row["refinement_picked"] not in VALID_REFINEMENT_PICKS:
        issues.append(("invalid_refinement_picked", {"refinement_picked": row["refinement_picked"]}))
    if outcome in PUBLISHABLE_OUTCOMES and row["final_score_float"] is None:
        issues.append(("missing_final_score", {"final_score": row["final_score"]}))
    if _malformed_json_object(row["filter_stats"]):
        issues.append(("malformed_filter_stats", {"filter_stats": row["filter_stats"]}))
    return issues


def _finding(row: dict[str, Any], issue_type: str, details: dict[str, Any]) -> dict[str, Any]:
    return {
        "pipeline_run_id": row["pipeline_run_id"],
        "batch_id": row["batch_id"],
        "content_type": row["content_type"] or "unknown",
        "outcome": row["outcome"],
        "published": row["published_bool"],
        "content_id": row["content_id"],
        "issue_type": issue_type,
        "details": details,
        "created_at": row["created_at"],
    }


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "pipeline_run_id": _text(row.get("pipeline_run_id")) or _text(row.get("id")) or "unknown",
        "batch_id": _text(row.get("batch_id")),
        "content_type": _text(row.get("content_type")),
        "published": row.get("published"),
        "published_bool": _truthy(row.get("published")),
        "outcome": _text(row.get("outcome")),
        "content_id": _text(row.get("content_id")),
        "candidates_generated": row.get("candidates_generated"),
        "candidates_generated_int": _int_or_none(row.get("candidates_generated")),
        "best_candidate_index": row.get("best_candidate_index"),
        "best_candidate_index_int": _int_or_none(row.get("best_candidate_index")),
        "refinement_picked": _text(row.get("refinement_picked")),
        "final_score": row.get("final_score"),
        "final_score_float": _float_or_none(row.get("final_score")),
        "filter_stats": row.get("filter_stats"),
        "created_at": row.get("created_at"),
    }
    normalized["outcome_normalized"] = normalized["outcome"].lower()
    return normalized


def _malformed_json_object(value: Any) -> bool:
    if value in (None, ""):
        return False
    if isinstance(value, dict):
        return False
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return True
    return not isinstance(parsed, dict)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name").fetchall()
    return {row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _expr(columns: set[str], *names: str, fallback: str) -> str:
    for name in names:
        if name in columns:
            return name
    return fallback


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "published"}
    return bool(value)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
