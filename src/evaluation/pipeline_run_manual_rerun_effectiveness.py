"""Measure whether manually initiated pipeline reruns resolved failures."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from ._report_utils import clean, connection, dt, expr, json_dumps, lower, now_iso, positive, schema


ARTIFACT_TYPE = "pipeline_run_manual_rerun_effectiveness"
DEFAULT_LIMIT = 50
SUCCESS = {"success", "succeeded", "passed", "complete", "completed"}
FAILURE = {"failed", "failure", "error", "cancelled"}
RUNNING = {"running", "queued", "in_progress", "pending"}


def build_pipeline_run_manual_rerun_effectiveness_report(rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None, now: Any = None) -> dict[str, Any]:
    positive("limit", limit)
    by_id = {str(r.get("run_id")): r for r in rows if r.get("run_id") is not None}
    findings: list[dict[str, Any]] = []
    for rerun in rows:
        original_id = clean(rerun.get("original_run_id") or rerun.get("parent_run_id") or rerun.get("retry_of"))
        if not original_id:
            continue
        original = by_id.get(original_id, {})
        rerun_status = lower(rerun.get("status"), "unknown")
        original_status = lower(original.get("status"), "unknown")
        started = dt(original.get("finished_at") or original.get("created_at") or original.get("started_at"))
        ended = dt(rerun.get("finished_at") or rerun.get("created_at") or rerun.get("started_at"))
        elapsed = round((ended - started).total_seconds() / 60, 2) if started and ended else None
        outcome = _outcome(original_status, rerun_status)
        findings.append({"original_run_id": original_id, "rerun_run_id": str(rerun.get("run_id")), "stage": clean(rerun.get("stage") or original.get("stage"), "unknown"), "original_status": original_status, "rerun_status": rerun_status, "initiated_by": clean(rerun.get("initiated_by"), "unknown"), "elapsed_minutes": elapsed, "outcome": outcome})
    findings.sort(key=lambda r: (r["outcome"], r["stage"], r["original_run_id"], r["rerun_run_id"]))
    shown = findings[:limit]
    by_stage: dict[str, dict[str, int]] = defaultdict(lambda: Counter())
    by_actor: dict[str, dict[str, int]] = defaultdict(lambda: Counter())
    for f in findings:
        by_stage[f["stage"]][f["outcome"]] += 1
        by_actor[f["initiated_by"]][f["outcome"]] += 1
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_iso(now), "thresholds": {"limit": limit}, "summary": {"run_count": len(rows), "rerun_count": len(findings), "shown_count": len(shown), "by_outcome": dict(sorted(Counter(f["outcome"] for f in findings).items())), "by_stage": {k: dict(sorted(v.items())) for k, v in sorted(by_stage.items())}, "by_initiated_by": {k: dict(sorted(v.items())) for k, v in sorted(by_actor.items())}}, "findings": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}}


def build_pipeline_run_manual_rerun_effectiveness_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    s = schema(conn)
    table = next((t for t in ("pipeline_runs", "workflow_runs", "pipeline_run_history") if t in s), None)
    missing_tables = [] if table else ["pipeline_runs|workflow_runs"]
    missing_columns: dict[str, list[str]] = {}
    rows = _load_rows(conn, table, s[table], missing_columns) if table else []
    return build_pipeline_run_manual_rerun_effectiveness_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_pipeline_run_manual_rerun_effectiveness_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_pipeline_run_manual_rerun_effectiveness_text(report: dict[str, Any]) -> str:
    lines = ["Pipeline Run Manual Rerun Effectiveness", f"Generated: {report['generated_at']}", f"Totals: runs={report['summary']['run_count']} reruns={report['summary']['rerun_count']} shown={report['summary']['shown_count']}"]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append("No manual reruns found.")
        return "\n".join(lines)
    lines.extend(["", "original_run_id | rerun_run_id | stage | original_status | rerun_status | initiated_by | elapsed_minutes | outcome"])
    for r in report["findings"]:
        lines.append(f"{r['original_run_id']} | {r['rerun_run_id']} | {r['stage']} | {r['original_status']} | {r['rerun_status']} | {r['initiated_by']} | {r['elapsed_minutes'] if r['elapsed_minutes'] is not None else '-'} | {r['outcome']}")
    return "\n".join(lines)


def _outcome(original: str, rerun: str) -> str:
    if rerun in RUNNING:
        return "still_running"
    if original in FAILURE and rerun in SUCCESS:
        return "resolved"
    if original in FAILURE and rerun in FAILURE:
        return "repeated_failure"
    if original in SUCCESS and rerun in FAILURE:
        return "new_failure"
    return "resolved" if rerun in SUCCESS else "new_failure"


def _load_rows(conn: Any, table: str, cols: set[str], missing: dict[str, list[str]]) -> list[dict[str, Any]]:
    run_id = next((c for c in ("id", "run_id") if c in cols), None)
    if not run_id:
        missing[table] = ["id"]
        return []
    selected = [f"{run_id} AS run_id", expr(cols, "parent_run_id", default="NULL", out="parent_run_id"), expr(cols, "original_run_id", default="NULL", out="original_run_id"), expr(cols, "retry_of", default="NULL", out="retry_of"), expr(cols, "stage", "pipeline_stage", default="NULL", out="stage"), expr(cols, "status", default="'unknown'", out="status"), expr(cols, "initiated_by", "actor", "triggered_by", default="'unknown'", out="initiated_by"), expr(cols, "created_at", "started_at", default="NULL", out="created_at"), expr(cols, "finished_at", "completed_at", default="NULL", out="finished_at")]
    return [dict(r) for r in conn.execute(f"SELECT {', '.join(selected)} FROM {table} ORDER BY rowid")]

