"""Correlate prompt template versions with downstream failure signals."""

from __future__ import annotations

import sqlite3
from typing import Any

from ._batch_report_utils import connection, dump_json, first_table, pick, schema, text, number, utc_now

DEFAULT_MIN_ATTEMPTS = 2
DEFAULT_FAILURE_RATE_THRESHOLD = 0.5
DEFAULT_SCORE_THRESHOLD = 0.6
DEFAULT_LIMIT = 100


def build_prompt_template_failure_correlation_report_from_db(db_or_conn: Any, *, min_attempts: int = DEFAULT_MIN_ATTEMPTS, failure_rate_threshold: float = DEFAULT_FAILURE_RATE_THRESHOLD, score_threshold: float = DEFAULT_SCORE_THRESHOLD, limit: int = DEFAULT_LIMIT, now=None) -> dict[str, Any]:
    if min_attempts <= 0 or limit <= 0:
        raise ValueError("min_attempts and limit must be positive")
    conn = connection(db_or_conn)
    generated_at = utc_now(now)
    sch = schema(conn)
    table = first_table(sch, ("pipeline_runs", "generated_content", "evaluations"))
    missing_tables = [] if table else ["pipeline_runs|generated_content|evaluations"]
    rows: list[dict[str, Any]] = []
    missing_columns: dict[str, list[str]] = {}
    if table:
        cols = sch[table]
        if not {"prompt_template", "prompt_type", "template"} & cols:
            missing_columns[table] = ["prompt_template|prompt_type|template"]
        else:
            rows = _load(conn, table, cols)
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    findings = []
    for row in rows:
        template = text(row.get("template")) or "missing"
        version = text(row.get("version")) or "missing"
        g = groups.setdefault((template, version), {"prompt_template": template, "prompt_version": version, "attempts": 0, "failures": 0, "rejections": 0, "scores": [], "representative_ids": []})
        g["attempts"] += 1
        status = text(row.get("status")).lower()
        if status in {"failed", "failure", "error", "rejected"}:
            g["failures"] += 1
        if status in {"rejected", "rejection"}:
            g["rejections"] += 1
        if row.get("score") is not None:
            g["scores"].append(number(row.get("score")))
        if len(g["representative_ids"]) < 3:
            g["representative_ids"].append(row.get("run_id"))
        if template == "missing" or version == "missing":
            findings.append({"finding_type": "missing_prompt_version_link", "run_id": row.get("run_id"), "prompt_template": template, "prompt_version": version})
    summaries = []
    for g in groups.values():
        avg = round(sum(g["scores"]) / len(g["scores"]), 3) if g["scores"] else None
        failure_rate = round(g["failures"] / g["attempts"], 3) if g["attempts"] else 0
        summary = {**g, "failure_rate": failure_rate, "average_score": avg}
        summaries.append(summary)
        if g["attempts"] >= min_attempts and failure_rate >= failure_rate_threshold:
            findings.append({"finding_type": "high_failure_rate_template", **summary})
        if g["attempts"] >= min_attempts and g["rejections"] >= 2:
            findings.append({"finding_type": "rejection_cluster", **summary})
        if g["attempts"] >= min_attempts and avg is not None and avg <= score_threshold:
            findings.append({"finding_type": "low_score_cluster", **summary})
    findings.sort(key=lambda f: (f["finding_type"], f.get("prompt_template", ""), f.get("prompt_version", "")))
    return {"artifact_type": "prompt_template_failure_correlation", "generated_at": generated_at.isoformat(), "filters": {"min_attempts": min_attempts, "failure_rate_threshold": failure_rate_threshold, "score_threshold": score_threshold, "limit": limit}, "summary": {"row_count": len(rows), "group_count": len(groups), "finding_count": min(len(findings), limit)}, "templates": sorted(summaries, key=lambda g: (g["prompt_template"], g["prompt_version"])), "findings": findings[:limit], "missing_tables": missing_tables, "missing_columns": missing_columns, "empty_state": {"is_empty": not rows or not findings, "message": "No prompt template failure correlation findings found." if rows and not findings else "No prompt template attempts found." if not rows and not (missing_tables or missing_columns) else None}}


def build_prompt_template_failure_correlation_report(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return build_prompt_template_failure_correlation_report_from_db(*args, **kwargs)


def format_prompt_template_failure_correlation_json(report: dict[str, Any]) -> str:
    return dump_json(report)


def format_prompt_template_failure_correlation_text(report: dict[str, Any]) -> str:
    lines = ["Prompt Template Failure Correlation", f"Generated: {report['generated_at']}", f"Totals: rows={report['summary']['row_count']} groups={report['summary']['group_count']} findings={report['summary']['finding_count']}"]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"] and report["empty_state"]["message"]:
        lines.append(report["empty_state"]["message"])
    lines.extend(f"  - {f['finding_type']} template={f.get('prompt_template')} version={f.get('prompt_version')} rate={f.get('failure_rate', '-')}" for f in report["findings"])
    return "\n".join(lines)


def _load(conn: sqlite3.Connection, table: str, cols: set[str]) -> list[dict[str, Any]]:
    select = [f"{pick(cols, 'id', 'run_id', 'content_id', default='rowid')} AS run_id", f"{pick(cols, 'prompt_template', 'prompt_type', 'template')} AS template", f"{pick(cols, 'prompt_version', 'version', default='NULL')} AS version", f"{pick(cols, 'status', 'outcome', 'review_status', default='NULL')} AS status", f"{pick(cols, 'evaluation_score', 'score', 'quality_score', default='NULL')} AS score"]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY run_id ASC")]
