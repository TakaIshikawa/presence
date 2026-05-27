"""Audit eval_results against eval_batches."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
ISSUE_TYPES = (
    "missing_batch",
    "content_type_mismatch",
    "generator_model_mismatch",
    "evaluator_model_mismatch",
    "threshold_mismatch",
    "invalid_counter",
    "final_content_without_score",
)
COUNTERS = ("candidate_count", "accepted_count", "rejected_count")


def build_eval_result_batch_integrity_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    content_type: str | None = None,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    wanted_type = _clean(content_type).lower() or None
    findings: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    scanned = 0

    for row in rows:
        row_type = _clean(row.get("content_type")).lower()
        if wanted_type and row_type != wanted_type:
            continue
        scanned += 1
        base = {
            "result_id": row.get("result_id"),
            "batch_id": row.get("batch_id"),
            "content_type": row.get("content_type"),
            "batch_content_type": row.get("batch_content_type"),
            "generator_model": row.get("generator_model"),
            "batch_generator_model": row.get("batch_generator_model"),
            "evaluator_model": row.get("evaluator_model"),
            "batch_evaluator_model": row.get("batch_evaluator_model"),
            "threshold": _float(row.get("threshold")),
            "batch_threshold": _float(row.get("batch_threshold")),
        }
        if row.get("resolved_batch_id") is None:
            _record(findings, counts, {**base, "issue_type": "missing_batch", "detail": "eval_results.batch_id has no eval_batches row"})
        else:
            _compare(findings, counts, base, "content_type", "batch_content_type", "content_type_mismatch")
            _compare(findings, counts, base, "generator_model", "batch_generator_model", "generator_model_mismatch")
            _compare(findings, counts, base, "evaluator_model", "batch_evaluator_model", "evaluator_model_mismatch")
            if base["threshold"] is not None and base["batch_threshold"] is not None and abs(base["threshold"] - base["batch_threshold"]) > 1e-9:
                _record(findings, counts, {**base, "issue_type": "threshold_mismatch", "detail": "result threshold differs from batch threshold"})
        for counter in COUNTERS:
            value = _float(row.get(counter))
            if value is not None and value < 0:
                _record(findings, counts, {**base, "issue_type": "invalid_counter", "counter_name": counter, "counter_value": value, "detail": "counter cannot be negative"})
        if _clean(row.get("final_content")) and _float(row.get("final_score")) is None:
            _record(findings, counts, {**base, "issue_type": "final_content_without_score", "detail": "final_content exists but final_score is missing"})

    findings.sort(key=_finding_sort_key)
    return {
        "artifact_type": "eval_result_batch_integrity",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "content_type": wanted_type or "all"},
        "totals": {
            "result_count": scanned,
            "finding_count": len(findings),
            "issue_counts": {issue: counts[issue] for issue in ISSUE_TYPES},
        },
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
            if columns
        },
        "findings": findings,
    }


def build_eval_result_batch_integrity_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {"eval_results": {"id", "batch_id"}, "eval_batches": {"id"}}
    missing_tables = sorted(table for table in required if table not in schema)
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    if missing_tables or missing_columns:
        return build_eval_result_batch_integrity_report([], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)
    report_kwargs = dict(kwargs)
    days = int(report_kwargs.get("days", DEFAULT_DAYS))
    now = _utc(report_kwargs["now"]) if report_kwargs.get("now") is not None else (
        _latest_timestamp(conn, "eval_results", schema["eval_results"], ("created_at", "evaluated_at"))
        or datetime.now(timezone.utc)
    )
    report_kwargs["now"] = now
    return build_eval_result_batch_integrity_report(
        _load_rows(conn, schema, cutoff=now - timedelta(days=days)),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **report_kwargs,
    )


def format_eval_result_batch_integrity_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_eval_result_batch_integrity_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Eval Result Batch Integrity",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['days']} days",
        f"Content type: {report['filters']['content_type']}",
        f"Totals: results={totals['result_count']} findings={totals['finding_count']}",
        "Issue counts: " + ", ".join(f"{issue}={totals['issue_counts'][issue]}" for issue in ISSUE_TYPES),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if not report["findings"]:
        lines.append("No eval result batch integrity issues found.")
        return "\n".join(lines)
    lines.extend(["", "Findings:"])
    for finding in report["findings"]:
        lines.append(f"- {finding['issue_type']} result_id={finding.get('result_id')} batch_id={finding.get('batch_id')} detail={finding['detail']}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], *, cutoff: datetime) -> list[dict[str, Any]]:
    er = schema["eval_results"]
    eb = schema["eval_batches"]
    created = _col(er, "created_at", "evaluated_at", fallback="NULL", alias="er")
    filters: list[str] = []
    params: list[Any] = []
    if created != "NULL":
        filters.append(f"({created} IS NULL OR datetime({created}) >= datetime(?))")
        params.append(cutoff.isoformat())
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    select = {
        "content_type": _col(er, "content_type", fallback="NULL", alias="er"),
        "batch_content_type": _col(eb, "content_type", fallback="NULL", alias="eb"),
        "generator_model": _col(er, "generator_model", fallback="NULL", alias="er"),
        "batch_generator_model": _col(eb, "generator_model", fallback="NULL", alias="eb"),
        "evaluator_model": _col(er, "evaluator_model", fallback="NULL", alias="er"),
        "batch_evaluator_model": _col(eb, "evaluator_model", fallback="NULL", alias="eb"),
        "threshold": _col(er, "threshold", "score_threshold", fallback="NULL", alias="er"),
        "batch_threshold": _col(eb, "threshold", "score_threshold", fallback="NULL", alias="eb"),
        "candidate_count": _col(er, "candidate_count", fallback="NULL", alias="er"),
        "accepted_count": _col(er, "accepted_count", fallback="NULL", alias="er"),
        "rejected_count": _col(er, "rejected_count", fallback="NULL", alias="er"),
        "final_content": _col(er, "final_content", fallback="NULL", alias="er"),
        "final_score": _col(er, "final_score", fallback="NULL", alias="er"),
    }
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT er.id AS result_id, er.batch_id, eb.id AS resolved_batch_id,
                       {', '.join(f'{expr} AS {alias}' for alias, expr in select.items())}
                FROM eval_results er
                LEFT JOIN eval_batches eb ON eb.id = er.batch_id
                {where}
                ORDER BY er.id ASC""",
            params,
        ).fetchall()
    ]


def _compare(findings: list[dict[str, Any]], counts: Counter[str], base: dict[str, Any], left: str, right: str, issue_type: str) -> None:
    if _clean(base.get(left)) and _clean(base.get(right)) and _clean(base.get(left)) != _clean(base.get(right)):
        _record(findings, counts, {**base, "issue_type": issue_type, "detail": f"{left} differs from eval_batches"})


def _record(findings: list[dict[str, Any]], counts: Counter[str], finding: dict[str, Any]) -> None:
    findings.append(finding)
    counts[finding["issue_type"]] += 1


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _latest_timestamp(conn: sqlite3.Connection, table: str, columns: set[str], names: tuple[str, ...]) -> datetime | None:
    for name in names:
        if name not in columns:
            continue
        row = conn.execute(f"SELECT MAX(datetime({name})) FROM {table}").fetchone()
        if row and row[0]:
            return _utc(datetime.fromisoformat(str(row[0]).replace(" ", "T")))
    return None


def _col(columns: set[str], *names: str, fallback: str, alias: str) -> str:
    for name in names:
        if name in columns:
            return f"{alias}.{name}"
    return fallback


def _float(value: Any) -> float | None:
    try:
        return None if value in (None, "") else float(value)
    except (TypeError, ValueError):
        return None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    return (ISSUE_TYPES.index(finding["issue_type"]), finding.get("result_id") or 0)
