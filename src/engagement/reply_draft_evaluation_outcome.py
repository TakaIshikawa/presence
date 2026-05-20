"""Audit reply drafts with missing, stale, or failing evaluator outcomes."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 50
DEFAULT_MIN_QUALITY_SCORE = 7.0
DEFAULT_STATUSES = ("draft", "pending", "queued", "ready_for_review")
FAIL_FLAGS = {"sycophantic", "generic"}


def build_reply_draft_evaluation_outcome_report(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    min_quality_score: float = DEFAULT_MIN_QUALITY_SCORE,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a deterministic report from normalized draft/evaluation rows."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    if not 0 <= min_quality_score <= 10:
        raise ValueError("min_quality_score must be between 0 and 10")

    generated_at = _utc(now or datetime.now(timezone.utc))
    findings = []
    for row in rows:
        reason_codes: list[str] = []
        draft_updated_at = _parse_dt(_first(row, "draft_updated_at", "updated_at", "created_at", "drafted_at"))
        evaluated_at = _parse_dt(_first(row, "evaluated_at", "evaluation_created_at", "evaluation_updated_at"))
        score = _float_or_none(_first(row, "quality_score", "score"))
        flags = _parse_flags(_first(row, "quality_flags", "flags"))
        actionable_flags = sorted(set(flags) & FAIL_FLAGS)

        has_evaluation = bool(_first(row, "evaluation_id", "quality_score", "score", "quality_flags", "flags", "evaluated_at"))
        if not has_evaluation:
            reason_codes.append("missing_evaluation")
        else:
            if score is not None and score < min_quality_score:
                reason_codes.append("low_quality_score")
            for flag in actionable_flags:
                reason_codes.append(f"{flag}_flag")
            if draft_updated_at is not None and evaluated_at is not None and evaluated_at < draft_updated_at:
                reason_codes.append("stale_evaluation")

        if not reason_codes:
            continue
        findings.append(
            {
                "draft_id": _text(_first(row, "draft_id", "id")) or "unknown",
                "mention_id": _optional_text(_first(row, "mention_id", "inbound_id", "inbound_tweet_id")),
                "status": _clean_status(_first(row, "status")) or "pending",
                "reason_codes": reason_codes,
                "quality_score": score,
                "quality_flags": flags,
                "evaluation_id": _optional_text(_first(row, "evaluation_id")),
                "draft_updated_at": _iso(draft_updated_at),
                "evaluated_at": _iso(evaluated_at),
            }
        )

    findings.sort(key=_finding_sort_key)
    findings = findings[:limit]
    reason_counts = Counter(reason for finding in findings for reason in finding["reason_codes"])
    return {
        "artifact_type": "reply_draft_evaluation_outcome",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit, "min_quality_score": min_quality_score},
        "summary": {
            "draft_count": len(rows),
            "finding_count": len(findings),
            "missing_evaluation_count": reason_counts.get("missing_evaluation", 0),
            "stale_evaluation_count": reason_counts.get("stale_evaluation", 0),
            "low_quality_score_count": reason_counts.get("low_quality_score", 0),
            "sycophantic_flag_count": reason_counts.get("sycophantic_flag", 0),
            "generic_flag_count": reason_counts.get("generic_flag", 0),
        },
        "findings": findings,
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def build_reply_draft_evaluation_outcome_report_from_db(
    db_or_conn: Any,
    *,
    limit: int = DEFAULT_LIMIT,
    min_quality_score: float = DEFAULT_MIN_QUALITY_SCORE,
    statuses: tuple[str, ...] | list[str] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load reply drafts from SQLite and audit evaluator outcomes."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    rows = [] if gaps["missing_tables"] else _load_rows(conn, schema, _normalize_statuses(statuses))
    return build_reply_draft_evaluation_outcome_report(
        rows,
        limit=limit,
        min_quality_score=min_quality_score,
        now=now,
        schema_gaps=gaps,
    )


def format_reply_draft_evaluation_outcome_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_draft_evaluation_outcome_text(report: dict[str, Any]) -> str:
    lines = [
        "Reply Draft Evaluation Outcome",
        f"Generated: {report['generated_at']}",
        f"Minimum quality score: {report['filters']['min_quality_score']:.1f}",
        (
            "Totals: "
            f"drafts={report['summary']['draft_count']} "
            f"findings={report['summary']['finding_count']} "
            f"missing={report['summary']['missing_evaluation_count']} "
            f"stale={report['summary']['stale_evaluation_count']} "
            f"low_score={report['summary']['low_quality_score_count']} "
            f"sycophantic={report['summary']['sycophantic_flag_count']} "
            f"generic={report['summary']['generic_flag_count']}"
        ),
    ]
    if report["schema_gaps"].get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["schema_gaps"]["missing_tables"]))
    if not report["findings"]:
        lines.append("No reply draft evaluation outcome issues found.")
        return "\n".join(lines)
    lines.extend(["", "draft_id | mention_id | status | score | flags | reasons | evaluated_at | draft_updated_at"])
    for finding in report["findings"]:
        score = "-" if finding["quality_score"] is None else f"{finding['quality_score']:.1f}"
        flags = ",".join(finding["quality_flags"]) if finding["quality_flags"] else "-"
        lines.append(
            f"{finding['draft_id']} | {finding['mention_id'] or '-'} | {finding['status']} | {score} | "
            f"{flags} | {','.join(finding['reason_codes'])} | {finding['evaluated_at'] or '-'} | {finding['draft_updated_at'] or '-'}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], statuses: tuple[str, ...]) -> list[dict[str, Any]]:
    draft_table = "reply_drafts" if "reply_drafts" in schema else "reply_queue"
    dcols = schema[draft_table]
    draft_id = _col(dcols, "id", "draft_id")
    selected = [
        f"d.{draft_id} AS draft_id",
        _expr(dcols, "status", "'pending'", "d"),
        _expr(dcols, "mention_id", "NULL", "d", alias="mention_id"),
        _expr(dcols, "inbound_tweet_id", "NULL", "d", alias="inbound_tweet_id"),
        _coalesce(dcols, ("updated_at", "drafted_at", "created_at", "detected_at"), "NULL", "d", alias="draft_updated_at"),
    ]
    eval_table = _evaluation_table(schema)
    joins = ""
    if eval_table:
        ecols = schema[eval_table]
        link = _col(ecols, "reply_draft_id", "draft_id", "reply_queue_id")
        evaluated = _coalesce(ecols, ("evaluated_at", "updated_at", "created_at"), "NULL", "e", alias="evaluated_at")
        selected.extend(
            [
                _expr(ecols, "id", "NULL", "e", alias="evaluation_id"),
                _expr(ecols, "quality_score", "NULL", "e", alias="quality_score"),
                _expr(ecols, "score", "NULL", "e", alias="score"),
                _expr(ecols, "quality_flags", "NULL", "e", alias="quality_flags"),
                _expr(ecols, "flags", "NULL", "e", alias="flags"),
                evaluated,
            ]
        )
        joins = (
            f" LEFT JOIN {eval_table} e ON e.{link} = d.{draft_id}"
            f" AND {evaluated.removesuffix(' AS evaluated_at')} = ("
            f"SELECT MAX({_coalesce(ecols, ('evaluated_at', 'updated_at', 'created_at'), 'NULL', 'e2').removesuffix(' AS evaluated_at')}) "
            f"FROM {eval_table} e2 WHERE e2.{link} = d.{draft_id})"
        )
    else:
        selected.extend(
            [
                "NULL AS evaluation_id",
                _expr(dcols, "quality_score", "NULL", "d", alias="quality_score"),
                "NULL AS score",
                _expr(dcols, "quality_flags", "NULL", "d", alias="quality_flags"),
                "NULL AS flags",
                _coalesce(dcols, ("quality_evaluated_at", "evaluated_at", "updated_at", "detected_at", "created_at"), "NULL", "d", alias="evaluated_at"),
            ]
        )
    placeholders = ", ".join("?" for _ in statuses)
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM {draft_table} d
            {joins}
            WHERE LOWER(COALESCE(d.status, 'pending')) IN ({placeholders})
            ORDER BY d.{draft_id} ASC""",
        statuses,
    ).fetchall()
    return [dict(row) for row in rows]


def _evaluation_table(schema: dict[str, set[str]]) -> str | None:
    for table in ("reply_draft_evaluations", "reply_evaluations", "reply_evaluator_results"):
        if table in schema and schema[table] & {"reply_draft_id", "draft_id", "reply_queue_id"}:
            return table
    return None


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    draft_table = "reply_drafts" if "reply_drafts" in schema else "reply_queue" if "reply_queue" in schema else None
    if draft_table is None:
        return {"missing_tables": ["reply_drafts_or_reply_queue"], "missing_columns": {}}
    missing = sorted({"id"} - schema[draft_table])
    return {"missing_tables": [], "missing_columns": {draft_table: missing} if missing else {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _col(columns: set[str], *names: str) -> str:
    for name in names:
        if name in columns:
            return name
    return names[0]


def _expr(columns: set[str], column: str, default: str, table_alias: str, *, alias: str | None = None) -> str:
    expression = f"{table_alias}.{column}" if column in columns else default
    return f"{expression} AS {alias or column}"


def _coalesce(columns: set[str], names: tuple[str, ...], default: str, table_alias: str, *, alias: str | None = None) -> str:
    expressions = [f"{table_alias}.{name}" for name in names if name in columns]
    expression = expressions[0] if len(expressions) == 1 else f"COALESCE({', '.join(expressions)})" if expressions else default
    return f"{expression} AS {alias or names[0]}"


def _normalize_statuses(statuses: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    values = statuses or DEFAULT_STATUSES
    normalized = tuple(sorted({_clean_status(value) for value in values if _clean_status(value)}))
    if not normalized:
        raise ValueError("at least one status is required")
    return normalized


def _parse_flags(raw: Any) -> list[str]:
    if raw is None or str(raw).strip() == "":
        return []
    if isinstance(raw, (list, tuple)):
        values = raw
    else:
        try:
            values = json.loads(str(raw))
        except json.JSONDecodeError:
            values = [part.strip() for part in str(raw).split(",")]
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, (list, tuple)):
        return []
    return sorted({str(value).strip().lower() for value in values if str(value).strip()})


def _finding_sort_key(finding: dict[str, Any]) -> tuple[int, float, str]:
    severity = 0 if "missing_evaluation" in finding["reason_codes"] else 1 if "stale_evaluation" in finding["reason_codes"] else 2
    score = finding["quality_score"] if finding["quality_score"] is not None else -1.0
    return (severity, score, finding["draft_id"])


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return None if value in (None, "") else float(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _clean_status(value: Any) -> str:
    return _text(value).lower()


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
