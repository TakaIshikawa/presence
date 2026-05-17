"""Aggregate generation candidate review outcomes into a rejection matrix."""

from __future__ import annotations

from collections import Counter, defaultdict
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


def build_candidate_rejection_matrix_report(rows: list[dict[str, Any]], *, now: datetime | None = None) -> dict[str, Any]:
    generated_at = _utc(now or datetime.now(timezone.utc))
    groups: dict[tuple[str, str, str, str], Counter[str]] = defaultdict(Counter)
    reviewed = rejected = accepted = 0
    for row in rows:
        outcome = _outcome(_first(row, "review_outcome", "outcome", "status", "decision"))
        reason = _reason(_first(row, "rejection_reason", "reason", "review_reason"), outcome)
        key = (
            _text(_first(row, "prompt_version", "prompt", "prompt_id")) or "unknown",
            _text(_first(row, "content_format", "format", "content_type", "type")) or "unknown",
            reason,
            outcome,
        )
        groups[key]["count"] += 1
        reviewed += 1
        rejected += int(outcome == "rejected")
        accepted += int(outcome == "accepted")
    matrix = []
    for (prompt_version, content_format, rejection_reason, outcome), counts in groups.items():
        count = counts["count"]
        matrix.append(
            {
                "prompt_version": prompt_version,
                "content_format": content_format,
                "rejection_reason": rejection_reason,
                "review_outcome": outcome,
                "count": count,
                "rejection_count": count if outcome == "rejected" else 0,
            }
        )
    matrix.sort(key=lambda item: (-item["rejection_count"], item["prompt_version"], item["content_format"], item["rejection_reason"], item["review_outcome"]))
    return {
        "artifact_type": "candidate_rejection_matrix",
        "generated_at": generated_at.isoformat(),
        "totals": {
            "reviewed_count": reviewed,
            "rejected_count": rejected,
            "accepted_count": accepted,
            "rejection_rate": round(rejected / reviewed, 4) if reviewed else 0.0,
        },
        "matrix": matrix,
        "empty_state": {"is_empty": not matrix, "message": "No reviewed generation candidate rows found." if not matrix else None},
    }


def build_candidate_rejection_matrix_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    return build_candidate_rejection_matrix_report(_load_rows(conn, _schema(conn)), **kwargs)


def format_candidate_rejection_matrix_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_candidate_rejection_matrix_text(report: dict[str, Any]) -> str:
    lines = [
        "Candidate Rejection Matrix",
        f"Generated: {report['generated_at']}",
        (
            f"Totals: reviewed={report['totals']['reviewed_count']} rejected={report['totals']['rejected_count']} "
            f"accepted={report['totals']['accepted_count']} rejection_rate={report['totals']['rejection_rate']:.2f}"
        ),
    ]
    if not report["matrix"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "prompt_version | format | reason | outcome | count"])
    for row in report["matrix"]:
        lines.append(f"{row['prompt_version']} | {row['content_format']} | {row['rejection_reason']} | {row['review_outcome']} | {row['count']}")
    return "\n".join(lines)


format_candidate_rejection_matrix_table = format_candidate_rejection_matrix_text


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("generation_candidates", "content_candidates", "generated_content"):
        if table not in schema:
            continue
        cols = schema[table]
        selected = [
            _col(cols, "prompt_version", "prompt_id", "prompt", default="NULL") + " AS prompt_version",
            _col(cols, "content_format", "format", "content_type", "type", default="NULL") + " AS content_format",
            _col(cols, "rejection_reason", "reason", "review_reason", default="NULL") + " AS rejection_reason",
            _col(cols, "review_outcome", "outcome", "status", "decision", default="NULL") + " AS review_outcome",
        ]
        return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]
    return []


def _outcome(value: Any) -> str:
    text = _text(value).lower()
    if text in {"accepted", "approved", "selected", "published", "success"}:
        return "accepted"
    if text in {"rejected", "declined", "failed", "discarded"}:
        return "rejected"
    return "pending"


def _reason(value: Any, outcome: str) -> str:
    reason = _text(value).lower().replace(" ", "_")
    if reason:
        return reason
    return "none" if outcome != "rejected" else "unspecified"


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _col(columns: set[str], *names: str, default: str = "NULL") -> str:
    for name in names:
        if name in columns:
            return name
    return default


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
