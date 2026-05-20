"""Report weak or aging commit-to-prompt links."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_MIN_CONFIDENCE = 0.5
DEFAULT_MAX_GAP_HOURS = 2.0

REQUIRED_COLUMNS = {
    "commit_prompt_links": {"id", "commit_id", "message_id", "confidence"},
    "github_commits": {"id", "commit_sha", "timestamp"},
    "claude_messages": {"id", "message_uuid", "timestamp"},
}

ISSUE_ORDER = {
    "low_confidence": 0,
    "missing_commit": 1,
    "missing_message": 2,
    "timestamp_gap_exceeded": 3,
}


def build_commit_prompt_link_confidence_decay_report(
    link_rows: list[dict[str, Any]],
    *,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    max_gap_hours: float = DEFAULT_MAX_GAP_HOURS,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic report from preloaded commit_prompt_links rows."""
    if min_confidence < 0 or min_confidence > 1:
        raise ValueError("min_confidence must be between 0 and 1")
    if max_gap_hours < 0:
        raise ValueError("max_gap_hours must be non-negative")

    generated_at = _utc(now or datetime.now(timezone.utc))
    findings = [
        finding
        for row in link_rows
        for finding in _findings_for_link(
            row,
            min_confidence=min_confidence,
            max_gap_hours=max_gap_hours,
        )
    ]
    findings.sort(key=_finding_sort_key)
    issue_counts = Counter(finding["issue_type"] for finding in findings)
    return {
        "artifact_type": "commit_prompt_link_confidence_decay",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "min_confidence": min_confidence,
            "max_gap_hours": max_gap_hours,
        },
        "totals": {
            "links_scanned": len(link_rows),
            "finding_count": len(findings),
            "low_confidence": issue_counts.get("low_confidence", 0),
            "missing_commit": issue_counts.get("missing_commit", 0),
            "missing_message": issue_counts.get("missing_message", 0),
            "timestamp_gap_exceeded": issue_counts.get("timestamp_gap_exceeded", 0),
        },
        "findings": findings,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
            if columns
        },
    }


def build_commit_prompt_link_confidence_decay_report_from_db(
    db_or_conn: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = sorted(table for table in REQUIRED_COLUMNS if table not in schema)
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in REQUIRED_COLUMNS.items()
        if table in schema and columns - schema.get(table, set())
    }
    if missing_tables or missing_columns:
        return build_commit_prompt_link_confidence_decay_report(
            [],
            missing_tables=missing_tables,
            missing_columns=missing_columns,
            **kwargs,
        )
    return build_commit_prompt_link_confidence_decay_report(
        _load_link_rows(conn),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_commit_prompt_link_confidence_decay_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_commit_prompt_link_confidence_decay_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    filters = report["filters"]
    lines = [
        "Commit Prompt Link Confidence Decay",
        f"Generated: {report['generated_at']}",
        (
            "Thresholds: "
            f"min_confidence={filters['min_confidence']:g} "
            f"max_gap_hours={filters['max_gap_hours']:g}"
        ),
        (
            "Totals: "
            f"links_scanned={totals['links_scanned']} "
            f"findings={totals['finding_count']} "
            f"low_confidence={totals['low_confidence']} "
            f"missing_commit={totals['missing_commit']} "
            f"missing_message={totals['missing_message']} "
            f"timestamp_gap_exceeded={totals['timestamp_gap_exceeded']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in report["missing_columns"].items()
                for column in columns
            )
        )
    if not report["findings"]:
        lines.append("No weak or aging commit prompt links found.")
        return "\n".join(lines)

    lines.extend(["", "Findings:"])
    for finding in report["findings"]:
        lines.append(
            f"- {finding['issue_type']} link_id={finding['link_id']} "
            f"commit_id={finding['commit_id']} message_id={finding['message_id']} "
            f"confidence={_format_optional_float(finding['confidence'])} "
            f"gap_hours={_format_optional_float(finding['timestamp_gap_hours'])}"
        )
        lines.append(
            "  "
            f"commit_sha={finding['commit_sha'] or '-'} "
            f"message_uuid={finding['message_uuid'] or '-'} "
            f"detail={finding['detail']}"
        )
    return "\n".join(lines)


def _load_link_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT
              cpl.id AS link_id,
              cpl.commit_id,
              cpl.message_id,
              cpl.confidence,
              gc.commit_sha,
              gc.timestamp AS commit_timestamp,
              cm.message_uuid,
              cm.timestamp AS message_timestamp
           FROM commit_prompt_links cpl
           LEFT JOIN github_commits gc ON gc.id = cpl.commit_id
           LEFT JOIN claude_messages cm ON cm.id = cpl.message_id
           ORDER BY cpl.id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _findings_for_link(
    row: dict[str, Any],
    *,
    min_confidence: float,
    max_gap_hours: float,
) -> list[dict[str, Any]]:
    confidence = _optional_float(row.get("confidence"))
    gap_hours = _gap_hours(row.get("commit_timestamp"), row.get("message_timestamp"))
    base = {
        "link_id": _first(row, "link_id", "id"),
        "commit_id": row.get("commit_id"),
        "message_id": row.get("message_id"),
        "commit_sha": row.get("commit_sha"),
        "message_uuid": row.get("message_uuid"),
        "confidence": confidence,
        "commit_timestamp": row.get("commit_timestamp"),
        "message_timestamp": row.get("message_timestamp"),
        "timestamp_gap_hours": gap_hours,
    }
    findings: list[dict[str, Any]] = []
    if confidence is None or confidence < min_confidence:
        findings.append(
            {
                **base,
                "issue_type": "low_confidence",
                "detail": (
                    "missing confidence"
                    if confidence is None
                    else f"confidence {confidence:g} below threshold {min_confidence:g}"
                ),
            }
        )
    if row.get("commit_sha") is None:
        findings.append({**base, "issue_type": "missing_commit", "detail": "linked github_commits row is missing"})
    if row.get("message_uuid") is None:
        findings.append({**base, "issue_type": "missing_message", "detail": "linked claude_messages row is missing"})
    if gap_hours is not None and gap_hours > max_gap_hours:
        findings.append(
            {
                **base,
                "issue_type": "timestamp_gap_exceeded",
                "detail": f"timestamp gap {gap_hours:g}h exceeds {max_gap_hours:g}h",
            }
        )
    return findings


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    gap = finding.get("timestamp_gap_hours")
    return (
        ISSUE_ORDER.get(str(finding["issue_type"]), 99),
        -(float(gap) if gap is not None else -1.0),
        _int_or_text(finding.get("link_id")),
    )


def _gap_hours(left: Any, right: Any) -> float | None:
    commit_at = _parse_timestamp(left)
    message_at = _parse_timestamp(right)
    if commit_at is None or message_at is None:
        return None
    return round(abs((commit_at - message_at).total_seconds()) / 3600, 2)


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    return None


def _int_or_text(value: Any) -> int | str:
    try:
        return int(value)
    except (TypeError, ValueError):
        return str(value or "")


def _format_optional_float(value: float | None) -> str:
    return "n/a" if value is None else f"{value:g}"
