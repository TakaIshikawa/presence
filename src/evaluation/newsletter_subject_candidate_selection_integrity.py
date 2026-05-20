"""Audit newsletter subject candidate selection integrity."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_SCORE_THRESHOLD = 0.0
REQUIRED_COLUMNS = {"id", "selected", "rank", "score"}
OPTIONAL_COLUMNS = ("newsletter_send_id", "issue_id", "week_start", "week_end", "subject", "created_at")


def build_newsletter_subject_candidate_selection_integrity_report(
    db_or_conn: Any,
    *,
    score_threshold: float = DEFAULT_SCORE_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a subject candidate selection integrity report."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get("newsletter_subject_candidates")
    if columns is None:
        return _report(generated_at, score_threshold, limit, [], 0, ["newsletter_subject_candidates"], {})
    missing = sorted(REQUIRED_COLUMNS - columns)
    if missing:
        return _report(generated_at, score_threshold, limit, [], 0, [], {"newsletter_subject_candidates": missing})

    rows = _load_candidates(conn, columns)
    groups = _group_candidates(rows)
    items: list[dict[str, Any]] = []
    for group_key, candidates in groups.items():
        selected = [row for row in candidates if _truthy(row.get("selected"))]
        base = _group_base(group_key, candidates)
        if not selected:
            items.append(_item(base, None, "no_selected_candidate"))
            continue
        if len(selected) > 1:
            items.append(_item(base, None, "multiple_selected_candidates", selected_count=len(selected)))
        for row in selected:
            if _as_float(row.get("rank")) != 1.0:
                items.append(_item(base, row, "selected_not_top_ranked"))
            score = _as_float(row.get("score"))
            if score is not None and score < score_threshold:
                items.append(_item(base, row, "selected_below_threshold"))

    items.sort(key=lambda item: (_gap_rank(item["gap_type"]), _clean(item["group_key"]), _int_or_text(item.get("candidate_id"))))
    return _report(generated_at, score_threshold, limit, items, len(groups), [], {})


def format_newsletter_subject_candidate_selection_integrity_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_subject_candidate_selection_integrity_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Newsletter Subject Candidate Selection Integrity",
        f"Generated: {report['generated_at']}",
        f"Score threshold: {report['filters']['score_threshold']}",
        f"Limit: {report['filters']['limit']}",
        f"Totals: groups={summary['group_count']} gaps={summary['gap_count']} shown={summary['shown_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["items"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "group_key | candidate_id | issue_id | rank | score | gap_type"])
    for item in report["items"]:
        lines.append(
            f"{item['group_key']} | {item['candidate_id'] or '-'} | {item['issue_id'] or '-'} | "
            f"{item['rank'] if item['rank'] is not None else '-'} | "
            f"{item['score'] if item['score'] is not None else '-'} | {item['gap_type']}"
        )
    return "\n".join(lines)


def _load_candidates(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select_parts = ["id AS candidate_id", "selected", "rank", "score"]
    for column in OPTIONAL_COLUMNS:
        select_parts.append(f"{column}" if column in columns else f"NULL AS {column}")
    rows = conn.execute(
        f"""SELECT {', '.join(select_parts)}
            FROM newsletter_subject_candidates
            ORDER BY COALESCE(created_at, ''), id"""
    ).fetchall()
    return [dict(row) for row in rows]


def _group_candidates(rows: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        group_key = _group_key(row)
        groups.setdefault(group_key, []).append(row)
    return groups


def _group_key(row: dict[str, Any]) -> tuple[str, str]:
    if _clean(row.get("newsletter_send_id")):
        return ("newsletter_send_id", _clean(row.get("newsletter_send_id")))
    if _clean(row.get("issue_id")):
        return ("issue_id", _clean(row.get("issue_id")))
    week_start = _clean(row.get("week_start"))
    week_end = _clean(row.get("week_end"))
    if week_start or week_end:
        return ("week_window", f"{week_start}..{week_end}")
    return ("candidate", _clean(row.get("candidate_id")))


def _group_base(group_key: tuple[str, str], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    first = candidates[0]
    return {
        "group_type": group_key[0],
        "group_key": group_key[1],
        "newsletter_send_id": first.get("newsletter_send_id"),
        "issue_id": first.get("issue_id"),
        "week_start": first.get("week_start"),
        "week_end": first.get("week_end"),
        "candidate_count": len(candidates),
    }


def _item(base: dict[str, Any], row: dict[str, Any] | None, gap_type: str, *, selected_count: int | None = None) -> dict[str, Any]:
    return {
        **base,
        "candidate_id": (row or {}).get("candidate_id"),
        "subject": (row or {}).get("subject"),
        "rank": (row or {}).get("rank"),
        "score": (row or {}).get("score"),
        "selected_count": selected_count,
        "gap_type": gap_type,
    }


def _report(
    generated_at: datetime,
    score_threshold: float,
    limit: int,
    items: list[dict[str, Any]],
    group_count: int,
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    shown = items[:limit]
    return {
        "artifact_type": "newsletter_subject_candidate_selection_integrity",
        "generated_at": generated_at.isoformat(),
        "filters": {"score_threshold": score_threshold, "limit": limit},
        "summary": {
            "group_count": group_count,
            "gap_count": len(items),
            "shown_count": len(shown),
            "by_gap_type": _counts(items, "gap_type"),
        },
        "items": shown,
        "missing_tables": sorted(missing_tables),
        "missing_columns": {table: sorted(columns) for table, columns in sorted(missing_columns.items())},
        "empty_state": {
            "is_empty": not items,
            "message": "No newsletter subject candidate selection integrity gaps found." if not items else None,
        },
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _truthy(value: Any) -> bool:
    return _clean(value).lower() in {"1", "true", "yes", "selected"}


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _counts(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = _clean(item.get(key))
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _gap_rank(gap_type: str) -> int:
    return {
        "no_selected_candidate": 0,
        "multiple_selected_candidates": 1,
        "selected_not_top_ranked": 2,
        "selected_below_threshold": 3,
    }.get(gap_type, 99)


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_text(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, "" if value is None else str(value))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
