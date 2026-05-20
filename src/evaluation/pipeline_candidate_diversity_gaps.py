"""Report synthesis pipeline runs with low candidate diversity."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import difflib
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_UNIQUE_FORMATS = 3
DEFAULT_SIMILARITY_THRESHOLD = 0.82
DEFAULT_LIMIT = 100
TIME_COLUMNS = ("created_at", "generated_at", "updated_at")


def build_pipeline_candidate_diversity_gaps_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    min_unique_formats: int = DEFAULT_MIN_UNIQUE_FORMATS,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if days <= 0 or min_unique_formats <= 0 or limit <= 0:
        raise ValueError("days, min_unique_formats, and limit must be positive")
    if not 0 < similarity_threshold <= 1:
        raise ValueError("similarity_threshold must be between 0 and 1")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    buckets: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        created_at = _parse_timestamp(row.get("created_at"))
        if created_at is None or created_at >= cutoff:
            buckets[row.get("pipeline_run_id")].append(row)
    findings = [_finding(run_id, vals, min_unique_formats, similarity_threshold) for run_id, vals in buckets.items()]
    findings = [f for f in findings if f["gap_reasons"]]
    findings.sort(key=lambda f: (-len(f["gap_reasons"]), f["unique_format_count"], str(f["pipeline_run_id"])))
    shown = findings[:limit]
    return {
        "artifact_type": "pipeline_candidate_diversity_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "min_unique_formats": min_unique_formats, "similarity_threshold": similarity_threshold, "limit": limit},
        "summary": {"candidate_count": len(rows), "run_count": len(buckets), "finding_count": len(findings), "shown_count": len(shown)},
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {t: sorted(c) for t, c in sorted((missing_columns or {}).items()) if c},
        "empty_state": {"is_empty": not findings, "message": "No pipeline candidate diversity gaps found." if not findings else None},
    }


def build_pipeline_candidate_diversity_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    table = next((name for name in ("pipeline_candidates", "synthesis_candidates", "generated_candidates") if name in schema), None)
    if table is None:
        return build_pipeline_candidate_diversity_gaps_report([], missing_tables=["pipeline_candidates|synthesis_candidates|generated_candidates"], **kwargs)
    columns = schema[table]
    missing = []
    if "pipeline_run_id" not in columns and "run_id" not in columns:
        missing.append("pipeline_run_id|run_id")
    if "format" not in columns and "candidate_format" not in columns:
        missing.append("format|candidate_format")
    if not set(TIME_COLUMNS) & columns:
        missing.append("|".join(TIME_COLUMNS))
    if missing:
        return build_pipeline_candidate_diversity_gaps_report([], missing_columns={table: missing}, **kwargs)
    return build_pipeline_candidate_diversity_gaps_report(_load_rows(conn, table, columns), **kwargs)


def format_pipeline_candidate_diversity_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_pipeline_candidate_diversity_gaps_text(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = [
        "Pipeline Candidate Diversity Gaps",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} min_unique_formats={report['filters']['min_unique_formats']} similarity_threshold={report['filters']['similarity_threshold']} limit={report['filters']['limit']}",
        f"Totals: candidates={s['candidate_count']} runs={s['run_count']} findings={s['finding_count']} shown={s['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("Findings:")
    for f in report["findings"]:
        lines.append(f"  - run={f['pipeline_run_id']} candidates={f['candidate_count']} unique_formats={f['unique_format_count']} reasons={','.join(f['gap_reasons'])}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str]) -> list[dict[str, Any]]:
    aliases = {
        "pipeline_run_id": ("pipeline_run_id", "run_id"),
        "candidate_id": ("id", "candidate_id"),
        "format": ("format", "candidate_format"),
        "hook": ("hook", "candidate_hook"),
        "text": ("text", "body", "content"),
        "created_at": TIME_COLUMNS,
    }
    select = ", ".join(f"{_coalesce(columns, names)} AS {alias}" for alias, names in aliases.items())
    rows = conn.execute(f"SELECT {select} FROM {table} ORDER BY datetime({_coalesce(columns, TIME_COLUMNS)}) DESC").fetchall()
    return [dict(row) for row in rows]


def _finding(run_id: Any, rows: list[dict[str, Any]], min_unique_formats: int, threshold: float) -> dict[str, Any]:
    formats = [_norm(row.get("format")) or "unknown" for row in rows]
    openings = [_opening(row.get("hook") or row.get("text")) for row in rows]
    duplicate_openings = _duplicate_openings(openings, threshold)
    reasons = []
    if len(set(formats)) < min_unique_formats:
        reasons.append("fewer_unique_candidate_formats_than_threshold")
    if any(count > 1 for count in Counter(formats).values()):
        reasons.append("repeated_formats")
    if duplicate_openings:
        reasons.append("near_identical_opening_clauses")
    return {
        "pipeline_run_id": run_id,
        "candidate_count": len(rows),
        "unique_format_count": len(set(formats)),
        "format_counts": dict(sorted(Counter(formats).items())),
        "duplicate_opening_examples": duplicate_openings[:3],
        "gap_reasons": reasons,
    }


def _duplicate_openings(openings: list[str], threshold: float) -> list[dict[str, Any]]:
    out = []
    for i, left in enumerate(openings):
        for right in openings[i + 1 :]:
            if left and right:
                score = difflib.SequenceMatcher(None, left, right).ratio()
                if score >= threshold:
                    out.append({"left": left, "right": right, "similarity": round(score, 3)})
    return out


def _opening(value: Any) -> str:
    text = re.sub(r"\s+", " ", "" if value is None else str(value).strip().lower())
    return " ".join(text.split()[:8])


def _coalesce(columns: set[str], names: tuple[str, ...]) -> str:
    available = [name for name in names if name in columns]
    return "COALESCE(" + ", ".join(available) + ")" if len(available) > 1 else (available[0] if available else "NULL")


def _norm(value: Any) -> str | None:
    text = "" if value is None else str(value).strip().lower()
    return text or None


def _parse_timestamp(value: Any) -> datetime | None:
    text = "" if value is None else str(value).strip()
    if not text:
        return None
    try:
        return _utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
