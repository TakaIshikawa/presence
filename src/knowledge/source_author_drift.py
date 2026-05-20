"""Report curated knowledge sources whose author metadata drifted."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse, urlunparse


DEFAULT_DAYS = 90
DEFAULT_LIMIT = 100
URL_COLUMNS = ("canonical_url", "url", "source_url")
AUTHOR_HANDLE_COLUMNS = ("author_handle", "handle")
AUTHOR_NAME_COLUMNS = ("author_name", "author")
TIME_COLUMNS = ("ingested_at", "created_at", "updated_at", "seen_at")


def build_source_author_drift_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        seen = _parse_timestamp(row.get("seen_at"))
        if seen and seen >= cutoff:
            buckets[_key(row)].append(row)
    findings = [_finding(key, vals) for key, vals in buckets.items() if _drift_reasons(vals)]
    findings.sort(key=lambda f: (f["drift_reason"], str(f["source_id_or_url"])))
    shown = findings[:limit]
    return {
        "artifact_type": "source_author_drift",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit},
        "summary": {"row_count": len(rows), "source_count": len(buckets), "finding_count": len(findings), "shown_count": len(shown)},
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {t: sorted(c) for t, c in sorted((missing_columns or {}).items()) if c},
        "empty_state": {"is_empty": not findings, "message": "No source author drift found." if not findings else None},
    }


def build_source_author_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    table = next((name for name in ("knowledge_sources", "curated_sources", "source_ingests", "sources") if name in schema), None)
    if table is None:
        return build_source_author_drift_report([], missing_tables=["knowledge_sources|curated_sources|source_ingests"], **kwargs)
    columns = schema[table]
    missing = []
    if not set(URL_COLUMNS) & columns and "source_id" not in columns and "id" not in columns:
        missing.append("source_id|id|canonical_url|url")
    if not set(TIME_COLUMNS) & columns:
        missing.append("|".join(TIME_COLUMNS))
    if missing:
        return build_source_author_drift_report([], missing_columns={table: missing}, **kwargs)
    days = int(kwargs.get("days", DEFAULT_DAYS))
    now = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    return build_source_author_drift_report(_load_rows(conn, table, columns, now - timedelta(days=days)), **kwargs)


def format_source_author_drift_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_source_author_drift_text(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = [
        "Knowledge Source Author Drift",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} limit={report['filters']['limit']}",
        f"Totals: rows={s['row_count']} sources={s['source_count']} findings={s['finding_count']} shown={s['shown_count']}",
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
        lines.append(f"  - source={f['source_id_or_url']} reason={f['drift_reason']} previous={f['previous_author']} current={f['current_author']}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str], cutoff: datetime) -> list[dict[str, Any]]:
    aliases = {
        "source_id": ("source_id", "id"),
        "url": URL_COLUMNS,
        "author_handle": AUTHOR_HANDLE_COLUMNS,
        "author_name": AUTHOR_NAME_COLUMNS,
        "seen_at": TIME_COLUMNS,
    }
    select = ", ".join(f"{_coalesce(columns, names)} AS {alias}" for alias, names in aliases.items())
    rows = conn.execute(
        f"SELECT {select} FROM {table} WHERE datetime({_coalesce(columns, TIME_COLUMNS)}) >= datetime(?) ORDER BY datetime({_coalesce(columns, TIME_COLUMNS)}) ASC, source_id ASC",
        (cutoff.isoformat(),),
    ).fetchall()
    return [dict(row) for row in rows]


def _finding(key: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda r: (_parse_timestamp(r.get("seen_at")) or datetime.min.replace(tzinfo=timezone.utc), str(r.get("source_id") or "")))
    reasons = _drift_reasons(ordered)
    return {
        "source_id_or_url": ordered[-1].get("source_id") or key,
        "canonical_url": _canonical_url(ordered[-1].get("url")),
        "previous_author": _author(ordered[0]),
        "current_author": _author(ordered[-1]),
        "drift_reason": ",".join(reasons),
        "first_seen": (_parse_timestamp(ordered[0].get("seen_at")) or datetime.min.replace(tzinfo=timezone.utc)).isoformat(),
        "last_seen": (_parse_timestamp(ordered[-1].get("seen_at")) or datetime.min.replace(tzinfo=timezone.utc)).isoformat(),
        "row_count": len(ordered),
    }


def _drift_reasons(rows: list[dict[str, Any]]) -> list[str]:
    authors = {_author_key(r) for r in rows if _author_key(r)}
    missing_recent = any(not _author_key(r) for r in rows[-2:])
    source_ids = {r.get("source_id") for r in rows if r.get("source_id")}
    reasons = []
    if len(authors) > 1:
        reasons.append("changed_author_metadata")
    if missing_recent:
        reasons.append("missing_recent_author_metadata")
    if len(source_ids) > 1 and len(authors) > 1:
        reasons.append("same_url_multiple_authors")
    return reasons


def _key(row: dict[str, Any]) -> str:
    return _canonical_url(row.get("url")) or str(row.get("source_id") or "")


def _author(row: dict[str, Any]) -> dict[str, Any]:
    return {"handle": row.get("author_handle"), "name": row.get("author_name")}


def _author_key(row: dict[str, Any]) -> tuple[str, str] | None:
    handle = _norm(row.get("author_handle"))
    name = _norm(row.get("author_name"))
    return (handle or "", name or "") if handle or name else None


def _canonical_url(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    if not text:
        return None
    parsed = urlparse(text if "://" in text else f"https://{text}")
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/"), "", "", ""))


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
