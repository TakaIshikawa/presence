"""Group failed publication attempts by stable error fingerprints."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
MAX_FINGERPRINT_LENGTH = 180

_URL_RE = re.compile(r"\b(?:[a-z][a-z0-9+.-]*://|www\.)\S+", re.IGNORECASE)
_ISO_TIMESTAMP_RE = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}"
    r"(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b",
    re.IGNORECASE,
)
_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
_TIME_RE = re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?(?:\.\d+)?\b")
_UUID_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)
_LONG_HEX_RE = re.compile(r"\b[0-9a-f]{12,}\b", re.IGNORECASE)
_LONG_TOKEN_RE = re.compile(r"\b[A-Za-z0-9_-]{20,}\b")
_KEYED_ID_RE = re.compile(
    r"\b(id|ids|queue|attempt|request|trace|tweet|post|record|cid|uri)"
    r"([ #:=/-]+)[A-Za-z0-9_.:-]{3,}\b",
    re.IGNORECASE,
)
_NUMBER_RE = re.compile(r"\b\d+(?:\.\d+)?\b")
_WHITESPACE_RE = re.compile(r"\s+")


def build_publication_error_fingerprints_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a deterministic report from publication attempt-like rows."""
    if days <= 0:
        raise ValueError("days must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    failures = [_normalize_row(row) for row in rows if _in_window(row, cutoff)]
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in failures:
        groups.setdefault((row["platform"], row["fingerprint"]), []).append(row)

    fingerprints = []
    for (platform, fingerprint), group_rows in groups.items():
        ordered = sorted(group_rows, key=lambda item: (item["attempted_at"] or "", item["attempt_id"] or 0))
        seen_at = sorted(item["attempted_at"] for item in group_rows if item["attempted_at"])
        latest = max(ordered, key=lambda item: (item["attempted_at"] or "", item["attempt_id"] or 0))
        fingerprints.append(
            {
                "platform": platform,
                "fingerprint": fingerprint,
                "attempt_count": len(group_rows),
                "affected_content_ids": _ids(item["content_id"] for item in group_rows),
                "first_seen": seen_at[0] if seen_at else None,
                "last_seen": seen_at[-1] if seen_at else None,
                "latest_queue_id": latest["queue_id"],
                "latest_error": latest["error"],
            }
        )

    fingerprints.sort(
        key=lambda item: (
            item["platform"],
            item["fingerprint"],
            item["first_seen"] or "",
            item["latest_queue_id"] or -1,
        )
    )
    return {
        "artifact_type": "publication_error_fingerprints",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "window_start": cutoff.isoformat(),
            "window_end": generated_at.isoformat(),
        },
        "totals": {
            "failed_attempts": len(failures),
            "fingerprint_count": len(fingerprints),
        },
        "fingerprints": fingerprints,
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
        "empty_state": {
            "is_empty": not fingerprints,
            "message": "No failed publication attempts found." if not fingerprints else None,
        },
    }


def build_publication_error_fingerprints_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load failed publication attempts and build a fingerprint report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    rows = _load_rows(conn, schema) if not gaps["missing_tables"] and not gaps["missing_columns"] else []
    return build_publication_error_fingerprints_report(rows, schema_gaps=gaps, **kwargs)


def format_publication_error_fingerprints_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_error_fingerprints_text(report: dict[str, Any]) -> str:
    lines = [
        "Publication Error Fingerprints",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['window_start']} to {report['filters']['window_end']} ({report['filters']['days']} days)",
        f"Totals: failed_attempts={report['totals']['failed_attempts']} fingerprints={report['totals']['fingerprint_count']}",
    ]
    gaps = report.get("schema_gaps", {})
    if gaps.get("missing_tables"):
        lines.append(f"Missing tables: {', '.join(gaps['missing_tables'])}")
    missing_columns = gaps.get("missing_columns") or {}
    if missing_columns:
        lines.append(
            "Missing columns: "
            + "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing_columns.items()))
        )
    if not report["fingerprints"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "platform | attempts | first_seen | last_seen | latest_queue_id | content_ids | fingerprint"])
    for row in report["fingerprints"]:
        lines.append(
            f"{row['platform']} | {row['attempt_count']} | {row['first_seen'] or '-'} | "
            f"{row['last_seen'] or '-'} | {row['latest_queue_id'] or '-'} | "
            f"{','.join(str(value) for value in row['affected_content_ids']) or '-'} | {row['fingerprint']}"
        )
    return "\n".join(lines)


format_publication_error_fingerprints_table = format_publication_error_fingerprints_text


def normalize_publication_error_fingerprint(error: Any) -> str:
    """Remove volatile values so equivalent publication errors collapse together."""
    text = str(error or "").strip().lower()
    if not text:
        return "(empty error)"
    text = _URL_RE.sub("<url>", text)
    text = _ISO_TIMESTAMP_RE.sub("<timestamp>", text)
    text = _DATE_RE.sub("<date>", text)
    text = _TIME_RE.sub("<time>", text)
    text = _UUID_RE.sub("<id>", text)
    text = _KEYED_ID_RE.sub(lambda match: f"{match.group(1).lower()}{match.group(2)}<id>", text)
    text = _LONG_HEX_RE.sub("<id>", text)
    text = _LONG_TOKEN_RE.sub("<id>", text)
    text = _NUMBER_RE.sub("<number>", text)
    text = _WHITESPACE_RE.sub(" ", text).strip(" .")
    if len(text) > MAX_FINGERPRINT_LENGTH:
        text = text[:MAX_FINGERPRINT_LENGTH].rstrip()
    return text or "(empty error)"


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    error = _text(_first(row, "last_error", "error_message", "error"))
    return {
        "attempt_id": _int(_first(row, "attempt_id", "id")),
        "queue_id": _int(row.get("queue_id")),
        "content_id": _int(row.get("content_id")),
        "platform": _text(row.get("platform")) or "unknown",
        "attempted_at": _text(_first(row, "attempted_at", "created_at")),
        "error": error,
        "fingerprint": normalize_publication_error_fingerprint(error),
    }


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema["publication_attempts"]
    select = [
        _select(columns, ("id",), "attempt_id"),
        _select(columns, ("queue_id",), "queue_id"),
        _select(columns, ("content_id",), "content_id"),
        _select(columns, ("platform",), "platform"),
        _select(columns, ("attempted_at", "created_at"), "attempted_at"),
        _select(columns, ("last_error",), "last_error"),
        _select(columns, ("error_message",), "error_message"),
        _select(columns, ("error",), "error"),
    ]
    filters = ["COALESCE(success, 0) = 0"]
    error_columns = [column for column in ("last_error", "error_message", "error") if column in columns]
    if error_columns:
        filters.append("(" + " OR ".join(f"NULLIF(TRIM({column}), '') IS NOT NULL" for column in error_columns) + ")")
    rows = conn.execute(
        f"SELECT {', '.join(select)} FROM publication_attempts WHERE {' AND '.join(filters)} ORDER BY attempted_at ASC, id ASC"
    ).fetchall()
    return [dict(row) for row in rows]


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    required = {"publication_attempts": {"content_id", "platform", "attempted_at", "success"}}
    missing_tables = [table for table in sorted(required) if table not in schema]
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    if "publication_attempts" in schema and not {"last_error", "error_message", "error"} & schema["publication_attempts"]:
        missing_columns.setdefault("publication_attempts", []).append("last_error|error_message|error")
    return {"missing_tables": missing_tables, "missing_columns": missing_columns}


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()}
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for column in candidates:
        if column in columns:
            return f"{column} AS {alias}"
    return f"NULL AS {alias}"


def _in_window(row: dict[str, Any], cutoff: datetime) -> bool:
    attempted_at = _parse_ts(_first(row, "attempted_at", "created_at"))
    return bool(attempted_at and attempted_at >= cutoff)


def _parse_ts(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip() != "":
            return value
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ids(values: Any) -> list[int]:
    return sorted({value for value in values if isinstance(value, int)})
