"""Detect recurring publication attempt failures by normalized signature."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_MIN_COUNT = 2
DEFAULT_LIMIT = 50

_URL_RE = re.compile(r"\b(?:[a-z][a-z0-9+.-]*://|www\.)\S+", re.IGNORECASE)
_UUID_RE = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b", re.IGNORECASE)
_TIMESTAMP_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b", re.IGNORECASE)
_QUOTED_RE = re.compile(r"(['\"])(?:(?=(\\?))\2.)*?\1")
_NUMBER_RE = re.compile(r"\b\d+\b")
_SPACE_RE = re.compile(r"\s+")
FAILED_STATUSES = {"failed", "failure", "error", "errored", "rejected", "timeout"}


def build_publication_attempt_error_recurrence_report(
    attempts: list[dict[str, Any]],
    *,
    min_count: int = DEFAULT_MIN_COUNT,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if min_count <= 0:
        raise ValueError("min_count must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    failures = [_failure(row) for row in attempts if _is_failed(row)]
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for failure in failures:
        grouped[(failure["content_id"], failure["platform"], failure["error_signature"])].append(failure)

    recurring = []
    for (content_id, platform, signature), rows in grouped.items():
        if len(rows) < min_count:
            continue
        rows.sort(key=lambda row: row["attempted_at"] or "")
        recurring.append(
            {
                "content_id": content_id,
                "platform": platform,
                "error_signature": signature,
                "recurrence_count": len(rows),
                "first_attempted_at": rows[0]["attempted_at"],
                "last_attempted_at": rows[-1]["attempted_at"],
                "latest_examples": list(reversed(rows[-3:])),
            }
        )
    recurring.sort(key=lambda item: (-item["recurrence_count"], item["platform"], item["content_id"], item["error_signature"]))
    platform_counts = Counter(row["platform"] for row in failures)
    return {
        "artifact_type": "publication_attempt_error_recurrence",
        "generated_at": generated_at.isoformat(),
        "thresholds": {"min_count": min_count, "limit": limit},
        "recurring_errors": recurring[:limit],
        "platform_summary": {
            platform: {
                "failed_attempts": count,
                "recurring_error_groups": sum(1 for item in recurring if item["platform"] == platform),
            }
            for platform, count in sorted(platform_counts.items())
        },
        "normalization_metadata": {
            "normalizes": ["urls", "uuids", "timestamps", "numeric_ids", "quoted_payload_fragments"],
            "failed_statuses": sorted(FAILED_STATUSES),
        },
        "missing_schema": missing_schema or {"missing_tables": [], "missing_columns": {}},
    }


def build_publication_attempt_error_recurrence_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_schema = _missing_schema(schema)
    rows = _load(conn, schema["publication_attempts"]) if "publication_attempts" in schema else []
    return build_publication_attempt_error_recurrence_report(rows, missing_schema=missing_schema, **kwargs)


def format_publication_attempt_error_recurrence_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_error_recurrence_text(report: dict[str, Any]) -> str:
    lines = ["Publication Attempt Error Recurrence", f"Generated: {report['generated_at']}"]
    if report["missing_schema"]["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_schema"]["missing_tables"]))
    lines.append(f"Recurring errors: {len(report['recurring_errors'])}")
    for item in report["recurring_errors"]:
        lines.append(
            f"  - content_id={item['content_id']} platform={item['platform']} "
            f"count={item['recurrence_count']} signature={item['error_signature']}"
        )
    return "\n".join(lines)


def normalize_publication_attempt_error(error: Any) -> str:
    text = str(error or "").strip().lower()
    if not text:
        return "(empty error)"
    text = _URL_RE.sub("<url>", text)
    text = _TIMESTAMP_RE.sub("<timestamp>", text)
    text = _UUID_RE.sub("<id>", text)
    text = _QUOTED_RE.sub("<quoted>", text)
    text = _NUMBER_RE.sub("<id>", text)
    text = _SPACE_RE.sub(" ", text).strip(" .")
    return text or "(empty error)"


def _failure(row: dict[str, Any]) -> dict[str, Any]:
    error = _first(row, "error", "error_message", "message", "last_error", "response_body")
    return {
        "attempt_id": _first(row, "attempt_id", "id"),
        "content_id": _clean(_first(row, "content_id", "generated_content_id")) or "unknown",
        "platform": _clean(_first(row, "platform", "channel")) or "unknown",
        "status": _clean(_first(row, "status", "state")).lower(),
        "attempted_at": _first(row, "attempted_at", "created_at", "updated_at"),
        "error": _clean(error),
        "error_signature": normalize_publication_attempt_error(error),
    }


def _is_failed(row: dict[str, Any]) -> bool:
    status = _clean(_first(row, "status", "state")).lower()
    if status in {"success", "succeeded", "published", "ok", "complete", "completed"}:
        return False
    return status in FAILED_STATUSES or bool(_first(row, "error", "error_message", "last_error"))


def _load(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = ", ".join(f"{column} AS {column}" for column in sorted(columns)) or "rowid"
    order = "id" if "id" in columns else "rowid"
    return [dict(row) for row in conn.execute(f"SELECT {select} FROM publication_attempts ORDER BY {order}").fetchall()]


def _missing_schema(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "publication_attempts" not in schema:
        return {"missing_tables": ["publication_attempts"], "missing_columns": {}}
    required_any = {"error", "error_message", "message", "last_error", "response_body"}
    missing_columns = {}
    if not required_any & schema["publication_attempts"]:
        missing_columns["publication_attempts"] = sorted(required_any)
    return {"missing_tables": [], "missing_columns": missing_columns}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
    return {table: {column[1] for column in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()
