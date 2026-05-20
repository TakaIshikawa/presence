"""Report publish queue entries that appear stuck beyond recovery."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 7
DEFAULT_MIN_FAILED_ATTEMPTS = 3
DEFAULT_LIMIT = 100
ARTIFACT_TYPE = "publish_queue_dead_letter_candidates"
TARGET_STATUSES = {"queued", "held", "failed"}


def build_publish_queue_dead_letter_candidates_report(
    queue_rows: list[dict[str, Any]],
    attempt_rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    min_failed_attempts: int = DEFAULT_MIN_FAILED_ATTEMPTS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic dead-letter candidate report from row dicts."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_failed_attempts <= 0:
        raise ValueError("min_failed_attempts must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    failures = _failure_index(attempt_rows)
    candidates = []
    for row in queue_rows:
        status = _clean(row.get("status"), "unknown").lower()
        if status not in TARGET_STATUSES:
            continue
        reference_at = _parse_dt(row.get("scheduled_at") or row.get("updated_at") or row.get("created_at"))
        if reference_at is None or reference_at > cutoff:
            continue
        queue_id = _int_or_none(row.get("queue_id") or row.get("id"))
        content_id = _int_or_none(row.get("content_id") or row.get("generated_content_id"))
        queue_failures = failures["queue"].get(queue_id, []) if queue_id is not None else []
        content_failures = failures["content"].get(content_id, []) if content_id is not None else []
        repeated = len(queue_failures) >= min_failed_attempts or len(content_failures) >= min_failed_attempts
        candidates.append(
            {
                "queue_id": queue_id,
                "content_id": content_id,
                "platform": _clean(row.get("platform") or row.get("channel"), "unknown").lower(),
                "status": status,
                "scheduled_at": _iso(_parse_dt(row.get("scheduled_at"))),
                "created_at": _iso(_parse_dt(row.get("created_at"))),
                "updated_at": _iso(_parse_dt(row.get("updated_at"))),
                "age_days": int((generated_at - reference_at).total_seconds() // 86400),
                "reference_at": reference_at.isoformat(),
                "failed_attempt_count": max(len(queue_failures), len(content_failures)),
                "repeated_failure_evidence": repeated,
                "latest_failure_at": _latest_failure_at([*queue_failures, *content_failures]),
                "latest_error": _latest_error([*queue_failures, *content_failures]),
            }
        )

    candidates.sort(key=lambda item: (item["reference_at"], item["platform"], item["status"], item["queue_id"] or 0, item["content_id"] or 0))
    shown = candidates[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "min_failed_attempts": min_failed_attempts, "limit": limit},
        "summary": {
            "queue_rows_scanned": len(queue_rows),
            "attempt_rows_scanned": len(attempt_rows),
            "candidate_count": len(candidates),
            "shown_count": len(shown),
            "repeated_failure_candidate_count": sum(1 for item in candidates if item["repeated_failure_evidence"]),
            "by_platform": dict(sorted(Counter(item["platform"] for item in candidates).items())),
            "by_status": dict(sorted(Counter(item["status"] for item in candidates).items())),
            "by_platform_status": _platform_status_counts(candidates),
            "oldest_scheduled_at": min((item["scheduled_at"] for item in candidates if item["scheduled_at"]), default=None),
            "example_queue_ids": [item["queue_id"] for item in shown[:5] if item["queue_id"] is not None],
            "example_content_ids": [item["content_id"] for item in shown[:5] if item["content_id"] is not None],
        },
        "candidates": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
    }


def build_publish_queue_dead_letter_candidates_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load publish queue and attempt rows from SQLite and build the report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [table for table in ("publication_attempts", "publish_queue") if table not in schema]
    missing_columns = _missing_columns(schema)
    queue_rows = _load_queue(conn, schema["publish_queue"]) if "publish_queue" in schema and "status" in schema["publish_queue"] else []
    attempt_rows = _load_attempts(conn, schema["publication_attempts"]) if "publication_attempts" in schema else []
    return build_publish_queue_dead_letter_candidates_report(queue_rows, attempt_rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_publish_queue_dead_letter_candidates_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publish_queue_dead_letter_candidates_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    filters = report["filters"]
    lines = [
        "Publish Queue Dead Letter Candidates",
        f"Generated: {report['generated_at']}",
        f"Filters: days={filters['days']} min_failed_attempts={filters['min_failed_attempts']} limit={filters['limit']}",
        f"Totals: candidates={summary['candidate_count']} shown={summary['shown_count']} repeated_failures={summary['repeated_failure_candidate_count']} oldest_scheduled_at={summary['oldest_scheduled_at'] or '-'}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["candidates"]:
        lines.append("No publish queue dead-letter candidates found.")
        return "\n".join(lines)
    lines.extend(["", "queue_id | content_id | platform | status | scheduled_at | age_days | failed_attempts | repeated"])
    for item in report["candidates"]:
        lines.append(
            f"{_display(item['queue_id'])} | {_display(item['content_id'])} | {item['platform']} | {item['status']} | "
            f"{_display(item['scheduled_at'])} | {item['age_days']} | {item['failed_attempt_count']} | {str(item['repeated_failure_evidence']).lower()}"
        )
    return "\n".join(lines)


def _load_queue(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        _expr(columns, "id", "queue_id", "rowid"),
        _expr(columns, "content_id", "content_id", "NULL"),
        _expr(columns, "generated_content_id", "generated_content_id", "NULL"),
        _expr(columns, "platform", "platform", "'unknown'"),
        _expr(columns, "channel", "channel", "NULL"),
        _expr(columns, "status", "status", "'unknown'"),
        _expr(columns, "scheduled_at", "scheduled_at", "NULL"),
        _expr(columns, "created_at", "created_at", "NULL"),
        _expr(columns, "updated_at", "updated_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM publish_queue ORDER BY rowid")]


def _load_attempts(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        _expr(columns, "id", "attempt_id", "rowid"),
        _expr(columns, "queue_id", "queue_id", "NULL"),
        _expr(columns, "content_id", "content_id", "NULL"),
        _expr(columns, "status", "status", "NULL"),
        _expr(columns, "success", "success", "NULL"),
        _expr(columns, "attempted_at", "attempted_at", "NULL"),
        _expr(columns, "created_at", "created_at", "NULL"),
        _expr(columns, "error", "error", "NULL"),
        _expr(columns, "error_message", "error_message", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM publication_attempts ORDER BY rowid")]


def _failure_index(rows: list[dict[str, Any]]) -> dict[str, dict[int, list[dict[str, Any]]]]:
    indexed: dict[str, dict[int, list[dict[str, Any]]]] = {"queue": defaultdict(list), "content": defaultdict(list)}
    for row in rows:
        if not _failed(row):
            continue
        queue_id = _int_or_none(row.get("queue_id"))
        content_id = _int_or_none(row.get("content_id"))
        if queue_id is not None:
            indexed["queue"][queue_id].append(row)
        if content_id is not None:
            indexed["content"][content_id].append(row)
    return indexed


def _failed(row: dict[str, Any]) -> bool:
    success = row.get("success")
    if success is not None and str(success).strip().lower() in {"0", "false", "no"}:
        return True
    return _clean(row.get("status")).lower() in {"failed", "error", "failure"}


def _latest_failure_at(rows: list[dict[str, Any]]) -> str | None:
    return max((_iso(_parse_dt(row.get("attempted_at") or row.get("created_at"))) for row in rows), default=None)


def _latest_error(rows: list[dict[str, Any]]) -> str | None:
    ordered = sorted(rows, key=lambda row: _parse_dt(row.get("attempted_at") or row.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc))
    for row in reversed(ordered):
        text = _clean(row.get("error") or row.get("error_message"))
        if text:
            return text
    return None


def _platform_status_counts(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((item["platform"], item["status"]) for item in candidates)
    return [{"platform": platform, "status": status, "count": count} for (platform, status), count in sorted(counts.items())]


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    missing: dict[str, list[str]] = {}
    if "publish_queue" in schema:
        needed = {"status", "scheduled_at"}
        gap = sorted(needed - schema["publish_queue"])
        if gap:
            missing["publish_queue"] = gap
    if "publication_attempts" in schema and not ({"queue_id", "content_id"} & schema["publication_attempts"]):
        missing["publication_attempts"] = ["queue_id|content_id"]
    return missing


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row["name"]): {str(column["name"]) for column in conn.execute(f"PRAGMA table_info({_quote(str(row['name']))})")} for row in rows}


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{_quote(column)} AS {output}" if column in columns else f"{default} AS {output}"


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _display(value: Any) -> str:
    return "-" if value is None or value == "" else str(value)


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
