"""Analyze generated content that missed publication scheduling windows."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
DEFAULT_WINDOW_MINUTES = 60


def build_publication_schedule_miss_analysis_report(
    content_rows: list[dict[str, Any]],
    attempt_rows: list[dict[str, Any]] | None = None,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if window_minutes < 0:
        raise ValueError("window_minutes must be non-negative")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    attempts_by_content: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in attempt_rows or []:
        attempts_by_content[_text(row.get("content_id") or row.get("generated_content_id"))].append(row)
    findings = []
    scanned = 0
    for row in content_rows:
        intended = _parse_dt(row.get("scheduled_at") or row.get("intended_publish_at") or row.get("approved_at"))
        if intended and intended < cutoff:
            continue
        content_id = _text(row.get("id"))
        attempts = sorted(attempts_by_content.get(content_id, []), key=lambda item: _parse_dt(item.get("attempted_at") or item.get("created_at")) or datetime.max.replace(tzinfo=timezone.utc))
        first_attempt = _parse_dt(attempts[0].get("attempted_at") or attempts[0].get("created_at")) if attempts else None
        published = _parse_dt(row.get("published_at")) or next((_parse_dt(item.get("published_at")) for item in attempts if _parse_dt(item.get("published_at"))), None)
        scanned += 1
        reason = None
        lateness = 0
        if not attempts and not published:
            reason = "missing_attempt"
            lateness = _minutes_after(intended, generated_at)
        elif first_attempt and intended and first_attempt > intended + timedelta(minutes=window_minutes):
            reason = "late_attempt"
            lateness = _minutes_after(intended, first_attempt)
        elif published and intended and published > intended + timedelta(minutes=window_minutes):
            reason = "late_publication"
            lateness = _minutes_after(intended, published)
        elif attempts and not published and not _success(attempts):
            reason = "unpublished_after_attempt"
            lateness = _minutes_after(intended, generated_at)
        if reason:
            findings.append(
                {
                    "content_id": content_id,
                    "content_type": _text(row.get("content_type")),
                    "intended_at": intended.isoformat() if intended else None,
                    "first_attempt_at": first_attempt.isoformat() if first_attempt else None,
                    "published_at": published.isoformat() if published else None,
                    "lateness_minutes": lateness,
                    "reason_code": reason,
                }
            )
    findings.sort(key=lambda item: (-item["lateness_minutes"], item["content_id"]))
    counts = Counter(item["reason_code"] for item in findings)
    return {
        "artifact_type": "publication_schedule_miss_analysis",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit, "window_minutes": window_minutes, "lookback_start": cutoff.isoformat()},
        "summary": {
            "scanned": scanned,
            "on_time": max(scanned - len(findings), 0),
            "late": counts["late_attempt"] + counts["late_publication"],
            "missing_attempt": counts["missing_attempt"],
            "unpublished_after_attempt": counts["unpublished_after_attempt"],
            "reason_counts": dict(sorted(counts.items())),
        },
        "findings": findings[:limit],
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def build_publication_schedule_miss_analysis_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    content_rows = _load_content(conn, schema) if not gaps["missing_tables"] else []
    attempt_rows = _load_attempts(conn, schema)
    return build_publication_schedule_miss_analysis_report(content_rows, attempt_rows, schema_gaps=gaps, **kwargs)


def format_publication_schedule_miss_analysis_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_schedule_miss_analysis_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Publication Schedule Miss Analysis",
        f"Generated: {report['generated_at']}",
        f"Totals: scanned={summary['scanned']} on_time={summary['on_time']} late={summary['late']} missing_attempt={summary['missing_attempt']} unpublished_after_attempt={summary['unpublished_after_attempt']}",
    ]
    if not report["findings"]:
        lines.extend(["", "No publication schedule misses found."])
        return "\n".join(lines)
    lines.extend(["", "Findings:"])
    for item in report["findings"]:
        lines.append(
            f"  - content={item['content_id']} type={item['content_type'] or '-'} reason={item['reason_code']} lateness_minutes={item['lateness_minutes']}"
        )
    return "\n".join(lines)


def _load_content(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    select = [
        _select(columns, ("id",), "id"),
        _select(columns, ("content_type", "type"), "content_type"),
        _select(columns, ("scheduled_at", "intended_publish_at", "approved_at", "created_at"), "scheduled_at"),
        _select(columns, ("approved_at",), "approved_at"),
        _select(columns, ("published_at",), "published_at"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM generated_content").fetchall()]


def _load_attempts(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "publication_attempts" if "publication_attempts" in schema else "publish_attempts" if "publish_attempts" in schema else ""
    if not table:
        return []
    columns = schema[table]
    select = [
        _select(columns, ("content_id", "generated_content_id"), "content_id"),
        _select(columns, ("attempted_at", "created_at"), "attempted_at"),
        _select(columns, ("published_at",), "published_at"),
        _select(columns, ("status", "outcome"), "status"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}").fetchall()]


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "generated_content" not in schema:
        return {"missing_tables": ["generated_content"], "missing_columns": {}}
    return {"missing_tables": [], "missing_columns": {}}


def _success(attempts: list[dict[str, Any]]) -> bool:
    return any(_text(item.get("status")).lower() in {"published", "success", "succeeded"} or item.get("published_at") for item in attempts)


def _minutes_after(start: datetime | None, end: datetime | None) -> int:
    if not start or not end:
        return 0
    return max(int((end - start).total_seconds() // 60), 0)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
