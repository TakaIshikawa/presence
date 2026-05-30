"""Small helpers for deterministic SQLite-backed evaluation reports."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100


def connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def column(columns: set[str], name: str, fallback: str = "NULL", *, alias: str | None = None) -> str:
    prefix = f"{alias}." if alias else ""
    return f"{prefix}{name}" if name in columns else fallback


def require_positive(name: str, value: int | float) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be positive")


def require_non_negative(name: str, value: int | float) -> None:
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def require_probability(name: str, value: float) -> None:
    if value < 0 or value > 1:
        raise ValueError(f"{name} must be between 0 and 1")


def utc(value: datetime | str | None = None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, str):
        parsed = parse_ts(value)
        if parsed is None:
            raise ValueError(f"invalid timestamp: {value}")
        return parsed
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_ts(value: Any) -> datetime | None:
    text = clean(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return utc(parsed)


def clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def text(value: Any, default: str = "") -> str:
    return clean(value) or default


def display(value: Any) -> str:
    value = clean(value)
    return value if value else "-"


def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return clean(value).lower() in {"1", "true", "yes", "y", "published", "selected", "success"}


def valid_json_object(value: Any) -> bool:
    if isinstance(value, dict):
        return True
    if value in (None, ""):
        return False
    try:
        return isinstance(json.loads(str(value)), dict)
    except (TypeError, ValueError):
        return False


def json_shape(value: Any) -> tuple[str, ...] | None:
    if isinstance(value, dict):
        obj = value
    else:
        try:
            obj = json.loads(str(value))
        except (TypeError, ValueError):
            return None
    if not isinstance(obj, dict):
        return None
    return tuple(sorted(str(key) for key in obj))


def reason_counts(findings: list[dict[str, Any]], reasons: tuple[str, ...]) -> dict[str, int]:
    counts = Counter(finding["reason"] for finding in findings)
    return {reason: counts[reason] for reason in reasons}


def group_by_reason(findings: list[dict[str, Any]], reasons: tuple[str, ...]) -> list[dict[str, Any]]:
    grouped = []
    for reason in reasons:
        items = [finding for finding in findings if finding["reason"] == reason]
        if items:
            grouped.append({"reason": reason, "count": len(items), "items": items})
    return grouped


def base_report(
    *,
    artifact_type: str,
    generated_at: datetime,
    filters: dict[str, Any],
    rows_scanned: int,
    findings: list[dict[str, Any]],
    reasons: tuple[str, ...],
    limit: int,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    grouped: bool = False,
    empty_message: str | None = None,
    extra_summary: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    findings = sorted(findings, key=lambda item: (item.get("reason", ""), str(item.get("content_id", item.get("id", ""))), str(item)))
    shown = findings[:limit]
    summary = {
        "rows_scanned": rows_scanned,
        "finding_count": len(findings),
        "shown_count": len(shown),
        "by_reason": reason_counts(findings, reasons),
    }
    if extra_summary:
        summary.update(extra_summary)
    report = {
        "artifact_type": artifact_type,
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "summary": summary,
        "findings": group_by_reason(shown, reasons) if grouped else shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items()) if cols},
        "empty_state": {
            "is_empty": not findings,
            "message": empty_message or (f"No {artifact_type} findings found." if not findings else None),
        },
    }
    if extra:
        report.update(extra)
    return report


def format_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_text(title: str, report: dict[str, Any]) -> str:
    lines = [
        title,
        f"Generated: {report['generated_at']}",
        "Filters: " + ", ".join(f"{key}={value}" for key, value in report["filters"].items()),
        f"Totals: rows={report['summary']['rows_scanned']} findings={report['summary']['finding_count']} shown={report['summary']['shown_count']}",
        "Reason counts: " + ", ".join(f"{key}={value}" for key, value in report["summary"]["by_reason"].items()),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + "; ".join(f"{table}({', '.join(cols)})" for table, cols in report["missing_columns"].items())
        )
    findings = report["findings"]
    if not findings:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("")
    lines.append("reason | id | content_id | detail")
    flat = []
    for item in findings:
        if "items" in item:
            flat.extend(item["items"])
        else:
            flat.append(item)
    for item in flat:
        lines.append(
            f"{item.get('reason', '-')} | {display(item.get('id') or item.get('link_id') or item.get('row_id'))} | "
            f"{display(item.get('content_id'))} | {display(item.get('detail'))}"
        )
    return "\n".join(lines)
