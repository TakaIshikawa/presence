"""Small helpers for deterministic SQLite-backed evaluation reports."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


def connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {col[1] for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in tables}


def missing(schema_map: dict[str, set[str]], required: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    tables = sorted(table for table in required if table not in schema_map)
    columns = {
        table: sorted(cols - schema_map.get(table, set()))
        for table, cols in required.items()
        if table in schema_map and cols - schema_map.get(table, set())
    }
    return tables, columns


def col(columns: set[str], name: str, alias: str, fallback: str = "NULL") -> str:
    return f"{alias}.{name}" if name in columns else fallback


def clean(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", clean(value).lower())).strip()


def parse_time(value: Any) -> datetime | None:
    text = clean(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def utc(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return clean(value).lower() in {"1", "true", "yes", "y", "selected", "published", "posted", "sent", "approved"}


def valid_json_object(value: Any) -> bool:
    if isinstance(value, dict):
        return True
    if value in (None, ""):
        return False
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return False
    return isinstance(parsed, dict)


def grouped(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for finding in findings:
        groups.setdefault(finding["reason"], []).append(finding)
    return [{"reason": reason, "count": len(items), "items": items} for reason, items in sorted(groups.items())]


def base_report(
    *,
    artifact_type: str,
    generated_at: datetime,
    filters: dict[str, Any],
    rows_count: int,
    findings: list[dict[str, Any]],
    reasons: tuple[str, ...],
    limit: int,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    counts = Counter(f["reason"] for f in findings)
    shown = findings[:limit]
    report = {
        "artifact_type": artifact_type,
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "row_count": rows_count,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_reason": {reason: counts[reason] for reason in reasons},
        },
        "findings": grouped(shown),
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items()) if cols},
        "empty_state": {"is_empty": not findings, "message": "No findings." if not findings else None},
    }
    if extra:
        report.update(extra)
    return report


def json_format(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def text_format(title: str, report: dict[str, Any]) -> str:
    lines = [
        title,
        f"Generated: {report['generated_at']}",
        f"Totals: rows={report['totals']['row_count']} findings={report['totals']['finding_count']} shown={report['totals']['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + "; ".join(f"{t}({', '.join(c)})" for t, c in report["missing_columns"].items()))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    for group in report["findings"]:
        lines.append(f"{group['reason']}: {group['count']}")
        for item in group["items"]:
            ident = item.get("content_id") or item.get("id") or item.get("row_id") or "-"
            lines.append(f"- id={ident} detail={item.get('detail', '-')}")
    return "\n".join(lines)
