"""Rank broken newsletter links by reader impact."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEAD_STATUSES = {"dead", "broken", "error", "timeout", "failed", "404", "410", "500", "502", "503"}


def build_newsletter_dead_link_impact_report(rows: list[dict[str, Any]], *, now: datetime | None = None) -> dict[str, Any]:
    generated_at = _utc(now or datetime.now(timezone.utc))
    flagged = [_impact_item(row) for row in rows if _is_dead(row)]
    flagged.sort(key=lambda item: (-item["impact_score"], -item["affected_sends"], -item["click_count"], item["url"]))
    return {
        "artifact_type": "newsletter_dead_link_impact",
        "generated_at": generated_at.isoformat(),
        "totals": {
            "rows_scanned": len(rows),
            "broken_link_count": len(flagged),
            "affected_sends": sum(item["affected_sends"] for item in flagged),
            "click_count": sum(item["click_count"] for item in flagged),
        },
        "links": flagged,
        "empty_state": {
            "is_empty": not flagged,
            "message": "No broken newsletter links found." if not flagged else None,
        },
    }


def build_newsletter_dead_link_impact_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_newsletter_dead_link_impact_report(_load_rows(conn, schema), **kwargs)


def format_newsletter_dead_link_impact_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_dead_link_impact_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Newsletter Dead Link Impact",
        f"Generated: {report['generated_at']}",
        f"Totals: broken={totals['broken_link_count']} affected_sends={totals['affected_sends']} clicks={totals['click_count']}",
    ]
    if not report["links"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "Broken links:"])
    for item in report["links"]:
        lines.append(
            f"- {item['url']} issue={item['issue']} sends={item['affected_sends']} "
            f"clicks={item['click_count']} score={item['impact_score']} reason={item['remediation_reason']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "newsletter_links" in schema:
        columns = schema["newsletter_links"]
        selected = [
            _col(columns, "url", "link_url", "raw_url") + " AS url",
            _col(columns, "status", "link_status", "http_status", default="'unknown'") + " AS status",
            _col(columns, "error", "error_message", default="NULL") + " AS error",
            _col(columns, "newsletter_send_id", "send_id", "issue_id", default="NULL") + " AS content_id",
            _col(columns, "click_count", "clicks", "unique_clicks", default="0") + " AS click_count",
        ]
        return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM newsletter_links").fetchall()]
    if "newsletter_link_clicks" in schema:
        columns = schema["newsletter_link_clicks"]
        selected = [
            _col(columns, "link_url", "url", "raw_url") + " AS url",
            _col(columns, "status", "link_status", "http_status", default="'unknown'") + " AS status",
            _col(columns, "error", "error_message", default="NULL") + " AS error",
            _col(columns, "newsletter_send_id", "send_id", default="NULL") + " AS content_id",
            _col(columns, "click_count", "clicks", "unique_clicks", default="0") + " AS click_count",
        ]
        return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM newsletter_link_clicks").fetchall()]
    return []


def _impact_item(row: dict[str, Any]) -> dict[str, Any]:
    url = _text(_first(row, "url", "link_url", "raw_url"))
    status = _text(_first(row, "status", "link_status", "http_status")) or "unknown"
    error = _text(_first(row, "error", "error_message"))
    ids = _content_ids(row)
    clicks = _int(_first(row, "click_count", "clicks", "unique_clicks"), 0)
    affected = max(len(ids), _int(_first(row, "affected_sends", "send_count"), len(ids) or 1))
    issue = error or status
    score = affected * 10 + clicks
    return {
        "url": url,
        "status": status,
        "error": error or None,
        "issue": issue,
        "affected_content_ids": ids,
        "affected_sends": affected,
        "click_count": clicks,
        "impact_score": score,
        "remediation_reason": _remediation_reason(status, clicks, affected),
    }


def _is_dead(row: dict[str, Any]) -> bool:
    status = _text(_first(row, "status", "link_status", "http_status")).lower()
    error = _text(_first(row, "error", "error_message")).lower()
    return status in DEAD_STATUSES or status.startswith("4") or status.startswith("5") or bool(error)


def _remediation_reason(status: str, clicks: int, affected: int) -> str:
    if clicks > 0:
        return "Replace or redirect; readers have clicked this broken link."
    if affected > 1:
        return "Fix shared newsletter URL before resending affected issues."
    if status.startswith("4") or status in {"404", "410"}:
        return "Replace with a live source or archived URL."
    return "Recheck and update the newsletter link target."


def _content_ids(row: dict[str, Any]) -> list[str]:
    value = _first(row, "affected_content_ids", "content_ids", "content_id", "newsletter_send_id", "issue_id")
    if isinstance(value, (list, tuple, set)):
        return sorted({_text(item) for item in value if _text(item)})
    text = _text(value)
    if not text:
        return []
    return [part.strip() for part in text.replace(";", ",").split(",") if part.strip()]


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _col(columns: set[str], *names: str, default: str = "NULL") -> str:
    for name in names:
        if name in columns:
            return name
    return default


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
