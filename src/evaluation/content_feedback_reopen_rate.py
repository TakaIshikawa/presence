"""Report content feedback items reopened after resolution."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any

from ._report_utils import clean, connection, dt, expr, iso, json_dumps, median, now_iso, positive, schema


ARTIFACT_TYPE = "content_feedback_reopen_rate"
DEFAULT_WINDOW_DAYS = 30
DEFAULT_MIN_RESOLVED = 1
DEFAULT_LIMIT = 50
RESOLVED_STATUSES = {"resolved", "closed", "done", "accepted", "rejected"}
REOPENED_STATUSES = {"reopened", "open", "needs_review", "active"}


def build_content_feedback_reopen_rate_report(
    event_rows: list[dict[str, Any]],
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    min_resolved: int = DEFAULT_MIN_RESOLVED,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    positive("window_days", window_days)
    positive("min_resolved", min_resolved)
    positive("limit", limit)
    generated = now or datetime.now(timezone.utc)
    cutoff = generated.astimezone(timezone.utc) - timedelta(days=window_days)
    by_item: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in event_rows:
        item_id = clean(row.get("feedback_id") or row.get("id") or row.get("item_id"))
        event_at = dt(row.get("event_at") or row.get("updated_at") or row.get("created_at"))
        if item_id and (event_at is None or event_at >= cutoff):
            by_item[item_id].append(row)

    group_stats: dict[tuple[str, str, str], dict[str, Any]] = {}
    item_findings: list[dict[str, Any]] = []
    for item_id, rows in by_item.items():
        ordered = sorted(rows, key=lambda r: (iso(r.get("event_at") or r.get("updated_at") or r.get("created_at")) or "", clean(r.get("event_id") or r.get("id"))))
        resolved_at: datetime | None = None
        resolved_reason = "unknown"
        resolved_reviewer = "unknown"
        resolved_type = "unknown"
        counted_resolution = False
        for row in ordered:
            status = _event_status(row)
            event_at = dt(row.get("event_at") or row.get("updated_at") or row.get("created_at"))
            reviewer = clean(row.get("reviewer") or row.get("reviewer_id") or row.get("assignee"), "unknown")
            content_type = clean(row.get("content_type") or row.get("type"), "unknown")
            reason = clean(row.get("resolution_reason") or row.get("reason"), "unknown")
            if status in RESOLVED_STATUSES:
                key = (reviewer, content_type, reason)
                stat = group_stats.setdefault(key, {"resolved_count": 0, "reopened_count": 0, "hours": []})
                if not counted_resolution:
                    stat["resolved_count"] += 1
                    counted_resolution = True
                resolved_at = event_at
                resolved_reason = reason
                resolved_reviewer = reviewer
                resolved_type = content_type
                continue
            if resolved_at and event_at and event_at > resolved_at and (status in REOPENED_STATUSES or "reopen" in status):
                key = (resolved_reviewer, resolved_type, resolved_reason)
                hours = round((event_at - resolved_at).total_seconds() / 3600, 4)
                stat = group_stats.setdefault(key, {"resolved_count": 0, "reopened_count": 0, "hours": []})
                stat["reopened_count"] += 1
                stat["hours"].append(hours)
                item_findings.append(
                    {
                        "feedback_id": item_id,
                        "reviewer": resolved_reviewer,
                        "content_type": resolved_type,
                        "resolution_reason": resolved_reason,
                        "resolved_at": resolved_at.isoformat(),
                        "reopened_at": event_at.isoformat(),
                        "time_to_reopen_hours": hours,
                    }
                )
                resolved_at = None

    breakdown = []
    reviewer_counts: Counter[str] = Counter()
    total_resolved = 0
    total_reopened = 0
    all_hours: list[float] = []
    for (reviewer, content_type, reason), stat in group_stats.items():
        resolved = int(stat["resolved_count"])
        reopened = int(stat["reopened_count"])
        if resolved < min_resolved and reopened == 0:
            continue
        hours = [float(x) for x in stat["hours"]]
        total_resolved += resolved
        total_reopened += reopened
        all_hours.extend(hours)
        reviewer_counts[reviewer] += reopened
        breakdown.append(
            {
                "reviewer": reviewer,
                "content_type": content_type,
                "resolution_reason": reason,
                "resolved_count": resolved,
                "reopened_count": reopened,
                "reopen_rate": round(reopened / resolved, 4) if resolved else 0.0,
                "median_time_to_reopen_hours": median(hours),
            }
        )
    breakdown.sort(key=lambda r: (-r["reopen_rate"], -r["reopened_count"], r["reviewer"], r["content_type"], r["resolution_reason"]))
    item_findings.sort(key=lambda r: (-r["time_to_reopen_hours"], r["reviewer"], r["feedback_id"]))
    findings = [
        {
            "severity": "high" if row["reopen_rate"] >= 0.5 else "medium",
            "reviewer": row["reviewer"],
            "content_type": row["content_type"],
            "resolution_reason": row["resolution_reason"],
            "reopen_rate": row["reopen_rate"],
            "reopened_count": row["reopened_count"],
            "resolved_count": row["resolved_count"],
        }
        for row in breakdown
        if row["reopened_count"] > 0
    ][:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": now_iso(generated),
        "filters": {"window_days": window_days, "min_resolved": min_resolved, "limit": limit},
        "totals": {
            "feedback_items": len(by_item),
            "resolved_count": total_resolved,
            "reopened_count": total_reopened,
            "reopen_rate": round(total_reopened / total_resolved, 4) if total_resolved else 0.0,
            "median_time_to_reopen_hours": median(all_hours),
        },
        "reviewer_breakdown": [{"reviewer": k, "reopened_count": v} for k, v in sorted(reviewer_counts.items())],
        "breakdown": breakdown[:limit],
        "reopen_events": item_findings[:limit],
        "findings": findings,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())},
        "empty_state": {"is_empty": total_reopened == 0, "message": "No feedback reopen events found." if total_reopened == 0 else None},
    }


def build_content_feedback_reopen_rate_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    s = schema(conn)
    if "content_feedback" not in s:
        return build_content_feedback_reopen_rate_report([], missing_tables=["content_feedback"], **kwargs)
    missing_tables = [] if "content_feedback_events" in s else ["content_feedback_events"]
    rows = _load_feedback(conn, s["content_feedback"])
    if "content_feedback_events" in s:
        rows.extend(_load_events(conn, s["content_feedback_events"]))
    return build_content_feedback_reopen_rate_report(rows, missing_tables=missing_tables, **kwargs)


def format_content_feedback_reopen_rate_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_content_feedback_reopen_rate_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Content Feedback Reopen Rate",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['window_days']} days",
        f"Totals: resolved={totals['resolved_count']} reopened={totals['reopened_count']} reopen_rate={totals['reopen_rate']} median_hours={totals['median_time_to_reopen_hours']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + "; ".join(f"{t}({', '.join(c)})" for t, c in report["missing_columns"].items()))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "reviewer | content_type | resolution_reason | resolved | reopened | reopen_rate | median_hours"])
    for row in report["breakdown"]:
        lines.append(f"{row['reviewer']} | {row['content_type']} | {row['resolution_reason']} | {row['resolved_count']} | {row['reopened_count']} | {row['reopen_rate']} | {row['median_time_to_reopen_hours']}")
    return "\n".join(lines)


def _load_feedback(conn: sqlite3.Connection, cols: set[str]) -> list[dict[str, Any]]:
    select = [
        expr(cols, "id", "feedback_id", out="feedback_id"),
        expr(cols, "reviewer", "reviewer_id", "assignee", default="'unknown'", out="reviewer"),
        expr(cols, "content_type", "type", default="'unknown'", out="content_type"),
        expr(cols, "status", default="'unknown'", out="status"),
        expr(cols, "resolution_reason", "reason", default="'unknown'", out="resolution_reason"),
        expr(cols, "resolved_at", "closed_at", "updated_at", "created_at", default="NULL", out="event_at"),
    ]
    return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM content_feedback ORDER BY rowid")]


def _load_events(conn: sqlite3.Connection, cols: set[str]) -> list[dict[str, Any]]:
    select = [
        expr(cols, "id", "event_id", default="rowid", out="event_id"),
        expr(cols, "feedback_id", "content_feedback_id", "item_id", out="feedback_id"),
        expr(cols, "event_type", "status", default="'unknown'", out="event_type"),
        expr(cols, "status", "to_status", default="NULL", out="status"),
        expr(cols, "reviewer", "reviewer_id", "assignee", default="'unknown'", out="reviewer"),
        expr(cols, "content_type", "type", default="'unknown'", out="content_type"),
        expr(cols, "resolution_reason", "reason", default="'unknown'", out="resolution_reason"),
        expr(cols, "event_at", "created_at", "updated_at", default="NULL", out="event_at"),
    ]
    return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM content_feedback_events ORDER BY rowid")]


def _event_status(row: dict[str, Any]) -> str:
    return clean(row.get("status") or row.get("event_type"), "unknown").lower()
