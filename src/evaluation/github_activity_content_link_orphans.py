"""Audit generated content links to GitHub activity rows."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
ISSUE_TYPES = (
    "malformed_source_activity_ids",
    "invalid_activity_id",
    "missing_activity",
    "unlinked_recent_activity",
)


def build_github_activity_content_link_orphans_report(
    content_rows: list[dict[str, Any]],
    activity_rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    activity_type: str | None = None,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    wanted_type = _clean(activity_type).lower() or None
    activity_by_id = {_to_int(row.get("activity_id")): row for row in activity_rows if _to_int(row.get("activity_id")) is not None}
    linked_ids: set[int] = set()
    findings: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    grouped: dict[str, Counter[str]] = defaultdict(Counter)

    for row in content_rows:
        content_id = row.get("content_id")
        raw_ids = row.get("source_activity_ids")
        try:
            decoded = json.loads(raw_ids) if raw_ids not in (None, "") else []
        except (TypeError, json.JSONDecodeError):
            _add(
                findings,
                counts,
                grouped,
                {
                    "issue_type": "malformed_source_activity_ids",
                    "activity_type": "unknown",
                    "content_id": content_id,
                    "source_activity_ids": raw_ids,
                    "detail": "source_activity_ids is not valid JSON",
                },
            )
            continue
        if not isinstance(decoded, list):
            _add(
                findings,
                counts,
                grouped,
                {
                    "issue_type": "malformed_source_activity_ids",
                    "activity_type": "unknown",
                    "content_id": content_id,
                    "source_activity_ids": raw_ids,
                    "detail": "source_activity_ids must decode to a JSON array",
                },
            )
            continue
        for value in decoded:
            activity_id = value if isinstance(value, int) and not isinstance(value, bool) else None
            if activity_id is None:
                _add(
                    findings,
                    counts,
                    grouped,
                    {
                        "issue_type": "invalid_activity_id",
                        "activity_type": "unknown",
                        "content_id": content_id,
                        "activity_id": value,
                        "source_activity_ids": raw_ids,
                        "detail": "activity id is not an integer",
                    },
                )
                continue
            activity = activity_by_id.get(activity_id)
            if activity is None:
                _add(
                    findings,
                    counts,
                    grouped,
                    {
                        "issue_type": "missing_activity",
                        "activity_type": "unknown",
                        "content_id": content_id,
                        "activity_id": activity_id,
                        "source_activity_ids": raw_ids,
                        "detail": "referenced github_activity row is missing",
                    },
                )
                continue
            row_type = _activity_type(activity)
            if wanted_type and row_type.lower() != wanted_type:
                continue
            linked_ids.add(activity_id)

    cutoff = generated_at - timedelta(days=days)
    for activity in activity_rows:
        activity_id = _to_int(activity.get("activity_id"))
        row_type = _activity_type(activity)
        if wanted_type and row_type.lower() != wanted_type:
            continue
        if activity_id is None or activity_id in linked_ids:
            continue
        closed_at = _parse_time(_first(activity, "closed_at", "merged_at"))
        if closed_at is None or closed_at < cutoff:
            continue
        _add(
            findings,
            counts,
            grouped,
            {
                "issue_type": "unlinked_recent_activity",
                "activity_type": row_type,
                "activity_id": activity_id,
                "closed_at": _first(activity, "closed_at", "merged_at"),
                "state": activity.get("state"),
                "detail": "recently closed or merged GitHub activity has no generated content link",
            },
        )

    findings.sort(key=_finding_sort_key)
    return {
        "artifact_type": "github_activity_content_link_orphans",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "activity_type": wanted_type or "all"},
        "totals": {
            "content_count": len(content_rows),
            "activity_count": len(activity_rows),
            "finding_count": len(findings),
            "issue_counts": {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES},
            "issue_counts_by_activity_type": {
                key: {issue_type: counter[issue_type] for issue_type in ISSUE_TYPES}
                for key, counter in sorted(grouped.items())
            },
        },
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
            if columns
        },
        "findings": findings,
    }


def build_github_activity_content_link_orphans_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {"generated_content": {"id", "source_activity_ids"}, "github_activity": {"id"}}
    missing_tables = sorted(table for table in required if table not in schema)
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    if missing_tables or missing_columns:
        return build_github_activity_content_link_orphans_report(
            [],
            [],
            missing_tables=missing_tables,
            missing_columns=missing_columns,
            **kwargs,
        )
    return build_github_activity_content_link_orphans_report(
        _load_content(conn),
        _load_activity(conn, schema["github_activity"]),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_github_activity_content_link_orphans_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_github_activity_content_link_orphans_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "GitHub Activity Content Link Orphans",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['days']} days",
        f"Activity type: {report['filters']['activity_type']}",
        f"Totals: content={totals['content_count']} activity={totals['activity_count']} findings={totals['finding_count']}",
        "Issue counts: " + ", ".join(f"{issue}={totals['issue_counts'][issue]}" for issue in ISSUE_TYPES),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + ", ".join(f"{table}.{column}" for table, columns in report["missing_columns"].items() for column in columns)
        )
    if not report["findings"]:
        lines.append("No GitHub activity content link orphans found.")
        return "\n".join(lines)
    lines.extend(["", "Findings:"])
    for finding in report["findings"]:
        lines.append(
            f"- {finding['issue_type']} activity_type={finding.get('activity_type', 'unknown')} "
            f"content_id={finding.get('content_id', '-')} activity_id={finding.get('activity_id', '-')} "
            f"detail={finding['detail']}"
        )
    return "\n".join(lines)


def _load_content(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            "SELECT id AS content_id, source_activity_ids FROM generated_content ORDER BY id ASC"
        ).fetchall()
    ]


def _load_activity(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    activity_type = _column_expr(columns, "activity_type", "type", "kind", fallback="'unknown'")
    state = _column_expr(columns, "state", "status", fallback="NULL")
    closed_at = _column_expr(columns, "closed_at", "merged_at", "completed_at", "updated_at", fallback="NULL")
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT id AS activity_id, {activity_type} AS activity_type, {state} AS state,
                       {closed_at} AS closed_at
                FROM github_activity
                ORDER BY id ASC"""
        ).fetchall()
    ]


def _add(findings: list[dict[str, Any]], counts: Counter[str], grouped: dict[str, Counter[str]], finding: dict[str, Any]) -> None:
    findings.append(finding)
    issue_type = finding["issue_type"]
    activity_type = _clean(finding.get("activity_type")) or "unknown"
    counts[issue_type] += 1
    grouped[activity_type][issue_type] += 1


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], *names: str, fallback: str) -> str:
    for name in names:
        if name in columns:
            return name
    return fallback


def _activity_type(row: dict[str, Any]) -> str:
    return _clean(row.get("activity_type")) or "unknown"


def _parse_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _to_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    return (
        ISSUE_TYPES.index(finding["issue_type"]),
        finding.get("activity_type") or "",
        _to_int(finding.get("content_id")) or 0,
        _to_int(finding.get("activity_id")) or 0,
    )
