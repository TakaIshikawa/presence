"""Report conversion of GitHub activity seeds into generated content."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_DAYS = 14
DEFAULT_MIN_CONVERSION_RATE = 0.5
ARTIFACT_TYPE = "github_activity_seed_conversion"
ISSUE_TYPES = (
    "invalid_source_activity_ids",
    "stale_unconverted_activity",
    "closed_merged_without_content",
    "low_conversion_group",
)
SEVERITY_ORDER = {"critical": 0, "warning": 1, "info": 2}


def build_github_activity_seed_conversion_report(
    activity_rows: list[dict[str, Any]],
    content_rows: list[dict[str, Any]],
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    min_conversion_rate: float = DEFAULT_MIN_CONVERSION_RATE,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic conversion report from in-memory rows."""
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if min_conversion_rate < 0 or min_conversion_rate > 1:
        raise ValueError("min_conversion_rate must be between 0 and 1")

    generated_at = _utc(now or datetime.now(timezone.utc))
    stale_cutoff = generated_at - timedelta(days=stale_days)
    activities = [_normalize_activity(row) for row in activity_rows]
    content, parse_findings = _normalize_content_rows(content_rows)
    links = _index_content(content)

    groups: dict[tuple[str, str], dict[str, Any]] = {}
    findings = parse_findings[:]
    for activity in activities:
        key = (activity["repo_name"], activity["activity_type"])
        group = groups.setdefault(
            key,
            {
                "repo_name": key[0],
                "activity_type": key[1],
                "total_activities": 0,
                "linked_activities": 0,
                "generated_content_count": 0,
                "published_content_count": 0,
                "stale_unconverted_activities": 0,
                "closed_merged_without_content": 0,
                "conversion_rate": 0.0,
                "published_conversion_rate": 0.0,
            },
        )
        group["total_activities"] += 1
        matched = sorted({content_id for ref in activity["refs"] for content_id in links.get(ref, set())})
        if matched:
            group["linked_activities"] += 1
            group["generated_content_count"] += len(matched)
            group["published_content_count"] += sum(1 for item in content if item["content_id"] in matched and item["published"])
        elif activity["updated_at_dt"] and activity["updated_at_dt"] <= stale_cutoff:
            group["stale_unconverted_activities"] += 1
            findings.append(_activity_finding("stale_unconverted_activity", "warning", activity, generated_at, f"activity is older than {stale_days} days and has no generated content"))
        if not matched and activity["state"] in {"closed", "merged"}:
            group["closed_merged_without_content"] += 1
            findings.append(_activity_finding("closed_merged_without_content", "warning", activity, generated_at, "closed or merged activity never produced content"))

    group_rows = []
    for group in groups.values():
        total = group["total_activities"]
        group["conversion_rate"] = round(group["linked_activities"] / total, 4) if total else 0.0
        group["published_conversion_rate"] = round(group["published_content_count"] / total, 4) if total else 0.0
        if total and group["conversion_rate"] < min_conversion_rate:
            findings.append(
                {
                    "issue_type": "low_conversion_group",
                    "severity": "info",
                    "repo_name": group["repo_name"],
                    "activity_type": group["activity_type"],
                    "activity_id": None,
                    "content_id": None,
                    "age_days": None,
                    "detail": f"group conversion_rate={group['conversion_rate']:.4f} is below {min_conversion_rate:.4f}",
                }
            )
        group_rows.append(group)
    group_rows.sort(key=lambda item: (item["repo_name"], item["activity_type"]))
    findings.sort(key=_sort_key)

    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"stale_days": stale_days, "min_conversion_rate": min_conversion_rate},
        "totals": {
            "activity_count": len(activities),
            "content_count": len(content_rows),
            "linked_activity_count": sum(group["linked_activities"] for group in group_rows),
            "published_content_count": sum(group["published_content_count"] for group in group_rows),
            "finding_count": len(findings),
            "by_issue_type": {issue_type: Counter(item["issue_type"] for item in findings)[issue_type] for issue_type in ISSUE_TYPES},
        },
        "groups": group_rows,
        "findings": findings,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
        "empty_state": {
            "is_empty": not activities,
            "message": "No GitHub activity rows found." if not activities else None,
        },
    }


def build_github_activity_seed_conversion_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load activity and content rows from SQLite and build the report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [table for table in ("github_activity", "generated_content") if table not in schema]
    missing_columns = _missing_columns(schema)
    activities = _load_activities(conn, schema["github_activity"]) if "github_activity" in schema else []
    content = _load_content(conn, schema["generated_content"]) if "generated_content" in schema else []
    return build_github_activity_seed_conversion_report(
        activities,
        content,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_github_activity_seed_conversion_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_github_activity_seed_conversion_text(report: dict[str, Any]) -> str:
    """Render operator-readable text."""
    totals = report["totals"]
    filters = report["filters"]
    lines = [
        "GitHub Activity Seed Conversion",
        f"Generated: {report['generated_at']}",
        f"Filters: stale_days={filters['stale_days']} min_conversion_rate={filters['min_conversion_rate']:.4f}",
        (
            "Totals: "
            f"activities={totals['activity_count']} content={totals['content_count']} "
            f"linked={totals['linked_activity_count']} published_content={totals['published_content_count']} "
            f"findings={totals['finding_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["groups"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "repo_name | activity_type | total | linked | generated_content | published_content | stale_unconverted | rate"])
    for group in report["groups"]:
        lines.append(
            f"{group['repo_name']} | {group['activity_type']} | {group['total_activities']} | "
            f"{group['linked_activities']} | {group['generated_content_count']} | "
            f"{group['published_content_count']} | {group['stale_unconverted_activities']} | "
            f"{group['conversion_rate']:.4f}"
        )
    return "\n".join(lines)


def _load_activities(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    wanted = ["id", "repo_name", "activity_type", "number", "state", "updated_at", "created_at"]
    select = [_expr(columns, column, column, "NULL") for column in wanted]
    order = "id" if "id" in columns else "rowid"
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM github_activity ORDER BY {order} ASC")]


def _load_content(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    wanted = ["id", "source_activity_ids", "published", "published_at"]
    select = [_expr(columns, column, column, "NULL") for column in wanted]
    order = "id" if "id" in columns else "rowid"
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM generated_content ORDER BY {order} ASC")]


def _normalize_activity(row: dict[str, Any]) -> dict[str, Any]:
    repo = _clean(row.get("repo_name") or row.get("repository")) or "unknown"
    activity_type = _clean(row.get("activity_type") or row.get("type")) or "unknown"
    number = _clean(row.get("number"))
    activity_id = _int_or_none(row.get("activity_id") or row.get("id"))
    logical_id = f"{repo}#{number}:{activity_type}" if repo and number and activity_type else ""
    updated = _parse_timestamp(row.get("updated_at") or row.get("created_at"))
    refs = {ref for ref in (str(activity_id) if activity_id is not None else "", logical_id) if ref}
    return {
        "activity_id": activity_id,
        "repo_name": repo,
        "activity_type": activity_type,
        "number": number or None,
        "logical_activity_id": logical_id or None,
        "refs": refs,
        "state": (_clean(row.get("state")) or "unknown").lower(),
        "updated_at": _iso(updated),
        "updated_at_dt": updated,
    }


def _normalize_content_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    content = []
    findings = []
    for row in rows:
        content_id = _int_or_none(row.get("content_id") or row.get("id"))
        refs, invalid = _parse_source_activity_ids(row.get("source_activity_ids"))
        if invalid:
            findings.append(
                {
                    "issue_type": "invalid_source_activity_ids",
                    "severity": "critical",
                    "repo_name": None,
                    "activity_type": None,
                    "activity_id": None,
                    "content_id": content_id,
                    "age_days": None,
                    "detail": "generated_content.source_activity_ids is not valid JSON list data",
                }
            )
        content.append(
            {
                "content_id": content_id,
                "source_activity_ids": refs,
                "published": _truthy(row.get("published")) or bool(row.get("published_at")),
            }
        )
    return content, findings


def _index_content(content_rows: list[dict[str, Any]]) -> dict[str, set[int]]:
    indexed: dict[str, set[int]] = {}
    for row in content_rows:
        content_id = row["content_id"]
        if content_id is None:
            continue
        for ref in row["source_activity_ids"]:
            indexed.setdefault(str(ref), set()).add(content_id)
    return indexed


def _activity_finding(issue_type: str, severity: str, activity: dict[str, Any], now: datetime, detail: str) -> dict[str, Any]:
    return {
        "issue_type": issue_type,
        "severity": severity,
        "repo_name": activity["repo_name"],
        "activity_type": activity["activity_type"],
        "activity_id": activity["activity_id"],
        "logical_activity_id": activity["logical_activity_id"],
        "content_id": None,
        "age_days": _age_days(now, activity["updated_at_dt"]),
        "detail": detail,
    }


def _parse_source_activity_ids(value: Any) -> tuple[list[str], bool]:
    if value in (None, ""):
        return [], False
    if isinstance(value, list):
        return [str(item) for item in value if item is not None], False
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return [], True
    if not isinstance(parsed, list):
        return [], True
    return [str(item) for item in parsed if item is not None], False


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    expected = {
        "github_activity": {"id", "repo_name", "activity_type", "number", "updated_at"},
        "generated_content": {"id", "source_activity_ids"},
    }
    return {
        table: sorted(required - schema.get(table, set()))
        for table, required in expected.items()
        if table in schema and required - schema.get(table, set())
    }


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        SEVERITY_ORDER.get(item["severity"], 99),
        ISSUE_TYPES.index(item["issue_type"]),
        -(item.get("age_days") or 0.0),
        item.get("repo_name") or "",
        item.get("activity_type") or "",
        item.get("activity_id") or 0,
        item.get("content_id") or 0,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({_quote_identifier(str(row[0]))})")} for row in rows}


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{_quote_identifier(column)} AS {output}" if column in columns else f"{default} AS {output}"


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    text = _clean(value)
    if not text:
        return None
    try:
        return _utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def _age_days(now: datetime, value: datetime | None) -> float | None:
    if not value:
        return None
    return round(max((_utc(now) - _utc(value)).total_seconds() / 86400, 0.0), 2)


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: Any) -> bool:
    return str(value).lower() in {"1", "true", "yes", "published"}


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
