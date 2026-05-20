"""Report API rate-limit recovery windows from stored snapshots."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_THRESHOLD = 5
DEFAULT_STALE_MINUTES = 30
ARTIFACT_TYPE = "api_rate_limit_recovery_windows"
ISSUE_TYPES = ("low_quota_window", "stale_reset_at", "missing_provider_platform_metadata")
SEVERITY_ORDER = {"critical": 0, "warning": 1, "info": 2}


def build_api_rate_limit_recovery_windows_report(
    rows: list[dict[str, Any]],
    *,
    threshold: int = DEFAULT_THRESHOLD,
    stale_minutes: int = DEFAULT_STALE_MINUTES,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic report of low-quota recovery windows."""
    if threshold < 0:
        raise ValueError("threshold must be non-negative")
    if stale_minutes <= 0:
        raise ValueError("stale_minutes must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    normalized = [_normalize(row) for row in rows]
    findings = _metadata_findings(normalized)

    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in normalized:
        if row["fetched_at_dt"] is None:
            continue
        grouped[(row["provider"], row["platform"], row["endpoint"])].append(row)

    stale_delta = timedelta(minutes=stale_minutes)
    for (provider, platform, endpoint), snapshots in grouped.items():
        snapshots.sort(key=lambda row: (row["fetched_at_dt"], row["snapshot_id"] or 0))
        findings.extend(_low_windows(provider, platform, endpoint, snapshots, threshold, stale_delta))

    findings.sort(key=_sort_key)
    grouped_findings = _group_findings(findings)
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"stale_minutes": stale_minutes, "threshold": threshold},
        "summary": {
            "snapshot_count": len(rows),
            "provider_platform_count": len({(row["provider"], row["platform"]) for row in normalized}),
            "finding_count": len(findings),
            "by_issue_type": {issue_type: Counter(item["issue_type"] for item in findings)[issue_type] for issue_type in ISSUE_TYPES},
            "by_severity": dict(sorted(Counter(item["severity"] for item in findings).items())),
        },
        "findings": grouped_findings,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
        "empty_state": {
            "is_empty": not findings,
            "message": "No API rate-limit recovery window issues found." if not findings else None,
        },
    }


def build_api_rate_limit_recovery_windows_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load API rate-limit snapshots from SQLite and build the recovery window report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "api_rate_limit_snapshots" not in schema:
        return build_api_rate_limit_recovery_windows_report([], missing_tables=["api_rate_limit_snapshots"], **kwargs)
    columns = schema["api_rate_limit_snapshots"]
    required = {"provider", "endpoint", "remaining", "fetched_at"}
    missing = sorted(required - columns)
    if missing:
        return build_api_rate_limit_recovery_windows_report([], missing_columns={"api_rate_limit_snapshots": missing}, **kwargs)
    return build_api_rate_limit_recovery_windows_report(_load_rows(conn, columns), **kwargs)


def format_api_rate_limit_recovery_windows_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_api_rate_limit_recovery_windows_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "API Rate-limit Recovery Windows",
        f"Generated: {report['generated_at']}",
        f"Filters: threshold={report['filters']['threshold']} stale_minutes={report['filters']['stale_minutes']}",
        (
            "Totals: "
            f"snapshots={summary['snapshot_count']} "
            f"provider_platforms={summary['provider_platform_count']} "
            f"findings={summary['finding_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "provider | platform | issue_type | severity | endpoint | duration_minutes | latest_remaining | reset_at"])
    for group in report["findings"]:
        for item in group["issues"]:
            lines.append(
                f"{group['provider']} | {group['platform']} | {item['issue_type']} | {item['severity']} | "
                f"{item['endpoint']} | {_display(item.get('duration_minutes'))} | "
                f"{_display(item.get('latest_remaining'))} | {_display(item.get('reset_at'))}"
            )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        _expr(columns, "id", "snapshot_id", "NULL"),
        "provider AS provider",
        _expr(columns, "platform", "platform", "NULL"),
        "endpoint AS endpoint",
        "remaining AS remaining",
        _expr(columns, "limit_value", "limit_value", "NULL"),
        _expr(columns, "reset_at", "reset_at", "NULL"),
        "fetched_at AS fetched_at",
    ]
    order = "id" if "id" in columns else "rowid"
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(select)}
                FROM api_rate_limit_snapshots
                ORDER BY provider ASC, endpoint ASC, datetime(fetched_at) ASC, {order} ASC"""
        )
    ]


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    provider = _clean(row.get("provider"))
    platform = _clean(row.get("platform")) or provider
    fetched_at_dt = _parse_timestamp(row.get("fetched_at"))
    reset_at_dt = _parse_timestamp(row.get("reset_at"))
    return {
        "snapshot_id": _int_or_none(row.get("snapshot_id") or row.get("id")),
        "provider": provider or "unknown",
        "platform": platform or "unknown",
        "provider_missing": not bool(provider),
        "platform_missing": not bool(_clean(row.get("platform"))) and "platform" in row,
        "endpoint": _clean(row.get("endpoint")) or "default",
        "remaining": _int_or_none(row.get("remaining")),
        "limit_value": _int_or_none(row.get("limit_value")),
        "reset_at": reset_at_dt.isoformat() if reset_at_dt else None,
        "reset_at_dt": reset_at_dt,
        "fetched_at": fetched_at_dt.isoformat() if fetched_at_dt else _clean(row.get("fetched_at")) or None,
        "fetched_at_dt": fetched_at_dt,
    }


def _metadata_findings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    findings = []
    for row in rows:
        missing = []
        if row["provider_missing"]:
            missing.append("provider")
        if row["platform_missing"]:
            missing.append("platform")
        if missing:
            findings.append(
                {
                    "issue_type": "missing_provider_platform_metadata",
                    "severity": "info",
                    "provider": row["provider"],
                    "platform": row["platform"],
                    "endpoint": row["endpoint"],
                    "snapshot_id": row["snapshot_id"],
                    "missing_fields": missing,
                    "duration_minutes": None,
                    "latest_remaining": row["remaining"],
                    "reset_at": row["reset_at"],
                    "latest_fetched_at": row["fetched_at"],
                }
            )
    return findings


def _low_windows(
    provider: str,
    platform: str,
    endpoint: str,
    snapshots: list[dict[str, Any]],
    threshold: int,
    stale_delta: timedelta,
) -> list[dict[str, Any]]:
    findings = []
    current: list[dict[str, Any]] = []
    for snapshot in snapshots:
        remaining = snapshot["remaining"]
        if remaining is not None and remaining <= threshold:
            current.append(snapshot)
            continue
        findings.extend(_window_findings(provider, platform, endpoint, current, threshold, stale_delta))
        current = []
    findings.extend(_window_findings(provider, platform, endpoint, current, threshold, stale_delta))
    return findings


def _window_findings(
    provider: str,
    platform: str,
    endpoint: str,
    window: list[dict[str, Any]],
    threshold: int,
    stale_delta: timedelta,
) -> list[dict[str, Any]]:
    if not window:
        return []
    duration = _duration_minutes(window[0]["fetched_at_dt"], window[-1]["fetched_at_dt"])
    if timedelta(minutes=duration) < stale_delta:
        return []
    remaining_values = _remaining_values(window)
    reset_at = window[-1]["reset_at_dt"]
    findings = [
        {
            "issue_type": "low_quota_window",
            "severity": "critical",
            "provider": provider,
            "platform": platform,
            "endpoint": endpoint,
            "threshold": threshold,
            "snapshot_count": len(window),
            "min_remaining": min(remaining_values),
            "latest_remaining": window[-1]["remaining"],
            "window_started_at": window[0]["fetched_at"],
            "window_ended_at": window[-1]["fetched_at"],
            "duration_minutes": duration,
            "reset_at": reset_at.isoformat() if reset_at else None,
            "latest_fetched_at": window[-1]["fetched_at"],
        }
    ]
    if reset_at is not None and reset_at <= window[-1]["fetched_at_dt"] - stale_delta:
        findings.append(
            {
                "issue_type": "stale_reset_at",
                "severity": "warning",
                "provider": provider,
                "platform": platform,
                "endpoint": endpoint,
                "snapshot_count": len(window),
                "min_remaining": min(remaining_values),
                "latest_remaining": window[-1]["remaining"],
                "window_started_at": window[0]["fetched_at"],
                "window_ended_at": window[-1]["fetched_at"],
                "duration_minutes": duration,
                "reset_at": reset_at.isoformat(),
                "latest_fetched_at": window[-1]["fetched_at"],
            }
        )
    return findings


def _group_findings(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in findings:
        grouped[(item["provider"], item["platform"])].append(item)
    return [
        {"provider": provider, "platform": platform, "issue_count": len(items), "issues": items}
        for (provider, platform), items in sorted(grouped.items())
    ]


def _remaining_values(rows: list[dict[str, Any]]) -> list[int]:
    return [row["remaining"] for row in rows if row["remaining"] is not None] or [0]


def _duration_minutes(start: datetime, end: datetime) -> int:
    return int((end - start).total_seconds() // 60)


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        SEVERITY_ORDER.get(item["severity"], 99),
        ISSUE_TYPES.index(item["issue_type"]),
        item["provider"],
        item["platform"],
        item["endpoint"],
        item.get("window_started_at") or item.get("latest_fetched_at") or "",
    )


def _parse_timestamp(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _utc(parsed)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({_quote_identifier(str(row[0]))})")} for row in rows}


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{_quote_identifier(column)} AS {output}" if column in columns else f"{default} AS {output}"


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _display(value: Any) -> str:
    if value is None or value == "":
        return "-"
    return str(value)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
