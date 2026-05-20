"""Audit profile metrics snapshot gaps and invalid samples by platform."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_MAX_AGE_HOURS = 48.0
DEFAULT_MAX_GAP_HOURS = 24.0
DEFAULT_PLATFORM = "all"
ISSUE_TYPES = (
    "stale_platform_snapshot",
    "snapshot_gap",
    "non_monotonic_snapshot",
    "negative_metric",
)
METRIC_COLUMNS = ("follower_count", "following_count", "tweet_count", "listed_count")
REQUIRED_COLUMNS = {"platform", "fetched_at", *METRIC_COLUMNS}


def build_profile_metrics_platform_snapshot_gaps_report(
    snapshot_rows: list[dict[str, Any]],
    *,
    platform: str = DEFAULT_PLATFORM,
    max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
    max_gap_hours: float = DEFAULT_MAX_GAP_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a platform snapshot gaps report from profile_metrics row dicts."""
    if max_age_hours <= 0:
        raise ValueError("max_age_hours must be positive")
    if max_gap_hours <= 0:
        raise ValueError("max_gap_hours must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    normalized_platform = _clean(platform).lower() or DEFAULT_PLATFORM
    rows = [
        {**row, "platform": _clean(row.get("platform")).lower() or "unknown"}
        for row in snapshot_rows
        if normalized_platform == DEFAULT_PLATFORM
        or (_clean(row.get("platform")).lower() or "unknown") == normalized_platform
    ]
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row["platform"], []).append(row)

    findings: list[dict[str, Any]] = []
    for platform_name, platform_rows in sorted(grouped.items()):
        findings.extend(
            _platform_findings(
                platform_name,
                platform_rows,
                now=generated_at,
                max_age_hours=max_age_hours,
                max_gap_hours=max_gap_hours,
            )
        )

    counts = Counter(finding["issue_type"] for finding in findings)
    by_platform: dict[str, Counter[str]] = {}
    for finding in findings:
        by_platform.setdefault(finding["platform"], Counter())[finding["issue_type"]] += 1

    findings.sort(key=_finding_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": "profile_metrics_platform_snapshot_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "platform": normalized_platform,
            "max_age_hours": _round_hours(max_age_hours),
            "max_gap_hours": _round_hours(max_gap_hours),
            "limit": limit,
        },
        "summary": {
            "snapshot_count": len(rows),
            "platform_count": len(grouped),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES},
            "by_platform_issue_type": {
                platform_key: {issue_type: counter[issue_type] for issue_type in ISSUE_TYPES}
                for platform_key, counter in sorted(by_platform.items())
            },
        },
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
        },
        "findings": shown,
        "empty_state": {
            "is_empty": not findings,
            "message": "No profile metrics platform snapshot gaps found." if not findings else None,
        },
    }


def build_profile_metrics_platform_snapshot_gaps_report_from_db(
    db_or_conn: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    """Load profile metrics snapshots from SQLite and build the report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get("profile_metrics")
    if columns is None:
        return build_profile_metrics_platform_snapshot_gaps_report(
            [],
            missing_tables=["profile_metrics"],
            **kwargs,
        )

    missing = sorted(REQUIRED_COLUMNS - columns)
    if missing:
        return build_profile_metrics_platform_snapshot_gaps_report(
            [],
            missing_columns={"profile_metrics": missing},
            **kwargs,
        )

    platform = kwargs.get("platform", DEFAULT_PLATFORM)
    return build_profile_metrics_platform_snapshot_gaps_report(
        _load_snapshots(conn, columns, platform=platform),
        **kwargs,
    )


def format_profile_metrics_platform_snapshot_gaps_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_profile_metrics_platform_snapshot_gaps_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Profile Metrics Platform Snapshot Gaps",
        f"Generated: {report['generated_at']}",
        (
            f"Platform: {report['filters']['platform']} "
            f"max_age_hours={report['filters']['max_age_hours']} "
            f"max_gap_hours={report['filters']['max_gap_hours']} "
            f"limit={report['filters']['limit']}"
        ),
        (
            "Totals: "
            f"platforms={summary['platform_count']} "
            f"snapshots={summary['snapshot_count']} "
            f"findings={summary['finding_count']} "
            f"shown={summary['shown_count']}"
        ),
        "Issue counts: "
        + ", ".join(f"{issue_type}={summary['by_issue_type'].get(issue_type, 0)}" for issue_type in ISSUE_TYPES),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in report["missing_columns"].items()
        ]
        lines.append("Missing columns: " + "; ".join(missing))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "platform | issue_type | fetched_at | detail"])
    for finding in report["findings"]:
        lines.append(
            f"{finding['platform']} | {finding['issue_type']} | "
            f"{finding.get('fetched_at') or '-'} | {finding['detail']}"
        )
    return "\n".join(lines)


def _platform_findings(
    platform: str,
    rows: list[dict[str, Any]],
    *,
    now: datetime,
    max_age_hours: float,
    max_gap_hours: float,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    parsed_rows = [
        {**row, "_fetched_at": _parse_datetime(row.get("fetched_at"))}
        for row in rows
        if _parse_datetime(row.get("fetched_at")) is not None
    ]
    if not parsed_rows:
        return findings

    for previous, current in zip(parsed_rows, parsed_rows[1:]):
        if previous["_fetched_at"] > current["_fetched_at"]:
            findings.append(
                {
                    "issue_type": "non_monotonic_snapshot",
                    "platform": platform,
                    "fetched_at": current["_fetched_at"].isoformat(),
                    "previous_fetched_at": previous["_fetched_at"].isoformat(),
                    "detail": (
                        f"snapshot at {current['_fetched_at'].isoformat()} follows later "
                        f"snapshot {previous['_fetched_at'].isoformat()}"
                    ),
                }
            )

    ordered = sorted(parsed_rows, key=lambda row: row["_fetched_at"])
    latest = ordered[-1]
    latest_age = _hours_between(latest["_fetched_at"], now)
    if latest_age > max_age_hours:
        findings.append(
            {
                "issue_type": "stale_platform_snapshot",
                "platform": platform,
                "fetched_at": latest["_fetched_at"].isoformat(),
                "age_hours": _round_hours(latest_age),
                "max_age_hours": _round_hours(max_age_hours),
                "detail": f"latest snapshot is {_round_hours(latest_age)} hours old",
            }
        )

    for previous, current in zip(ordered, ordered[1:]):
        gap = _hours_between(previous["_fetched_at"], current["_fetched_at"])
        if gap > max_gap_hours:
            findings.append(
                {
                    "issue_type": "snapshot_gap",
                    "platform": platform,
                    "fetched_at": current["_fetched_at"].isoformat(),
                    "previous_fetched_at": previous["_fetched_at"].isoformat(),
                    "gap_hours": _round_hours(gap),
                    "max_gap_hours": _round_hours(max_gap_hours),
                    "detail": f"{_round_hours(gap)} hour gap since previous snapshot",
                }
            )

    for row in parsed_rows:
        for metric in METRIC_COLUMNS:
            value = _int_or_none(row.get(metric))
            if value is not None and value < 0:
                findings.append(
                    {
                        "issue_type": "negative_metric",
                        "platform": platform,
                        "fetched_at": row["_fetched_at"].isoformat(),
                        "metric": metric,
                        "value": value,
                        "detail": f"{metric} is {value}",
                    }
                )
    return findings


def _load_snapshots(conn: sqlite3.Connection, columns: set[str], *, platform: str) -> list[dict[str, Any]]:
    normalized_platform = _clean(platform).lower() or DEFAULT_PLATFORM
    sequence_expr = "id" if "id" in columns else "rowid"
    params: list[Any] = []
    where = ""
    if normalized_platform != DEFAULT_PLATFORM:
        where = "WHERE lower(platform) = ?"
        params.append(normalized_platform)
    rows = conn.execute(
        f"""SELECT
               {sequence_expr} AS snapshot_id,
               platform,
               follower_count,
               following_count,
               tweet_count,
               listed_count,
               fetched_at
           FROM profile_metrics
           {where}
           ORDER BY platform ASC, {sequence_expr} ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {
            str(info[1]) for info in conn.execute(f"PRAGMA table_info({table})")
        }
    return schema


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    return (
        finding["platform"],
        ISSUE_TYPES.index(finding["issue_type"]),
        _clean(finding.get("fetched_at")),
        _clean(finding.get("metric")),
    )


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return _utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def _hours_between(start: datetime, end: datetime) -> float:
    return (end - start).total_seconds() / 3600


def _round_hours(value: float) -> float:
    return round(float(value), 2)


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
