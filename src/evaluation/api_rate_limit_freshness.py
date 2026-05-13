"""Report freshness and reset issues for API rate-limit snapshots."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_LOW_REMAINING = 10
DEFAULT_RESET_OVERDUE_MINUTES = 5
DEFAULT_STALE_AFTER_MINUTES = 60


@dataclass(frozen=True)
class ApiRateLimitLatestSnapshot:
    """Latest stored snapshot for one provider/endpoint."""

    provider: str
    endpoint: str
    remaining: int
    limit: int | None
    reset_at: str | None
    fetched_at: str
    snapshot_age_minutes: float
    minutes_until_reset: float | None
    finding_labels: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["finding_labels"] = list(self.finding_labels)
        return payload


@dataclass(frozen=True)
class ApiRateLimitFreshnessReport:
    """Deterministic operational report for rate-limit snapshot health."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    findings: tuple[dict[str, Any], ...]
    latest_snapshots: tuple[ApiRateLimitLatestSnapshot, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "api_rate_limit_freshness",
            "filters": dict(self.filters),
            "findings": [dict(finding) for finding in self.findings],
            "generated_at": self.generated_at,
            "latest_snapshots": [
                snapshot.to_dict() for snapshot in self.latest_snapshots
            ],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_api_rate_limit_freshness_report(
    db_or_conn: Any,
    *,
    low_remaining: int = DEFAULT_LOW_REMAINING,
    stale_after_minutes: int = DEFAULT_STALE_AFTER_MINUTES,
    reset_overdue_minutes: int = DEFAULT_RESET_OVERDUE_MINUTES,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ApiRateLimitFreshnessReport:
    """Build a read-only freshness report from latest provider/endpoint snapshots."""
    if low_remaining <= 0:
        raise ValueError("low_remaining must be positive")
    if stale_after_minutes <= 0:
        raise ValueError("stale_after_minutes must be positive")
    if reset_overdue_minutes <= 0:
        raise ValueError("reset_overdue_minutes must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _aware(now or datetime.now(timezone.utc))
    filters = {
        "limit": limit,
        "low_remaining": low_remaining,
        "reset_overdue_minutes": reset_overdue_minutes,
        "stale_after_minutes": stale_after_minutes,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or _missing_required_columns(missing_columns):
        return ApiRateLimitFreshnessReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals=_totals((), ()),
            findings=(),
            latest_snapshots=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    snapshots = tuple(
        _snapshot_for_row(
            row,
            now=generated_at,
            low_remaining=low_remaining,
            stale_after_minutes=stale_after_minutes,
            reset_overdue_minutes=reset_overdue_minutes,
        )
        for row in _latest_snapshot_rows(conn, schema=schema)
    )
    findings = tuple(
        sorted(
            (
                finding
                for snapshot in snapshots
                for finding in _findings_for_snapshot(snapshot)
            ),
            key=_finding_sort_key,
        )
    )
    ordered_snapshots = tuple(sorted(snapshots, key=_snapshot_sort_key)[:limit])
    return ApiRateLimitFreshnessReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(ordered_snapshots, findings),
        findings=findings,
        latest_snapshots=ordered_snapshots,
        missing_columns=missing_columns,
    )


def format_api_rate_limit_freshness_json(report: ApiRateLimitFreshnessReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_api_rate_limit_freshness_text(report: ApiRateLimitFreshnessReport) -> str:
    """Render a compact human-readable freshness report."""
    lines = [
        "API Rate-limit Freshness",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"low_remaining={report.filters['low_remaining']} "
            f"stale_after_minutes={report.filters['stale_after_minutes']} "
            f"reset_overdue_minutes={report.filters['reset_overdue_minutes']} "
            f"limit={report.filters['limit']}"
        ),
        (
            "Totals: "
            f"snapshots={report.totals['snapshot_count']} "
            f"findings={report.totals['finding_count']} "
            f"low_remaining={report.totals['low_remaining_count']} "
            f"stale={report.totals['stale_snapshot_count']} "
            f"reset_overdue={report.totals['reset_overdue_count']} "
            f"missing_reset_at={report.totals['missing_reset_at_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in sorted(report.missing_columns.items())
                for column in columns
            )
        )
    if not report.latest_snapshots:
        lines.append("No API rate-limit snapshots found.")
        return "\n".join(lines)

    lines.extend(["", "Latest snapshots:"])
    columns = [
        ("provider", "PROVIDER", 10),
        ("endpoint", "ENDPOINT", 24),
        ("remaining", "REMAIN", 8),
        ("limit", "LIMIT", 8),
        ("reset_at", "RESET_AT", 25),
        ("fetched_at", "FETCHED_AT", 25),
        ("snapshot_age_minutes", "AGE_MIN", 8),
        ("finding_labels", "FINDINGS", 48),
    ]
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for snapshot in report.latest_snapshots:
        values = snapshot.to_dict()
        values["limit"] = snapshot.limit if snapshot.limit is not None else "-"
        values["reset_at"] = snapshot.reset_at or "-"
        values["snapshot_age_minutes"] = f"{snapshot.snapshot_age_minutes:.2f}"
        values["finding_labels"] = ",".join(snapshot.finding_labels) or "-"
        lines.append(
            "  ".join(
                _format_cell(values.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )

    lines.append("")
    lines.append("Findings:")
    if not report.findings:
        lines.append("No API rate-limit freshness findings.")
    else:
        for finding in report.findings:
            lines.append(
                "- "
                f"{finding['label']} {finding['provider']}:{finding['endpoint']} "
                f"remaining={finding['remaining']} "
                f"reset_at={finding['reset_at'] or '-'} "
                f"fetched_at={finding['fetched_at']}"
            )
    return "\n".join(lines)


def _latest_snapshot_rows(
    conn: sqlite3.Connection,
    *,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    columns = schema["api_rate_limit_snapshots"]
    rows = conn.execute(
        f"""SELECT id, provider, endpoint, remaining, limit_value, reset_at, fetched_at
            FROM (
                SELECT
                    {_column_expr(columns, "id", "rowid")} AS id,
                    provider,
                    endpoint,
                    remaining,
                    {_column_expr(columns, "limit_value", "NULL")} AS limit_value,
                    {_column_expr(columns, "reset_at", "NULL")} AS reset_at,
                    fetched_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY provider, endpoint
                        ORDER BY datetime(fetched_at) DESC,
                                 fetched_at DESC,
                                 {_column_expr(columns, "id", "rowid")} DESC
                    ) AS rn
                FROM api_rate_limit_snapshots
            )
            WHERE rn = 1
            ORDER BY provider ASC, endpoint ASC""",
    ).fetchall()
    return [dict(row) for row in rows]


def _snapshot_for_row(
    row: dict[str, Any],
    *,
    now: datetime,
    low_remaining: int,
    stale_after_minutes: int,
    reset_overdue_minutes: int,
) -> ApiRateLimitLatestSnapshot:
    remaining = _int(row.get("remaining"), default=0)
    reset_time = _parse_timestamp(row.get("reset_at"))
    fetched_time = _parse_timestamp(row.get("fetched_at")) or now
    age_minutes = round((now - fetched_time).total_seconds() / 60, 2)
    minutes_until_reset = (
        round((reset_time - now).total_seconds() / 60, 2)
        if reset_time is not None
        else None
    )
    labels = _labels(
        remaining=remaining,
        reset_time=reset_time,
        fetched_time=fetched_time,
        now=now,
        low_remaining=low_remaining,
        stale_after_minutes=stale_after_minutes,
        reset_overdue_minutes=reset_overdue_minutes,
    )
    return ApiRateLimitLatestSnapshot(
        provider=str(row.get("provider") or "unknown"),
        endpoint=str(row.get("endpoint") or "default"),
        remaining=remaining,
        limit=_optional_int(row.get("limit_value")),
        reset_at=reset_time.isoformat() if reset_time is not None else None,
        fetched_at=fetched_time.isoformat(),
        snapshot_age_minutes=age_minutes,
        minutes_until_reset=minutes_until_reset,
        finding_labels=labels,
    )


def _labels(
    *,
    remaining: int,
    reset_time: datetime | None,
    fetched_time: datetime,
    now: datetime,
    low_remaining: int,
    stale_after_minutes: int,
    reset_overdue_minutes: int,
) -> tuple[str, ...]:
    labels: list[str] = []
    if remaining <= low_remaining:
        labels.append("low_remaining")
    if fetched_time < now - timedelta(minutes=stale_after_minutes):
        labels.append("stale_snapshot")
    if reset_time is None:
        labels.append("missing_reset_at")
    elif reset_time < now - timedelta(minutes=reset_overdue_minutes):
        labels.append("reset_overdue")
    return tuple(labels)


def _findings_for_snapshot(
    snapshot: ApiRateLimitLatestSnapshot,
) -> list[dict[str, Any]]:
    return [
        {
            "label": label,
            "provider": snapshot.provider,
            "endpoint": snapshot.endpoint,
            "remaining": snapshot.remaining,
            "limit": snapshot.limit,
            "reset_at": snapshot.reset_at,
            "fetched_at": snapshot.fetched_at,
            "snapshot_age_minutes": snapshot.snapshot_age_minutes,
            "minutes_until_reset": snapshot.minutes_until_reset,
        }
        for label in snapshot.finding_labels
    ]


def _totals(
    snapshots: tuple[ApiRateLimitLatestSnapshot, ...],
    findings: tuple[dict[str, Any], ...],
) -> dict[str, int]:
    labels = [finding["label"] for finding in findings]
    return {
        "finding_count": len(findings),
        "low_remaining_count": labels.count("low_remaining"),
        "missing_reset_at_count": labels.count("missing_reset_at"),
        "reset_overdue_count": labels.count("reset_overdue"),
        "snapshot_count": len(snapshots),
        "stale_snapshot_count": labels.count("stale_snapshot"),
    }


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    if "api_rate_limit_snapshots" not in schema:
        return ("api_rate_limit_snapshots",), {}
    required = {"provider", "endpoint", "remaining", "fetched_at"}
    optional = {"id", "limit_value", "reset_at"}
    missing = tuple(sorted((required | optional) - schema["api_rate_limit_snapshots"]))
    return (), {"api_rate_limit_snapshots": missing} if missing else {}


def _missing_required_columns(
    missing_columns: dict[str, tuple[str, ...]]
) -> bool:
    required = {"provider", "endpoint", "remaining", "fetched_at"}
    return bool(
        required.intersection(missing_columns.get("api_rate_limit_snapshots", ()))
    )


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _snapshot_sort_key(snapshot: ApiRateLimitLatestSnapshot) -> tuple[Any, ...]:
    return (
        not snapshot.finding_labels,
        snapshot.provider,
        snapshot.endpoint,
    )


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    order = {
        "low_remaining": 0,
        "stale_snapshot": 1,
        "reset_overdue": 2,
        "missing_reset_at": 3,
    }
    return (
        order.get(str(finding["label"]), 99),
        finding["provider"],
        finding["endpoint"],
    )


def _column_expr(columns: set[str], name: str, fallback: str) -> str:
    return name if name in columns else fallback


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _format_cell(value: Any, width: int) -> str:
    text = "-" if value is None or value == "" else str(value)
    if len(text) <= width:
        return text
    return text[: max(width - 3, 0)] + "..."


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)
