"""Export an operational API rate-limit reset calendar."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_STALE_AFTER_MINUTES = 60


@dataclass(frozen=True)
class ApiRateLimitCalendarRow:
    """Latest reset calendar entry for one provider/endpoint."""

    provider: str
    endpoint: str
    remaining: int
    limit: int | None
    remaining_ratio: float | None
    reset_at: str | None
    minutes_until_reset: float | None
    fetched_at: str
    snapshot_age_minutes: float
    snapshot_status: str
    depletion_status: str
    recommended_action: str
    recommended_next_poll_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ApiRateLimitCalendar:
    """API rate-limit reset calendar with filters and schema state."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[ApiRateLimitCalendarRow, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "api_rate_limit_calendar",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_api_rate_limit_calendar(
    db_or_conn: Any,
    *,
    provider: str | None = None,
    endpoint: str | None = None,
    stale_after_minutes: int = DEFAULT_STALE_AFTER_MINUTES,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ApiRateLimitCalendar:
    """Return a reset calendar built from the latest provider/endpoint snapshots."""
    if stale_after_minutes <= 0:
        raise ValueError("stale_after_minutes must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _aware(now or datetime.now(timezone.utc))
    filters = {
        "provider": provider,
        "endpoint": endpoint,
        "stale_after_minutes": stale_after_minutes,
        "limit": limit,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or _missing_required_columns(missing_columns):
        return ApiRateLimitCalendar(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals={"row_count": 0, "stale_count": 0, "unknown_reset_count": 0},
            rows=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = [
        _build_row(
            row,
            now=generated_at,
            stale_after_minutes=stale_after_minutes,
        )
        for row in _latest_snapshot_rows(
            conn,
            schema=schema,
            provider=provider,
            endpoint=endpoint,
        )
    ]
    rows.sort(key=_sort_key)
    rows = rows[:limit]
    return ApiRateLimitCalendar(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "row_count": len(rows),
            "stale_count": sum(row.snapshot_status == "stale" for row in rows),
            "unknown_reset_count": sum(
                row.recommended_action == "unknown_reset" for row in rows
            ),
        },
        rows=tuple(rows),
        missing_tables=(),
        missing_columns=missing_columns,
    )


def format_api_rate_limit_calendar_json(report: ApiRateLimitCalendar) -> str:
    """Serialize the API rate-limit calendar as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_api_rate_limit_calendar_text(report: ApiRateLimitCalendar) -> str:
    """Render the API rate-limit calendar for operators."""
    lines = [
        "API Rate-limit Reset Calendar",
        f"Generated: {report.generated_at}",
        (
            f"Filters: provider={report.filters['provider'] or '*'} "
            f"endpoint={report.filters['endpoint'] or '*'} "
            f"stale_after_minutes={report.filters['stale_after_minutes']} "
            f"limit={report.filters['limit']}"
        ),
        (
            "Totals: "
            f"rows={report.totals['row_count']} "
            f"stale={report.totals['stale_count']} "
            f"unknown_reset={report.totals['unknown_reset_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.append(f"Missing columns: {'; '.join(missing)}")
    lines.append("")

    if not report.rows:
        lines.append("No API rate-limit snapshots found.")
        return "\n".join(lines)

    columns = [
        ("provider", "PROVIDER", 10),
        ("endpoint", "ENDPOINT", 24),
        ("remaining", "REMAIN", 8),
        ("limit", "LIMIT", 8),
        ("remaining_ratio", "RATIO", 7),
        ("reset_at", "RESET_AT", 25),
        ("minutes_until_reset", "RESET_MIN", 9),
        ("snapshot_status", "SNAPSHOT", 8),
        ("depletion_status", "DEPLETION", 13),
        ("recommended_action", "ACTION", 16),
        ("recommended_next_poll_at", "NEXT_POLL_AT", 25),
    ]
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for row in report.rows:
        values = row.to_dict()
        values["remaining_ratio"] = _format_ratio(row.remaining_ratio)
        values["minutes_until_reset"] = _format_minutes(row.minutes_until_reset)
        values["reset_at"] = row.reset_at or "-"
        values["recommended_next_poll_at"] = row.recommended_next_poll_at or "-"
        lines.append(
            "  ".join(
                _format_cell(values.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)


def _latest_snapshot_rows(
    conn: sqlite3.Connection,
    *,
    schema: dict[str, set[str]],
    provider: str | None,
    endpoint: str | None,
) -> list[dict[str, Any]]:
    columns = schema["api_rate_limit_snapshots"]
    where = []
    params: list[Any] = []
    if provider:
        where.append("provider = ?")
        params.append(provider)
    if endpoint:
        where.append("endpoint = ?")
        params.append(endpoint)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
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
                        ORDER BY datetime(fetched_at) DESC, fetched_at DESC, id DESC
                    ) AS rn
                FROM api_rate_limit_snapshots
                {where_sql}
            )
            WHERE rn = 1
            ORDER BY provider ASC, endpoint ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _build_row(
    row: dict[str, Any],
    *,
    now: datetime,
    stale_after_minutes: int,
) -> ApiRateLimitCalendarRow:
    remaining = _int(row.get("remaining"), default=0)
    limit = _optional_int(row.get("limit_value"))
    remaining_ratio = _remaining_ratio(remaining, limit)
    reset_time = _parse_timestamp(row.get("reset_at"))
    fetched_time = _parse_timestamp(row.get("fetched_at")) or now
    snapshot_age_minutes = round((now - fetched_time).total_seconds() / 60, 2)
    snapshot_status = (
        "stale" if snapshot_age_minutes > stale_after_minutes else "fresh"
    )
    minutes_until_reset = (
        round((reset_time - now).total_seconds() / 60, 2)
        if reset_time is not None
        else None
    )
    depletion_status = _depletion_status(remaining, remaining_ratio)
    recommended_action = _recommended_action(
        reset_time=reset_time,
        snapshot_status=snapshot_status,
        remaining=remaining,
        remaining_ratio=remaining_ratio,
        now=now,
    )
    return ApiRateLimitCalendarRow(
        provider=str(row.get("provider") or "unknown"),
        endpoint=str(row.get("endpoint") or "default"),
        remaining=remaining,
        limit=limit,
        remaining_ratio=remaining_ratio,
        reset_at=reset_time.isoformat() if reset_time is not None else None,
        minutes_until_reset=minutes_until_reset,
        fetched_at=fetched_time.isoformat(),
        snapshot_age_minutes=snapshot_age_minutes,
        snapshot_status=snapshot_status,
        depletion_status=depletion_status,
        recommended_action=recommended_action,
        recommended_next_poll_at=_recommended_next_poll_at(
            recommended_action=recommended_action,
            reset_time=reset_time,
            now=now,
        ),
    )


def _depletion_status(remaining: int, remaining_ratio: float | None) -> str:
    if remaining <= 0:
        return "depleted"
    if remaining_ratio is None:
        return "unknown_limit"
    if remaining_ratio <= 0.1:
        return "low"
    return "available"


def _recommended_action(
    *,
    reset_time: datetime | None,
    snapshot_status: str,
    remaining: int,
    remaining_ratio: float | None,
    now: datetime,
) -> str:
    if reset_time is None:
        return "unknown_reset"
    if snapshot_status == "stale":
        return "refresh_snapshot"
    if reset_time <= now:
        return "poll_now"
    if remaining <= 0:
        return "wait_for_reset"
    if remaining_ratio is not None and remaining_ratio <= 0.1:
        return "poll_after_reset"
    return "poll_now"


def _recommended_next_poll_at(
    *,
    recommended_action: str,
    reset_time: datetime | None,
    now: datetime,
) -> str | None:
    if recommended_action == "unknown_reset":
        return None
    if recommended_action in {"poll_after_reset", "wait_for_reset"}:
        return reset_time.isoformat() if reset_time is not None else None
    return now.isoformat()


def _sort_key(row: ApiRateLimitCalendarRow) -> tuple[Any, ...]:
    return (
        row.recommended_next_poll_at is None,
        row.recommended_next_poll_at or "9999",
        row.provider,
        row.endpoint,
    )


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    if "api_rate_limit_snapshots" not in schema:
        return ("api_rate_limit_snapshots",), {}
    required = {"provider", "endpoint", "remaining", "fetched_at"}
    optional = {"limit_value", "reset_at"}
    missing = tuple(
        sorted((required | optional) - schema["api_rate_limit_snapshots"])
    )
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


def _remaining_ratio(remaining: int, limit: int | None) -> float | None:
    if limit is None or limit <= 0:
        return None
    return round(remaining / limit, 4)


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


def _format_ratio(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.4f}"


def _format_minutes(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}"


def _format_cell(value: Any, width: int) -> str:
    text = "-" if value is None or value == "" else str(value)
    if len(text) <= width:
        return text
    return text[: max(width - 3, 0)] + "..."


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)
