"""Report current publication reliability streaks by platform."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any

from output.publish_errors import normalize_error_category


DEFAULT_DAYS = 7
DEFAULT_FAILURE_THRESHOLD = 3


@dataclass(frozen=True)
class PublicationAttemptRecord:
    """Normalized publication attempt input used for streak calculations."""

    id: int | None
    platform: str
    attempted_at: str
    success: bool
    error_category: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PlatformReliabilityStreak:
    """Reliability streak summary for one publication platform."""

    platform: str
    total_attempts: int
    success_count: int
    failure_count: int
    success_rate: float
    current_streak_type: str
    current_streak_count: int
    longest_failure_streak: int
    most_recent_attempt_at: str | None
    most_recent_error_category: str | None
    needs_attention: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PublicationReliabilityStreakReport:
    """Publication reliability streak report plus applied filters."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    platforms: tuple[PlatformReliabilityStreak, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "platforms": [platform.to_dict() for platform in self.platforms],
            "totals": dict(sorted(self.totals.items())),
        }


def build_publication_reliability_streak_report(
    db_or_conn: Any | None = None,
    *,
    attempts: list[PublicationAttemptRecord | dict[str, Any]] | None = None,
    days: int = DEFAULT_DAYS,
    failure_threshold: int = DEFAULT_FAILURE_THRESHOLD,
    now: datetime | None = None,
) -> PublicationReliabilityStreakReport:
    """Calculate per-platform publication success and failure streaks."""
    if days <= 0:
        raise ValueError("days must be positive")
    if failure_threshold <= 0:
        raise ValueError("failure_threshold must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff_dt = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "cutoff": cutoff_dt.isoformat(),
        "failure_threshold": failure_threshold,
    }

    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = {}
    if attempts is None:
        conn = _connection(db_or_conn)
        schema = _schema(conn)
        missing_tables, missing_columns = _schema_gaps(schema)
        if missing_tables or missing_columns:
            return _report(
                generated_at=generated_at,
                filters=filters,
                platforms=(),
                missing_tables=missing_tables,
                missing_columns=missing_columns,
            )
        records = _load_attempts(conn, cutoff=cutoff_dt.isoformat())
    else:
        records = [_normalize_attempt(item) for item in attempts]
        records = [
            record
            for record in records
            if (attempted_at := _parse_timestamp(record.attempted_at)) is not None
            and attempted_at >= cutoff_dt
        ]

    grouped: dict[str, list[PublicationAttemptRecord]] = {}
    for record in records:
        grouped.setdefault(record.platform, []).append(record)

    platforms = tuple(
        _summarize_platform(platform, items, failure_threshold=failure_threshold)
        for platform, items in sorted(grouped.items())
    )
    return _report(
        generated_at=generated_at,
        filters=filters,
        platforms=platforms,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_publication_reliability_streak_json(
    report: PublicationReliabilityStreakReport,
) -> str:
    """Render publication reliability streaks as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publication_reliability_streak_text(
    report: PublicationReliabilityStreakReport,
) -> str:
    """Render publication reliability streaks for terminal review."""
    lines = [
        "Publication Reliability Streaks",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['days']} days",
        f"Failure threshold: {report.filters['failure_threshold']}",
        (
            "Totals: "
            f"platforms={report.totals['platform_count']} "
            f"attempts={report.totals['total_attempts']} "
            f"attention={report.totals['attention_platform_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + missing)
    lines.append("")

    if not report.platforms:
        lines.append("No publication attempts found.")
        return "\n".join(lines)

    lines.append("Platforms:")
    for row in report.platforms:
        lines.append(
            "- "
            f"{row.platform}: "
            f"current={row.current_streak_type}:{row.current_streak_count} "
            f"longest_failure={row.longest_failure_streak} "
            f"attempts={row.total_attempts} "
            f"success_rate={_format_percent(row.success_rate)} "
            f"latest_error={row.most_recent_error_category or '-'} "
            f"attention={'yes' if row.needs_attention else 'no'}"
        )
    return "\n".join(lines)


def _load_attempts(
    conn: sqlite3.Connection,
    *,
    cutoff: str,
) -> list[PublicationAttemptRecord]:
    rows = conn.execute(
        """SELECT id, platform, attempted_at, success, error_category
           FROM publication_attempts
           WHERE attempted_at >= ?
           ORDER BY platform ASC, attempted_at ASC, id ASC""",
        (cutoff,),
    ).fetchall()
    return [_normalize_attempt(dict(row)) for row in rows]


def _summarize_platform(
    platform: str,
    attempts: list[PublicationAttemptRecord],
    *,
    failure_threshold: int,
) -> PlatformReliabilityStreak:
    ordered = sorted(
        attempts,
        key=lambda item: (
            _parse_timestamp(item.attempted_at) or datetime.min.replace(tzinfo=timezone.utc),
            item.id or 0,
        ),
    )
    success_count = sum(1 for attempt in ordered if attempt.success)
    failure_count = len(ordered) - success_count
    current_type = "none"
    current_count = 0
    longest_failure = 0
    active_failure = 0
    most_recent_error: str | None = None

    for attempt in ordered:
        if attempt.success:
            current_count = current_count + 1 if current_type == "success" else 1
            current_type = "success"
            active_failure = 0
            continue

        current_count = current_count + 1 if current_type == "failure" else 1
        current_type = "failure"
        active_failure += 1
        longest_failure = max(longest_failure, active_failure)
        most_recent_error = normalize_error_category(attempt.error_category)

    latest = ordered[-1].attempted_at if ordered else None
    success_rate = _rate(success_count, len(ordered))
    return PlatformReliabilityStreak(
        platform=platform,
        total_attempts=len(ordered),
        success_count=success_count,
        failure_count=failure_count,
        success_rate=success_rate,
        current_streak_type=current_type,
        current_streak_count=current_count,
        longest_failure_streak=longest_failure,
        most_recent_attempt_at=latest,
        most_recent_error_category=most_recent_error,
        needs_attention=current_type == "failure" and current_count >= failure_threshold,
    )


def _report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    platforms: tuple[PlatformReliabilityStreak, ...],
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> PublicationReliabilityStreakReport:
    total_attempts = sum(row.total_attempts for row in platforms)
    success_count = sum(row.success_count for row in platforms)
    failure_count = sum(row.failure_count for row in platforms)
    return PublicationReliabilityStreakReport(
        artifact_type="publication_reliability_streaks",
        generated_at=generated_at.isoformat(),
        filters=dict(filters),
        totals={
            "attention_platform_count": sum(1 for row in platforms if row.needs_attention),
            "failure_count": failure_count,
            "platform_count": len(platforms),
            "success_count": success_count,
            "success_rate": _rate(success_count, total_attempts),
            "total_attempts": total_attempts,
        },
        platforms=platforms,
        missing_tables=missing_tables,
        missing_columns=missing_columns or {},
    )


def _normalize_attempt(
    item: PublicationAttemptRecord | dict[str, Any],
) -> PublicationAttemptRecord:
    if isinstance(item, PublicationAttemptRecord):
        return item
    platform = str(item.get("platform") or "").strip()
    if not platform:
        raise ValueError("attempt platform must not be blank")
    attempted_at = item.get("attempted_at")
    if attempted_at is None:
        raise ValueError("attempted_at is required")
    success = _is_success(item.get("success"))
    return PublicationAttemptRecord(
        id=int(item["id"]) if item.get("id") is not None else None,
        platform=platform,
        attempted_at=str(attempted_at),
        success=success,
        error_category=(
            normalize_error_category(item.get("error_category"))
            if not success
            else None
        ),
    )


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "publication_attempts": {
            "id",
            "platform",
            "attempted_at",
            "success",
            "error_category",
        },
    }
    missing_tables = tuple(table for table in sorted(required) if table not in schema)
    missing_columns = {
        table: tuple(column for column in sorted(columns) if column not in schema.get(table, set()))
        for table, columns in required.items()
        if table in schema
        and any(column not in schema.get(table, set()) for column in columns)
    }
    return missing_tables, missing_columns


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


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if conn is None:
        raise ValueError("db_or_conn is required when attempts are not provided")
    return conn


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _as_utc(value)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _is_success(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def _format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"
