"""Compare actual publication cadence against configured channel targets."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Mapping


DEFAULT_LOOKBACK_DAYS = 28
DEFAULT_TARGETS = {"x": 5, "bluesky": 5}
OVER_POST_THRESHOLD = 1.25
UNDER_POST_THRESHOLD = 0.75


@dataclass(frozen=True)
class PublishCadenceVarianceRow:
    """Expected versus actual publication count for one channel period."""

    channel: str
    period_type: str
    period_start: str
    period_end: str
    expected_count: float
    actual_count: int
    variance_count: float
    variance_percent: float | None
    status: str

    @property
    def quiet_window(self) -> bool:
        return self.expected_count > 0 and self.actual_count == 0

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["quiet_window"] = self.quiet_window
        return payload


@dataclass(frozen=True)
class PublishCadenceVarianceReport:
    """Read-only publish cadence variance report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[PublishCadenceVarianceRow, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "publish_cadence_variance",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "row_count": len(self.rows),
            "rows": [row.to_dict() for row in self.rows],
            "totals": _stable_totals(self.totals),
        }


@dataclass(frozen=True)
class _Publication:
    channel: str
    published_at: datetime


def build_publish_cadence_variance_report(
    db_or_conn: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    targets: Mapping[str, int | float] | None = None,
    channels: tuple[str, ...] | list[str] | None = None,
    now: datetime | None = None,
) -> PublishCadenceVarianceReport:
    """Return day/week publication cadence variance by channel."""
    lookback_days = _positive_int(lookback_days, "lookback_days")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    window_start = generated_at - timedelta(days=lookback_days)
    target_map = _normalise_targets(targets or DEFAULT_TARGETS)
    selected_channels = _normalise_channels(channels) or tuple(sorted(target_map))
    target_map = {
        channel: target_map.get(channel, 0.0)
        for channel in selected_channels
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if not _has_publication_source(schema, missing_columns):
        return _empty_report(
            generated_at=generated_at,
            filters=_filters(lookback_days, window_start, generated_at, target_map, selected_channels),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    publications = _load_publications(
        conn,
        schema,
        window_start=window_start,
        window_end=generated_at,
        channels=selected_channels,
    )
    rows = _build_rows(
        publications,
        target_map=target_map,
        channels=selected_channels,
        window_start=window_start,
        window_end=generated_at,
    )
    totals = _totals(rows, publications)
    return PublishCadenceVarianceReport(
        generated_at=generated_at.isoformat(),
        filters=_filters(lookback_days, window_start, generated_at, target_map, selected_channels),
        totals=totals,
        rows=tuple(rows),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_publish_cadence_variance_json(report: PublishCadenceVarianceReport) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publish_cadence_variance_text(report: PublishCadenceVarianceReport) -> str:
    """Render a compact cadence variance report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Publish Cadence Variance",
        f"Generated: {report.generated_at}",
        (
            f"Window: lookback_days={filters['lookback_days']} "
            f"start={filters['window_start']} end={filters['window_end']}"
        ),
        "Targets/week: " + _format_counts(filters["targets_per_week"]),
        (
            f"Totals: actual={totals['actual_count']} expected={totals['expected_count']} "
            f"over={totals['over_posting_count']} under={totals['under_posting_count']} "
            f"quiet={totals['quiet_window_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing optional tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + missing)
    lines.append("")

    if not report.rows:
        lines.append("No publication cadence rows available.")
        return "\n".join(lines)

    lines.append("Variance rows:")
    for row in report.rows:
        percent = "-" if row.variance_percent is None else f"{row.variance_percent:+.1f}%"
        lines.append(
            f"- {row.period_type} {row.period_start} {row.channel}: "
            f"actual={row.actual_count} expected={row.expected_count} "
            f"variance={row.variance_count:+.2f} ({percent}) status={row.status}"
        )
    return "\n".join(lines)


def _build_rows(
    publications: list[_Publication],
    *,
    target_map: dict[str, float],
    channels: tuple[str, ...],
    window_start: datetime,
    window_end: datetime,
) -> list[PublishCadenceVarianceRow]:
    actual_daily: Counter[tuple[str, str]] = Counter()
    actual_weekly: Counter[tuple[str, str]] = Counter()
    for publication in publications:
        actual_daily[(publication.channel, publication.published_at.date().isoformat())] += 1
        actual_weekly[(publication.channel, _iso_week(publication.published_at))] += 1

    rows: list[PublishCadenceVarianceRow] = []
    for channel in channels:
        weekly_target = target_map[channel]
        daily_expected = round(weekly_target / 7, 2)
        for day in _dates(window_start.date(), window_end.date()):
            rows.append(
                _row(
                    channel=channel,
                    period_type="day",
                    period_start=day.isoformat(),
                    period_end=(day + timedelta(days=1)).isoformat(),
                    expected=daily_expected,
                    actual=actual_daily[(channel, day.isoformat())],
                )
            )
        for week_start, week_end in _weeks(window_start.date(), window_end.date()):
            week_key = _iso_week(datetime.combine(week_start, datetime.min.time(), timezone.utc))
            rows.append(
                _row(
                    channel=channel,
                    period_type="week",
                    period_start=week_start.isoformat(),
                    period_end=week_end.isoformat(),
                    expected=round(weekly_target, 2),
                    actual=actual_weekly[(channel, week_key)],
                )
            )
    rows.sort(key=lambda row: (row.channel, row.period_type, row.period_start))
    return rows


def _row(
    *,
    channel: str,
    period_type: str,
    period_start: str,
    period_end: str,
    expected: float,
    actual: int,
) -> PublishCadenceVarianceRow:
    variance = round(actual - expected, 2)
    variance_percent = None
    if expected > 0:
        variance_percent = round((variance / expected) * 100, 1)
    status = _status(actual=actual, expected=expected)
    return PublishCadenceVarianceRow(
        channel=channel,
        period_type=period_type,
        period_start=period_start,
        period_end=period_end,
        expected_count=expected,
        actual_count=actual,
        variance_count=variance,
        variance_percent=variance_percent,
        status=status,
    )


def _status(*, actual: int, expected: float) -> str:
    if expected <= 0:
        return "unconfigured"
    if actual == 0:
        return "quiet_window"
    ratio = actual / expected
    if ratio > OVER_POST_THRESHOLD:
        return "over_posting"
    if ratio < UNDER_POST_THRESHOLD:
        return "under_posting"
    return "balanced"


def _load_publications(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    window_start: datetime,
    window_end: datetime,
    channels: tuple[str, ...],
) -> list[_Publication]:
    publications: list[_Publication] = []
    seen: set[tuple[str, Any, str]] = set()

    if "publication_attempts" in schema and {
        "content_id",
        "platform",
        "attempted_at",
        "success",
    }.issubset(schema["publication_attempts"]):
        placeholders = ",".join("?" for _ in channels)
        rows = conn.execute(
            f"""SELECT content_id, platform, attempted_at
                FROM publication_attempts
                WHERE success = 1
                  AND platform IN ({placeholders})
                  AND datetime(attempted_at) >= datetime(?)
                  AND datetime(attempted_at) <= datetime(?)""",
            (*channels, window_start.isoformat(), window_end.isoformat()),
        ).fetchall()
        for row in rows:
            published_at = _parse_datetime(row["attempted_at"])
            if not published_at:
                continue
            key = (str(row["platform"]), row["content_id"], published_at.isoformat())
            seen.add(key)
            publications.append(_Publication(channel=str(row["platform"]), published_at=published_at))

    if "content_publications" in schema and {
        "content_id",
        "platform",
        "status",
        "published_at",
    }.issubset(schema["content_publications"]):
        placeholders = ",".join("?" for _ in channels)
        rows = conn.execute(
            f"""SELECT content_id, platform, published_at
                FROM content_publications
                WHERE status = 'published'
                  AND platform IN ({placeholders})
                  AND datetime(published_at) >= datetime(?)
                  AND datetime(published_at) <= datetime(?)""",
            (*channels, window_start.isoformat(), window_end.isoformat()),
        ).fetchall()
        for row in rows:
            published_at = _parse_datetime(row["published_at"])
            if not published_at:
                continue
            key = (str(row["platform"]), row["content_id"], published_at.isoformat())
            if key in seen:
                continue
            seen.add(key)
            publications.append(_Publication(channel=str(row["platform"]), published_at=published_at))
    return publications


def _totals(
    rows: list[PublishCadenceVarianceRow],
    publications: list[_Publication],
) -> dict[str, Any]:
    weekly_rows = [row for row in rows if row.period_type == "week"]
    channel_counts = Counter(publication.channel for publication in publications)
    return {
        "actual_count": sum(row.actual_count for row in weekly_rows),
        "expected_count": round(sum(row.expected_count for row in weekly_rows), 2),
        "over_posting_count": sum(1 for row in rows if row.status == "over_posting"),
        "under_posting_count": sum(1 for row in rows if row.status == "under_posting"),
        "quiet_window_count": sum(1 for row in rows if row.quiet_window),
        "balanced_count": sum(1 for row in rows if row.status == "balanced"),
        "by_channel": dict(sorted(channel_counts.items())),
    }


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    expected = {
        "publication_attempts": {"content_id", "platform", "attempted_at", "success"},
        "content_publications": {"content_id", "platform", "status", "published_at"},
    }
    missing_tables = tuple(table for table in expected if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in expected.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _has_publication_source(
    schema: dict[str, set[str]],
    missing_columns: dict[str, tuple[str, ...]],
) -> bool:
    return (
        "publication_attempts" in schema
        and "publication_attempts" not in missing_columns
    ) or (
        "content_publications" in schema
        and "content_publications" not in missing_columns
    )


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> PublishCadenceVarianceReport:
    return PublishCadenceVarianceReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "actual_count": 0,
            "expected_count": 0.0,
            "over_posting_count": 0,
            "under_posting_count": 0,
            "quiet_window_count": 0,
            "balanced_count": 0,
            "by_channel": {},
        },
        rows=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _filters(
    lookback_days: int,
    window_start: datetime,
    window_end: datetime,
    targets: dict[str, float],
    channels: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "lookback_days": lookback_days,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "channels": list(channels),
        "targets_per_week": dict(sorted(targets.items())),
    }


def _normalise_targets(targets: Mapping[str, int | float]) -> dict[str, float]:
    result: dict[str, float] = {}
    for channel, value in targets.items():
        label = str(channel or "").strip().lower()
        if not label:
            raise ValueError("target channel must be non-empty")
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("target counts must be numeric") from exc
        if parsed < 0:
            raise ValueError("target counts must be non-negative")
        result[label] = round(parsed, 2)
    if not result:
        raise ValueError("at least one target is required")
    return result


def _normalise_channels(channels: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    if not channels:
        return ()
    result = tuple(sorted({str(channel).strip().lower() for channel in channels if str(channel).strip()}))
    if not result:
        raise ValueError("channels must be non-empty")
    return result


def _dates(start: date, end: date) -> list[date]:
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _weeks(start: date, end: date) -> list[tuple[date, date]]:
    first = start - timedelta(days=start.weekday())
    weeks = []
    current = first
    while current <= end:
        weeks.append((current, current + timedelta(days=7)))
        current += timedelta(days=7)
    return weeks


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    except sqlite3.Error:
        return {}
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _positive_int(value: int, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be positive") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_week(value: datetime) -> str:
    year, week, _weekday = value.isocalendar()
    return f"{year}-W{week:02d}"


def _format_counts(counts: Mapping[str, Any]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))


def _stable_totals(totals: dict[str, Any]) -> dict[str, Any]:
    result = dict(sorted(totals.items()))
    if isinstance(result.get("by_channel"), dict):
        result["by_channel"] = dict(sorted(result["by_channel"].items()))
    return result
