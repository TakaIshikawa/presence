"""Analyze published content spacing by channel."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LONG_GAP_HOURS = 72.0
DEFAULT_BURST_THRESHOLD = 2
DEFAULT_UNEVEN_RATIO = 2.0


@dataclass(frozen=True)
class ContentCalendarSpacingRow:
    channel: str
    publication_count: int
    average_gap_hours: float | None
    max_gap_hours: float | None
    burst_day_count: int
    spacing_status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ContentCalendarSpacingReport:
    generated_at: str
    filters: dict[str, Any]
    rows: tuple[ContentCalendarSpacingRow, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "content_calendar_spacing",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "rows": [row.to_dict() for row in self.rows],
        }


def build_content_calendar_spacing_report(
    db_or_conn: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    long_gap_hours: float = DEFAULT_LONG_GAP_HOURS,
    burst_threshold: int = DEFAULT_BURST_THRESHOLD,
    uneven_ratio: float = DEFAULT_UNEVEN_RATIO,
    now: datetime | None = None,
) -> ContentCalendarSpacingReport:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if long_gap_hours <= 0:
        raise ValueError("long_gap_hours must be positive")
    if burst_threshold <= 1:
        raise ValueError("burst_threshold must be greater than 1")
    if uneven_ratio <= 1:
        raise ValueError("uneven_ratio must be greater than 1")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    conn = _connection(db_or_conn)
    publications = _load_publications(conn, cutoff)
    by_channel: dict[str, list[datetime]] = defaultdict(list)
    for publication in publications:
        by_channel[publication["channel"]].append(publication["published_at"])

    rows = [
        _row(channel, sorted(times), long_gap_hours=long_gap_hours, burst_threshold=burst_threshold, uneven_ratio=uneven_ratio)
        for channel, times in by_channel.items()
    ]
    rows.sort(key=lambda item: (item.channel, _severity_rank(item.spacing_status)))
    return ContentCalendarSpacingReport(
        generated_at=generated_at.isoformat(),
        filters={
            "lookback_days": lookback_days,
            "long_gap_hours": long_gap_hours,
            "burst_threshold": burst_threshold,
            "uneven_ratio": uneven_ratio,
            "lookback_start": cutoff.isoformat(),
        },
        rows=tuple(rows),
    )


def format_content_calendar_spacing_json(report: ContentCalendarSpacingReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_calendar_spacing_table(report: ContentCalendarSpacingReport) -> str:
    lines = [
        "Content Calendar Spacing",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['lookback_days']} days",
        "",
        "channel | publication_count | average_gap_hours | max_gap_hours | burst_day_count | spacing_status",
    ]
    if not report.rows:
        lines.append("No published content found.")
        return "\n".join(lines)
    for row in report.rows:
        lines.append(
            " | ".join(
                [
                    row.channel,
                    str(row.publication_count),
                    _fmt(row.average_gap_hours),
                    _fmt(row.max_gap_hours),
                    str(row.burst_day_count),
                    row.spacing_status,
                ]
            )
        )
    return "\n".join(lines)


def _load_publications(conn: sqlite3.Connection, cutoff: datetime) -> list[dict[str, Any]]:
    schema = _schema(conn)
    records: list[dict[str, Any]] = []
    cp = schema.get("content_publications", set())
    if {"platform", "status", "published_at"}.issubset(cp):
        rows = conn.execute(
            """SELECT platform, published_at
               FROM content_publications
               WHERE status = 'published'
                 AND published_at IS NOT NULL
                 AND datetime(published_at) >= datetime(?)
               ORDER BY platform ASC, published_at ASC""",
            (cutoff.isoformat(),),
        ).fetchall()
        records.extend(_publication(str(row["platform"] or "unknown"), row["published_at"]) for row in rows)
    gc = schema.get("generated_content", set())
    if {"content_type", "published_at", "published"}.issubset(gc):
        rows = conn.execute(
            """SELECT content_type, published_at
               FROM generated_content
               WHERE COALESCE(published, 0) = 1
                 AND published_at IS NOT NULL
                 AND datetime(published_at) >= datetime(?)
               ORDER BY content_type ASC, published_at ASC""",
            (cutoff.isoformat(),),
        ).fetchall()
        records.extend(_publication(str(row["content_type"] or "unknown"), row["published_at"]) for row in rows)
    return [record for record in records if record["published_at"] is not None]


def _publication(channel: str, published_at: Any) -> dict[str, Any]:
    return {"channel": channel, "published_at": _parse_datetime(published_at)}


def _row(
    channel: str,
    times: list[datetime],
    *,
    long_gap_hours: float,
    burst_threshold: int,
    uneven_ratio: float,
) -> ContentCalendarSpacingRow:
    gaps = [(times[index] - times[index - 1]).total_seconds() / 3600 for index in range(1, len(times))]
    average_gap = round(sum(gaps) / len(gaps), 2) if gaps else None
    max_gap = round(max(gaps), 2) if gaps else None
    by_day: dict[str, int] = defaultdict(int)
    for value in times:
        by_day[value.date().isoformat()] += 1
    burst_day_count = sum(count >= burst_threshold for count in by_day.values())
    statuses: list[str] = []
    if max_gap is not None and max_gap > long_gap_hours:
        statuses.append("long_gap")
    if burst_day_count:
        statuses.append("same_day_burst")
    if gaps and max_gap is not None and average_gap is not None and max_gap > average_gap * uneven_ratio:
        statuses.append("uneven_cadence")
    return ContentCalendarSpacingRow(
        channel=channel,
        publication_count=len(times),
        average_gap_hours=average_gap,
        max_gap_hours=max_gap,
        burst_day_count=burst_day_count,
        spacing_status=",".join(statuses) if statuses else "healthy",
    )


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _severity_rank(status: str) -> int:
    if "long_gap" in status:
        return 0
    if "same_day_burst" in status:
        return 1
    if "uneven_cadence" in status:
        return 2
    return 3


def _fmt(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}"
