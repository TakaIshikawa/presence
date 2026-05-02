"""Read-only publication cadence anomaly report."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LOOKAHEAD_DAYS = 14
DEFAULT_WINDOW_HOURS = 2
DEFAULT_MAX_POSTS_PER_WINDOW = 3
DEFAULT_MAX_GAP_HOURS = 48
DEFAULT_REPEATED_HOUR_THRESHOLD = 3
DEFAULT_LIMIT = 50
ACTIVE_QUEUE_STATUSES = ("queued", "held")
PUBLISHED_STATUSES = ("published",)
SUPPORTED_PLATFORMS = ("x", "bluesky")


@dataclass(frozen=True)
class CadenceEvent:
    """One effective publication event in the cadence timeline."""

    platform: str
    timestamp: str
    source: str
    source_id: int | None
    content_id: int | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BurstWindow:
    """A dense platform-specific posting window."""

    platform: str
    start_at: str
    end_at: str
    window_hours: int
    post_count: int
    threshold: int
    events: tuple[CadenceEvent, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "start_at": self.start_at,
            "end_at": self.end_at,
            "window_hours": self.window_hours,
            "post_count": self.post_count,
            "threshold": self.threshold,
            "events": [event.to_dict() for event in self.events],
        }


@dataclass(frozen=True)
class SilenceWindow:
    """A long platform-specific gap between adjacent events."""

    platform: str
    previous_at: str
    next_at: str
    gap_hours: float
    threshold_hours: int
    previous_event: CadenceEvent
    next_event: CadenceEvent

    def to_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "previous_at": self.previous_at,
            "next_at": self.next_at,
            "gap_hours": self.gap_hours,
            "threshold_hours": self.threshold_hours,
            "previous_event": self.previous_event.to_dict(),
            "next_event": self.next_event.to_dict(),
        }


@dataclass(frozen=True)
class RepeatedHourPattern:
    """Repeated posting at the same platform-local UTC hour."""

    platform: str
    hour: int
    event_count: int
    threshold: int
    first_at: str
    last_at: str
    day_count: int
    events: tuple[CadenceEvent, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "hour": self.hour,
            "event_count": self.event_count,
            "threshold": self.threshold,
            "first_at": self.first_at,
            "last_at": self.last_at,
            "day_count": self.day_count,
            "events": [event.to_dict() for event in self.events],
        }


@dataclass(frozen=True)
class PublicationCadenceAnomalyReport:
    """Publication cadence timeline and anomaly findings."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    timeline: tuple[CadenceEvent, ...]
    hour_buckets: dict[str, int]
    day_buckets: dict[str, int]
    bursts: tuple[BurstWindow, ...]
    silences: tuple[SilenceWindow, ...]
    repeated_hours: tuple[RepeatedHourPattern, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "publication_cadence_anomaly",
            "day_buckets": dict(sorted(self.day_buckets.items())),
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "hour_buckets": dict(sorted(self.hour_buckets.items())),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "bursts": [burst.to_dict() for burst in self.bursts],
            "repeated_hours": [pattern.to_dict() for pattern in self.repeated_hours],
            "silences": [silence.to_dict() for silence in self.silences],
            "timeline": [event.to_dict() for event in self.timeline],
            "totals": dict(sorted(self.totals.items())),
        }


def build_publication_cadence_anomaly_report(
    db_or_conn: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    lookahead_days: int = DEFAULT_LOOKAHEAD_DAYS,
    window_hours: int = DEFAULT_WINDOW_HOURS,
    max_posts_per_window: int = DEFAULT_MAX_POSTS_PER_WINDOW,
    max_gap_hours: int = DEFAULT_MAX_GAP_HOURS,
    repeated_hour_threshold: int = DEFAULT_REPEATED_HOUR_THRESHOLD,
    platform: str = "all",
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> PublicationCadenceAnomalyReport:
    """Detect bursts, long gaps, and repeated same-hour publication patterns."""
    if lookback_days < 0:
        raise ValueError("lookback_days must be non-negative")
    if lookahead_days < 0:
        raise ValueError("lookahead_days must be non-negative")
    if window_hours <= 0:
        raise ValueError("window_hours must be positive")
    if max_posts_per_window <= 0:
        raise ValueError("max_posts_per_window must be positive")
    if max_gap_hours <= 0:
        raise ValueError("max_gap_hours must be positive")
    if repeated_hour_threshold <= 0:
        raise ValueError("repeated_hour_threshold must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    selected_platform = _normalize_platform(platform)
    if selected_platform != "all" and selected_platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"unsupported platform: {platform}")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    start = generated_at - timedelta(days=lookback_days)
    end = generated_at + timedelta(days=lookahead_days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: set[str] = set()
    missing_columns: dict[str, set[str]] = defaultdict(set)

    events = _load_events(
        conn,
        schema=schema,
        start=start,
        end=end,
        platform=selected_platform,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    events.sort(key=lambda event: (event.platform, event.timestamp, event.source, event.source_id or 0))
    timeline = tuple(events[:limit])
    by_platform = _events_by_platform(events)
    bursts = _detect_bursts(
        by_platform,
        window_hours=window_hours,
        max_posts_per_window=max_posts_per_window,
        limit=limit,
    )
    silences = _detect_silences(
        by_platform,
        max_gap_hours=max_gap_hours,
        limit=limit,
    )
    repeated_hours = _detect_repeated_hours(
        by_platform,
        repeated_hour_threshold=repeated_hour_threshold,
        limit=limit,
    )

    return PublicationCadenceAnomalyReport(
        generated_at=generated_at.isoformat(),
        filters={
            "lookback_days": lookback_days,
            "lookahead_days": lookahead_days,
            "window_hours": window_hours,
            "max_posts_per_window": max_posts_per_window,
            "max_gap_hours": max_gap_hours,
            "repeated_hour_threshold": repeated_hour_threshold,
            "platform": selected_platform,
            "limit": limit,
            "timeline_start": start.isoformat(),
            "timeline_end": end.isoformat(),
        },
        totals={
            "event_count": len(events),
            "timeline_count": len(timeline),
            "burst_count": len(bursts),
            "silence_count": len(silences),
            "repeated_hour_count": len(repeated_hours),
            "published_count": sum(event.status == "published" for event in events),
            "queued_count": sum(event.status in ACTIVE_QUEUE_STATUSES for event in events),
            "platform_count": len(by_platform),
        },
        timeline=timeline,
        hour_buckets=bucket_events_by_hour(events),
        day_buckets=bucket_events_by_day(events),
        bursts=tuple(bursts),
        silences=tuple(silences),
        repeated_hours=tuple(repeated_hours),
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns={
            table: tuple(sorted(columns))
            for table, columns in sorted(missing_columns.items())
            if columns
        },
    )


def bucket_events_by_hour(events: list[CadenceEvent] | tuple[CadenceEvent, ...]) -> dict[str, int]:
    """Return UTC hour buckets for cadence events."""
    counts: Counter[str] = Counter()
    for event in events:
        timestamp = _parse_timestamp(event.timestamp)
        if timestamp is not None:
            counts[timestamp.strftime("%Y-%m-%dT%H:00:00+00:00")] += 1
    return dict(sorted(counts.items()))


def bucket_events_by_day(events: list[CadenceEvent] | tuple[CadenceEvent, ...]) -> dict[str, int]:
    """Return UTC day buckets for cadence events."""
    counts: Counter[str] = Counter()
    for event in events:
        timestamp = _parse_timestamp(event.timestamp)
        if timestamp is not None:
            counts[timestamp.date().isoformat()] += 1
    return dict(sorted(counts.items()))


def format_publication_cadence_anomaly_json(
    report: PublicationCadenceAnomalyReport,
) -> str:
    """Serialize the cadence anomaly report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publication_cadence_anomaly_text(
    report: PublicationCadenceAnomalyReport,
) -> str:
    """Render cadence anomalies for terminal review."""
    lines = [
        "Publication Cadence Anomaly Report",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['timeline_start']} to "
            f"{report.filters['timeline_end']} platform={report.filters['platform']}"
        ),
        (
            f"Thresholds: bursts>{report.filters['max_posts_per_window']} posts/"
            f"{report.filters['window_hours']}h gaps>{report.filters['max_gap_hours']}h "
            f"same_hour>={report.filters['repeated_hour_threshold']}"
        ),
        (
            f"Totals: events={report.totals['event_count']} "
            f"published={report.totals['published_count']} "
            f"queued={report.totals['queued_count']} "
            f"bursts={report.totals['burst_count']} "
            f"silences={report.totals['silence_count']} "
            f"same_hour={report.totals['repeated_hour_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing optional tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        details = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + details)

    if not report.timeline:
        lines.append("No published or queued scheduled events found.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Bursts:")
    if report.bursts:
        for burst in report.bursts:
            lines.append(
                f"- {burst.platform}: {burst.post_count} posts "
                f"{burst.start_at} to {burst.end_at} threshold={burst.threshold}"
            )
    else:
        lines.append("- none")

    lines.append("")
    lines.append("Silences:")
    if report.silences:
        for silence in report.silences:
            lines.append(
                f"- {silence.platform}: {silence.gap_hours}h "
                f"{silence.previous_at} to {silence.next_at} "
                f"threshold={silence.threshold_hours}h"
            )
    else:
        lines.append("- none")

    lines.append("")
    lines.append("Repeated same-hour patterns:")
    if report.repeated_hours:
        for pattern in report.repeated_hours:
            lines.append(
                f"- {pattern.platform}: hour={pattern.hour:02d}:00 "
                f"events={pattern.event_count} days={pattern.day_count} "
                f"{pattern.first_at} to {pattern.last_at}"
            )
    else:
        lines.append("- none")
    return "\n".join(lines)


def _load_events(
    conn: sqlite3.Connection,
    *,
    schema: dict[str, set[str]],
    start: datetime,
    end: datetime,
    platform: str,
    missing_tables: set[str],
    missing_columns: dict[str, set[str]],
) -> list[CadenceEvent]:
    events = _published_publication_events(
        conn,
        schema=schema,
        start=start,
        end=end,
        platform=platform,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    events.extend(
        _queue_events(
            conn,
            schema=schema,
            start=start,
            end=end,
            platform=platform,
            existing_events=events,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )
    )
    return events


def _published_publication_events(
    conn: sqlite3.Connection,
    *,
    schema: dict[str, set[str]],
    start: datetime,
    end: datetime,
    platform: str,
    missing_tables: set[str],
    missing_columns: dict[str, set[str]],
) -> list[CadenceEvent]:
    if "content_publications" not in schema:
        missing_tables.add("content_publications")
        return []
    required = ("content_id", "platform", "status", "published_at")
    missing = tuple(column for column in required if column not in schema["content_publications"])
    if missing:
        missing_columns["content_publications"].update(missing)
        return []

    filters = [
        "status = 'published'",
        "published_at IS NOT NULL",
        "published_at >= ?",
        "published_at <= ?",
    ]
    params: list[Any] = [start.isoformat(), end.isoformat()]
    if platform != "all":
        filters.append("LOWER(platform) = ?")
        params.append(platform)
    rows = _fetch_dicts(
        conn,
        f"""SELECT {_column_expr(schema['content_publications'], 'id', 'NULL')} AS id,
                  content_id, platform, status, published_at
           FROM content_publications
           WHERE {' AND '.join(filters)}
           ORDER BY published_at ASC, id ASC""",
        params,
    )
    events: list[CadenceEvent] = []
    for row in rows:
        timestamp = _parse_timestamp(row.get("published_at"))
        if timestamp is None:
            continue
        events.append(
            CadenceEvent(
                platform=_normalize_platform(row.get("platform")),
                timestamp=timestamp.isoformat(),
                source="content_publications",
                source_id=_optional_int(row.get("id")),
                content_id=_optional_int(row.get("content_id")),
                status="published",
            )
        )
    return events


def _queue_events(
    conn: sqlite3.Connection,
    *,
    schema: dict[str, set[str]],
    start: datetime,
    end: datetime,
    platform: str,
    existing_events: list[CadenceEvent],
    missing_tables: set[str],
    missing_columns: dict[str, set[str]],
) -> list[CadenceEvent]:
    if "publish_queue" not in schema:
        missing_tables.add("publish_queue")
        return []
    required = ("id", "content_id", "platform", "status", "scheduled_at", "published_at")
    missing = tuple(column for column in required if column not in schema["publish_queue"])
    if missing:
        missing_columns["publish_queue"].update(missing)
        return []

    statuses = ACTIVE_QUEUE_STATUSES + PUBLISHED_STATUSES
    placeholders = ", ".join("?" for _ in statuses)
    filters = [
        f"status IN ({placeholders})",
        "((status IN ('queued', 'held') AND scheduled_at IS NOT NULL AND scheduled_at >= ? AND scheduled_at <= ?)"
        " OR (status = 'published' AND published_at IS NOT NULL AND published_at >= ? AND published_at <= ?))",
    ]
    params: list[Any] = [*statuses, start.isoformat(), end.isoformat(), start.isoformat(), end.isoformat()]
    if platform != "all":
        filters.append("(LOWER(platform) = ? OR LOWER(platform) = 'all')")
        params.append(platform)
    rows = _fetch_dicts(
        conn,
        f"""SELECT id, content_id, platform, status, scheduled_at, published_at
           FROM publish_queue
           WHERE {' AND '.join(filters)}
           ORDER BY scheduled_at ASC, published_at ASC, id ASC""",
        params,
    )
    seen_published = {
        (event.content_id, event.platform)
        for event in existing_events
        if event.status == "published" and event.content_id is not None
    }
    events: list[CadenceEvent] = []
    for row in rows:
        status = _normalize_status(row.get("status"))
        timestamp_value = row.get("published_at") if status == "published" else row.get("scheduled_at")
        timestamp = _parse_timestamp(timestamp_value)
        if timestamp is None:
            continue
        for target_platform in _effective_platforms(row.get("platform")):
            if platform != "all" and target_platform != platform:
                continue
            content_id = _optional_int(row.get("content_id"))
            if status == "published" and (content_id, target_platform) in seen_published:
                continue
            events.append(
                CadenceEvent(
                    platform=target_platform,
                    timestamp=timestamp.isoformat(),
                    source="publish_queue",
                    source_id=_optional_int(row.get("id")),
                    content_id=content_id,
                    status=status,
                )
            )
    return events


def _detect_bursts(
    events_by_platform: dict[str, list[CadenceEvent]],
    *,
    window_hours: int,
    max_posts_per_window: int,
    limit: int,
) -> list[BurstWindow]:
    findings: list[BurstWindow] = []
    window = timedelta(hours=window_hours)
    for platform, events in sorted(events_by_platform.items()):
        parsed = [(event, _parse_timestamp(event.timestamp)) for event in events]
        parsed = [(event, timestamp) for event, timestamp in parsed if timestamp is not None]
        last_finding_end: datetime | None = None
        for index, (event, start) in enumerate(parsed):
            if last_finding_end is not None and start <= last_finding_end:
                continue
            group = [
                candidate
                for candidate, timestamp in parsed[index:]
                if timestamp is not None and timestamp <= start + window
            ]
            if len(group) <= max_posts_per_window:
                continue
            end_time = _parse_timestamp(group[-1].timestamp) or start
            findings.append(
                BurstWindow(
                    platform=platform,
                    start_at=event.timestamp,
                    end_at=group[-1].timestamp,
                    window_hours=window_hours,
                    post_count=len(group),
                    threshold=max_posts_per_window,
                    events=tuple(group),
                )
            )
            last_finding_end = end_time
    findings.sort(key=lambda burst: (-burst.post_count, burst.platform, burst.start_at))
    return findings[:limit]


def _detect_silences(
    events_by_platform: dict[str, list[CadenceEvent]],
    *,
    max_gap_hours: int,
    limit: int,
) -> list[SilenceWindow]:
    findings: list[SilenceWindow] = []
    for platform, events in sorted(events_by_platform.items()):
        for previous, current in zip(events, events[1:]):
            previous_at = _parse_timestamp(previous.timestamp)
            current_at = _parse_timestamp(current.timestamp)
            if previous_at is None or current_at is None:
                continue
            gap_hours = (current_at - previous_at).total_seconds() / 3600
            if gap_hours <= max_gap_hours:
                continue
            findings.append(
                SilenceWindow(
                    platform=platform,
                    previous_at=previous.timestamp,
                    next_at=current.timestamp,
                    gap_hours=round(gap_hours, 2),
                    threshold_hours=max_gap_hours,
                    previous_event=previous,
                    next_event=current,
                )
            )
    findings.sort(key=lambda silence: (-silence.gap_hours, silence.platform, silence.previous_at))
    return findings[:limit]


def _detect_repeated_hours(
    events_by_platform: dict[str, list[CadenceEvent]],
    *,
    repeated_hour_threshold: int,
    limit: int,
) -> list[RepeatedHourPattern]:
    findings: list[RepeatedHourPattern] = []
    for platform, events in sorted(events_by_platform.items()):
        by_hour: dict[int, list[CadenceEvent]] = defaultdict(list)
        for event in events:
            timestamp = _parse_timestamp(event.timestamp)
            if timestamp is not None:
                by_hour[timestamp.hour].append(event)
        for hour, hour_events in sorted(by_hour.items()):
            if len(hour_events) < repeated_hour_threshold:
                continue
            days = {
                (_parse_timestamp(event.timestamp) or datetime.min.replace(tzinfo=timezone.utc)).date().isoformat()
                for event in hour_events
            }
            findings.append(
                RepeatedHourPattern(
                    platform=platform,
                    hour=hour,
                    event_count=len(hour_events),
                    threshold=repeated_hour_threshold,
                    first_at=hour_events[0].timestamp,
                    last_at=hour_events[-1].timestamp,
                    day_count=len(days),
                    events=tuple(hour_events[:limit]),
                )
            )
    findings.sort(key=lambda pattern: (-pattern.event_count, pattern.platform, pattern.hour))
    return findings[:limit]


def _events_by_platform(events: list[CadenceEvent]) -> dict[str, list[CadenceEvent]]:
    grouped: dict[str, list[CadenceEvent]] = defaultdict(list)
    for event in events:
        grouped[event.platform].append(event)
    for platform_events in grouped.values():
        platform_events.sort(key=lambda event: (event.timestamp, event.source, event.source_id or 0))
    return dict(grouped)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _fetch_dicts(
    conn: sqlite3.Connection,
    sql: str,
    params: list[Any],
) -> list[dict[str, Any]]:
    cursor = conn.execute(sql, params)
    return [dict(row) for row in cursor.fetchall()]


def _column_expr(columns: set[str], column: str, fallback: str) -> str:
    return column if column in columns else fallback


def _effective_platforms(platform: Any) -> tuple[str, ...]:
    normalized = _normalize_platform(platform)
    if normalized == "all":
        return SUPPORTED_PLATFORMS
    return (normalized,)


def _normalize_platform(value: Any) -> str:
    text = str(value or "all").strip().lower()
    return text or "all"


def _normalize_status(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text or "queued"


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
