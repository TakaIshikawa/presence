"""Dry-run reschedule planner for stale publish queue items."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from types import SimpleNamespace
from typing import Any

from evaluation.posting_windows import DAY_NAMES, PostingWindowRecommender


DEFAULT_DAYS_AHEAD = 7
DEFAULT_LIMIT = 20
DEFAULT_HISTORY_DAYS = 90
PLATFORMS = ("x", "bluesky")
VALID_PLATFORMS = {"all", *PLATFORMS}
STATUSES = ("queued", "held")
VALID_STATUSES = {"all", *STATUSES}
FALLBACK_HOURS = (9, 15)
ACTIVE_QUEUE_STATUSES = ("queued", "held", "failed")


@dataclass(frozen=True)
class PublishQueueRescheduleSuggestion:
    """One proposed publish_queue scheduled_at replacement."""

    queue_id: int
    content_id: int
    platform: str
    target_platforms: tuple[str, ...]
    status: str
    current_scheduled_at: str
    suggested_at: str
    content_type: str | None
    content_format: str | None
    hold_reason: str | None
    reason_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "queue_id": self.queue_id,
            "content_id": self.content_id,
            "platform": self.platform,
            "target_platforms": list(self.target_platforms),
            "status": self.status,
            "current_scheduled_at": self.current_scheduled_at,
            "suggested_at": self.suggested_at,
            "content_type": self.content_type,
            "content_format": self.content_format,
            "hold_reason": self.hold_reason,
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True)
class PublishQueueRescheduleReport:
    """Read-only publish queue reschedule plan."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    suggestions: tuple[PublishQueueRescheduleSuggestion, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "publish_queue_reschedule",
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "totals": dict(self.totals),
            "suggestions": [suggestion.to_dict() for suggestion in self.suggestions],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


@dataclass(frozen=True)
class _QueueItem:
    queue_id: int
    content_id: int
    platform: str
    status: str
    scheduled_at: datetime
    scheduled_at_raw: str
    content_type: str | None
    content_format: str | None
    hold_reason: str | None

    @property
    def target_platforms(self) -> tuple[str, ...]:
        return _target_platforms(self.platform)


@dataclass(frozen=True)
class _Slot:
    platform: str
    scheduled_at: datetime
    source: str


def build_publish_queue_reschedule_report(
    db_or_conn: Any,
    *,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    platform: str = "all",
    status: str = "all",
    limit: int = DEFAULT_LIMIT,
    history_days: int = DEFAULT_HISTORY_DAYS,
    now: datetime | None = None,
) -> PublishQueueRescheduleReport:
    """Suggest new scheduled_at values for overdue queued or held queue items."""
    if days_ahead <= 0:
        raise ValueError("days_ahead must be positive")
    if limit < 0:
        raise ValueError("limit must be non-negative")
    selected_platforms = _selected_platforms(platform)
    selected_statuses = _selected_statuses(status)

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    horizon_end = generated_at + timedelta(days=days_ahead)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    filters = {
        "days_ahead": days_ahead,
        "horizon_start": generated_at.isoformat(),
        "horizon_end": horizon_end.isoformat(),
        "platform": platform,
        "status": status,
        "limit": limit,
        "history_days": history_days,
    }
    if missing_tables or missing_columns:
        return PublishQueueRescheduleReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals={"stale_count": 0, "suggestion_count": 0, "unscheduled_count": 0},
            suggestions=(),
            missing_tables=tuple(sorted(missing_tables)),
            missing_columns=missing_columns,
        )

    stale_items = _load_stale_items(
        conn,
        now=generated_at,
        selected_platforms=selected_platforms,
        selected_statuses=selected_statuses,
    )
    occupied = _occupied_slots(
        conn,
        start=generated_at,
        end=horizon_end,
        selected_platforms=selected_platforms,
    )
    suggestions = tuple(
        _suggestions(
            conn,
            items=stale_items,
            occupied=occupied,
            selected_platforms=selected_platforms,
            start=generated_at,
            end=horizon_end,
            limit=limit,
            history_days=history_days,
        )
    )
    return PublishQueueRescheduleReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "stale_count": len(stale_items),
            "suggestion_count": len(suggestions),
            "unscheduled_count": max(0, len(stale_items) - len(suggestions)),
            "occupied_slot_count": len(occupied),
        },
        suggestions=suggestions,
    )


def format_publish_queue_reschedule_json(report: PublishQueueRescheduleReport) -> str:
    """Serialize a reschedule report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publish_queue_reschedule_text(report: PublishQueueRescheduleReport) -> str:
    """Format a reschedule report for terminal review."""
    lines = [
        "Publish Queue Reschedule Plan",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['horizon_start']} to "
            f"{report.filters['horizon_end']}"
        ),
        f"Platform: {report.filters['platform']}",
        f"Status: {report.filters['status']}",
        f"Limit: {report.filters['limit']}",
        "Dry run: no publish_queue rows were changed.",
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + details)
    if report.missing_tables or report.missing_columns:
        return "\n".join(lines)

    if not report.suggestions:
        lines.append("No stale publish queue items could be rescheduled.")
        return "\n".join(lines)

    lines.append("Suggestions:")
    for suggestion in report.suggestions:
        hold = f"; hold_reason={suggestion.hold_reason}" if suggestion.hold_reason else ""
        codes = ",".join(suggestion.reason_codes)
        lines.append(
            f"- queue {suggestion.queue_id}: {suggestion.current_scheduled_at} -> "
            f"{suggestion.suggested_at} ({suggestion.platform}; {codes}){hold}"
        )
    return "\n".join(lines)


def _suggestions(
    conn: sqlite3.Connection,
    *,
    items: list[_QueueItem],
    occupied: set[tuple[str, datetime]],
    selected_platforms: tuple[str, ...],
    start: datetime,
    end: datetime,
    limit: int,
    history_days: int,
) -> list[PublishQueueRescheduleSuggestion]:
    suggestions: list[PublishQueueRescheduleSuggestion] = []
    reserved = set(occupied)
    slot_cache: dict[tuple[str, str | None], list[_Slot]] = {}

    for item in items:
        slots = _slots_for_item(
            conn,
            item,
            selected_platforms=selected_platforms,
            start=start,
            end=end,
            history_days=history_days,
            cache=slot_cache,
        )
        slot = _first_available_slot(item, slots, reserved)
        if slot is None:
            continue
        for target in item.target_platforms:
            if target in selected_platforms:
                reserved.add((target, _bucket_start(slot.scheduled_at)))
        reason_codes = ["overdue"]
        if item.status == "held":
            reason_codes.append("held")
        reason_codes.extend([f"{slot.source}_window", "platform_slot_open"])
        suggestions.append(
            PublishQueueRescheduleSuggestion(
                queue_id=item.queue_id,
                content_id=item.content_id,
                platform=item.platform,
                target_platforms=item.target_platforms,
                status=item.status,
                current_scheduled_at=item.scheduled_at_raw,
                suggested_at=slot.scheduled_at.isoformat(),
                content_type=item.content_type,
                content_format=item.content_format,
                hold_reason=item.hold_reason,
                reason_codes=tuple(dict.fromkeys(reason_codes)),
            )
        )
        if len(suggestions) >= limit:
            break
    return suggestions


def _first_available_slot(
    item: _QueueItem,
    slots: list[_Slot],
    reserved: set[tuple[str, datetime]],
) -> _Slot | None:
    targets = set(item.target_platforms)
    for slot in slots:
        if slot.platform not in targets:
            continue
        bucket = _bucket_start(slot.scheduled_at)
        if any((target, bucket) in reserved for target in targets):
            continue
        return slot
    return None


def _slots_for_item(
    conn: sqlite3.Connection,
    item: _QueueItem,
    *,
    selected_platforms: tuple[str, ...],
    start: datetime,
    end: datetime,
    history_days: int,
    cache: dict[tuple[str, str | None], list[_Slot]],
) -> list[_Slot]:
    slots: list[_Slot] = []
    for platform in item.target_platforms:
        if platform not in selected_platforms:
            continue
        content_key = (platform, item.content_type)
        if content_key not in cache:
            cache[content_key] = _candidate_slots(
                conn,
                platform=platform,
                content_type=item.content_type,
                start=start,
                end=end,
                history_days=history_days,
            )
        slots.extend(cache[content_key])
    return sorted(slots, key=lambda slot: (slot.scheduled_at, slot.platform))


def _candidate_slots(
    conn: sqlite3.Connection,
    *,
    platform: str,
    content_type: str | None,
    start: datetime,
    end: datetime,
    history_days: int,
) -> list[_Slot]:
    recommender = PostingWindowRecommender(SimpleNamespace(conn=conn))
    windows = recommender.recommend(
        days=max(1, int(history_days)),
        platform=platform,
        limit=12,
        content_type=content_type,
    )
    source = "history"
    if not windows and content_type:
        windows = recommender.recommend(
            days=max(1, int(history_days)),
            platform=platform,
            limit=12,
        )
    if not windows:
        source = "fallback"
        windows = [
            _FallbackWindow(day_of_week=day, hour_utc=hour)
            for day in range(7)
            for hour in FALLBACK_HOURS
        ]

    slots: list[_Slot] = []
    for window in windows:
        current = _next_occurrence(window.day_of_week, window.hour_utc, start)
        while current <= end:
            slots.append(_Slot(platform=platform, scheduled_at=current, source=source))
            current += timedelta(days=7)
    return sorted(set(slots), key=lambda slot: (slot.scheduled_at, slot.platform, slot.source))


@dataclass(frozen=True, order=True)
class _FallbackWindow:
    day_of_week: int
    hour_utc: int

    @property
    def day_name(self) -> str:
        return DAY_NAMES[self.day_of_week]


def _load_stale_items(
    conn: sqlite3.Connection,
    *,
    now: datetime,
    selected_platforms: tuple[str, ...],
    selected_statuses: tuple[str, ...],
) -> list[_QueueItem]:
    placeholders = ", ".join("?" for _ in selected_statuses)
    rows = conn.execute(
        f"""SELECT
               pq.id AS queue_id,
               pq.content_id AS content_id,
               COALESCE(pq.platform, 'all') AS platform,
               LOWER(COALESCE(pq.status, 'queued')) AS status,
               pq.scheduled_at AS scheduled_at,
               pq.hold_reason AS hold_reason,
               gc.content_type AS content_type,
               gc.content_format AS content_format
           FROM publish_queue pq
           INNER JOIN generated_content gc ON gc.id = pq.content_id
           WHERE LOWER(COALESCE(pq.status, 'queued')) IN ({placeholders})
           ORDER BY pq.scheduled_at ASC, pq.id ASC""",
        selected_statuses,
    ).fetchall()
    items: list[_QueueItem] = []
    for row in rows:
        data = dict(row)
        scheduled_at = _parse_timestamp(data.get("scheduled_at"))
        if scheduled_at is None or scheduled_at >= now:
            continue
        targets = _target_platforms(data.get("platform"))
        if not any(target in selected_platforms for target in targets):
            continue
        items.append(
            _QueueItem(
                queue_id=int(data["queue_id"]),
                content_id=int(data["content_id"]),
                platform=_normalize(data.get("platform"), default="all"),
                status=_normalize(data.get("status"), default="queued"),
                scheduled_at=scheduled_at,
                scheduled_at_raw=str(data["scheduled_at"]),
                content_type=data.get("content_type"),
                content_format=data.get("content_format"),
                hold_reason=data.get("hold_reason"),
            )
        )
    return items


def _occupied_slots(
    conn: sqlite3.Connection,
    *,
    start: datetime,
    end: datetime,
    selected_platforms: tuple[str, ...],
) -> set[tuple[str, datetime]]:
    occupied: set[tuple[str, datetime]] = set()
    queue_rows = conn.execute(
        f"""SELECT platform, scheduled_at
            FROM publish_queue
            WHERE LOWER(COALESCE(status, 'queued')) IN ({", ".join("?" for _ in ACTIVE_QUEUE_STATUSES)})
            ORDER BY scheduled_at ASC, id ASC""",
        ACTIVE_QUEUE_STATUSES,
    ).fetchall()
    for row in queue_rows:
        scheduled_at = _parse_timestamp(row["scheduled_at"])
        if scheduled_at is None or not (start <= scheduled_at <= end):
            continue
        for platform in _target_platforms(row["platform"]):
            if platform in selected_platforms:
                occupied.add((platform, _bucket_start(scheduled_at)))

    publication_rows = conn.execute(
        f"""SELECT platform, published_at
            FROM content_publications
            WHERE status = 'published'
              AND published_at IS NOT NULL
              AND platform IN ({", ".join("?" for _ in selected_platforms)})
            ORDER BY published_at ASC, id ASC""",
        selected_platforms,
    ).fetchall()
    for row in publication_rows:
        published_at = _parse_timestamp(row["published_at"])
        if published_at is not None and start <= published_at <= end:
            occupied.add((row["platform"], _bucket_start(published_at)))
    return occupied


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[set[str], dict[str, tuple[str, ...]]]:
    missing_tables: set[str] = set()
    missing_columns: dict[str, tuple[str, ...]] = {}
    required = {
        "publish_queue": ("id", "content_id", "scheduled_at", "platform", "status"),
        "generated_content": ("id", "content_type", "content_format"),
        "content_publications": ("platform", "status", "published_at"),
    }
    for table, columns in required.items():
        if table not in schema:
            missing_tables.add(table)
            continue
        missing = tuple(column for column in columns if column not in schema[table])
        if missing:
            missing_columns[table] = missing
    return missing_tables, missing_columns


def _selected_platforms(platform: str) -> tuple[str, ...]:
    normalized = _normalize(platform, default="all")
    if normalized not in VALID_PLATFORMS:
        raise ValueError("platform must be one of: all, x, bluesky")
    return PLATFORMS if normalized == "all" else (normalized,)


def _selected_statuses(status: str) -> tuple[str, ...]:
    normalized = _normalize(status, default="all")
    if normalized not in VALID_STATUSES:
        raise ValueError("status must be one of: all, queued, held")
    return STATUSES if normalized == "all" else (normalized,)


def _target_platforms(value: Any) -> tuple[str, ...]:
    normalized = _normalize(value, default="all")
    if normalized == "all":
        return PLATFORMS
    if normalized in PLATFORMS:
        return (normalized,)
    return ()


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


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


def _parse_timestamp(value: Any) -> datetime | None:
    if not str(value or "").strip():
        return None
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return _as_utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _bucket_start(value: datetime) -> datetime:
    return _as_utc(value).replace(minute=0, second=0, microsecond=0)


def _next_occurrence(day_of_week: int, hour_utc: int, after: datetime) -> datetime:
    current = after.replace(minute=0, second=0, microsecond=0)
    if current <= after:
        current += timedelta(hours=1)
    hours_ahead = ((day_of_week - current.weekday()) % 7) * 24 + (hour_utc - current.hour)
    if hours_ahead < 0:
        hours_ahead += 7 * 24
    return current + timedelta(hours=hours_ahead)


def _normalize(value: Any, *, default: str = "unknown") -> str:
    return str(value or default).strip().lower() or default
