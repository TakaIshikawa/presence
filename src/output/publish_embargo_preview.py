"""Preview publishing embargo windows and affected queued publications."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import json
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_DAYS_AHEAD = 7
PREVIEW_LENGTH = 96
SUPPORTED_PLATFORMS = ("x", "bluesky")

_DAY_NAMES = {
    "monday": 0,
    "mon": 0,
    "tuesday": 1,
    "tue": 1,
    "wednesday": 2,
    "wed": 2,
    "thursday": 3,
    "thu": 3,
    "friday": 4,
    "fri": 4,
    "saturday": 5,
    "sat": 5,
    "sunday": 6,
    "sun": 6,
}


@dataclass(frozen=True)
class ExpandedEmbargoWindow:
    """One concrete embargo interval in UTC."""

    index: int
    start_at: datetime
    end_at: datetime
    timezone: str
    source: dict[str, Any]
    platforms: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "start_at": self.start_at.isoformat(),
            "end_at": self.end_at.isoformat(),
            "timezone": self.timezone,
            "platforms": list(self.platforms),
            "source": self.source,
        }


@dataclass(frozen=True)
class AffectedPublication:
    """Queued or retryable publication scheduled inside an embargo window."""

    source: str
    id: int
    content_id: int
    scheduled_at: datetime
    scheduled_at_raw: str
    platform: str
    status: str
    content_type: str | None = None
    content_preview: str | None = None
    window_index: int | None = None

    @property
    def queue_item_id(self) -> int | None:
        return self.id if self.source == "publish_queue" else None

    @property
    def publication_id(self) -> int | None:
        return self.id if self.source == "content_publications" else None

    def to_dict(self) -> dict[str, Any]:
        data = {
            "source": self.source,
            "id": self.id,
            "content_id": self.content_id,
            "scheduled_at": self.scheduled_at.isoformat(),
            "scheduled_at_raw": self.scheduled_at_raw,
            "platform": self.platform,
            "status": self.status,
        }
        if self.window_index is not None:
            data["window_index"] = self.window_index
        if self.queue_item_id is not None:
            data["queue_item_id"] = self.queue_item_id
        if self.publication_id is not None:
            data["publication_id"] = self.publication_id
        generated_content = {
            "content_type": self.content_type,
            "content_preview": self.content_preview,
        }
        data["generated_content"] = {
            key: value for key, value in generated_content.items() if value is not None
        }
        return data


@dataclass(frozen=True)
class PublishEmbargoPreviewReport:
    """Aggregated publish embargo preview."""

    now: datetime
    horizon_days: int
    horizon_end: datetime
    windows: tuple[ExpandedEmbargoWindow, ...]
    affected_items: tuple[AffectedPublication, ...]

    @property
    def affected_queue_item_ids(self) -> list[int]:
        return sorted(
            {item.id for item in self.affected_items if item.source == "publish_queue"}
        )

    @property
    def affected_publication_ids(self) -> list[int]:
        return sorted(
            {item.id for item in self.affected_items if item.source == "content_publications"}
        )

    @property
    def totals_by_platform(self) -> dict[str, int]:
        totals: dict[str, set[tuple[str, int]]] = {}
        for item in self.affected_items:
            for platform in _effective_platforms(item.platform):
                totals.setdefault(platform, set()).add((item.source, item.id))
        return {platform: len(totals.get(platform, set())) for platform in SUPPORTED_PLATFORMS}

    def to_dict(self) -> dict[str, Any]:
        return {
            "now": self.now.isoformat(),
            "horizon_days": self.horizon_days,
            "horizon_end": self.horizon_end.isoformat(),
            "window_count": len(self.windows),
            "affected_count": len({(item.source, item.id) for item in self.affected_items}),
            "affected_queue_item_ids": self.affected_queue_item_ids,
            "affected_publication_ids": self.affected_publication_ids,
            "totals_by_platform": self.totals_by_platform,
            "windows": [window.to_dict() for window in self.windows],
            "affected_items": [item.to_dict() for item in self.affected_items],
        }


def build_publish_embargo_preview(
    db: Any,
    config: Any,
    *,
    days: int = DEFAULT_DAYS_AHEAD,
    now: datetime | None = None,
) -> PublishEmbargoPreviewReport:
    """Expand configured embargo windows and find queued items inside them."""
    if days < 0:
        raise ValueError("days must be non-negative")

    base_now = _normalize_datetime(now or datetime.now(timezone.utc))
    horizon_end = base_now + timedelta(days=days)
    windows = expand_embargo_windows(config, now=base_now, days=days)
    queued_items = _load_scheduled_items(db, base_now, horizon_end)
    affected = _affected_items(queued_items, windows)
    return PublishEmbargoPreviewReport(
        now=base_now,
        horizon_days=days,
        horizon_end=horizon_end,
        windows=tuple(windows),
        affected_items=tuple(affected),
    )


def expand_embargo_windows(
    config: Any,
    *,
    now: datetime,
    days: int = DEFAULT_DAYS_AHEAD,
) -> tuple[ExpandedEmbargoWindow, ...]:
    """Expand one-time and recurring config windows over the supplied horizon."""
    if days < 0:
        raise ValueError("days must be non-negative")

    base_now = _normalize_datetime(now)
    horizon_end = base_now + timedelta(days=days)
    expanded: list[ExpandedEmbargoWindow] = []

    for source_index, raw_window in enumerate(embargo_windows_from_config(config), 1):
        if _window_value(raw_window, "enabled", True) is False:
            continue
        source = dict(raw_window) if isinstance(raw_window, dict) else _object_to_dict(raw_window)
        tz_name, tzinfo = _window_timezone(raw_window)
        platforms = _window_platforms(raw_window)
        for start_at, end_at in _expand_one_window(raw_window, tzinfo, base_now, horizon_end):
            if end_at <= base_now or start_at >= horizon_end:
                continue
            expanded.append(
                ExpandedEmbargoWindow(
                    index=len(expanded) + 1,
                    start_at=max(start_at, base_now),
                    end_at=min(end_at, horizon_end),
                    timezone=tz_name,
                    source={"config_index": source_index, **source},
                    platforms=platforms,
                )
            )

    return tuple(
        sorted(
            expanded,
            key=lambda window: (window.start_at, window.end_at, window.index),
        )
    )


def embargo_windows_from_config(config: Any) -> list[Any]:
    """Return publishing embargo windows from a config object or YAML dict."""
    if isinstance(config, dict):
        publishing = config.get("publishing")
        if isinstance(publishing, dict) and isinstance(publishing.get("embargo_windows"), list):
            return publishing["embargo_windows"]
        windows = config.get("embargo_windows")
        return windows if isinstance(windows, list) else []

    publishing = getattr(config, "publishing", None)
    windows = getattr(publishing, "embargo_windows", None)
    return windows if isinstance(windows, list) else []


def format_publish_embargo_preview_json(report: PublishEmbargoPreviewReport) -> str:
    """Serialize a publish embargo preview as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publish_embargo_preview_text(report: PublishEmbargoPreviewReport) -> str:
    """Format a publish embargo preview for operator review."""
    lines = [
        "Publish Embargo Preview",
        f"Now: {report.now.isoformat()}",
        f"Horizon: {report.horizon_days} days ({report.horizon_end.isoformat()})",
        f"Expanded windows: {len(report.windows)}",
        f"Affected queued items: {len({(item.source, item.id) for item in report.affected_items})}",
    ]
    if report.totals_by_platform:
        totals = ", ".join(
            f"{platform}={count}" for platform, count in report.totals_by_platform.items()
        )
        lines.append(f"Totals by platform: {totals}")

    if not report.windows:
        lines.append("")
        lines.append("No embargo windows are configured for this horizon.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Windows:")
    affected_by_window: dict[int, list[AffectedPublication]] = {}
    for item in report.affected_items:
        if item.window_index is not None:
            affected_by_window.setdefault(item.window_index, []).append(item)

    for window in report.windows:
        affected = affected_by_window.get(window.index, [])
        lines.append(
            f"- #{window.index} {window.start_at.isoformat()} to {window.end_at.isoformat()} "
            f"tz={window.timezone} platforms={','.join(window.platforms)} "
            f"affected={len({(item.source, item.id) for item in affected})}"
        )

    if not report.affected_items:
        lines.append("")
        lines.append("No queued publications are scheduled inside embargo windows.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Affected items:")
    seen: set[tuple[str, int]] = set()
    for item in report.affected_items:
        key = (item.source, item.id)
        if key in seen:
            continue
        seen.add(key)
        label = "queue" if item.source == "publish_queue" else "publication"
        lines.append(
            f"- {label} {item.id} content {item.content_id} "
            f"{item.scheduled_at.isoformat()} platform={item.platform} status={item.status}"
        )
        if item.content_preview:
            lines.append(f"  {item.content_preview}")

    return "\n".join(lines)


def _expand_one_window(
    window: Any,
    tzinfo: timezone | ZoneInfo,
    now: datetime,
    horizon_end: datetime,
) -> list[tuple[datetime, datetime]]:
    start_time = _parse_local_time(_window_value(window, "start"))
    end_time = _parse_local_time(_window_value(window, "end"))
    start_day = (now.astimezone(tzinfo) - timedelta(days=1)).date()
    end_day = (horizon_end.astimezone(tzinfo) + timedelta(days=1)).date()
    intervals: list[tuple[datetime, datetime]] = []

    current_day = start_day
    while current_day <= end_day:
        if start_time and end_time:
            calendar_day = current_day
            if _matches_embargo_calendar(window, calendar_day, require_filter=False):
                local_start = datetime.combine(calendar_day, start_time, tzinfo=tzinfo)
                local_end_day = calendar_day if start_time < end_time else calendar_day + timedelta(days=1)
                local_end = datetime.combine(local_end_day, end_time, tzinfo=tzinfo)
                intervals.append(
                    (
                        local_start.astimezone(timezone.utc),
                        local_end.astimezone(timezone.utc),
                    )
                )
        elif _matches_embargo_calendar(window, current_day, require_filter=True):
            local_start = datetime.combine(current_day, time.min, tzinfo=tzinfo)
            local_end = local_start + timedelta(days=1)
            intervals.append(
                (
                    local_start.astimezone(timezone.utc),
                    local_end.astimezone(timezone.utc),
                )
            )
        current_day += timedelta(days=1)

    return intervals


def _load_scheduled_items(
    db: Any,
    now: datetime,
    horizon_end: datetime,
) -> list[AffectedPublication]:
    conn = getattr(db, "conn", db)
    start = now.isoformat()
    end = horizon_end.isoformat()
    items: list[AffectedPublication] = []

    queue_rows = conn.execute(
        """SELECT pq.id,
                  pq.content_id,
                  pq.scheduled_at,
                  pq.platform,
                  pq.status,
                  gc.content,
                  gc.content_type
           FROM publish_queue pq
           INNER JOIN generated_content gc ON gc.id = pq.content_id
           WHERE pq.status = 'queued'
             AND pq.scheduled_at >= ?
             AND pq.scheduled_at <= ?
           ORDER BY pq.scheduled_at ASC, pq.id ASC""",
        (start, end),
    ).fetchall()
    for row in queue_rows:
        data = dict(row)
        items.append(_row_to_item(data, source="publish_queue", scheduled_column="scheduled_at"))

    publication_rows = conn.execute(
        """SELECT cp.id,
                  cp.content_id,
                  cp.next_retry_at,
                  cp.platform,
                  cp.status,
                  gc.content,
                  gc.content_type
           FROM content_publications cp
           INNER JOIN generated_content gc ON gc.id = cp.content_id
           WHERE cp.status IN ('queued', 'failed')
             AND cp.next_retry_at IS NOT NULL
             AND cp.next_retry_at >= ?
             AND cp.next_retry_at <= ?
           ORDER BY cp.next_retry_at ASC, cp.id ASC""",
        (start, end),
    ).fetchall()
    for row in publication_rows:
        data = dict(row)
        items.append(_row_to_item(data, source="content_publications", scheduled_column="next_retry_at"))

    return sorted(items, key=lambda item: (item.scheduled_at, item.source, item.id))


def _row_to_item(
    row: dict[str, Any],
    *,
    source: str,
    scheduled_column: str,
) -> AffectedPublication:
    scheduled_raw = str(row[scheduled_column])
    return AffectedPublication(
        source=source,
        id=int(row["id"]),
        content_id=int(row["content_id"]),
        scheduled_at=_parse_datetime(scheduled_raw),
        scheduled_at_raw=scheduled_raw,
        platform=row.get("platform") or "all",
        status=row.get("status") or "queued",
        content_type=row.get("content_type"),
        content_preview=_preview(row.get("content")),
    )


def _affected_items(
    items: list[AffectedPublication],
    windows: tuple[ExpandedEmbargoWindow, ...],
) -> list[AffectedPublication]:
    affected: list[AffectedPublication] = []
    for item in items:
        for window in windows:
            if not _platforms_overlap(item.platform, window.platforms):
                continue
            if window.start_at <= item.scheduled_at < window.end_at:
                affected.append(
                    AffectedPublication(
                        source=item.source,
                        id=item.id,
                        content_id=item.content_id,
                        scheduled_at=item.scheduled_at,
                        scheduled_at_raw=item.scheduled_at_raw,
                        platform=item.platform,
                        status=item.status,
                        content_type=item.content_type,
                        content_preview=item.content_preview,
                        window_index=window.index,
                    )
                )
    return sorted(affected, key=lambda item: (item.scheduled_at, item.source, item.id, item.window_index or 0))


def _matches_embargo_calendar(window: Any, local_day: date, *, require_filter: bool) -> bool:
    explicit_dates = _normalise_date_values(
        _window_value(window, "dates", _window_value(window, "date"))
    )
    start_date = _parse_local_date(_window_value(window, "start_date"))
    end_date = _parse_local_date(_window_value(window, "end_date"))
    weekdays = _normalise_weekdays(
        _window_value(window, "weekdays", _window_value(window, "days"))
    )
    has_filter = bool(explicit_dates or start_date or end_date or weekdays is not None)
    if require_filter and not has_filter:
        return False
    if explicit_dates and local_day not in explicit_dates:
        return False
    if start_date and local_day < start_date:
        return False
    if end_date and local_day > end_date:
        return False
    if weekdays is not None and local_day.weekday() not in weekdays:
        return False
    return True


def _normalise_date_values(value: Any) -> set[date]:
    if value is None:
        return set()
    values = value if isinstance(value, list) else [value]
    return {parsed for item in values if (parsed := _parse_local_date(item))}


def _normalise_weekdays(value: Any) -> set[int] | None:
    if value is None:
        return None
    values = value if isinstance(value, list) else [value]
    weekdays = set()
    for item in values:
        if isinstance(item, int) and 0 <= item <= 6:
            weekdays.add(item)
            continue
        mapped = _DAY_NAMES.get(str(item).strip().lower())
        if mapped is not None:
            weekdays.add(mapped)
    return weekdays


def _window_platforms(window: Any) -> tuple[str, ...]:
    raw = _window_value(window, "platforms", _window_value(window, "platform"))
    if raw in (None, "", "all"):
        return SUPPORTED_PLATFORMS
    values = raw if isinstance(raw, list) else [raw]
    platforms = tuple(
        platform
        for platform in (str(value).strip() for value in values)
        if platform in SUPPORTED_PLATFORMS
    )
    return platforms or SUPPORTED_PLATFORMS


def _platforms_overlap(item_platform: str, window_platforms: tuple[str, ...]) -> bool:
    return bool(set(_effective_platforms(item_platform)) & set(window_platforms))


def _effective_platforms(platform: str) -> tuple[str, ...]:
    if platform == "all":
        return SUPPORTED_PLATFORMS
    return (platform,)


def _window_timezone(window: Any) -> tuple[str, timezone | ZoneInfo]:
    tz_name = str(_window_value(window, "timezone", _window_value(window, "tz", "UTC")) or "UTC")
    if tz_name.upper() == "UTC":
        return "UTC", timezone.utc
    try:
        return tz_name, ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        return "UTC", timezone.utc


def _parse_local_time(value: Any) -> time | None:
    if value is None:
        return None
    if isinstance(value, time):
        return value.replace(tzinfo=None)
    try:
        parts = str(value).split(":")
        if len(parts) < 2:
            return None
        return time(hour=int(parts[0]), minute=int(parts[1]))
    except (TypeError, ValueError):
        return None


def _parse_local_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return _normalize_datetime(parsed)


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _preview(value: Any, limit: int = PREVIEW_LENGTH) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def _window_value(window: Any, key: str, default: Any = None) -> Any:
    if isinstance(window, dict):
        return window.get(key, default)
    return getattr(window, key, default)


def _object_to_dict(value: Any) -> dict[str, Any]:
    return {
        key: item
        for key, item in vars(value).items()
        if not key.startswith("_")
    } if hasattr(value, "__dict__") else {"value": str(value)}
