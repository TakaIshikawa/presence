"""Plan publish queue adjustments around platform quiet hours."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
import json
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_DAYS_AHEAD = 7
SUPPORTED_PLATFORMS = ("x", "bluesky")
PREVIEW_LENGTH = 96


@dataclass(frozen=True)
class QuietHourWindow:
    """One recurring local quiet-hour window."""

    index: int
    start: time
    end: time
    timezone: str
    platforms: tuple[str, ...]
    source: dict[str, Any]

    @property
    def is_overnight(self) -> bool:
        return self.start >= self.end

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "start": self.start.strftime("%H:%M"),
            "end": self.end.strftime("%H:%M"),
            "timezone": self.timezone,
            "platforms": list(self.platforms),
            "source": self.source,
        }


@dataclass(frozen=True)
class QuietHourPlanItem:
    """Queued publication row with quiet-hour recommendation."""

    source: str
    id: int
    content_id: int
    platform: str
    status: str
    scheduled_at: datetime
    scheduled_at_raw: str
    recommended_at: datetime
    action: str
    reason: str
    window_index: int | None = None
    content_type: str | None = None
    content_preview: str | None = None

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
            "platform": self.platform,
            "status": self.status,
            "scheduled_at": self.scheduled_at.isoformat(),
            "scheduled_at_raw": self.scheduled_at_raw,
            "recommended_at": self.recommended_at.isoformat(),
            "action": self.action,
            "reason": self.reason,
        }
        if self.window_index is not None:
            data["window_index"] = self.window_index
        if self.queue_item_id is not None:
            data["queue_item_id"] = self.queue_item_id
        if self.publication_id is not None:
            data["publication_id"] = self.publication_id
        metadata = {
            "content_type": self.content_type,
            "content_preview": self.content_preview,
        }
        data["generated_content"] = {
            key: value for key, value in metadata.items() if value is not None
        }
        return data


@dataclass(frozen=True)
class PublishQuietHoursReport:
    """Read-only quiet-hours adjustment plan."""

    generated_at: datetime
    horizon_days: int
    horizon_end: datetime
    windows: tuple[QuietHourWindow, ...]
    items: tuple[QuietHourPlanItem, ...]

    @property
    def adjustment_count(self) -> int:
        return sum(1 for item in self.items if item.action == "reschedule")

    @property
    def unchanged_count(self) -> int:
        return sum(1 for item in self.items if item.action == "unchanged")

    @property
    def adjustment_queue_item_ids(self) -> list[int]:
        return sorted(
            item.id
            for item in self.items
            if item.source == "publish_queue" and item.action == "reschedule"
        )

    @property
    def adjustment_publication_ids(self) -> list[int]:
        return sorted(
            item.id
            for item in self.items
            if item.source == "content_publications" and item.action == "reschedule"
        )

    @property
    def totals_by_platform(self) -> dict[str, dict[str, int]]:
        totals = {
            platform: {"reschedule": 0, "unchanged": 0}
            for platform in SUPPORTED_PLATFORMS
        }
        seen: set[tuple[str, int, str]] = set()
        for item in self.items:
            for platform in _effective_platforms(item.platform):
                key = (item.source, item.id, platform)
                if key in seen:
                    continue
                seen.add(key)
                totals[platform][item.action] += 1
        return totals

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at.isoformat(),
            "horizon_days": self.horizon_days,
            "horizon_end": self.horizon_end.isoformat(),
            "window_count": len(self.windows),
            "scanned_count": len(self.items),
            "adjustment_count": self.adjustment_count,
            "unchanged_count": self.unchanged_count,
            "adjustment_queue_item_ids": self.adjustment_queue_item_ids,
            "adjustment_publication_ids": self.adjustment_publication_ids,
            "totals_by_platform": self.totals_by_platform,
            "windows": [window.to_dict() for window in self.windows],
            "items": [item.to_dict() for item in self.items],
        }


def build_publish_quiet_hours_plan(
    db: Any,
    config: Any | None = None,
    *,
    quiet_hours: list[Any] | None = None,
    days: int = DEFAULT_DAYS_AHEAD,
    now: datetime | None = None,
) -> PublishQuietHoursReport:
    """Build a read-only plan for queued rows scheduled inside quiet hours."""
    if days < 0:
        raise ValueError("days must be non-negative")

    generated_at = _normalize_datetime(now or datetime.now(timezone.utc))
    horizon_end = generated_at + timedelta(days=days)
    windows = quiet_hour_windows_from_config(config) if quiet_hours is None else quiet_hours
    parsed_windows = parse_quiet_hour_windows(windows)
    queued_items = _load_scheduled_items(db, generated_at, horizon_end)
    planned_items = _plan_items(queued_items, parsed_windows)
    return PublishQuietHoursReport(
        generated_at=generated_at,
        horizon_days=days,
        horizon_end=horizon_end,
        windows=tuple(parsed_windows),
        items=tuple(planned_items),
    )


def quiet_hour_windows_from_config(config: Any | None) -> list[Any]:
    """Return quiet-hour windows from a config object or YAML mapping."""
    if config is None:
        return []
    if isinstance(config, dict):
        publishing = config.get("publishing")
        if isinstance(publishing, dict) and isinstance(publishing.get("quiet_hours"), list):
            return publishing["quiet_hours"]
        windows = config.get("quiet_hours")
        return windows if isinstance(windows, list) else []

    publishing = getattr(config, "publishing", None)
    windows = getattr(publishing, "quiet_hours", None)
    return windows if isinstance(windows, list) else []


def parse_quiet_hour_windows(windows: list[Any] | None) -> tuple[QuietHourWindow, ...]:
    """Parse quiet-hour window config into stable window objects."""
    parsed: list[QuietHourWindow] = []
    for source_index, raw_window in enumerate(windows or [], 1):
        if _window_value(raw_window, "enabled", True) is False:
            continue
        start = _parse_local_time(_window_value(raw_window, "start"))
        end = _parse_local_time(_window_value(raw_window, "end"))
        if start is None or end is None:
            raise ValueError("quiet-hour windows require start and end times")
        if start == end:
            raise ValueError("quiet-hour start and end must differ")
        tz_name, _tzinfo = _window_timezone(raw_window)
        source = dict(raw_window) if isinstance(raw_window, dict) else _object_to_dict(raw_window)
        parsed.append(
            QuietHourWindow(
                index=len(parsed) + 1,
                start=start,
                end=end,
                timezone=tz_name,
                platforms=_window_platforms(raw_window),
                source={"config_index": source_index, **source},
            )
        )
    return tuple(parsed)


def format_publish_quiet_hours_json(report: PublishQuietHoursReport) -> str:
    """Serialize a quiet-hours plan as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publish_quiet_hours_text(report: PublishQuietHoursReport) -> str:
    """Format a quiet-hours plan for operator review."""
    lines = [
        "Publish Quiet-Hours Plan",
        f"Generated: {report.generated_at.isoformat()}",
        f"Horizon: {report.horizon_days} days ({report.horizon_end.isoformat()})",
        f"Windows: {len(report.windows)}",
        f"Scanned: {len(report.items)}",
        f"Adjustments: {report.adjustment_count}",
        f"Unchanged: {report.unchanged_count}",
    ]
    if report.totals_by_platform:
        totals = ", ".join(
            f"{platform}=reschedule:{counts['reschedule']}/unchanged:{counts['unchanged']}"
            for platform, counts in report.totals_by_platform.items()
        )
        lines.append(f"Totals by platform: {totals}")

    if not report.windows:
        lines.append("")
        lines.append("No quiet-hour windows are configured.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Windows:")
    for window in report.windows:
        lines.append(
            f"- #{window.index} {window.start.strftime('%H:%M')}-{window.end.strftime('%H:%M')} "
            f"tz={window.timezone} platforms={','.join(window.platforms)}"
        )

    if not report.items:
        lines.append("")
        lines.append("No queued publications matched the scan horizon.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Plan:")
    for item in report.items:
        label = "queue" if item.source == "publish_queue" else "publication"
        if item.action == "reschedule":
            detail = f"{item.scheduled_at.isoformat()} -> {item.recommended_at.isoformat()}"
        else:
            detail = f"{item.scheduled_at.isoformat()} unchanged"
        lines.append(
            f"- {item.action} {label} {item.id} content {item.content_id} "
            f"platform={item.platform} status={item.status} {detail}"
        )
        if item.content_preview:
            lines.append(f"  {item.content_preview}")
    return "\n".join(lines)


def _load_scheduled_items(
    db: Any,
    now: datetime,
    horizon_end: datetime,
) -> list[QuietHourPlanItem]:
    conn = getattr(db, "conn", db)
    start = now.isoformat()
    end = horizon_end.isoformat()
    items: list[QuietHourPlanItem] = []

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
        items.append(
            _row_to_item(dict(row), source="publish_queue", scheduled_column="scheduled_at")
        )

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
        items.append(
            _row_to_item(dict(row), source="content_publications", scheduled_column="next_retry_at")
        )

    return sorted(items, key=lambda item: (item.scheduled_at, item.source, item.id))


def _row_to_item(
    row: dict[str, Any],
    *,
    source: str,
    scheduled_column: str,
) -> QuietHourPlanItem:
    scheduled_raw = str(row[scheduled_column])
    scheduled_at = _parse_datetime(scheduled_raw)
    return QuietHourPlanItem(
        source=source,
        id=int(row["id"]),
        content_id=int(row["content_id"]),
        platform=row.get("platform") or "all",
        status=row.get("status") or "queued",
        scheduled_at=scheduled_at,
        scheduled_at_raw=scheduled_raw,
        recommended_at=scheduled_at,
        action="unchanged",
        reason="outside_quiet_hours",
        content_type=row.get("content_type"),
        content_preview=_preview(row.get("content")),
    )


def _plan_items(
    items: list[QuietHourPlanItem],
    windows: tuple[QuietHourWindow, ...],
) -> list[QuietHourPlanItem]:
    planned: list[QuietHourPlanItem] = []
    for item in items:
        window, recommended_at = _matching_window_and_recommendation(item, windows)
        if window is None or recommended_at is None:
            planned.append(item)
            continue
        planned.append(
            QuietHourPlanItem(
                source=item.source,
                id=item.id,
                content_id=item.content_id,
                platform=item.platform,
                status=item.status,
                scheduled_at=item.scheduled_at,
                scheduled_at_raw=item.scheduled_at_raw,
                recommended_at=recommended_at,
                action="reschedule",
                reason="inside_quiet_hours",
                window_index=window.index,
                content_type=item.content_type,
                content_preview=item.content_preview,
            )
        )
    return sorted(planned, key=lambda item: (item.scheduled_at, item.source, item.id))


def _matching_window_and_recommendation(
    item: QuietHourPlanItem,
    windows: tuple[QuietHourWindow, ...],
) -> tuple[QuietHourWindow | None, datetime | None]:
    matches: list[tuple[datetime, QuietHourWindow]] = []
    for window in windows:
        if not _platforms_overlap(item.platform, window.platforms):
            continue
        tzinfo = _zoneinfo(window.timezone)
        local_dt = item.scheduled_at.astimezone(tzinfo)
        if not _is_inside_quiet_window(local_dt, window):
            continue
        recommended_at = _next_allowed_time(local_dt, window).astimezone(timezone.utc)
        matches.append((recommended_at, window))
    if not matches:
        return None, None
    recommended_at, window = min(matches, key=lambda match: (match[0], match[1].index))
    return window, recommended_at


def _is_inside_quiet_window(local_dt: datetime, window: QuietHourWindow) -> bool:
    local_time = local_dt.timetz().replace(tzinfo=None)
    if window.start < window.end:
        return window.start <= local_time < window.end
    return local_time >= window.start or local_time < window.end


def _next_allowed_time(local_dt: datetime, window: QuietHourWindow) -> datetime:
    local_date = local_dt.date()
    local_time = local_dt.timetz().replace(tzinfo=None)
    if window.start < window.end:
        return datetime.combine(local_date, window.end, tzinfo=local_dt.tzinfo)
    if local_time >= window.start:
        return datetime.combine(local_date + timedelta(days=1), window.end, tzinfo=local_dt.tzinfo)
    return datetime.combine(local_date, window.end, tzinfo=local_dt.tzinfo)


def _platforms_overlap(item_platform: str, window_platforms: tuple[str, ...]) -> bool:
    return bool(set(_effective_platforms(item_platform)) & set(window_platforms))


def _effective_platforms(platform: str) -> tuple[str, ...]:
    if platform == "all":
        return SUPPORTED_PLATFORMS
    return (platform,)


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


def _window_timezone(window: Any) -> tuple[str, timezone | ZoneInfo]:
    tz_name = str(_window_value(window, "timezone", _window_value(window, "tz", "UTC")) or "UTC")
    if tz_name.upper() == "UTC":
        return "UTC", timezone.utc
    try:
        return tz_name, ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        return "UTC", timezone.utc


def _zoneinfo(tz_name: str) -> timezone | ZoneInfo:
    if tz_name.upper() == "UTC":
        return timezone.utc
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        return timezone.utc


def _parse_local_time(value: Any) -> time | None:
    if value is None:
        return None
    if isinstance(value, time):
        return value.replace(tzinfo=None)
    try:
        parts = str(value).split(":")
        if len(parts) not in (2, 3):
            return None
        hour = int(parts[0])
        minute = int(parts[1])
        second = int(parts[2]) if len(parts) == 3 else 0
        return time(hour, minute, second)
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return _normalize_datetime(parsed)


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _preview(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    if not text:
        return None
    return text if len(text) <= PREVIEW_LENGTH else f"{text[:PREVIEW_LENGTH - 1]}..."


def _window_value(window: Any, key: str, default: Any = None) -> Any:
    if isinstance(window, dict):
        return window.get(key, default)
    return getattr(window, key, default)


def _object_to_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, "__dict__"):
        return {
            key: item
            for key, item in vars(value).items()
            if not key.startswith("_")
        }
    return {}
