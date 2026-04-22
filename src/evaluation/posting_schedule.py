"""Posting schedule optimization based on historical engagement patterns."""

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from storage.db import Database


@dataclass
class TimeWindow:
    """Represents an optimal posting time window."""
    day_of_week: int  # 0=Monday, 6=Sunday
    hour_utc: int  # 0-23
    avg_engagement: float
    sample_size: int
    confidence: float  # 0-1, based on sample size


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


def _window_value(window: Any, key: str, default: Any = None) -> Any:
    if isinstance(window, dict):
        return window.get(key, default)
    return getattr(window, key, default)


def _window_timezone(window: Any) -> timezone | ZoneInfo:
    tz_name = _window_value(window, "timezone", _window_value(window, "tz", "UTC"))
    if not tz_name:
        return timezone.utc
    if str(tz_name).upper() == "UTC":
        return timezone.utc
    try:
        return ZoneInfo(str(tz_name))
    except ZoneInfoNotFoundError:
        return timezone.utc


def _parse_local_time(value: Any) -> Optional[time]:
    if value is None:
        return None
    if isinstance(value, time):
        return value.replace(tzinfo=None)
    try:
        parts = str(value).split(":")
        if len(parts) < 2:
            return None
        hour = int(parts[0])
        minute = int(parts[1])
        return time(hour=hour, minute=minute)
    except (TypeError, ValueError):
        return None


def _parse_local_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _normalise_date_values(value: Any) -> set[date]:
    if value is None:
        return set()
    values = value if isinstance(value, list) else [value]
    return {parsed for item in values if (parsed := _parse_local_date(item))}


def _normalise_weekdays(value: Any) -> Optional[set[int]]:
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


def _matches_embargo_calendar(window: Any, local_day: date) -> bool:
    explicit_dates = _normalise_date_values(
        _window_value(window, "dates", _window_value(window, "date"))
    )
    if explicit_dates and local_day not in explicit_dates:
        return False

    start_date = _parse_local_date(_window_value(window, "start_date"))
    end_date = _parse_local_date(_window_value(window, "end_date"))
    if start_date and local_day < start_date:
        return False
    if end_date and local_day > end_date:
        return False

    weekdays = _normalise_weekdays(
        _window_value(window, "weekdays", _window_value(window, "days"))
    )
    if weekdays is not None and local_day.weekday() not in weekdays:
        return False

    return bool(explicit_dates or start_date or end_date or weekdays is not None)


def is_embargoed(when: datetime, embargo_windows: Optional[list[Any]]) -> bool:
    """Return True when a UTC or aware datetime falls inside an embargo window.

    Window dictionaries support:
      - timezone/tz: IANA timezone name, default UTC
      - date/dates or start_date/end_date: local dates, inclusive
      - start/end: local HH:MM time range, including overnight ranges
      - days/weekdays: local weekdays, names or 0=Monday integers
    """
    if not embargo_windows:
        return False

    aware_when = when if when.tzinfo is not None else when.replace(tzinfo=timezone.utc)

    for window in embargo_windows:
        if _window_value(window, "enabled", True) is False:
            continue

        local_dt = aware_when.astimezone(_window_timezone(window))
        start_time = _parse_local_time(_window_value(window, "start"))
        end_time = _parse_local_time(_window_value(window, "end"))

        if start_time and end_time:
            local_time = local_dt.time().replace(tzinfo=None)
            if start_time <= end_time:
                in_time_range = start_time <= local_time < end_time
                calendar_day = local_dt.date()
            else:
                in_time_range = local_time >= start_time or local_time < end_time
                calendar_day = (
                    local_dt.date() - timedelta(days=1)
                    if local_time < end_time
                    else local_dt.date()
                )
            if not in_time_range:
                continue

            has_calendar_filter = any(
                _window_value(window, key) is not None
                for key in ("date", "dates", "start_date", "end_date", "days", "weekdays")
            )
            if not has_calendar_filter or _matches_embargo_calendar(window, calendar_day):
                return True
            continue

        if _matches_embargo_calendar(window, local_dt.date()):
            return True

    return False


def next_allowed_slot(
    after: datetime,
    embargo_windows: Optional[list[Any]],
    step_minutes: int = 15,
    max_days: int = 366,
) -> datetime:
    """Find the next non-embargoed slot at or after ``after``."""
    if not is_embargoed(after, embargo_windows):
        return after

    step = timedelta(minutes=max(1, step_minutes))
    current = after if after.tzinfo is not None else after.replace(tzinfo=timezone.utc)
    current = current.replace(second=0, microsecond=0) + step
    deadline = current + timedelta(days=max_days)

    while current <= deadline:
        if not is_embargoed(current, embargo_windows):
            return current
        current += step

    return deadline


def embargo_windows_from_config(config: Any) -> list[Any]:
    """Return configured publishing embargo windows, tolerating older configs."""
    publishing = getattr(config, "publishing", None)
    windows = getattr(publishing, "embargo_windows", None)
    return windows if isinstance(windows, list) else []


class PostingScheduleAnalyzer:
    """Analyzes engagement patterns by time-of-day and day-of-week."""

    def __init__(self, db: Database, min_samples: int = 3) -> None:
        """Initialize analyzer.

        Args:
            db: Database instance
            min_samples: Minimum posts per bucket to report (default: 3)
        """
        self.db = db
        self.min_samples = min_samples

    def analyze_optimal_windows(self, days: int = 90) -> list[TimeWindow]:
        """Analyze engagement patterns and return ranked time windows.

        Queries published posts with engagement data, buckets by hour-of-day
        and day-of-week, and returns ranked windows.

        Args:
            days: Number of days to look back (default: 90)

        Returns:
            List of TimeWindow objects, ranked by avg_engagement descending
        """
        # Query published content with engagement scores
        cursor = self.db.conn.execute(
            """SELECT gc.published_at, pe.engagement_score
               FROM generated_content gc
               INNER JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC
                          ) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               WHERE gc.published = 1
                 AND gc.published_at IS NOT NULL
                 AND gc.published_at >= datetime('now', ?)
               ORDER BY gc.published_at""",
            (f'-{days} days',)
        )

        # Bucket by day-of-week and hour
        buckets = {}  # (day_of_week, hour_utc) -> [engagement_scores]

        for row in cursor.fetchall():
            published_at_str = row[0]
            engagement_score = row[1]

            # Parse datetime
            dt = datetime.fromisoformat(published_at_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            day_of_week = dt.weekday()  # 0=Monday, 6=Sunday
            hour_utc = dt.hour

            key = (day_of_week, hour_utc)
            if key not in buckets:
                buckets[key] = []
            buckets[key].append(engagement_score)

        # Calculate stats for each bucket and filter by min_samples
        windows = []
        for (day_of_week, hour_utc), scores in buckets.items():
            sample_size = len(scores)
            if sample_size < self.min_samples:
                continue  # Not enough samples

            avg_engagement = sum(scores) / sample_size

            # Confidence based on sample size (sigmoid-ish curve)
            # 3 samples = ~0.5, 10+ samples = ~0.95
            confidence = min(1.0, sample_size / (sample_size + 5))

            windows.append(TimeWindow(
                day_of_week=day_of_week,
                hour_utc=hour_utc,
                avg_engagement=avg_engagement,
                sample_size=sample_size,
                confidence=confidence
            ))

        # Sort by avg_engagement descending, then confidence descending
        windows.sort(key=lambda w: (w.avg_engagement, w.confidence), reverse=True)

        return windows

    def next_optimal_slot(self, exclude_hours: int = 2) -> Optional[datetime]:
        """Find the next upcoming optimal posting window.

        Args:
            exclude_hours: Minimum hours from now to schedule (default: 2)

        Returns:
            Next optimal datetime to post, or None if no windows available
        """
        windows = self.analyze_optimal_windows()
        if not windows:
            return None

        # Get top 3 windows
        top_windows = windows[:3]
        now = datetime.now(timezone.utc)
        earliest_allowed = now + timedelta(hours=exclude_hours)

        # Find next occurrence of any top window
        candidates = []
        for window in top_windows:
            next_occurrence = self._next_occurrence(
                window.day_of_week,
                window.hour_utc,
                earliest_allowed
            )
            candidates.append((next_occurrence, window.avg_engagement))

        if not candidates:
            return None

        # Sort by time (earliest first), breaking ties with engagement
        candidates.sort(key=lambda x: (x[0], -x[1]))
        return candidates[0][0]

    def should_queue(self, current_hour_utc: int, current_dow: int) -> bool:
        """Determine if current time is outside optimal windows.

        Args:
            current_hour_utc: Current hour (0-23)
            current_dow: Current day of week (0=Monday, 6=Sunday)

        Returns:
            True if we should queue (not in top-3 window), False to post now
        """
        windows = self.analyze_optimal_windows()
        if not windows:
            # No data yet, post immediately
            return False

        # Check if current time matches any top-3 window
        top_windows = windows[:3]
        for window in top_windows:
            if window.day_of_week == current_dow and window.hour_utc == current_hour_utc:
                return False  # In a good window, post now

        return True  # Outside top windows, should queue

    def _next_occurrence(
        self,
        target_dow: int,
        target_hour: int,
        after: datetime
    ) -> datetime:
        """Find next occurrence of a day-of-week and hour after a given time.

        Args:
            target_dow: Target day of week (0=Monday, 6=Sunday)
            target_hour: Target hour (0-23)
            after: Find next occurrence after this datetime

        Returns:
            Next datetime matching the target day and hour
        """
        # Start from the next hour
        current = after.replace(minute=0, second=0, microsecond=0)
        if current <= after:
            current = current + timedelta(hours=1)

        # Search forward up to 7 days
        for _ in range(7 * 24):
            if current.weekday() == target_dow and current.hour == target_hour:
                return current
            current = current + timedelta(hours=1)

        # Fallback: just return target hour on next occurrence of target day
        days_ahead = (target_dow - after.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7  # Next week
        next_date = after + timedelta(days=days_ahead)
        return next_date.replace(hour=target_hour, minute=0, second=0, microsecond=0)
