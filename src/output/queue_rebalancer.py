"""Plan and apply publish queue schedule rebalancing."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from typing import Any, Mapping, Sequence

from evaluation.posting_schedule import is_embargoed, next_allowed_slot
from output.publish_caps import (
    DEFAULT_DAILY_PLATFORM_LIMITS,
    daily_platform_limits_from_config,
    utc_day_bounds,
)

DEFAULT_REBALANCE_DAYS = 7
REBALANCE_PLATFORMS = ("x", "bluesky")


@dataclass(frozen=True)
class QueueRebalanceChange:
    """Proposed schedule change for one publish_queue row."""

    queue_id: int
    content_id: int
    platform: str
    scheduled_at: str
    proposed_scheduled_at: str
    reasons: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "queue_id": self.queue_id,
            "content_id": self.content_id,
            "platform": self.platform,
            "scheduled_at": self.scheduled_at,
            "proposed_scheduled_at": self.proposed_scheduled_at,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class QueueCapViolation:
    """Cap pressure found for a platform on one UTC day."""

    platform: str
    day: str
    limit: int
    scheduled_count: int
    published_count: int
    excess_count: int
    queue_ids: list[int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "day": self.day,
            "limit": self.limit,
            "scheduled_count": self.scheduled_count,
            "published_count": self.published_count,
            "effective_count": self.scheduled_count + self.published_count,
            "excess_count": self.excess_count,
            "queue_ids": list(self.queue_ids),
        }


@dataclass
class QueueRebalanceReport:
    """Dry-run or applied publish queue rebalance result."""

    generated_at: str
    window_start: str
    window_end: str
    platform: str
    daily_platform_limits: dict[str, int]
    quiet_hours: list[dict[str, str]]
    violations: list[QueueCapViolation]
    changes: list[QueueRebalanceChange]
    applied_count: int = 0
    skipped_count: int = 0
    applied_queue_ids: list[int] = field(default_factory=list)

    @property
    def change_count(self) -> int:
        return len(self.changes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "platform": self.platform,
            "daily_platform_limits": dict(self.daily_platform_limits),
            "quiet_hours": list(self.quiet_hours),
            "violation_count": len(self.violations),
            "change_count": self.change_count,
            "applied_count": self.applied_count,
            "skipped_count": self.skipped_count,
            "applied_queue_ids": list(self.applied_queue_ids),
            "violations": [violation.to_dict() for violation in self.violations],
            "changes": [change.to_dict() for change in self.changes],
        }


def plan_publish_queue_rebalance(
    db: Any,
    config: Any | None = None,
    *,
    days: int = DEFAULT_REBALANCE_DAYS,
    platform: str = "all",
    daily_platform_limits: Mapping[str, int] | None = None,
    quiet_hours: Sequence[Any] | None = None,
    now: datetime | None = None,
) -> QueueRebalanceReport:
    """Return proposed schedule changes for queued rows that exceed daily caps."""
    if days < 0:
        raise ValueError("days must be non-negative")
    selected_platforms = _selected_platforms(platform)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    window_start = generated_at
    window_end = generated_at + timedelta(days=days)
    limits = _limits(config, daily_platform_limits)
    windows = normalise_quiet_hours(quiet_hours or [])
    rows = _queue_rows(db)
    queued_items = [_item_from_row(row) for row in rows]
    violations = _cap_violations(
        db,
        queued_items,
        selected_platforms,
        limits,
        window_start,
        window_end,
    )
    changes = _planned_changes(
        db,
        queued_items,
        selected_platforms,
        limits,
        windows,
        window_start,
        window_end,
    )
    return QueueRebalanceReport(
        generated_at=generated_at.isoformat(),
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        platform=platform,
        daily_platform_limits=limits,
        quiet_hours=windows,
        violations=violations,
        changes=changes,
    )


def apply_publish_queue_rebalance(db: Any, report: QueueRebalanceReport) -> QueueRebalanceReport:
    """Apply proposed changes to still-queued rows and return update counts."""
    applied_ids: list[int] = []
    skipped_count = 0
    for change in report.changes:
        cursor = db.conn.execute(
            """UPDATE publish_queue
               SET scheduled_at = ?
               WHERE id = ?
                 AND status = 'queued'
                 AND scheduled_at = ?""",
            (
                change.proposed_scheduled_at,
                change.queue_id,
                change.scheduled_at,
            ),
        )
        if cursor.rowcount:
            applied_ids.append(change.queue_id)
        else:
            skipped_count += 1
    db.conn.commit()
    report.applied_count = len(applied_ids)
    report.skipped_count = skipped_count
    report.applied_queue_ids = applied_ids
    return report


def parse_quiet_hours(value: str | None) -> list[dict[str, str]]:
    """Parse comma-separated quiet hour ranges as UTC embargo windows."""
    if not value:
        return []
    windows: list[dict[str, str]] = []
    for raw in value.split(","):
        part = raw.strip()
        if not part:
            continue
        if "-" not in part:
            raise ValueError("quiet hours must use HH:MM-HH:MM ranges")
        start, end = (item.strip() for item in part.split("-", 1))
        _parse_time(start)
        _parse_time(end)
        windows.append({"start": start, "end": end, "timezone": "UTC"})
    return windows


def normalise_quiet_hours(values: Sequence[Any]) -> list[dict[str, str]]:
    """Return quiet hours in the format accepted by posting_schedule helpers."""
    windows: list[dict[str, str]] = []
    for value in values:
        if isinstance(value, str):
            windows.extend(parse_quiet_hours(value))
            continue
        if isinstance(value, Mapping):
            start = str(value.get("start", "")).strip()
            end = str(value.get("end", "")).strip()
            if not start or not end:
                continue
            _parse_time(start)
            _parse_time(end)
            timezone_name = str(value.get("timezone") or value.get("tz") or "UTC")
            windows.append({"start": start, "end": end, "timezone": timezone_name})
    return windows


def format_queue_rebalance_report_json(report: QueueRebalanceReport) -> str:
    """Format a rebalance report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_queue_rebalance_report_text(
    report: QueueRebalanceReport,
    *,
    applied: bool = False,
) -> str:
    """Format a rebalance report for operator review."""
    lines = [
        "Publish queue rebalance report",
        f"Window: {report.window_start} to {report.window_end}",
        f"Platform: {report.platform}",
    ]
    if not report.daily_platform_limits:
        lines.append("No daily platform caps configured.")
    if report.quiet_hours:
        quiet = ", ".join(
            f"{window['start']}-{window['end']} {window['timezone']}"
            for window in report.quiet_hours
        )
        lines.append(f"Quiet hours: {quiet}")

    if report.violations:
        lines.append("Cap violations:")
        for violation in report.violations:
            lines.append(
                "- {platform} {day}: effective {effective}/{limit} "
                "({scheduled} queued, {published} published), excess {excess}; "
                "queue IDs {queue_ids}".format(
                    platform=violation.platform,
                    day=violation.day,
                    effective=violation.scheduled_count + violation.published_count,
                    limit=violation.limit,
                    scheduled=violation.scheduled_count,
                    published=violation.published_count,
                    excess=violation.excess_count,
                    queue_ids=", ".join(str(item) for item in violation.queue_ids),
                )
            )
    else:
        lines.append("No cap violations found.")

    if report.changes:
        lines.append("Proposed schedule changes:")
        for change in report.changes:
            lines.append(
                f"- queue {change.queue_id} ({change.platform}): "
                f"{change.scheduled_at} -> {change.proposed_scheduled_at}; "
                f"{'; '.join(change.reasons)}"
            )
    else:
        lines.append("No schedule changes proposed.")

    if applied:
        lines.append(
            f"Applied updates to {report.applied_count} queued row(s); "
            f"skipped {report.skipped_count} row(s)."
        )
    else:
        lines.append("Dry run: no queue rows were changed.")
    return "\n".join(lines)


@dataclass(frozen=True)
class _QueueItem:
    queue_id: int
    content_id: int
    platform: str
    scheduled_at: datetime
    scheduled_at_raw: str

    @property
    def targets(self) -> tuple[str, ...]:
        return _queue_targets(self.platform)


def _planned_changes(
    db: Any,
    items: list[_QueueItem],
    selected_platforms: list[str],
    limits: Mapping[str, int],
    quiet_hours: list[dict[str, str]],
    window_start: datetime,
    window_end: datetime,
) -> list[QueueRebalanceChange]:
    if not limits:
        return []

    day_counts = _initial_day_counts(db, items, limits)
    last_by_platform: dict[str, datetime] = {}
    changes: list[QueueRebalanceChange] = []

    for item in sorted(items, key=lambda value: (value.scheduled_at, value.queue_id)):
        targets = [target for target in item.targets if target in selected_platforms]
        if not targets:
            continue
        if not (window_start <= item.scheduled_at < window_end):
            _reserve(day_counts, item, item.scheduled_at, targets)
            for target in targets:
                last_by_platform[target] = max(
                    last_by_platform.get(target, item.scheduled_at),
                    item.scheduled_at,
                )
            continue

        candidate = item.scheduled_at
        for target in targets:
            previous = last_by_platform.get(target)
            if previous and candidate < previous:
                candidate = previous

        proposed, reasons = _next_candidate(
            item,
            candidate,
            targets,
            day_counts,
            limits,
            quiet_hours,
        )
        _reserve(day_counts, item, proposed, targets)
        for target in targets:
            last_by_platform[target] = proposed
        if proposed != item.scheduled_at:
            changes.append(
                QueueRebalanceChange(
                    queue_id=item.queue_id,
                    content_id=item.content_id,
                    platform=item.platform,
                    scheduled_at=item.scheduled_at_raw,
                    proposed_scheduled_at=proposed.isoformat(),
                    reasons=reasons,
                )
            )
    return changes


def _next_candidate(
    item: _QueueItem,
    candidate: datetime,
    targets: list[str],
    day_counts: dict[tuple[str, str], int],
    limits: Mapping[str, int],
    quiet_hours: list[dict[str, str]],
) -> tuple[datetime, list[str]]:
    reasons: list[str] = []
    quiet_adjusted = next_allowed_slot(candidate, quiet_hours)
    if quiet_adjusted != candidate:
        reasons.append(
            "quiet_hours: moved outside configured quiet hours "
            f"from {candidate.isoformat()}"
        )
        candidate = quiet_adjusted

    original_day = _day_key(item.scheduled_at)
    cap_reason_added = False
    deadline = candidate + timedelta(days=366)
    while candidate <= deadline:
        candidate = next_allowed_slot(candidate, quiet_hours)
        blocking = _blocking_caps(candidate, targets, day_counts, limits)
        if not blocking:
            if _day_key(candidate) != original_day and not cap_reason_added:
                reasons.append("daily_cap: moved to next day with available platform capacity")
            if not reasons:
                reasons.append("daily_cap: rebalanced overloaded platform day")
            return candidate, reasons
        if not cap_reason_added:
            reasons.append(
                "daily_cap: " + ", ".join(
                    f"{platform} {count}/{limit}"
                    for platform, count, limit in blocking
                )
            )
            cap_reason_added = True
        candidate = _next_day_same_time(candidate)

    raise ValueError(f"no available rebalance slot found for queue item {item.queue_id}")


def _blocking_caps(
    candidate: datetime,
    targets: list[str],
    day_counts: dict[tuple[str, str], int],
    limits: Mapping[str, int],
) -> list[tuple[str, int, int]]:
    day = _day_key(candidate)
    blocking: list[tuple[str, int, int]] = []
    for platform in targets:
        limit = limits.get(platform)
        if limit is None:
            continue
        current = day_counts.get((platform, day), 0)
        if current >= limit:
            blocking.append((platform, current, limit))
    return blocking


def _initial_day_counts(
    db: Any,
    items: list[_QueueItem],
    limits: Mapping[str, int],
) -> dict[tuple[str, str], int]:
    days = sorted({_day_key(item.scheduled_at) for item in items})
    counts: dict[tuple[str, str], int] = {}
    for platform in REBALANCE_PLATFORMS:
        if platform not in limits:
            continue
        for day in days:
            day_start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
            published = _published_count(db, platform, day_start)
            counts[(platform, day)] = published
    return counts


def _reserve(
    day_counts: dict[tuple[str, str], int],
    item: _QueueItem,
    scheduled_at: datetime,
    targets: list[str] | None = None,
) -> None:
    day = _day_key(scheduled_at)
    for platform in targets or list(item.targets):
        key = (platform, day)
        if key in day_counts:
            day_counts[key] += 1


def _cap_violations(
    db: Any,
    items: list[_QueueItem],
    selected_platforms: list[str],
    limits: Mapping[str, int],
    window_start: datetime,
    window_end: datetime,
) -> list[QueueCapViolation]:
    grouped: dict[tuple[str, str], list[int]] = {}
    for item in items:
        if not (window_start <= item.scheduled_at < window_end):
            continue
        for target in item.targets:
            if target in selected_platforms and target in limits:
                grouped.setdefault((target, _day_key(item.scheduled_at)), []).append(
                    item.queue_id
                )

    violations: list[QueueCapViolation] = []
    for (platform, day), queue_ids in sorted(grouped.items()):
        limit = limits[platform]
        day_start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
        published_count = _published_count(db, platform, day_start)
        effective_count = published_count + len(queue_ids)
        if effective_count <= limit:
            continue
        violations.append(
            QueueCapViolation(
                platform=platform,
                day=day,
                limit=limit,
                scheduled_count=len(queue_ids),
                published_count=published_count,
                excess_count=effective_count - limit,
                queue_ids=queue_ids,
            )
        )
    return violations


def _published_count(db: Any, platform: str, day_start: datetime) -> int:
    start, end = utc_day_bounds(day_start)
    row = db.conn.execute(
        """SELECT COUNT(*) AS count
           FROM content_publications
           WHERE platform = ?
             AND status = 'published'
             AND published_at IS NOT NULL
             AND published_at >= ?
             AND published_at < ?""",
        (platform, start.isoformat(), end.isoformat()),
    ).fetchone()
    return int(row["count"] if row else 0)


def _queue_rows(db: Any) -> list[dict[str, Any]]:
    cursor = db.conn.execute(
        """SELECT pq.id, pq.content_id, pq.scheduled_at, pq.platform, pq.status
           FROM publish_queue pq
           WHERE pq.status = 'queued'
           ORDER BY pq.scheduled_at ASC, pq.id ASC"""
    )
    return [dict(row) for row in cursor.fetchall()]


def _item_from_row(row: Mapping[str, Any]) -> _QueueItem:
    scheduled_at_raw = str(row["scheduled_at"])
    return _QueueItem(
        queue_id=int(row["id"]),
        content_id=int(row["content_id"]),
        platform=str(row.get("platform") or "all"),
        scheduled_at=_parse_datetime(scheduled_at_raw),
        scheduled_at_raw=scheduled_at_raw,
    )


def _limits(
    config: Any | None,
    daily_platform_limits: Mapping[str, int] | None,
) -> dict[str, int]:
    raw_limits = (
        dict(daily_platform_limits)
        if daily_platform_limits is not None
        else daily_platform_limits_from_config(config)
    )
    if not raw_limits:
        raw_limits = DEFAULT_DAILY_PLATFORM_LIMITS
    limits: dict[str, int] = {}
    for platform, limit in raw_limits.items():
        if platform not in REBALANCE_PLATFORMS:
            continue
        try:
            normalized = int(limit)
        except (TypeError, ValueError):
            continue
        if normalized >= 0:
            limits[platform] = normalized
    return limits


def _selected_platforms(platform: str) -> list[str]:
    normalized = str(platform).strip().lower()
    if normalized == "all":
        return list(REBALANCE_PLATFORMS)
    if normalized not in REBALANCE_PLATFORMS:
        raise ValueError("platform must be one of: all, x, bluesky")
    return [normalized]


def _queue_targets(platform: str) -> tuple[str, ...]:
    if platform == "all":
        return REBALANCE_PLATFORMS
    if platform in REBALANCE_PLATFORMS:
        return (platform,)
    return ()


def _next_day_same_time(value: datetime) -> datetime:
    return value + timedelta(days=1)


def _day_key(value: datetime) -> str:
    start, _ = utc_day_bounds(value)
    return start.date().isoformat()


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_time(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("quiet hours must use HH:MM-HH:MM ranges") from exc
