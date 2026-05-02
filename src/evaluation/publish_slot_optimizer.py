"""Plan non-mutating publish queue moves into stronger open slots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Iterable, Mapping

from evaluation.posting_windows import PostingWindowRecommender

DEFAULT_DAYS = 7
DEFAULT_LIMIT = 10
DEFAULT_HISTORY_DAYS = 90
SUPPORTED_PLATFORMS = ("x", "bluesky")
VALID_PLATFORMS = {"all", *SUPPORTED_PLATFORMS}
DEFAULT_ALLOWED_STATUSES = ("queued",)


@dataclass(frozen=True)
class PublishSlotMove:
    """A proposed publish_queue schedule move."""

    queue_id: int
    content_id: int
    platform: str
    current_scheduled_at: str
    proposed_scheduled_at: str
    reason: str
    current_hour_count: int
    target_score: float
    target_sample_size: int
    target_platforms: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "queue_id": self.queue_id,
            "content_id": self.content_id,
            "platform": self.platform,
            "current_scheduled_at": self.current_scheduled_at,
            "proposed_scheduled_at": self.proposed_scheduled_at,
            "reason": self.reason,
            "current_hour_count": self.current_hour_count,
            "target_score": self.target_score,
            "target_sample_size": self.target_sample_size,
            "target_platforms": list(self.target_platforms),
        }


@dataclass(frozen=True)
class CrowdedSlot:
    """A platform-specific scheduled hour with more than one effective item."""

    platform: str
    hour_start: str
    item_count: int
    queue_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "hour_start": self.hour_start,
            "item_count": self.item_count,
            "queue_ids": list(self.queue_ids),
        }


@dataclass(frozen=True)
class OpenWindow:
    """An empty high-performing platform hour inside the planning horizon."""

    platform: str
    start_time: str
    score: float
    sample_size: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "start_time": self.start_time,
            "score": self.score,
            "sample_size": self.sample_size,
        }


@dataclass(frozen=True)
class PublishSlotOptimizerReport:
    """Dry-run publish slot optimization report."""

    generated_at: str
    window_start: str
    window_end: str
    platform: str
    allowed_statuses: tuple[str, ...]
    crowded_slots: tuple[CrowdedSlot, ...]
    open_windows: tuple[OpenWindow, ...]
    moves: tuple[PublishSlotMove, ...]
    missing_required: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "publish_slot_optimizer",
            "generated_at": self.generated_at,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "platform": self.platform,
            "allowed_statuses": list(self.allowed_statuses),
            "missing_required": list(self.missing_required),
            "crowded_slot_count": len(self.crowded_slots),
            "open_window_count": len(self.open_windows),
            "move_count": len(self.moves),
            "crowded_slots": [slot.to_dict() for slot in self.crowded_slots],
            "open_windows": [window.to_dict() for window in self.open_windows],
            "moves": [move.to_dict() for move in self.moves],
        }


@dataclass(frozen=True)
class _QueueItem:
    queue_id: int
    content_id: int
    platform: str
    status: str
    scheduled_at: datetime
    scheduled_at_raw: str

    @property
    def targets(self) -> tuple[str, ...]:
        return _effective_platforms(self.platform)


@dataclass(frozen=True)
class _Candidate:
    start_time: datetime
    platform: str
    score: float
    sample_size: int


def build_publish_slot_optimizer_report(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str = "all",
    limit: int = DEFAULT_LIMIT,
    allowed_statuses: Iterable[str] | None = None,
    history_days: int = DEFAULT_HISTORY_DAYS,
    now: datetime | None = None,
) -> PublishSlotOptimizerReport:
    """Return proposed queue moves into empty high-performing publish windows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit < 0:
        raise ValueError("limit must be non-negative")
    selected_platforms = _selected_platforms(platform)
    statuses = _normalise_statuses(allowed_statuses)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    window_start = generated_at
    window_end = generated_at + timedelta(days=days)

    missing = _missing_required(db)
    if missing:
        return PublishSlotOptimizerReport(
            generated_at=generated_at.isoformat(),
            window_start=window_start.isoformat(),
            window_end=window_end.isoformat(),
            platform=platform,
            allowed_statuses=statuses,
            crowded_slots=(),
            open_windows=(),
            moves=(),
            missing_required=tuple(missing),
        )

    items = _load_queue_items(
        db,
        window_start=window_start,
        window_end=window_end,
        allowed_statuses=statuses,
    )
    crowded_slots = tuple(_crowded_slots(items, selected_platforms))
    occupied = _occupied_hours(items)
    candidates = _candidate_windows(
        db,
        selected_platforms=selected_platforms,
        occupied=occupied,
        now=generated_at,
        days=days,
        history_days=history_days,
    )
    open_windows = tuple(
        OpenWindow(
            platform=candidate.platform,
            start_time=candidate.start_time.isoformat(),
            score=round(candidate.score, 2),
            sample_size=candidate.sample_size,
        )
        for candidate in candidates
    )
    moves = tuple(
        _plan_moves(
            items=items,
            selected_platforms=selected_platforms,
            crowded_slots=crowded_slots,
            candidates=candidates,
            occupied=occupied,
            limit=limit,
        )
    )
    return PublishSlotOptimizerReport(
        generated_at=generated_at.isoformat(),
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        platform=platform,
        allowed_statuses=statuses,
        crowded_slots=crowded_slots,
        open_windows=open_windows,
        moves=moves,
    )


def format_publish_slot_optimizer_json(report: PublishSlotOptimizerReport) -> str:
    """Format a publish slot optimization report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publish_slot_optimizer_text(report: PublishSlotOptimizerReport) -> str:
    """Format a publish slot optimization report for operator review."""
    lines = [
        "Publish Slot Optimizer",
        f"Generated: {report.generated_at}",
        f"Window: {report.window_start} to {report.window_end}",
        f"Platform: {report.platform}",
        f"Statuses: {', '.join(report.allowed_statuses)}",
    ]
    if report.missing_required:
        lines.append(f"Missing required tables: {', '.join(report.missing_required)}")
        return "\n".join(lines)

    if report.crowded_slots:
        lines.append("")
        lines.append("Crowded scheduled hours:")
        for slot in report.crowded_slots:
            queue_ids = ", ".join(str(queue_id) for queue_id in slot.queue_ids)
            lines.append(
                f"  - {slot.platform} {slot.hour_start}: "
                f"{slot.item_count} items ({queue_ids})"
            )
    else:
        lines.append("")
        lines.append("No crowded scheduled hours found.")

    if report.moves:
        lines.append("")
        lines.append("Proposed moves:")
        for move in report.moves:
            lines.append(
                f"  - queue {move.queue_id}: {move.current_scheduled_at} -> "
                f"{move.proposed_scheduled_at}; {move.reason}"
            )
    else:
        lines.append("")
        lines.append("No queue moves proposed.")
    return "\n".join(lines)


def _plan_moves(
    *,
    items: list[_QueueItem],
    selected_platforms: tuple[str, ...],
    crowded_slots: tuple[CrowdedSlot, ...],
    candidates: list[_Candidate],
    occupied: set[tuple[str, str]],
    limit: int,
) -> list[PublishSlotMove]:
    crowded_keys = {
        (slot.platform, slot.hour_start): slot for slot in crowded_slots
    }
    movable: list[tuple[_QueueItem, CrowdedSlot]] = []
    kept: set[tuple[str, str]] = set()
    for item in sorted(items, key=lambda value: (value.scheduled_at, value.queue_id)):
        matching = [
            crowded_keys[(target, _hour_key(item.scheduled_at))]
            for target in item.targets
            if target in selected_platforms
            and (target, _hour_key(item.scheduled_at)) in crowded_keys
        ]
        if not matching:
            continue
        keep_key = (matching[0].platform, matching[0].hour_start)
        if keep_key not in kept:
            kept.add(keep_key)
            continue
        movable.append((item, max(matching, key=lambda slot: slot.item_count)))

    moves: list[PublishSlotMove] = []
    reserved = set(occupied)
    for item, crowded_slot in movable:
        candidate = _best_candidate_for_item(item, candidates, reserved)
        if candidate is None:
            continue
        for target in item.targets:
            reserved.add((target, _hour_key(candidate.start_time)))
        reason = (
            f"{crowded_slot.platform} hour has {crowded_slot.item_count} queued items; "
            f"proposed empty high-performing {candidate.platform} window "
            f"(score {candidate.score:.2f}, sample size {candidate.sample_size})."
        )
        moves.append(
            PublishSlotMove(
                queue_id=item.queue_id,
                content_id=item.content_id,
                platform=item.platform,
                current_scheduled_at=item.scheduled_at_raw,
                proposed_scheduled_at=candidate.start_time.isoformat(),
                reason=reason,
                current_hour_count=crowded_slot.item_count,
                target_score=round(candidate.score, 2),
                target_sample_size=candidate.sample_size,
                target_platforms=item.targets,
            )
        )
        if len(moves) >= limit:
            break
    return moves


def _best_candidate_for_item(
    item: _QueueItem,
    candidates: list[_Candidate],
    reserved: set[tuple[str, str]],
) -> _Candidate | None:
    targets = set(item.targets)
    for candidate in candidates:
        hour = _hour_key(candidate.start_time)
        if any((target, hour) in reserved for target in targets):
            continue
        if candidate.platform not in targets:
            continue
        return candidate
    return None


def _candidate_windows(
    db: Any,
    *,
    selected_platforms: tuple[str, ...],
    occupied: set[tuple[str, str]],
    now: datetime,
    days: int,
    history_days: int,
) -> list[_Candidate]:
    recommender = PostingWindowRecommender(db)
    candidates: dict[tuple[str, str], _Candidate] = {}
    for platform in selected_platforms:
        windows = recommender.recommend(
            days=max(1, int(history_days)),
            platform=platform,
            limit=None,
        )
        for window in windows:
            for start_time in _upcoming_start_times(
                window.day_of_week,
                window.hour_utc,
                now=now,
                days=days,
            ):
                key = (platform, _hour_key(start_time))
                if key in occupied:
                    continue
                candidates[key] = _Candidate(
                    start_time=start_time,
                    platform=platform,
                    score=window.normalized_engagement,
                    sample_size=window.sample_size,
                )
    return sorted(
        candidates.values(),
        key=lambda item: (item.score, item.sample_size, -item.start_time.timestamp()),
        reverse=True,
    )


def _crowded_slots(
    items: list[_QueueItem],
    selected_platforms: tuple[str, ...],
) -> list[CrowdedSlot]:
    grouped: dict[tuple[str, str], list[int]] = {}
    for item in items:
        hour = _hour_key(item.scheduled_at)
        for target in item.targets:
            if target in selected_platforms:
                grouped.setdefault((target, hour), []).append(item.queue_id)

    slots: list[CrowdedSlot] = []
    for (platform, hour), queue_ids in sorted(grouped.items()):
        if len(queue_ids) <= 1:
            continue
        slots.append(
            CrowdedSlot(
                platform=platform,
                hour_start=hour,
                item_count=len(queue_ids),
                queue_ids=tuple(queue_ids),
            )
        )
    return slots


def _occupied_hours(items: list[_QueueItem]) -> set[tuple[str, str]]:
    occupied: set[tuple[str, str]] = set()
    for item in items:
        hour = _hour_key(item.scheduled_at)
        for target in item.targets:
            occupied.add((target, hour))
    return occupied


def _load_queue_items(
    db: Any,
    *,
    window_start: datetime,
    window_end: datetime,
    allowed_statuses: tuple[str, ...],
) -> list[_QueueItem]:
    placeholders = ", ".join("?" for _ in allowed_statuses)
    cursor = db.conn.execute(
        f"""SELECT id, content_id, platform, status, scheduled_at
            FROM publish_queue
            WHERE status IN ({placeholders})
              AND scheduled_at >= ?
              AND scheduled_at < ?
            ORDER BY scheduled_at ASC, id ASC""",
        (*allowed_statuses, window_start.isoformat(), window_end.isoformat()),
    )
    items: list[_QueueItem] = []
    for row in cursor.fetchall():
        data = dict(row)
        scheduled_at = str(data["scheduled_at"])
        items.append(
            _QueueItem(
                queue_id=int(data["id"]),
                content_id=int(data["content_id"]),
                platform=str(data.get("platform") or "all"),
                status=str(data.get("status") or "queued"),
                scheduled_at=_parse_datetime(scheduled_at),
                scheduled_at_raw=scheduled_at,
            )
        )
    return items


def _missing_required(db: Any) -> list[str]:
    existing = _schema(db)
    required = ("publish_queue", "content_publications", "post_engagement")
    return [table for table in required if table not in existing]


def _schema(db: Any) -> dict[str, set[str]]:
    conn = getattr(db, "conn", db)
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    names = [row[0] if not isinstance(row, Mapping) else row["name"] for row in rows]
    return {
        name: {column[1] for column in conn.execute(f"PRAGMA table_info({name})")}
        for name in names
    }


def _selected_platforms(platform: str) -> tuple[str, ...]:
    normalized = str(platform).strip().lower()
    if normalized not in VALID_PLATFORMS:
        raise ValueError("platform must be one of: all, x, bluesky")
    return SUPPORTED_PLATFORMS if normalized == "all" else (normalized,)


def _normalise_statuses(statuses: Iterable[str] | None) -> tuple[str, ...]:
    raw = DEFAULT_ALLOWED_STATUSES if statuses is None else tuple(statuses)
    normalized = tuple(
        dict.fromkeys(str(status).strip().lower() for status in raw if str(status).strip())
    )
    if not normalized:
        raise ValueError("allowed_statuses must include at least one status")
    return normalized


def _effective_platforms(platform: str) -> tuple[str, ...]:
    normalized = str(platform or "all").strip().lower()
    if normalized == "all":
        return SUPPORTED_PLATFORMS
    if normalized in SUPPORTED_PLATFORMS:
        return (normalized,)
    return ()


def _upcoming_start_times(
    day_of_week: int,
    hour_utc: int,
    *,
    now: datetime,
    days: int,
) -> list[datetime]:
    horizon_end = now + timedelta(days=days)
    days_ahead = (day_of_week - now.weekday()) % 7
    candidate = now.replace(hour=hour_utc, minute=0, second=0, microsecond=0) + timedelta(
        days=days_ahead
    )
    if candidate < now:
        candidate += timedelta(days=7)

    starts: list[datetime] = []
    while candidate < horizon_end:
        starts.append(candidate)
        candidate += timedelta(days=7)
    return starts


def _hour_key(value: datetime) -> str:
    return value.replace(minute=0, second=0, microsecond=0).isoformat()


def _parse_datetime(value: str) -> datetime:
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return _ensure_utc(datetime.fromisoformat(text))


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def publish_slot_moves_to_dicts(moves: Iterable[PublishSlotMove]) -> list[dict[str, Any]]:
    """Serialize move proposals for JSON callers."""
    return [move.to_dict() for move in moves]
