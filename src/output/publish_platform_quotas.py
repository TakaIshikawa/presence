"""Read-only planner for publish queue platform daily quotas."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Mapping


DEFAULT_DAYS = 7
DEFAULT_LIMIT = 50
SUPPORTED_PLATFORMS = ("x", "bluesky", "mastodon", "linkedin")
DEFAULT_PLATFORM_QUOTAS: dict[str, int] = {}
LEGACY_ALL_PLATFORMS = ("x", "bluesky")
SUPPRESSED_PUBLICATION_STATUSES = {"published", "failed"}


@dataclass(frozen=True)
class QueuedPlatformItem:
    """One queued row expanded to one effective platform target."""

    queue_id: int
    content_id: int
    queue_platform: str
    platform: str
    scheduled_at: datetime
    scheduled_at_raw: str
    local_date: str
    ordinal: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "queue_id": self.queue_id,
            "content_id": self.content_id,
            "queue_platform": self.queue_platform,
            "platform": self.platform,
            "scheduled_at": self.scheduled_at.isoformat(),
            "scheduled_at_raw": self.scheduled_at_raw,
            "local_date": self.local_date,
            "ordinal": self.ordinal,
        }


@dataclass(frozen=True)
class PlatformQuotaBreach:
    """A platform/date bucket exceeding its configured daily quota."""

    platform: str
    local_date: str
    quota: int
    scheduled_count: int
    excess_count: int
    queue_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["queue_ids"] = list(self.queue_ids)
        return data


@dataclass(frozen=True)
class PlatformQuotaDeferral:
    """Suggested deferral for one queued item/platform target."""

    queue_id: int
    content_id: int
    platform: str
    current_scheduled_at: str
    reason: str
    suggested_date: str
    queue_platform: str
    current_local_date: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PublishPlatformQuotaReport:
    """Quota breach and deferral plan for queued publish items."""

    generated_at: str
    filters: dict[str, Any]
    quotas: dict[str, int]
    totals: dict[str, int]
    breaches: tuple[PlatformQuotaBreach, ...]
    deferrals: tuple[PlatformQuotaDeferral, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": self.filters,
            "quotas": dict(self.quotas),
            "totals": self.totals,
            "breach_count": len(self.breaches),
            "deferral_count": len(self.deferrals),
            "breaches": [breach.to_dict() for breach in self.breaches],
            "deferrals": [deferral.to_dict() for deferral in self.deferrals],
        }


def plan_publish_platform_quotas(
    db_or_conn: Any,
    *,
    platform: str = "all",
    quotas: Mapping[str, int] | None = None,
    days: int = DEFAULT_DAYS,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> PublishPlatformQuotaReport:
    """Detect daily platform quota breaches and suggest read-only deferrals."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    selected_platforms = _selected_platforms(platform)
    normalized_quotas = _normalize_quotas(quotas)
    active_quotas = {
        name: normalized_quotas[name]
        for name in selected_platforms
        if name in normalized_quotas
    }
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    window_end = generated_at + timedelta(days=days)
    conn = _connection(db_or_conn)

    rows = _queue_rows(conn, generated_at, window_end)
    publication_statuses = _publication_statuses(
        conn,
        sorted({int(row["content_id"]) for row in rows}),
    )
    items = _expand_rows(rows, publication_statuses, selected_platforms)
    breaches = _quota_breaches(items, active_quotas)
    deferrals = _suggest_deferrals(items, active_quotas)
    if limit is not None:
        deferrals = deferrals[:limit]

    return PublishPlatformQuotaReport(
        generated_at=generated_at.isoformat(),
        filters={
            "platform": platform,
            "days": days,
            "limit": limit,
            "window_start": generated_at.isoformat(),
            "window_end": window_end.isoformat(),
        },
        quotas=active_quotas,
        totals={
            "queue_rows": len(rows),
            "expanded_items": len(items),
            "breach_days": len(breaches),
            "deferrals": len(deferrals),
        },
        breaches=tuple(breaches),
        deferrals=tuple(deferrals),
    )


def format_publish_platform_quotas_json(report: PublishPlatformQuotaReport) -> str:
    """Serialize a publish platform quota report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publish_platform_quotas_text(report: PublishPlatformQuotaReport) -> str:
    """Format a publish platform quota report for terminal review."""
    lines = [
        "Publish Platform Quota Plan",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['window_start']} to "
            f"{report.filters['window_end']}"
        ),
        f"Platform: {report.filters['platform']}",
    ]
    if report.quotas:
        quota_text = ", ".join(
            f"{platform}={quota}" for platform, quota in sorted(report.quotas.items())
        )
        lines.append(f"Quotas: {quota_text}")
    else:
        lines.append("No platform quotas configured.")
    lines.append(
        f"Scanned: rows={report.totals['queue_rows']} "
        f"expanded={report.totals['expanded_items']}"
    )

    if not report.breaches:
        lines.append("No quota breaches found.")
        return "\n".join(lines)

    lines.append("Quota breaches:")
    for breach in report.breaches:
        lines.append(
            f"- {breach.platform} {breach.local_date}: "
            f"{breach.scheduled_count}/{breach.quota}, excess={breach.excess_count}; "
            f"queue IDs {', '.join(str(queue_id) for queue_id in breach.queue_ids)}"
        )

    if report.deferrals:
        lines.append("Suggested deferrals:")
        for deferral in report.deferrals:
            lines.append(
                f"- queue {deferral.queue_id} content {deferral.content_id} "
                f"platform={deferral.platform}: {deferral.current_scheduled_at} "
                f"-> {deferral.suggested_date}; {deferral.reason}"
            )
    else:
        lines.append("No deferrals proposed.")
    return "\n".join(lines)


def parse_quota_options(values: list[str] | None) -> dict[str, int]:
    """Parse repeated PLATFORM=N quota options."""
    quotas: dict[str, int] = {}
    for value in values or []:
        if "=" not in value:
            raise ValueError("quota must use PLATFORM=N")
        platform, raw_limit = (part.strip().lower() for part in value.split("=", 1))
        if platform not in SUPPORTED_PLATFORMS:
            raise ValueError(
                "quota platform must be one of: " + ", ".join(SUPPORTED_PLATFORMS)
            )
        try:
            limit = int(raw_limit)
        except ValueError as exc:
            raise ValueError("quota limit must be an integer") from exc
        if limit < 0:
            raise ValueError("quota limit must be non-negative")
        quotas[platform] = limit
    return quotas


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _queue_rows(
    conn: sqlite3.Connection,
    window_start: datetime,
    window_end: datetime,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT id, content_id, scheduled_at, platform, status
           FROM publish_queue
           WHERE status = 'queued'
             AND scheduled_at >= ?
             AND scheduled_at < ?
           ORDER BY scheduled_at ASC, id ASC""",
        (window_start.isoformat(), window_end.isoformat()),
    ).fetchall()
    return [dict(row) for row in rows]


def _publication_statuses(
    conn: sqlite3.Connection,
    content_ids: list[int],
) -> dict[int, dict[str, str]]:
    if not content_ids:
        return {}
    placeholders = ",".join("?" for _ in content_ids)
    rows = conn.execute(
        f"""SELECT content_id, platform, status
            FROM content_publications
            WHERE content_id IN ({placeholders})
            ORDER BY content_id ASC, platform ASC, id ASC""",
        content_ids,
    ).fetchall()
    statuses: dict[int, dict[str, str]] = defaultdict(dict)
    for row in rows:
        platform = str(row["platform"]).lower()
        content_id = int(row["content_id"])
        if platform in SUPPORTED_PLATFORMS and platform not in statuses[content_id]:
            statuses[content_id][platform] = str(row["status"]).lower()
    return statuses


def _expand_rows(
    rows: list[dict[str, Any]],
    publication_statuses: dict[int, dict[str, str]],
    selected_platforms: tuple[str, ...],
) -> list[QueuedPlatformItem]:
    items: list[QueuedPlatformItem] = []
    ordinal = 0
    for row in rows:
        content_id = int(row["content_id"])
        queue_platform = str(row.get("platform") or "all").lower()
        scheduled_at_raw = str(row["scheduled_at"])
        parsed_scheduled_at = datetime.fromisoformat(scheduled_at_raw)
        scheduled_at = _as_utc(parsed_scheduled_at)
        statuses = publication_statuses.get(content_id, {})
        for target in _effective_targets(queue_platform, statuses):
            if target not in selected_platforms:
                continue
            status = statuses.get(target)
            if status in SUPPRESSED_PUBLICATION_STATUSES:
                continue
            ordinal += 1
            items.append(
                QueuedPlatformItem(
                    queue_id=int(row["id"]),
                    content_id=content_id,
                    queue_platform=queue_platform,
                    platform=target,
                    scheduled_at=scheduled_at,
                    scheduled_at_raw=scheduled_at_raw,
                    local_date=parsed_scheduled_at.date().isoformat(),
                    ordinal=ordinal,
                )
            )
    return items


def _effective_targets(
    queue_platform: str,
    statuses: Mapping[str, str],
) -> tuple[str, ...]:
    if queue_platform == "all":
        queued_statuses = tuple(
            platform
            for platform in SUPPORTED_PLATFORMS
            if statuses.get(platform) == "queued"
        )
        if queued_statuses:
            return queued_statuses
        return LEGACY_ALL_PLATFORMS
    if queue_platform in SUPPORTED_PLATFORMS:
        return (queue_platform,)
    return ()


def _quota_breaches(
    items: list[QueuedPlatformItem],
    quotas: Mapping[str, int],
) -> list[PlatformQuotaBreach]:
    grouped: dict[tuple[str, str], list[QueuedPlatformItem]] = defaultdict(list)
    for item in items:
        if item.platform in quotas:
            grouped[(item.platform, item.local_date)].append(item)

    breaches: list[PlatformQuotaBreach] = []
    for (platform, local_date), matches in sorted(grouped.items()):
        quota = quotas[platform]
        if len(matches) <= quota:
            continue
        breaches.append(
            PlatformQuotaBreach(
                platform=platform,
                local_date=local_date,
                quota=quota,
                scheduled_count=len(matches),
                excess_count=len(matches) - quota,
                queue_ids=tuple(item.queue_id for item in matches),
            )
        )
    return breaches


def _suggest_deferrals(
    items: list[QueuedPlatformItem],
    quotas: Mapping[str, int],
) -> list[PlatformQuotaDeferral]:
    counts: dict[tuple[str, str], int] = defaultdict(int)
    grouped: dict[tuple[str, str], list[QueuedPlatformItem]] = defaultdict(list)
    for item in items:
        if item.platform not in quotas:
            continue
        counts[(item.platform, item.local_date)] += 1
        grouped[(item.platform, item.local_date)].append(item)

    suggestions: list[tuple[int, PlatformQuotaDeferral]] = []
    reservations = dict(counts)
    for (platform, local_date), matches in sorted(grouped.items()):
        quota = quotas[platform]
        if len(matches) <= quota:
            continue
        for item in matches[quota:]:
            suggested_date = _next_available_date(
                platform,
                _parse_date(local_date) + timedelta(days=1),
                quota,
                reservations,
            )
            reservations[(platform, suggested_date)] = (
                reservations.get((platform, suggested_date), 0) + 1
            )
            reason = (
                f"daily_quota_exceeded: {platform} {local_date} has "
                f"{len(matches)} queued items over quota {quota}"
            )
            suggestions.append(
                (
                    item.ordinal,
                    PlatformQuotaDeferral(
                        queue_id=item.queue_id,
                        content_id=item.content_id,
                        platform=item.platform,
                        queue_platform=item.queue_platform,
                        current_scheduled_at=item.scheduled_at_raw,
                        current_local_date=item.local_date,
                        reason=reason,
                        suggested_date=suggested_date,
                    ),
                )
            )
    suggestions.sort(key=lambda pair: pair[0])
    return [suggestion for _ordinal, suggestion in suggestions]


def _next_available_date(
    platform: str,
    candidate: date,
    quota: int,
    reservations: Mapping[tuple[str, str], int],
) -> str:
    for _ in range(366):
        key = candidate.isoformat()
        if reservations.get((platform, key), 0) < quota:
            return key
        candidate += timedelta(days=1)
    raise ValueError(f"no available deferral date found for platform {platform}")


def _normalize_quotas(quotas: Mapping[str, int] | None) -> dict[str, int]:
    raw = quotas if quotas is not None else DEFAULT_PLATFORM_QUOTAS
    normalized: dict[str, int] = {}
    for platform, quota in raw.items():
        key = str(platform).strip().lower()
        if key not in SUPPORTED_PLATFORMS:
            continue
        if isinstance(quota, bool):
            continue
        value = int(quota)
        if value < 0:
            raise ValueError("quota values must be non-negative")
        normalized[key] = value
    return normalized


def _selected_platforms(platform: str) -> tuple[str, ...]:
    normalized = str(platform).strip().lower()
    if normalized == "all":
        return SUPPORTED_PLATFORMS
    if normalized not in SUPPORTED_PLATFORMS:
        raise ValueError(
            "platform must be one of: all, " + ", ".join(SUPPORTED_PLATFORMS)
        )
    return (normalized,)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)
