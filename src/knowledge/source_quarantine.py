"""Classify and quarantine unhealthy curated sources."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class SourceQuarantineDecision:
    """Health classification for one curated source."""

    id: int
    source_type: str
    identifier: str
    classification: str
    reason: str
    status: str
    active: bool
    consecutive_failures: int
    last_fetch_status: str | None
    last_success_at: str | None
    would_pause: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _classify_row(
    row: dict[str, Any],
    *,
    failure_threshold: int,
    stale_days: int,
    now: datetime,
) -> SourceQuarantineDecision:
    status = row.get("status") or "active"
    active = bool(row.get("active", 1))
    failures = int(row.get("consecutive_failures") or 0)
    last_success_at = row.get("last_success_at")
    last_success = _parse_datetime(last_success_at)
    last_fetch_status = row.get("last_fetch_status")

    classification = "healthy"
    reason = "within thresholds"

    if status != "active" or not active:
        reason = f"not active ({status})"
    elif failure_threshold > 0 and failures >= failure_threshold:
        classification = "quarantine"
        reason = (
            f"consecutive failures {failures} >= threshold {failure_threshold}"
        )
    elif stale_days > 0 and last_success and now - last_success >= timedelta(days=stale_days):
        classification = "quarantine"
        reason = f"last success older than {stale_days} days"
    elif failures > 0 or last_fetch_status == "failure":
        classification = "watch"
        reason = (
            f"consecutive failures {failures} below threshold {failure_threshold}"
        )
    elif stale_days > 0 and last_success is None:
        classification = "watch"
        reason = "no successful fetch recorded"

    would_pause = classification == "quarantine" and status == "active" and active
    return SourceQuarantineDecision(
        id=int(row["id"]),
        source_type=row["source_type"],
        identifier=row["identifier"],
        classification=classification,
        reason=reason,
        status=status,
        active=active,
        consecutive_failures=failures,
        last_fetch_status=last_fetch_status,
        last_success_at=last_success_at,
        would_pause=would_pause,
    )


def classify_curated_sources(
    db,
    *,
    failure_threshold: int = 3,
    stale_days: int = 30,
    source_type: str | None = None,
    now: datetime | None = None,
) -> list[SourceQuarantineDecision]:
    """Classify curated source health from fetch status and freshness fields."""
    if failure_threshold < 0:
        raise ValueError("failure_threshold must be >= 0")
    if stale_days < 0:
        raise ValueError("stale_days must be >= 0")

    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    params: tuple[Any, ...] = ()
    where = ""
    if source_type:
        where = "WHERE source_type = ?"
        params = (source_type,)

    rows = db.conn.execute(
        f"""SELECT *
            FROM curated_sources
            {where}
            ORDER BY source_type ASC, identifier ASC""",
        params,
    ).fetchall()
    return [
        _classify_row(
            dict(row),
            failure_threshold=failure_threshold,
            stale_days=stale_days,
            now=now,
        )
        for row in rows
    ]


def apply_source_quarantine(db, decisions: list[SourceQuarantineDecision]) -> int:
    """Pause curated sources that are active and classified for quarantine."""
    ids = [decision.id for decision in decisions if decision.would_pause]
    if not ids:
        return 0

    placeholders = ", ".join("?" for _ in ids)
    cursor = db.conn.execute(
        f"""UPDATE curated_sources
            SET status = 'paused',
                active = 0
            WHERE id IN ({placeholders})
              AND status = 'active'
              AND active = 1""",
        ids,
    )
    db.conn.commit()
    return cursor.rowcount


def quarantine_curated_sources(
    db,
    *,
    failure_threshold: int = 3,
    stale_days: int = 30,
    source_type: str | None = None,
    apply: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Classify sources and optionally pause quarantined active rows."""
    decisions = classify_curated_sources(
        db,
        failure_threshold=failure_threshold,
        stale_days=stale_days,
        source_type=source_type,
        now=now,
    )
    planned = sum(1 for decision in decisions if decision.would_pause)
    updated = apply_source_quarantine(db, decisions) if apply else 0
    counts = {"healthy": 0, "watch": 0, "quarantine": 0}
    for decision in decisions:
        counts[decision.classification] += 1

    return {
        "applied": apply,
        "failure_threshold": failure_threshold,
        "stale_days": stale_days,
        "source_type": source_type,
        "counts": counts,
        "planned_pauses": planned,
        "updated": updated,
        "sources": [decision.to_dict() for decision in decisions],
    }
