"""Health enforcement for curated sources."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


DEFAULT_SOURCE_FAILURE_THRESHOLD = 3


@dataclass(frozen=True)
class SourcePauseDecision:
    """A curated source that should be paused."""

    id: int
    source_type: str
    identifier: str
    name: str | None
    consecutive_failures: int
    threshold: int
    last_failure_at: str | None
    last_success_at: str | None
    last_error: str | None
    status: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source_type": self.source_type,
            "identifier": self.identifier,
            "name": self.name,
            "consecutive_failures": self.consecutive_failures,
            "threshold": self.threshold,
            "last_failure_at": self.last_failure_at,
            "last_success_at": self.last_success_at,
            "last_error": self.last_error,
            "status": self.status,
        }


def normalize_failure_threshold(value: Any) -> int:
    """Return a positive source failure threshold."""
    return value if isinstance(value, int) and value > 0 else DEFAULT_SOURCE_FAILURE_THRESHOLD


def source_failure_threshold_from_config(config: Any) -> int:
    """Read curated source failure threshold from Config-like objects."""
    curated_sources = getattr(config, "curated_sources", None)
    return normalize_failure_threshold(
        getattr(curated_sources, "source_failure_threshold", DEFAULT_SOURCE_FAILURE_THRESHOLD)
    )


def should_pause_source(row: dict[str, Any], threshold: int) -> bool:
    """Return True when a curated source meets the pause criteria."""
    threshold = normalize_failure_threshold(threshold)
    if row.get("status", "active") != "active":
        return False
    if int(row.get("consecutive_failures") or 0) < threshold:
        return False

    last_failure = _parse_datetime(row.get("last_failure_at"))
    if last_failure is None:
        return False

    last_success = _parse_datetime(row.get("last_success_at"))
    return last_success is None or last_failure > last_success


def build_pause_decisions(
    rows: list[dict[str, Any]], threshold: int
) -> list[SourcePauseDecision]:
    """Build pause decisions from curated source rows."""
    threshold = normalize_failure_threshold(threshold)
    decisions = []
    for row in rows:
        if not should_pause_source(row, threshold):
            continue
        decisions.append(
            SourcePauseDecision(
                id=int(row["id"]),
                source_type=row.get("source_type") or "",
                identifier=row.get("identifier") or "",
                name=row.get("name"),
                consecutive_failures=int(row.get("consecutive_failures") or 0),
                threshold=threshold,
                last_failure_at=row.get("last_failure_at"),
                last_success_at=row.get("last_success_at"),
                last_error=row.get("last_error"),
                status=row.get("status"),
            )
        )
    return decisions


def find_sources_to_pause(db: Any, threshold: int) -> list[SourcePauseDecision]:
    """Return active curated sources that should be paused."""
    rows = db.get_pauseable_curated_sources(normalize_failure_threshold(threshold))
    return build_pause_decisions(rows, threshold)


def pause_failing_sources(
    db: Any, threshold: int, *, dry_run: bool = False
) -> list[SourcePauseDecision]:
    """Pause active curated sources whose failures exceed threshold."""
    decisions = find_sources_to_pause(db, threshold)
    if not dry_run and decisions:
        db.pause_curated_sources_by_ids([decision.id for decision in decisions])
    return decisions


def restore_sources(
    db: Any,
    *,
    source_ids: list[int] | None = None,
    identifiers: list[str] | None = None,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Restore paused curated sources by ID or identifier."""
    source_ids = source_ids or []
    identifiers = identifiers or []
    rows = db.get_paused_curated_sources(source_ids=source_ids, identifiers=identifiers)
    if not dry_run and rows:
        db.restore_curated_sources(source_ids=source_ids, identifiers=identifiers)
    return rows


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed
