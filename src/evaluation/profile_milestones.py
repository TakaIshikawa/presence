"""Detect profile growth milestones and seed content ideas."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any


SOURCE_NAME = "profile_milestone"
DEFAULT_STEP = 100
FOLLOWER_METRIC = "follower_count"


@dataclass(frozen=True)
class ProfileMilestoneCandidate:
    platform: str
    metric: str
    threshold: int
    previous_value: int
    current_value: int
    fetched_at: str
    note: str
    topic: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ProfileMilestoneResult:
    status: str
    platform: str
    metric: str
    threshold: int
    idea_id: int | None
    reason: str
    note: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _validate_step(step: int) -> int:
    if step <= 0:
        raise ValueError("step must be greater than 0")
    return step


def _platforms(db, platform: str | None = None) -> list[str]:
    if platform and platform != "all":
        return [platform]
    cursor = db.conn.execute(
        "SELECT DISTINCT platform FROM profile_metrics ORDER BY platform ASC"
    )
    return [str(row[0]) for row in cursor.fetchall()]


def _metric_rows(db, platform: str) -> list[dict[str, Any]]:
    cursor = db.conn.execute(
        """SELECT platform, follower_count, following_count, tweet_count,
                  listed_count, fetched_at
           FROM profile_metrics
           WHERE platform = ?
           ORDER BY fetched_at ASC, id ASC""",
        (platform,),
    )
    return [dict(row) for row in cursor.fetchall()]


def _crossed_thresholds(previous_value: int, current_value: int, step: int) -> list[int]:
    if current_value <= previous_value:
        return []
    first = ((previous_value // step) + 1) * step
    if first > current_value:
        return []
    return list(range(first, current_value + 1, step))


def _candidate(
    *,
    platform: str,
    metric: str,
    previous_value: int,
    current_value: int,
    threshold: int,
    fetched_at: str,
    step: int,
) -> ProfileMilestoneCandidate:
    platform_label = platform.upper() if platform == "x" else platform.title()
    topic = f"{platform_label} reached {threshold:,} followers"
    note = (
        f"{platform_label} crossed {threshold:,} followers, growing from "
        f"{previous_value:,} to {current_value:,}. Turn the real growth signal "
        "into a short lesson on what changed, what worked, and what to try next."
    )
    source_id = f"{platform}:{metric}:{threshold}"
    source_metadata = {
        "source": SOURCE_NAME,
        "source_id": source_id,
        "platform": platform,
        "metric": metric,
        "threshold": threshold,
        "step": step,
        "previous_value": previous_value,
        "current_value": current_value,
        "fetched_at": fetched_at,
    }
    return ProfileMilestoneCandidate(
        platform=platform,
        metric=metric,
        threshold=threshold,
        previous_value=previous_value,
        current_value=current_value,
        fetched_at=fetched_at,
        note=note,
        topic=topic,
        source_metadata=source_metadata,
    )


def detect_profile_milestones(
    db,
    *,
    platform: str | None = None,
    step: int = DEFAULT_STEP,
) -> list[ProfileMilestoneCandidate]:
    """Return follower threshold crossings from profile metric snapshots."""
    step = _validate_step(step)
    candidates: list[ProfileMilestoneCandidate] = []
    for selected_platform in _platforms(db, platform):
        rows = _metric_rows(db, selected_platform)
        for previous, current in zip(rows, rows[1:]):
            previous_value = int(previous[FOLLOWER_METRIC])
            current_value = int(current[FOLLOWER_METRIC])
            for threshold in _crossed_thresholds(previous_value, current_value, step):
                candidates.append(
                    _candidate(
                        platform=selected_platform,
                        metric=FOLLOWER_METRIC,
                        previous_value=previous_value,
                        current_value=current_value,
                        threshold=threshold,
                        fetched_at=str(current["fetched_at"]),
                        step=step,
                    )
                )
    return candidates


def seed_profile_milestone_ideas(
    db,
    *,
    platform: str | None = None,
    step: int = DEFAULT_STEP,
    dry_run: bool = False,
) -> list[ProfileMilestoneResult]:
    """Seed one content idea per new profile milestone crossing."""
    candidates = detect_profile_milestones(db, platform=platform, step=step)
    results: list[ProfileMilestoneResult] = []
    for candidate in candidates:
        existing = db.find_active_content_idea_for_source_metadata(
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        if existing:
            results.append(
                ProfileMilestoneResult(
                    status="skipped",
                    platform=candidate.platform,
                    metric=candidate.metric,
                    threshold=candidate.threshold,
                    idea_id=existing["id"],
                    reason=f"{existing['status']} duplicate",
                    note=candidate.note,
                    source_metadata=candidate.source_metadata,
                )
            )
            continue

        if dry_run:
            results.append(
                ProfileMilestoneResult(
                    status="proposed",
                    platform=candidate.platform,
                    metric=candidate.metric,
                    threshold=candidate.threshold,
                    idea_id=None,
                    reason="dry run",
                    note=candidate.note,
                    source_metadata=candidate.source_metadata,
                )
            )
            continue

        idea_id = db.add_content_idea(
            note=candidate.note,
            topic=candidate.topic,
            priority="normal",
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        results.append(
            ProfileMilestoneResult(
                status="created",
                platform=candidate.platform,
                metric=candidate.metric,
                threshold=candidate.threshold,
                idea_id=idea_id,
                reason="created",
                note=candidate.note,
                source_metadata=candidate.source_metadata,
            )
        )
    return results


def results_to_json(results: list[ProfileMilestoneResult]) -> str:
    """Serialize milestone seed results for CLI output."""
    return json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True)
