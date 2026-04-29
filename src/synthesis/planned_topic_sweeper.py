"""Sweep stale planned topics into reports, skips, or content ideas."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal


SweepAction = Literal["report", "skip", "idea"]


@dataclass(frozen=True)
class PlannedTopicSweepResult:
    topic_id: int
    topic: str
    angle: str | None
    target_date: str
    campaign_id: int | None
    action: str
    status: str
    reason: str
    content_idea_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def sweep_planned_topics(
    db: Any,
    *,
    older_than_days: int,
    action: SweepAction = "report",
    campaign_id: int | None = None,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[PlannedTopicSweepResult]:
    """Find stale planned topics and apply the requested action."""
    if action not in {"report", "skip", "idea"}:
        raise ValueError("action must be one of: report, skip, idea")
    cutoff = _cutoff_date(older_than_days, now=now)
    _validate_campaign(db, campaign_id)

    rows = _stale_rows(db, cutoff=cutoff, campaign_id=campaign_id)
    results = [_row_to_result(row, cutoff=cutoff, action=action) for row in rows]
    if action == "report" or dry_run or not rows:
        return [
            _replace_result(result, status="eligible" if action == "report" else "dry_run")
            for result in results
        ]

    applied: list[PlannedTopicSweepResult] = []
    with db.conn:
        for row, result in zip(rows, results, strict=True):
            if action == "skip":
                updated = _mark_skipped(db, int(row["id"]))
                applied.append(
                    _replace_result(
                        result,
                        status="skipped" if updated else "unchanged",
                    )
                )
                continue

            idea = db.find_open_content_idea_for_planned_topic(int(row["id"]))
            if idea is not None:
                applied.append(
                    _replace_result(
                        result,
                        status="duplicate_open_idea",
                        content_idea_id=int(idea["id"]),
                    )
                )
                continue

            idea_id = db.add_content_idea(
                note=_idea_note(row),
                topic=row.get("topic"),
                priority="normal",
                source="planned_topic_sweeper",
                source_metadata=_idea_source_metadata(row),
            )
            _mark_skipped(db, int(row["id"]))
            applied.append(
                _replace_result(result, status="idea_created", content_idea_id=idea_id)
            )
    return applied


def _as_utc_datetime(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        return current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def _cutoff_date(older_than_days: int, now: datetime | None = None) -> date:
    if not isinstance(older_than_days, int) or older_than_days <= 0:
        raise ValueError("older_than_days must be a positive integer")
    return _as_utc_datetime(now).date() - timedelta(days=older_than_days)


def _validate_campaign(db: Any, campaign_id: int | None) -> None:
    if campaign_id is None:
        return
    get_campaign = getattr(db, "get_campaign", None)
    if get_campaign is not None and get_campaign(campaign_id) is None:
        raise ValueError(f"Campaign {campaign_id} does not exist")


def _stale_rows(db: Any, *, cutoff: date, campaign_id: int | None) -> list[dict[str, Any]]:
    finder = getattr(db, "find_stale_planned_topics", None)
    if finder is not None:
        return finder(cutoff_date=cutoff.isoformat(), campaign_id=campaign_id)

    where = [
        "status = 'planned'",
        "content_id IS NULL",
        "target_date IS NOT NULL",
        "substr(target_date, 1, 10) < ?",
    ]
    params: list[Any] = [cutoff.isoformat()]
    if campaign_id is not None:
        where.append("campaign_id = ?")
        params.append(campaign_id)
    cursor = db.conn.execute(
        f"""SELECT *
              FROM planned_topics
             WHERE {' AND '.join(where)}
             ORDER BY target_date ASC, created_at ASC, id ASC""",
        params,
    )
    return [dict(row) for row in cursor.fetchall()]


def _row_to_result(
    row: dict[str, Any],
    *,
    cutoff: date,
    action: str,
) -> PlannedTopicSweepResult:
    target_date = str(row.get("target_date") or "")
    reason = f"target_date {target_date[:10]} is older than cutoff {cutoff.isoformat()}"
    return PlannedTopicSweepResult(
        topic_id=int(row["id"]),
        topic=str(row.get("topic") or ""),
        angle=row.get("angle"),
        target_date=target_date,
        campaign_id=row.get("campaign_id"),
        action=action,
        status="eligible",
        reason=reason,
    )


def _replace_result(
    result: PlannedTopicSweepResult,
    *,
    status: str,
    content_idea_id: int | None = None,
) -> PlannedTopicSweepResult:
    return PlannedTopicSweepResult(
        topic_id=result.topic_id,
        topic=result.topic,
        angle=result.angle,
        target_date=result.target_date,
        campaign_id=result.campaign_id,
        action=result.action,
        status=status,
        reason=result.reason,
        content_idea_id=content_idea_id,
    )


def _mark_skipped(db: Any, topic_id: int) -> bool:
    cursor = db.conn.execute(
        """UPDATE planned_topics
              SET status = 'skipped'
            WHERE id = ?
              AND status = 'planned'
              AND content_id IS NULL""",
        (topic_id,),
    )
    return cursor.rowcount == 1


def _idea_note(row: dict[str, Any]) -> str:
    angle = str(row.get("angle") or "").strip()
    topic = str(row.get("topic") or "").strip()
    if angle:
        return f"{topic}: {angle}"
    return topic


def _idea_source_metadata(row: dict[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "source": "planned_topic_sweeper",
        "planned_topic_id": int(row["id"]),
        "target_date": row.get("target_date"),
        "campaign_id": row.get("campaign_id"),
        "source_material": row.get("source_material"),
    }
    parsed = _parse_source_material(row.get("source_material"))
    if parsed is not None:
        metadata["parsed_source_material"] = parsed
    return metadata


def _parse_source_material(value: Any) -> Any | None:
    if value is None:
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None
