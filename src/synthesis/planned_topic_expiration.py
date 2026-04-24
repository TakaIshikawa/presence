"""Expire stale planned topics that were never generated."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class PlannedTopicExpirationResult:
    """A planned topic selected for expiration."""

    topic_id: int
    topic: str
    target_date: str
    reason: str
    campaign_id: int | None = None
    status: str = "eligible"

    def to_dict(self) -> dict[str, Any]:
        return {
            "topic_id": self.topic_id,
            "topic": self.topic,
            "target_date": self.target_date,
            "campaign_id": self.campaign_id,
            "status": self.status,
            "reason": self.reason,
        }


def _as_utc_datetime(value: datetime | None = None) -> datetime:
    now = value or datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _parse_target_date(value: str | None) -> date | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _cutoff_date(older_than_days: int, now: datetime | None = None) -> date:
    if not isinstance(older_than_days, int) or older_than_days <= 0:
        raise ValueError("older_than_days must be a positive integer")
    return (_as_utc_datetime(now).date() - timedelta(days=older_than_days))


def _validate_campaign(db: Any, campaign_id: int | None) -> None:
    if campaign_id is None:
        return
    get_campaign = getattr(db, "get_campaign", None)
    if get_campaign is not None and get_campaign(campaign_id) is None:
        raise ValueError(f"Campaign {campaign_id} does not exist")


def _eligible_rows(
    db: Any,
    *,
    older_than_days: int,
    campaign_id: int | None = None,
    now: datetime | None = None,
) -> tuple[list[dict[str, Any]], date]:
    cutoff = _cutoff_date(older_than_days, now=now)
    _validate_campaign(db, campaign_id)

    where = [
        "status = 'planned'",
        "content_id IS NULL",
        "target_date IS NOT NULL",
    ]
    params: list[Any] = []
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
    rows = []
    for row in cursor.fetchall():
        item = dict(row)
        target = _parse_target_date(item.get("target_date"))
        if target is None or target >= cutoff:
            continue
        rows.append(item)
    return rows, cutoff


def find_expired_planned_topics(
    db: Any,
    *,
    older_than_days: int,
    campaign_id: int | None = None,
    now: datetime | None = None,
) -> list[PlannedTopicExpirationResult]:
    """Return planned topics eligible for expiration without updating the database."""

    rows, cutoff = _eligible_rows(
        db,
        older_than_days=older_than_days,
        campaign_id=campaign_id,
        now=now,
    )
    return [_row_to_result(row, cutoff=cutoff) for row in rows]


def expire_planned_topics(
    db: Any,
    *,
    older_than_days: int,
    campaign_id: int | None = None,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[PlannedTopicExpirationResult]:
    """Mark eligible stale planned topics as skipped unless dry_run is set."""

    now_utc = _as_utc_datetime(now)
    rows, cutoff = _eligible_rows(
        db,
        older_than_days=older_than_days,
        campaign_id=campaign_id,
        now=now_utc,
    )
    results = [_row_to_result(row, cutoff=cutoff) for row in rows]
    if dry_run or not rows:
        return results

    for row, result in zip(rows, results, strict=True):
        source_material = _source_material_with_expiration(
            row.get("source_material"),
            result=result,
            older_than_days=older_than_days,
            cutoff=cutoff,
            expired_at=now_utc,
        )
        db.conn.execute(
            """UPDATE planned_topics
                  SET status = 'skipped',
                      source_material = ?
                WHERE id = ?
                  AND status = 'planned'
                  AND content_id IS NULL""",
            (source_material, row["id"]),
        )
    db.conn.commit()
    return [
        PlannedTopicExpirationResult(
            topic_id=result.topic_id,
            topic=result.topic,
            target_date=result.target_date,
            campaign_id=result.campaign_id,
            status="expired",
            reason=result.reason,
        )
        for result in results
    ]


def _row_to_result(row: dict[str, Any], *, cutoff: date) -> PlannedTopicExpirationResult:
    target_date = str(row.get("target_date") or "")
    reason = f"target_date {target_date[:10]} is older than cutoff {cutoff.isoformat()}"
    return PlannedTopicExpirationResult(
        topic_id=int(row["id"]),
        topic=str(row.get("topic") or ""),
        target_date=target_date,
        campaign_id=row.get("campaign_id"),
        reason=reason,
    )


def _source_material_with_expiration(
    source_material: str | None,
    *,
    result: PlannedTopicExpirationResult,
    older_than_days: int,
    cutoff: date,
    expired_at: datetime,
) -> str:
    expiration = {
        "source": "planned_topic_expiration",
        "expired_at": expired_at.isoformat(),
        "older_than_days": older_than_days,
        "cutoff_date": cutoff.isoformat(),
        "reason": result.reason,
        "previous_status": "planned",
    }

    text = source_material if source_material is not None else None
    if text is None or not str(text).strip():
        payload: dict[str, Any] = {"expiration": expiration}
    else:
        try:
            parsed = json.loads(text)
        except (TypeError, ValueError):
            payload = {
                "original_source_material": str(text),
                "expiration": expiration,
            }
        else:
            if isinstance(parsed, dict):
                payload = dict(parsed)
                existing = payload.get("expiration")
                if existing is not None:
                    history = payload.get("expiration_history")
                    if not isinstance(history, list):
                        history = []
                    history.append(existing)
                    payload["expiration_history"] = history
                payload["expiration"] = expiration
            else:
                payload = {
                    "original_source_material": parsed,
                    "expiration": expiration,
                }

    return json.dumps(payload, sort_keys=True)
