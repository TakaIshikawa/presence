"""Read-only campaign calendar gap planner."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
from typing import Any, Iterable


DEFAULT_DAYS_AHEAD = 30
DEFAULT_MIN_GAP_DAYS = 2


@dataclass(frozen=True)
class CampaignGapSuggestion:
    """A deterministic placeholder topic slot for an uncovered campaign gap."""

    target_date: str
    topic: str
    angle: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "angle": self.angle,
            "reason": self.reason,
            "target_date": self.target_date,
            "topic": self.topic,
        }


@dataclass(frozen=True)
class CampaignDateGap:
    """One uncovered date range inside a campaign horizon."""

    campaign_id: int
    campaign_name: str
    start_date: str
    end_date: str
    days: int
    previous_topic: str | None
    next_topic: str | None
    suggestion: CampaignGapSuggestion

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "campaign_name": self.campaign_name,
            "days": self.days,
            "end_date": self.end_date,
            "next_topic": self.next_topic,
            "previous_topic": self.previous_topic,
            "start_date": self.start_date,
            "suggestion": self.suggestion.to_dict(),
        }


@dataclass(frozen=True)
class CampaignDateGapReport:
    """Gap report for active campaigns."""

    generated_at: str
    days_ahead: int
    min_gap_days: int
    campaign_id: int | None
    horizon_start: str
    horizon_end: str
    gaps: list[CampaignDateGap]
    campaign_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign_count": self.campaign_count,
            "campaign_id": self.campaign_id,
            "days_ahead": self.days_ahead,
            "gaps": [gap.to_dict() for gap in self.gaps],
            "generated_at": self.generated_at,
            "horizon_end": self.horizon_end,
            "horizon_start": self.horizon_start,
            "min_gap_days": self.min_gap_days,
            "summary": {
                "campaigns": self.campaign_count,
                "gaps": len(self.gaps),
                "suggestions": len(self.gaps),
                "uncovered_days": sum(gap.days for gap in self.gaps),
            },
        }


def plan_campaign_date_gaps(
    db_or_conn: Any,
    *,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    min_gap_days: int = DEFAULT_MIN_GAP_DAYS,
    campaign_id: int | None = None,
    now: datetime | date | None = None,
) -> CampaignDateGapReport:
    """Report uncovered date ranges for active campaigns without writing topics."""
    if days_ahead <= 0:
        raise ValueError("days_ahead must be positive")
    if min_gap_days <= 0:
        raise ValueError("min_gap_days must be positive")

    current = _coerce_now(now)
    horizon_start = current.date()
    horizon_end = horizon_start + timedelta(days=days_ahead - 1)
    conn = _conn(db_or_conn)
    campaigns = _load_active_campaigns(conn, campaign_id)
    if campaign_id is not None and not _campaign_exists(conn, campaign_id):
        raise ValueError(f"Campaign {campaign_id} does not exist")

    gaps: list[CampaignDateGap] = []
    for campaign in campaigns:
        campaign_start = _max_date(
            horizon_start,
            _parse_optional_date(campaign.get("start_date"), "campaign start_date"),
        )
        campaign_end = _min_date(
            horizon_end,
            _parse_optional_date(campaign.get("end_date"), "campaign end_date"),
        )
        if campaign_end < campaign_start:
            continue

        planned = _load_planned_topics(conn, int(campaign["id"]))
        coverage = _coverage_dates(conn, int(campaign["id"]), campaign_start, campaign_end)
        for start, end in _uncovered_ranges(campaign_start, campaign_end, coverage):
            gap_days = (end - start).days + 1
            if gap_days < min_gap_days:
                continue
            previous_topic, next_topic = _nearby_topics(planned, start, end)
            gaps.append(
                CampaignDateGap(
                    campaign_id=int(campaign["id"]),
                    campaign_name=str(campaign.get("name") or campaign["id"]),
                    start_date=start.isoformat(),
                    end_date=end.isoformat(),
                    days=gap_days,
                    previous_topic=previous_topic,
                    next_topic=next_topic,
                    suggestion=_suggestion(campaign, start, previous_topic, next_topic),
                )
            )

    return CampaignDateGapReport(
        generated_at=current.isoformat(),
        days_ahead=days_ahead,
        min_gap_days=min_gap_days,
        campaign_id=campaign_id,
        horizon_start=horizon_start.isoformat(),
        horizon_end=horizon_end.isoformat(),
        gaps=sorted(gaps, key=lambda gap: (gap.start_date, gap.campaign_id, gap.end_date)),
        campaign_count=len(campaigns),
    )


def format_campaign_date_gaps_json(report: CampaignDateGapReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_campaign_date_gaps_text(report: CampaignDateGapReport) -> str:
    lines = [
        "Campaign Date Gaps",
        f"Window: {report.horizon_start} to {report.horizon_end}",
        (
            f"campaigns={report.campaign_count} gaps={len(report.gaps)} "
            f"uncovered_days={sum(gap.days for gap in report.gaps)}"
        ),
    ]
    if not report.gaps:
        lines.append("No uncovered campaign date gaps.")
        return "\n".join(lines)

    for gap in report.gaps:
        suggestion = gap.suggestion
        lines.extend(
            [
                "",
                f"{gap.campaign_name} (ID {gap.campaign_id})",
                f"- Gap: {gap.start_date} to {gap.end_date} ({gap.days} days)",
                f"- Nearby: previous={_display(gap.previous_topic)} next={_display(gap.next_topic)}",
                f"- Suggest: {suggestion.target_date} | {suggestion.topic} | {suggestion.angle}",
                f"- Reason: {suggestion.reason}",
            ]
        )
    return "\n".join(lines)


def _load_active_campaigns(conn: Any, campaign_id: int | None) -> list[dict[str, Any]]:
    params: tuple[Any, ...]
    if campaign_id is None:
        sql = """SELECT *
                 FROM content_campaigns
                 WHERE status = 'active'
                 ORDER BY start_date ASC NULLS LAST, end_date ASC NULLS LAST, id ASC"""
        params = ()
    else:
        sql = """SELECT *
                 FROM content_campaigns
                 WHERE id = ? AND status = 'active'
                 ORDER BY id ASC"""
        params = (campaign_id,)
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _campaign_exists(conn: Any, campaign_id: int) -> bool:
    row = conn.execute("SELECT 1 FROM content_campaigns WHERE id = ?", (campaign_id,)).fetchone()
    return row is not None


def _load_planned_topics(conn: Any, campaign_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT id, topic, angle, target_date, status, content_id
           FROM planned_topics
           WHERE campaign_id = ? AND status != 'skipped'
           ORDER BY target_date ASC NULLS LAST, created_at ASC, id ASC""",
        (campaign_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _coverage_dates(conn: Any, campaign_id: int, start: date, end: date) -> set[date]:
    covered: set[date] = set()
    params = (campaign_id,)

    planned_rows = conn.execute(
        """SELECT target_date
           FROM planned_topics
           WHERE campaign_id = ? AND status != 'skipped' AND target_date IS NOT NULL""",
        params,
    ).fetchall()
    for row in planned_rows:
        _add_if_in_window(covered, _parse_optional_date(row["target_date"], "target_date"), start, end)

    queue_rows = conn.execute(
        """SELECT pq.scheduled_at
           FROM planned_topics pt
           INNER JOIN publish_queue pq ON pq.content_id = pt.content_id
           WHERE pt.campaign_id = ?
             AND pt.status != 'skipped'
             AND pq.status IN ('queued', 'held', 'published')
             AND pq.scheduled_at IS NOT NULL""",
        params,
    ).fetchall()
    for row in queue_rows:
        _add_if_in_window(covered, _parse_optional_date(row["scheduled_at"], "scheduled_at"), start, end)

    generated_rows = conn.execute(
        """SELECT gc.published_at
           FROM planned_topics pt
           INNER JOIN generated_content gc ON gc.id = pt.content_id
           WHERE pt.campaign_id = ?
             AND pt.status != 'skipped'
             AND gc.published_at IS NOT NULL""",
        params,
    ).fetchall()
    for row in generated_rows:
        _add_if_in_window(covered, _parse_optional_date(row["published_at"], "published_at"), start, end)

    publication_rows = conn.execute(
        """SELECT cp.published_at
           FROM planned_topics pt
           INNER JOIN content_publications cp ON cp.content_id = pt.content_id
           WHERE pt.campaign_id = ?
             AND pt.status != 'skipped'
             AND cp.status = 'published'
             AND cp.published_at IS NOT NULL""",
        params,
    ).fetchall()
    for row in publication_rows:
        _add_if_in_window(covered, _parse_optional_date(row["published_at"], "published_at"), start, end)
    return covered


def _uncovered_ranges(start: date, end: date, covered: set[date]) -> Iterable[tuple[date, date]]:
    gap_start: date | None = None
    current = start
    while current <= end:
        if current in covered:
            if gap_start is not None:
                yield gap_start, current - timedelta(days=1)
                gap_start = None
        elif gap_start is None:
            gap_start = current
        current += timedelta(days=1)
    if gap_start is not None:
        yield gap_start, end


def _nearby_topics(planned: list[dict[str, Any]], start: date, end: date) -> tuple[str | None, str | None]:
    previous: tuple[date, int, str] | None = None
    next_topic: tuple[date, int, str] | None = None
    for row in planned:
        target = _parse_optional_date(row.get("target_date"), "target_date")
        topic = str(row.get("topic") or "").strip()
        if target is None or not topic:
            continue
        candidate = (target, int(row.get("id") or 0), topic)
        if target < start and (previous is None or candidate > previous):
            previous = candidate
        if target > end and (next_topic is None or candidate < next_topic):
            next_topic = candidate
    return (
        previous[2] if previous else None,
        next_topic[2] if next_topic else None,
    )


def _suggestion(
    campaign: dict[str, Any],
    target_date: date,
    previous_topic: str | None,
    next_topic: str | None,
) -> CampaignGapSuggestion:
    goal = _compact_text(str(campaign.get("goal") or campaign.get("name") or "campaign momentum"), 72)
    if previous_topic and next_topic:
        topic = f"Bridge: {previous_topic} to {next_topic}"
        angle = f"Connect recent {previous_topic} coverage to upcoming {next_topic} work."
    elif previous_topic:
        topic = f"Follow-up: {previous_topic}"
        angle = f"Extend {previous_topic} toward {goal}."
    elif next_topic:
        topic = f"Primer: {next_topic}"
        angle = f"Set up the upcoming {next_topic} topic in service of {goal}."
    else:
        topic = f"Placeholder: {goal}"
        angle = f"Keep the campaign active with a lightweight update tied to {goal}."
    return CampaignGapSuggestion(
        target_date=target_date.isoformat(),
        topic=_compact_text(topic, 96),
        angle=_compact_text(angle, 140),
        reason="First uncovered day in the gap; preview only, no planned topic was written.",
    )


def _add_if_in_window(covered: set[date], value: date | None, start: date, end: date) -> None:
    if value is not None and start <= value <= end:
        covered.add(value)


def _parse_optional_date(value: Any, field_name: str) -> date | None:
    if value in (None, ""):
        return None
    return _parse_date(value, field_name)


def _parse_date(value: Any, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO date string")
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text).date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date string") from exc


def _coerce_now(value: datetime | date | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)


def _conn(db_or_conn: Any) -> Any:
    return getattr(db_or_conn, "conn", db_or_conn)


def _max_date(left: date, right: date | None) -> date:
    return left if right is None else max(left, right)


def _min_date(left: date, right: date | None) -> date:
    return left if right is None else min(left, right)


def _compact_text(value: str, limit: int) -> str:
    text = " ".join(value.split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "..."


def _display(value: Any) -> str:
    return "n/a" if value in (None, "") else str(value)
