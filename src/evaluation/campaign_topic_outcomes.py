"""Planned-topic outcome reporting for campaign retrospectives."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 90
WIN_ENGAGEMENT_THRESHOLD = 5.0


@dataclass(frozen=True)
class CampaignTopicOutcomeRow:
    """One generated planned topic with downstream publication outcome."""

    planned_topic_id: int
    campaign_id: int | None
    topic: str
    angle: str | None
    target_date: str | None
    content_id: int
    content_type: str | None
    generated_at: str | None
    publish_status: str
    platforms: tuple[str, ...] = field(default_factory=tuple)
    total_engagement: float = 0.0
    outcome: str = "no_metrics"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["platforms"] = list(self.platforms)
        return payload


@dataclass(frozen=True)
class CampaignTopicOutcomesReport:
    """Campaign topic outcome rows plus applied filters."""

    campaign_id: int | None
    days: int | None
    generated_at: str
    win_engagement_threshold: float
    rows: tuple[CampaignTopicOutcomeRow, ...]

    def to_dict(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        for row in self.rows:
            counts[row.outcome] = counts.get(row.outcome, 0) + 1
        return {
            "campaign_id": self.campaign_id,
            "days": self.days,
            "generated_at": self.generated_at,
            "win_engagement_threshold": self.win_engagement_threshold,
            "topic_count": len(self.rows),
            "outcome_counts": counts,
            "rows": [row.to_dict() for row in self.rows],
        }


def build_campaign_topic_outcomes_report(
    db_or_conn: Any,
    *,
    campaign_id: int | None = None,
    days: int | None = DEFAULT_DAYS,
    now: datetime | None = None,
) -> CampaignTopicOutcomesReport:
    """Return one row per generated planned topic with publication outcomes."""
    if campaign_id is not None and campaign_id <= 0:
        raise ValueError("campaign_id must be positive")
    if days is not None and days <= 0:
        raise ValueError("days must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    now = _ensure_aware(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days) if days is not None else None

    planned = _load_generated_planned_topics(
        conn,
        schema,
        campaign_id=campaign_id,
        cutoff=cutoff,
        now=now,
    )
    content_ids = {int(row["content_id"]) for row in planned}
    publications = _load_publications(conn, schema, content_ids)
    engagement = _load_engagement(conn, schema, content_ids)

    rows = tuple(
        _build_row(row, publications.get(int(row["content_id"]), []), engagement)
        for row in planned
    )
    return CampaignTopicOutcomesReport(
        campaign_id=campaign_id,
        days=days,
        generated_at=now.isoformat(),
        win_engagement_threshold=WIN_ENGAGEMENT_THRESHOLD,
        rows=rows,
    )


def format_campaign_topic_outcomes_json(report: CampaignTopicOutcomesReport) -> str:
    """Render a campaign topic outcome report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_campaign_topic_outcomes_text(report: CampaignTopicOutcomesReport) -> str:
    """Render a readable fixed-width planned-topic outcome table."""
    lines = [
        "Campaign Planned Topic Outcomes",
        f"Generated: {report.generated_at}",
        f"Campaign: {report.campaign_id if report.campaign_id is not None else 'all'}",
        f"Window: {report.days if report.days is not None else 'all'} days",
        "",
    ]
    if not report.rows:
        lines.append("No generated planned topics found.")
        return "\n".join(lines)

    header = (
        f"{'Target':<12} {'Topic':<24} {'Content':>7} {'Status':<14} "
        f"{'Platforms':<18} {'Eng':>7} Outcome"
    )
    lines.extend([header, "-" * len(header)])
    for row in report.rows:
        lines.append(
            f"{_shorten(row.target_date or '-', 12):<12} "
            f"{_shorten(row.topic, 24):<24} "
            f"{row.content_id:>7} "
            f"{row.publish_status:<14} "
            f"{_shorten(','.join(row.platforms) or '-', 18):<18} "
            f"{row.total_engagement:>7.2f} "
            f"{row.outcome}"
        )
    return "\n".join(lines)


def _load_generated_planned_topics(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    campaign_id: int | None,
    cutoff: datetime | None,
    now: datetime,
) -> list[dict[str, Any]]:
    if "planned_topics" not in schema or "generated_content" not in schema:
        return []

    params: list[Any] = []
    where = ["pt.content_id IS NOT NULL"]
    if campaign_id is not None:
        where.append("pt.campaign_id = ?")
        params.append(campaign_id)

    rows = conn.execute(
        f"""SELECT pt.id AS planned_topic_id,
                  pt.campaign_id,
                  pt.topic,
                  pt.angle,
                  pt.target_date,
                  pt.status AS topic_status,
                  pt.content_id,
                  pt.created_at AS planned_at,
                  gc.content_type,
                  gc.created_at AS generated_at,
                  gc.published AS legacy_published,
                  gc.published_at AS legacy_published_at,
                  gc.auto_quality
           FROM planned_topics pt
           INNER JOIN generated_content gc ON gc.id = pt.content_id
           WHERE {' AND '.join(where)}
           ORDER BY pt.target_date ASC NULLS LAST,
                    gc.created_at ASC NULLS LAST,
                    pt.created_at ASC,
                    pt.id ASC""",
        params,
    ).fetchall()

    planned = []
    for row in rows:
        item = dict(row)
        if _matches_window(item, cutoff, now):
            planned.append(item)
    return planned


def _load_publications(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: set[int],
) -> dict[int, list[dict[str, Any]]]:
    if not content_ids or "content_publications" not in schema:
        return {}
    placeholders = ",".join("?" for _ in content_ids)
    rows = conn.execute(
        f"""SELECT content_id, platform, status, published_at
            FROM content_publications
            WHERE content_id IN ({placeholders})
            ORDER BY content_id ASC, platform ASC""",
        tuple(sorted(content_ids)),
    ).fetchall()
    publications: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        item = dict(row)
        publications.setdefault(int(item["content_id"]), []).append(item)
    return publications


def _load_engagement(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: set[int],
) -> dict[int, dict[str, float]]:
    engagement: dict[int, dict[str, float]] = {
        content_id: {} for content_id in content_ids
    }
    for table, platform in (
        ("post_engagement", "x"),
        ("linkedin_engagement", "linkedin"),
        ("bluesky_engagement", "bluesky"),
    ):
        if table not in schema or "engagement_score" not in schema[table]:
            continue
        for content_id, score in _latest_scores(conn, table, content_ids).items():
            engagement.setdefault(content_id, {})[platform] = score
    return engagement


def _latest_scores(
    conn: sqlite3.Connection,
    table: str,
    content_ids: set[int],
) -> dict[int, float]:
    if not content_ids:
        return {}
    placeholders = ",".join("?" for _ in content_ids)
    rows = conn.execute(
        f"""SELECT content_id, engagement_score
            FROM (
                SELECT content_id, engagement_score,
                       ROW_NUMBER() OVER (
                           PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                       ) AS rn
                FROM {table}
                WHERE engagement_score IS NOT NULL
                  AND content_id IN ({placeholders})
            )
            WHERE rn = 1""",
        tuple(sorted(content_ids)),
    ).fetchall()
    return {int(row["content_id"]): float(row["engagement_score"] or 0.0) for row in rows}


def _build_row(
    planned: dict[str, Any],
    publications: list[dict[str, Any]],
    engagement: dict[int, dict[str, float]],
) -> CampaignTopicOutcomeRow:
    content_id = int(planned["content_id"])
    published_platforms = tuple(
        sorted(
            {
                str(publication.get("platform"))
                for publication in publications
                if publication.get("platform") and publication.get("status") == "published"
            }
        )
    )
    legacy_published = bool(planned.get("legacy_published") == 1 or planned.get("legacy_published_at"))
    publish_status = _publish_status(
        legacy_published=legacy_published,
        published_platforms=published_platforms,
        publications=publications,
    )
    platforms = published_platforms
    if not platforms and legacy_published:
        platforms = ("legacy",)

    scores = engagement.get(content_id, {})
    total_engagement = round(sum(scores.values()), 2)
    has_metrics = bool(scores)
    outcome = _outcome(
        publish_status=publish_status,
        total_engagement=total_engagement,
        has_metrics=has_metrics,
        auto_quality=planned.get("auto_quality"),
    )

    return CampaignTopicOutcomeRow(
        planned_topic_id=int(planned["planned_topic_id"]),
        campaign_id=_int_or_none(planned.get("campaign_id")),
        topic=str(planned.get("topic") or ""),
        angle=planned.get("angle"),
        target_date=planned.get("target_date"),
        content_id=content_id,
        content_type=planned.get("content_type"),
        generated_at=planned.get("generated_at"),
        publish_status=publish_status,
        platforms=platforms,
        total_engagement=total_engagement,
        outcome=outcome,
    )


def _publish_status(
    *,
    legacy_published: bool,
    published_platforms: tuple[str, ...],
    publications: list[dict[str, Any]],
) -> str:
    if published_platforms or legacy_published:
        return "published"
    if any(publication.get("status") == "failed" for publication in publications):
        return "publish_failed"
    if publications:
        return "queued"
    return "unpublished"


def _outcome(
    *,
    publish_status: str,
    total_engagement: float,
    has_metrics: bool,
    auto_quality: Any,
) -> str:
    if publish_status != "published":
        return "missed_publish"
    if not has_metrics:
        return "no_metrics"
    if str(auto_quality or "").strip().lower() == "resonated":
        return "won"
    if total_engagement >= WIN_ENGAGEMENT_THRESHOLD:
        return "won"
    return "neutral"


def _matches_window(
    item: dict[str, Any],
    cutoff: datetime | None,
    now: datetime,
) -> bool:
    if cutoff is None:
        return True
    for field in ("target_date", "generated_at", "legacy_published_at", "planned_at"):
        parsed = _parse_timestamp(item.get(field))
        if parsed is not None and cutoff <= parsed <= now:
            return True
    return False


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if conn is None:
        raise ValueError("database connection is not available")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
    }


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_aware(parsed)


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _shorten(value: str, max_length: int) -> str:
    if len(value) <= max_length:
        return value
    return value[: max_length - 3] + "..."
