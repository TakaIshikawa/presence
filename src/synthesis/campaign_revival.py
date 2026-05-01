"""Plan read-only revival actions for quiet content campaigns."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS_IDLE = 14
CAMPAIGN_STATUSES = ("active", "paused")
REVIVAL_STATUSES = ("healthy", "dormant", "stalled", "ready_to_revive")


@dataclass(frozen=True)
class CampaignRevivalRecommendation:
    """One read-only revival recommendation for a campaign."""

    campaign_id: int
    campaign_name: str
    campaign_status: str | None
    status: str
    reason: str
    next_action: str
    last_generated_at: str | None
    idle_days: int | None
    planned_topic_count: int
    remaining_planned_topic_count: int
    generated_topic_count: int
    open_idea_count: int
    next_planned_topic_id: int | None
    next_planned_topic: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CampaignRevivalReport:
    """Read-only report of quiet campaign revival opportunities."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    recommendations: tuple[CampaignRevivalRecommendation, ...]
    missing_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "totals": dict(self.totals),
            "recommendations": [
                recommendation.to_dict()
                for recommendation in self.recommendations
            ],
            "missing_tables": list(self.missing_tables),
        }


def build_campaign_revival_report(
    db_or_conn: Any,
    *,
    days_idle: int = DEFAULT_DAYS_IDLE,
    campaign_id: int | None = None,
    now: datetime | None = None,
) -> CampaignRevivalReport:
    """Return revival recommendations for active or paused quiet campaigns."""
    if days_idle <= 0:
        raise ValueError("days-idle must be positive")
    if campaign_id is not None and campaign_id <= 0:
        raise ValueError("campaign-id must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days_idle)
    missing = tuple(
        table
        for table in (
            "content_campaigns",
            "planned_topics",
            "generated_content",
            "content_topics",
            "content_ideas",
        )
        if table not in schema
    )
    if {"content_campaigns", "planned_topics", "generated_content"} & set(missing):
        return _empty_report(generated_at, days_idle, campaign_id, cutoff, missing)

    campaigns = _load_campaigns(conn, schema, campaign_id)
    if campaign_id is not None and not campaigns:
        raise ValueError(f"campaign {campaign_id} does not exist")

    ideas = _load_open_ideas(conn, schema)
    recommendations = tuple(
        _recommend_campaign(
            conn,
            schema,
            campaign,
            ideas,
            days_idle=days_idle,
            now=generated_at,
        )
        for campaign in campaigns
    )
    totals = {
        "campaigns": len(recommendations),
        "healthy": sum(1 for item in recommendations if item.status == "healthy"),
        "dormant": sum(1 for item in recommendations if item.status == "dormant"),
        "stalled": sum(1 for item in recommendations if item.status == "stalled"),
        "ready_to_revive": sum(
            1 for item in recommendations if item.status == "ready_to_revive"
        ),
        "missing_tables": len(missing),
    }
    return CampaignRevivalReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days_idle": days_idle,
            "campaign_id": campaign_id,
            "idle_cutoff": cutoff.isoformat(),
            "campaign_statuses": list(CAMPAIGN_STATUSES),
        },
        totals=totals,
        recommendations=recommendations,
        missing_tables=missing,
    )


def format_campaign_revival_json(report: CampaignRevivalReport) -> str:
    """Serialize a campaign revival report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_campaign_revival_text(report: CampaignRevivalReport) -> str:
    """Format a campaign revival report for terminal review."""
    lines = [
        "Campaign Revival Planner",
        f"Generated: {report.generated_at}",
        (
            f"Idle threshold: {report.filters['days_idle']} days "
            f"(cutoff {report.filters['idle_cutoff']})"
        ),
        (
            "Summary: "
            f"campaigns={report.totals['campaigns']} "
            f"healthy={report.totals['healthy']} "
            f"dormant={report.totals['dormant']} "
            f"stalled={report.totals['stalled']} "
            f"ready_to_revive={report.totals['ready_to_revive']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if not report.recommendations:
        lines.append("No active or paused campaigns found.")
        return "\n".join(lines)

    lines.append("Recommendations:")
    for item in report.recommendations:
        idle = "never" if item.idle_days is None else f"{item.idle_days}d"
        lines.append(
            f"- campaign_id={item.campaign_id} {item.campaign_name} "
            f"[{item.status}]: idle={idle} "
            f"remaining={item.remaining_planned_topic_count} "
            f"ideas={item.open_idea_count}"
        )
        lines.append(f"  reason: {item.reason}")
        lines.append(f"  next_action: {item.next_action}")
    return "\n".join(lines)


def _recommend_campaign(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    campaign: dict[str, Any],
    ideas: list[dict[str, Any]],
    *,
    days_idle: int,
    now: datetime,
) -> CampaignRevivalRecommendation:
    campaign_id = int(campaign["id"])
    topics = _load_planned_topics(conn, schema, campaign_id)
    remaining = [
        topic for topic in topics
        if (topic.get("status") or "planned") == "planned"
        and topic.get("content_id") is None
    ]
    generated = [
        topic for topic in topics
        if topic.get("content_id") is not None
        or (topic.get("status") or "").lower() == "generated"
    ]
    last_generated_at = _last_generated_at(conn, schema, campaign_id, topics)
    idle_days = _idle_days(last_generated_at, now)
    open_ideas = _matching_ideas(campaign, topics, ideas)
    status, reason, action = _classify(
        campaign,
        remaining,
        generated,
        open_ideas,
        last_generated_at=last_generated_at,
        idle_days=idle_days,
        days_idle=days_idle,
    )
    next_topic = remaining[0] if remaining else None
    return CampaignRevivalRecommendation(
        campaign_id=campaign_id,
        campaign_name=str(campaign.get("name") or f"campaign-{campaign_id}"),
        campaign_status=campaign.get("status"),
        status=status,
        reason=reason,
        next_action=action,
        last_generated_at=last_generated_at.isoformat() if last_generated_at else None,
        idle_days=idle_days,
        planned_topic_count=len(topics),
        remaining_planned_topic_count=len(remaining),
        generated_topic_count=len(generated),
        open_idea_count=len(open_ideas),
        next_planned_topic_id=int(next_topic["id"]) if next_topic else None,
        next_planned_topic=str(next_topic["topic"]) if next_topic else None,
    )


def _classify(
    campaign: dict[str, Any],
    remaining: list[dict[str, Any]],
    generated: list[dict[str, Any]],
    ideas: list[dict[str, Any]],
    *,
    last_generated_at: datetime | None,
    idle_days: int | None,
    days_idle: int,
) -> tuple[str, str, str]:
    campaign_state = (campaign.get("status") or "").lower()
    if idle_days is not None and idle_days < days_idle:
        return (
            "healthy",
            f"generated content {idle_days} days ago, below the {days_idle}-day idle threshold",
            "continue the current campaign cadence",
        )
    if last_generated_at is None and not generated:
        if remaining:
            topic = remaining[0]
            return (
                "ready_to_revive",
                "campaign has not generated content yet but still has planned topics",
                _generate_action(topic),
            )
        return (
            "stalled",
            "campaign has no generated content and no remaining planned topics",
            "seed a new content idea for this campaign before planning revival content",
        )
    if remaining:
        return (
            "ready_to_revive",
            f"no generated content for {idle_days} days and planned topics remain",
            _generate_action(remaining[0]),
        )
    if ideas:
        return (
            "dormant",
            f"no generated content for {idle_days} days and no planned topics remain, but open ideas exist",
            "promote or seed an open idea into a planned topic for this campaign",
        )
    if campaign_state == "active":
        return (
            "dormant",
            f"no generated content for {idle_days} days and the topic backlog is exhausted",
            "schedule a campaign recap, then pause the campaign if no follow-up is needed",
        )
    return (
        "stalled",
        f"paused campaign has been quiet for {idle_days} days with no planned topics or open ideas",
        "seed a new content idea or keep the campaign paused",
    )


def _generate_action(topic: dict[str, Any]) -> str:
    action = f"generate planned_topic #{topic['id']}: {topic['topic']}"
    if topic.get("angle"):
        action += f" ({topic['angle']})"
    return action


def _load_campaigns(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    campaign_id: int | None,
) -> list[dict[str, Any]]:
    columns = schema["content_campaigns"]
    status_filter = "status IN (?, ?)" if "status" in columns else "1"
    params: list[Any] = list(CAMPAIGN_STATUSES) if "status" in columns else []
    if campaign_id is not None:
        rows = conn.execute(
            "SELECT * FROM content_campaigns WHERE id = ?",
            (campaign_id,),
        ).fetchall()
        return [_row_dict(row) for row in rows]
    order = _order_columns(
        columns,
        [("start_date", "ASC"), ("created_at", "ASC"), ("id", "ASC")],
    )
    rows = conn.execute(
        f"""SELECT * FROM content_campaigns
            WHERE {status_filter}
            ORDER BY {', '.join(order)}""",
        params,
    ).fetchall()
    return [_row_dict(row) for row in rows]


def _load_planned_topics(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    campaign_id: int,
) -> list[dict[str, Any]]:
    columns = schema["planned_topics"]
    order = _order_columns(
        columns,
        [("target_date", "ASC"), ("created_at", "ASC"), ("id", "ASC")],
        alias="pt",
    )
    rows = conn.execute(
        f"""SELECT pt.*
            FROM planned_topics pt
            WHERE pt.campaign_id = ?
            ORDER BY {', '.join(order)}""",
        (campaign_id,),
    ).fetchall()
    return [_row_dict(row) for row in rows]


def _last_generated_at(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    campaign_id: int,
    topics: list[dict[str, Any]],
) -> datetime | None:
    dates: list[datetime] = []
    if "created_at" in schema["generated_content"]:
        rows = conn.execute(
            """SELECT MAX(gc.created_at) AS last_generated_at
               FROM planned_topics pt
               INNER JOIN generated_content gc ON gc.id = pt.content_id
               WHERE pt.campaign_id = ?""",
            (campaign_id,),
        ).fetchall()
        for row in rows:
            parsed = _parse_datetime(_value(row, "last_generated_at", 0))
            if parsed:
                dates.append(parsed)
    if "content_topics" in schema:
        topic_labels = sorted(
            {
                str(topic.get("topic")).strip().lower()
                for topic in topics
                if topic.get("topic")
            }
        )
        if topic_labels:
            placeholders = ", ".join("?" for _ in topic_labels)
            rows = conn.execute(
                f"""SELECT MAX(gc.created_at) AS last_generated_at
                    FROM content_topics ct
                    INNER JOIN generated_content gc ON gc.id = ct.content_id
                    WHERE lower(ct.topic) IN ({placeholders})""",
                topic_labels,
            ).fetchall()
            for row in rows:
                parsed = _parse_datetime(_value(row, "last_generated_at", 0))
                if parsed:
                    dates.append(parsed)
    return max(dates) if dates else None


def _load_open_ideas(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    if "content_ideas" not in schema:
        return []
    columns = schema["content_ideas"]
    status_filter = "status = 'open'" if "status" in columns else "1"
    order = _order_columns(
        columns,
        [("created_at", "ASC"), ("id", "ASC")],
    )
    rows = conn.execute(
        f"""SELECT * FROM content_ideas
            WHERE {status_filter}
            ORDER BY {', '.join(order)}"""
    ).fetchall()
    return [_row_dict(row) for row in rows]


def _matching_ideas(
    campaign: dict[str, Any],
    topics: list[dict[str, Any]],
    ideas: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    campaign_id = int(campaign["id"])
    campaign_name = str(campaign.get("name") or "").strip().lower()
    topic_labels = {
        str(topic.get("topic")).strip().lower()
        for topic in topics
        if topic.get("topic")
    }
    matches = []
    for idea in ideas:
        metadata = _parse_metadata(idea.get("source_metadata"))
        if metadata.get("campaign_id") == campaign_id:
            matches.append(idea)
            continue
        idea_topic = str(idea.get("topic") or "").strip().lower()
        if idea_topic and idea_topic in topic_labels:
            matches.append(idea)
            continue
        note = str(idea.get("note") or "").lower()
        if campaign_name and campaign_name in note:
            matches.append(idea)
    return matches


def _parse_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _idle_days(last_generated_at: datetime | None, now: datetime) -> int | None:
    if last_generated_at is None:
        return None
    return max(0, int((now - last_generated_at).total_seconds() // 86400))


def _empty_report(
    generated_at: datetime,
    days_idle: int,
    campaign_id: int | None,
    cutoff: datetime,
    missing: tuple[str, ...],
) -> CampaignRevivalReport:
    return CampaignRevivalReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days_idle": days_idle,
            "campaign_id": campaign_id,
            "idle_cutoff": cutoff.isoformat(),
            "campaign_statuses": list(CAMPAIGN_STATUSES),
        },
        totals={
            "campaigns": 0,
            "healthy": 0,
            "dormant": 0,
            "stalled": 0,
            "ready_to_revive": 0,
            "missing_tables": len(missing),
        },
        recommendations=(),
        missing_tables=missing,
    )


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    names = [str(_value(row, "name", 0)) for row in rows]
    return {
        name: {
            str(_value(column, "name", 1))
            for column in conn.execute(f"PRAGMA table_info({name})").fetchall()
        }
        for name in names
    }


def _order_columns(
    columns: set[str],
    requested: list[tuple[str, str]],
    *,
    alias: str | None = None,
) -> list[str]:
    prefix = f"{alias}." if alias else ""
    order = [f"{prefix}{name} {direction}" for name, direction in requested if name in columns]
    return order or [f"{prefix}id ASC"]


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _row_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "keys"):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _value(row: Any, key: str, index: int) -> Any:
    if hasattr(row, "keys") and key in row.keys():
        return row[key]
    return row[index]


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
