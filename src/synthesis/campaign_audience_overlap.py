"""Report audience fatigue risk across active planned campaigns."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any, Iterable


DEFAULT_DAYS_AHEAD = 14
DEFAULT_THRESHOLD = 2
DEFAULT_ACTIVE_STATUSES = ("active",)


@dataclass(frozen=True)
class CampaignAudienceItem:
    """One planned campaign/content item with audience targeting metadata."""

    id: int
    title: str
    audience_tags: tuple[str, ...]
    planned_at: str
    channel: str
    status: str
    campaign_id: int | None = None
    campaign_name: str | None = None


@dataclass(frozen=True)
class CampaignAudienceOverlapItem:
    """A planned item inside a flagged audience cluster."""

    id: int
    title: str
    campaign_id: int | None
    campaign_name: str | None
    channel: str
    planned_at: str
    status: str


@dataclass(frozen=True)
class CampaignAudienceOverlapCluster:
    """A normalized audience tag with too many active planned items."""

    audience_tag: str
    count: int
    campaign_ids: list[int]
    channels: list[str]
    planned_dates: list[str]
    items: list[CampaignAudienceOverlapItem]


@dataclass(frozen=True)
class CampaignAudienceOverlapReport:
    """Audience overlap report and applied filters."""

    artifact_type: str
    generated_at: str
    window_start: str
    window_end: str
    days_ahead: int
    threshold: int
    active_statuses: tuple[str, ...]
    considered_item_count: int
    overlap_count: int
    missing_required_tables: list[str]
    clusters: list[CampaignAudienceOverlapCluster]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_campaign_audience_overlap_report_from_items(
    items: Iterable[CampaignAudienceItem | dict[str, Any]],
    *,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    threshold: int = DEFAULT_THRESHOLD,
    active_statuses: tuple[str, ...] = DEFAULT_ACTIVE_STATUSES,
    now: datetime | date | None = None,
) -> CampaignAudienceOverlapReport:
    """Build a deterministic overlap report from explicit planned campaign items."""

    if days_ahead <= 0:
        raise ValueError("days_ahead must be positive")
    if threshold <= 0:
        raise ValueError("threshold must be positive")
    if not active_statuses:
        raise ValueError("active_statuses must not be empty")

    generated_at = _coerce_now(now)
    window_start = generated_at.date()
    window_end = window_start + timedelta(days=days_ahead - 1)
    normalized_active = {_normalize_status(status) for status in active_statuses}

    considered: list[CampaignAudienceItem] = []
    by_tag: dict[str, list[CampaignAudienceItem]] = {}
    for raw in items:
        item = _coerce_item(raw)
        planned_date = _parse_date(item.planned_at)
        if planned_date is None or not window_start <= planned_date <= window_end:
            continue
        if _normalize_status(item.status) not in normalized_active:
            continue
        tags = tuple(_normalize_audience_tag(tag) for tag in item.audience_tags)
        tags = tuple(tag for tag in dict.fromkeys(tags) if tag)
        if not tags:
            continue
        item = CampaignAudienceItem(
            id=item.id,
            title=item.title,
            audience_tags=tags,
            planned_at=item.planned_at,
            channel=_normalize_channel(item.channel),
            status=item.status,
            campaign_id=item.campaign_id,
            campaign_name=item.campaign_name,
        )
        considered.append(item)
        for tag in tags:
            by_tag.setdefault(tag, []).append(item)

    clusters = [
        _cluster(tag, tag_items)
        for tag, tag_items in sorted(by_tag.items())
        if len(tag_items) > threshold
    ]
    clusters.sort(key=lambda cluster: (-cluster.count, cluster.audience_tag))
    return CampaignAudienceOverlapReport(
        artifact_type="campaign_audience_overlap",
        generated_at=generated_at.isoformat(),
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        days_ahead=days_ahead,
        threshold=threshold,
        active_statuses=active_statuses,
        considered_item_count=len(considered),
        overlap_count=len(clusters),
        missing_required_tables=[],
        clusters=clusters,
    )


def build_campaign_audience_overlap_report(
    db_or_conn: Any,
    *,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    threshold: int = DEFAULT_THRESHOLD,
    now: datetime | date | None = None,
) -> CampaignAudienceOverlapReport:
    """Load active planned campaign rows from SQLite and report audience overlap."""

    if days_ahead <= 0:
        raise ValueError("days_ahead must be positive")
    if threshold <= 0:
        raise ValueError("threshold must be positive")

    conn = getattr(db_or_conn, "conn", db_or_conn)
    schema = _schema(conn)
    missing = [
        table for table in ("content_campaigns", "planned_topics") if table not in schema
    ]
    if missing:
        generated_at = _coerce_now(now)
        start = generated_at.date()
        end = start + timedelta(days=days_ahead - 1)
        return CampaignAudienceOverlapReport(
            artifact_type="campaign_audience_overlap",
            generated_at=generated_at.isoformat(),
            window_start=start.isoformat(),
            window_end=end.isoformat(),
            days_ahead=days_ahead,
            threshold=threshold,
            active_statuses=DEFAULT_ACTIVE_STATUSES,
            considered_item_count=0,
            overlap_count=0,
            missing_required_tables=missing,
            clusters=[],
        )

    items = _load_planned_campaign_items(conn, schema)
    return build_campaign_audience_overlap_report_from_items(
        items,
        days_ahead=days_ahead,
        threshold=threshold,
        now=now,
    )


def format_campaign_audience_overlap_json(report: CampaignAudienceOverlapReport) -> str:
    """Serialize a campaign audience overlap report as stable JSON."""

    return json.dumps(report.as_dict(), indent=2, sort_keys=True)


def format_campaign_audience_overlap_text(report: CampaignAudienceOverlapReport) -> str:
    """Render audience overlap warnings for terminal review."""

    lines = [
        "Campaign Audience Overlap",
        f"Window: {report.window_start} to {report.window_end}",
        f"Threshold: more than {report.threshold} active item(s) per audience tag",
        (
            "Totals: "
            f"items={report.considered_item_count} overlaps={report.overlap_count}"
        ),
    ]
    if report.missing_required_tables:
        lines.append("Missing required tables: " + ", ".join(report.missing_required_tables))
        return "\n".join(lines)
    if not report.clusters:
        lines.append("No audience overlap warnings.")
        return "\n".join(lines)

    for cluster in report.clusters:
        lines.extend(
            [
                "",
                f"Audience: {cluster.audience_tag} ({cluster.count} items)",
                f"- Campaign IDs: {', '.join(map(str, cluster.campaign_ids)) or '-'}",
                f"- Channels: {', '.join(cluster.channels) or '-'}",
                f"- Planned dates: {', '.join(cluster.planned_dates) or '-'}",
            ]
        )
        for item in cluster.items:
            campaign = (
                f"campaign #{item.campaign_id}"
                if item.campaign_id is not None
                else "campaign n/a"
            )
            lines.append(
                f"  - {item.planned_at[:10]} [{item.channel}] {campaign} "
                f"item #{item.id}: {item.title}"
            )
    return "\n".join(lines)


def _load_planned_campaign_items(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[CampaignAudienceItem]:
    planned_cols = schema.get("planned_topics", set())
    campaign_cols = schema.get("content_campaigns", set())
    if not {"id", "campaign_id", "topic"}.issubset(planned_cols):
        return []

    planned_audience_expr = _first_column_expr(
        planned_cols,
        ("audience_tags", "audience_tag", "target_audience", "audience"),
        "pt",
    )
    campaign_audience_expr = _first_column_expr(
        campaign_cols,
        ("audience_tags", "audience_tag", "target_audience", "audience"),
        "cc",
    )
    channel_expr = _first_column_expr(
        planned_cols,
        ("channel", "platform", "content_type"),
        "pt",
    )
    planned_at_expr = _first_column_expr(planned_cols, ("planned_at", "target_date"), "pt")

    filters = ["cc.status = 'active'" if "status" in campaign_cols else "1"]
    if "status" in planned_cols:
        filters.append("COALESCE(pt.status, 'planned') NOT IN ('skipped', 'generated')")
    if "content_id" in planned_cols:
        filters.append("pt.content_id IS NULL")
    if planned_at_expr is not None:
        filters.append(f"{planned_at_expr} IS NOT NULL")

    rows = conn.execute(
        f"""SELECT
                  pt.id AS id,
                  pt.topic AS title,
                  COALESCE({planned_audience_expr or 'NULL'}, {campaign_audience_expr or 'NULL'}) AS audience_tags,
                  {planned_at_expr or 'NULL'} AS planned_at,
                  {channel_expr or "'all'"} AS channel,
                  {_column_expr(campaign_cols, 'status', 'cc', 'status')},
                  cc.id AS campaign_id,
                  cc.name AS campaign_name
            FROM planned_topics pt
            INNER JOIN content_campaigns cc ON cc.id = pt.campaign_id
            WHERE {' AND '.join(filters)}
            ORDER BY {planned_at_expr or 'pt.id'} ASC, pt.id ASC"""
    ).fetchall()
    return [_coerce_item(dict(row)) for row in rows]


def _cluster(
    audience_tag: str,
    items: list[CampaignAudienceItem],
) -> CampaignAudienceOverlapCluster:
    sorted_items = sorted(items, key=lambda item: (_parse_date(item.planned_at) or date.max, item.id))
    return CampaignAudienceOverlapCluster(
        audience_tag=audience_tag,
        count=len(sorted_items),
        campaign_ids=sorted({item.campaign_id for item in sorted_items if item.campaign_id is not None}),
        channels=sorted({_normalize_channel(item.channel) for item in sorted_items}),
        planned_dates=sorted({(_parse_date(item.planned_at) or date.max).isoformat() for item in sorted_items}),
        items=[
            CampaignAudienceOverlapItem(
                id=item.id,
                title=item.title,
                campaign_id=item.campaign_id,
                campaign_name=item.campaign_name,
                channel=_normalize_channel(item.channel),
                planned_at=item.planned_at,
                status=item.status,
            )
            for item in sorted_items
        ],
    )


def _coerce_item(raw: CampaignAudienceItem | dict[str, Any]) -> CampaignAudienceItem:
    if isinstance(raw, CampaignAudienceItem):
        return raw
    return CampaignAudienceItem(
        id=int(raw.get("id") or raw.get("campaign_id") or 0),
        title=str(raw.get("title") or raw.get("topic") or raw.get("name") or ""),
        audience_tags=_parse_audience_tags(raw.get("audience_tags")),
        planned_at=str(raw.get("planned_at") or raw.get("target_date") or ""),
        channel=str(raw.get("channel") or raw.get("platform") or "all"),
        status=str(raw.get("status") or "active"),
        campaign_id=_optional_int(raw.get("campaign_id")),
        campaign_name=raw.get("campaign_name"),
    )


def _parse_audience_tags(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple, set)):
        parts = [str(part) for part in value]
    else:
        text = str(value).strip()
        if not text:
            return ()
        if text.startswith("["):
            try:
                loaded = json.loads(text)
            except json.JSONDecodeError:
                loaded = None
            if isinstance(loaded, list):
                parts = [str(part) for part in loaded]
            else:
                parts = re.split(r"[,;|]", text)
        else:
            parts = re.split(r"[,;|]", text)
    return tuple(tag for tag in (_normalize_audience_tag(part) for part in parts) if tag)


def _normalize_audience_tag(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[_\s]+", "-", text)
    text = re.sub(r"[^a-z0-9-]+", "", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text


def _normalize_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_channel(value: Any) -> str:
    text = str(value or "all").strip().lower()
    text = re.sub(r"\s+", "-", text)
    return text or "all"


def _parse_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _coerce_now(value: datetime | date | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _first_column_expr(columns: set[str], names: tuple[str, ...], alias: str) -> str | None:
    for name in names:
        if name in columns:
            return f"{alias}.{name}"
    return None


def _column_expr(columns: set[str], column: str, alias: str, output: str) -> str:
    if column in columns:
        return f"{alias}.{column}"
    return f"NULL AS {output}"


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }
