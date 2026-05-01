"""Annotate planned topics with source-material freshness hints."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import json
import re
import sqlite3
from typing import Any, Iterable


DEFAULT_DAYS_STALE = 30
SOURCE_DATE_KEYS = {
    "date",
    "published_at",
    "published_date",
    "source_date",
    "source_published_at",
    "timestamp",
}
KNOWLEDGE_ID_KEYS = {"knowledge_id", "knowledge_ids", "linked_knowledge_ids"}
_ISO_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}(?:[T ][0-9:.+-]+Z?)?\b")


@dataclass(frozen=True)
class PlannedTopicFreshnessHint:
    planned_topic_id: int
    topic: str
    angle: str | None
    campaign_id: int | None
    campaign_name: str | None
    target_date: str | None
    source_material: str | None
    source_date: str | None
    source_date_origin: str | None
    days_since_source: int | None
    days_stale: int
    aging_after_days: int
    hints: list[str]
    linked_knowledge_ids: list[int]
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_planned_topic_freshness_hints(
    planned_topic_rows: Iterable[dict[str, Any]],
    knowledge_rows: Iterable[dict[str, Any]] | None = None,
    *,
    days_stale: int = DEFAULT_DAYS_STALE,
    now: date | datetime | None = None,
) -> list[PlannedTopicFreshnessHint]:
    """Return source freshness hints for planned topic rows.

    The topic's target date is carried through for context but is not used for
    classification. Freshness is based only on dates in ``source_material`` or,
    when those are absent, ``published_at`` from referenced knowledge rows.
    """
    if days_stale <= 0:
        raise ValueError("days_stale must be positive")

    current_date = _coerce_date(now)
    aging_after_days = max(1, days_stale // 2)
    knowledge_by_id = {
        int(row["id"]): dict(row)
        for row in knowledge_rows or []
        if row.get("id") is not None
    }

    hints = [
        _hint_for_topic(
            dict(row),
            knowledge_by_id,
            today=current_date,
            days_stale=days_stale,
            aging_after_days=aging_after_days,
        )
        for row in planned_topic_rows
    ]
    return sorted(
        hints,
        key=lambda item: (
            _hint_priority(item.hints),
            -(item.days_since_source or -1),
            item.campaign_id is None,
            item.campaign_id or 0,
            item.target_date or "",
            item.planned_topic_id,
        ),
    )


def build_planned_topic_freshness_hints_report(
    db_or_conn: Any,
    *,
    days_stale: int = DEFAULT_DAYS_STALE,
    campaign: str | int | None = None,
    limit: int | None = None,
    now: date | datetime | None = None,
) -> dict[str, Any]:
    """Load planned topics and linked knowledge, then build a read-only report."""
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    campaign_row = _resolve_campaign(conn, schema, campaign)
    topic_rows = _planned_topic_rows(
        conn,
        schema,
        campaign_id=campaign_row["id"] if campaign_row else None,
        limit=limit,
    )
    knowledge_rows = _knowledge_rows(conn, schema)
    generated_at = _coerce_datetime(now)
    hints = build_planned_topic_freshness_hints(
        topic_rows,
        knowledge_rows,
        days_stale=days_stale,
        now=generated_at,
    )

    return {
        "generated_at": generated_at.isoformat(),
        "campaign": campaign_row,
        "thresholds": {
            "days_stale": days_stale,
            "aging_after_days": max(1, days_stale // 2),
        },
        "totals": {
            "planned_topics": len(hints),
            "fresh": sum("fresh" in item.hints for item in hints),
            "aging": sum("aging" in item.hints for item in hints),
            "stale_source": sum("stale_source" in item.hints for item in hints),
            "missing_source_date": sum(
                "missing_source_date" in item.hints for item in hints
            ),
            "refresh_recommended": sum(
                "refresh_recommended" in item.hints for item in hints
            ),
        },
        "hints": [item.to_dict() for item in hints],
    }


def format_planned_topic_freshness_hints_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_planned_topic_freshness_hints_text(report: dict[str, Any]) -> str:
    lines = [
        "Planned topic source freshness hints",
        f"Generated: {report['generated_at']}",
        f"Days stale: {report['thresholds']['days_stale']}",
        f"Aging after: {report['thresholds']['aging_after_days']} days",
    ]
    campaign = report.get("campaign")
    if campaign:
        lines.append(f"Campaign: {campaign.get('name')} (ID {campaign.get('id')})")
    lines.extend(
        [
            (
                "Totals: "
                f"planned_topics={report['totals']['planned_topics']} "
                f"stale_source={report['totals']['stale_source']} "
                f"missing_source_date={report['totals']['missing_source_date']} "
                f"refresh_recommended={report['totals']['refresh_recommended']}"
            ),
            "",
        ]
    )

    if not report["hints"]:
        lines.append("No planned topics matched the selected filters.")
        return "\n".join(lines)

    for item in report["hints"]:
        age = "-" if item["days_since_source"] is None else f"{item['days_since_source']}d"
        source_date = item["source_date"] or "unknown"
        campaign_label = (
            f" campaign={item['campaign_name'] or item['campaign_id']}"
            if item["campaign_id"] is not None
            else ""
        )
        lines.append(
            f"- #{item['planned_topic_id']}{campaign_label} {item['topic']}: "
            f"{', '.join(item['hints'])}; source={source_date} age={age}. "
            f"{item['reason']}"
        )
    return "\n".join(lines)


def _hint_for_topic(
    row: dict[str, Any],
    knowledge_by_id: dict[int, dict[str, Any]],
    *,
    today: date,
    days_stale: int,
    aging_after_days: int,
) -> PlannedTopicFreshnessHint:
    source_material = row.get("source_material")
    explicit_dates = _source_material_dates(source_material)
    linked_knowledge_ids = _linked_knowledge_ids(source_material)
    source_date = None
    origin = None

    if explicit_dates:
        source_date = max(explicit_dates)
        origin = "source_material"
    elif linked_knowledge_ids:
        knowledge_dates = [
            parsed
            for knowledge_id in linked_knowledge_ids
            if (knowledge := knowledge_by_id.get(knowledge_id)) is not None
            if (parsed := parse_date(knowledge.get("published_at"))) is not None
        ]
        if knowledge_dates:
            source_date = max(knowledge_dates)
            origin = "knowledge.published_at"

    if source_date is None:
        labels = ["missing_source_date", "refresh_recommended"]
        days_since = None
        reason = "source material has no date and no linked knowledge published_at fallback"
    else:
        days_since = max(0, (today - source_date).days)
        if days_since >= days_stale:
            labels = ["stale_source", "refresh_recommended"]
            reason = (
                f"source material is {days_since} day(s) old, meeting the "
                f"{days_stale}-day stale threshold"
            )
        elif days_since >= aging_after_days:
            labels = ["aging"]
            reason = (
                f"source material is {days_since} day(s) old, past the "
                f"{aging_after_days}-day aging hint"
            )
        else:
            labels = ["fresh"]
            reason = f"source material is {days_since} day(s) old"

    return PlannedTopicFreshnessHint(
        planned_topic_id=int(row["id"]),
        topic=str(row.get("topic") or ""),
        angle=row.get("angle"),
        campaign_id=row.get("campaign_id"),
        campaign_name=row.get("campaign_name"),
        target_date=row.get("target_date"),
        source_material=source_material,
        source_date=source_date.isoformat() if source_date else None,
        source_date_origin=origin,
        days_since_source=days_since,
        days_stale=days_stale,
        aging_after_days=aging_after_days,
        hints=labels,
        linked_knowledge_ids=linked_knowledge_ids,
        reason=reason,
    )


def parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _source_material_dates(source_material: Any) -> list[date]:
    parsed = _parse_source_material(source_material)
    values: list[Any] = []
    if parsed is not None:
        _collect_values_for_keys(parsed, SOURCE_DATE_KEYS, values)
    elif source_material:
        values.extend(match.group(0) for match in _ISO_DATE_RE.finditer(str(source_material)))
    return [date_value for value in values if (date_value := parse_date(value)) is not None]


def _linked_knowledge_ids(source_material: Any) -> list[int]:
    parsed = _parse_source_material(source_material)
    values: list[Any] = []
    if parsed is not None:
        _collect_values_for_keys(parsed, KNOWLEDGE_ID_KEYS, values)
    elif source_material:
        text = str(source_material)
        values.extend(
            match.group(1)
            for match in re.finditer(r"\bknowledge(?:_id)?[:#= ]+(\d+)\b", text)
        )
    ids: list[int] = []
    for value in values:
        candidates = value if isinstance(value, list | tuple | set) else [value]
        for candidate in candidates:
            if isinstance(candidate, list | tuple) and candidate:
                candidate = candidate[0]
            try:
                knowledge_id = int(candidate)
            except (TypeError, ValueError):
                continue
            if knowledge_id not in ids:
                ids.append(knowledge_id)
    return ids


def _parse_source_material(source_material: Any) -> Any | None:
    if isinstance(source_material, dict | list):
        return source_material
    if not source_material:
        return None
    try:
        return json.loads(str(source_material))
    except (TypeError, ValueError):
        return None


def _collect_values_for_keys(value: Any, keys: set[str], output: list[Any]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key) in keys:
                output.append(item)
            _collect_values_for_keys(item, keys, output)
    elif isinstance(value, list | tuple):
        for item in value:
            _collect_values_for_keys(item, keys, output)


def _planned_topic_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    campaign_id: int | None,
    limit: int | None,
) -> list[dict[str, Any]]:
    if "planned_topics" not in schema:
        return []
    params: list[Any] = []
    where = ["pt.status = 'planned'"]
    if campaign_id is not None:
        where.append("pt.campaign_id = ?")
        params.append(campaign_id)
    limit_clause = ""
    if limit is not None:
        limit_clause = " LIMIT ?"
        params.append(limit)
    rows = conn.execute(
        f"""SELECT pt.*,
                  cc.name AS campaign_name
           FROM planned_topics pt
           LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
           WHERE {" AND ".join(where)}
           ORDER BY cc.start_date ASC NULLS LAST,
                    pt.campaign_id ASC NULLS LAST,
                    pt.target_date ASC NULLS LAST,
                    pt.created_at ASC,
                    pt.id ASC
           {limit_clause}""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _knowledge_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    columns = schema.get("knowledge", set())
    if not {"id", "published_at"}.issubset(columns):
        return []
    where = "WHERE approved = 1" if "approved" in columns else ""
    rows = conn.execute(
        f"""SELECT id, published_at
            FROM knowledge
            {where}
            ORDER BY id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _resolve_campaign(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    campaign: str | int | None,
) -> dict[str, Any] | None:
    if campaign is None:
        return None
    if "content_campaigns" not in schema:
        raise ValueError(f"Campaign {campaign} does not exist")

    text = str(campaign).strip()
    if text.isdigit():
        row = conn.execute(
            "SELECT id, name, status, start_date, end_date FROM content_campaigns WHERE id = ?",
            (int(text),),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT id, name, status, start_date, end_date FROM content_campaigns WHERE name = ?",
            (text,),
        ).fetchone()
    if row is None:
        raise ValueError(f"Campaign {campaign} does not exist")
    return dict(row)


def _hint_priority(labels: list[str]) -> int:
    if "stale_source" in labels:
        return 0
    if "missing_source_date" in labels:
        return 1
    if "aging" in labels:
        return 2
    return 3


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


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _coerce_date(value: date | datetime | None) -> date:
    if value is None:
        return datetime.now(timezone.utc).date()
    if isinstance(value, datetime):
        return value.date()
    return value


def _coerce_datetime(value: date | datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
