"""Content idea conversion funnel reporting."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_GROUP_BY = "source-topic"
DEFAULT_RESONANCE_SCORE_THRESHOLD = 5.0
VALID_GROUP_BY = {"source", "topic", "source-topic"}
STAGES = ("created", "promoted", "planned", "generated", "published", "resonated")
STAGE_LABELS = {
    "created": "Created",
    "promoted": "Promoted",
    "planned": "Planned",
    "generated": "Generated",
    "published": "Published",
    "resonated": "Resonated",
}


@dataclass(frozen=True)
class FunnelDropoff:
    """Largest conversion loss inside one funnel row."""

    from_stage: str | None
    to_stage: str | None
    lost_count: int
    drop_rate: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ContentIdeaFunnelRow:
    """One grouped content idea funnel row."""

    source: str | None
    topic: str | None
    counts: dict[str, int]
    conversion_rates: dict[str, float]
    previous_stage_rates: dict[str, float]
    largest_dropoff: FunnelDropoff
    idea_ids: tuple[int, ...] = field(default_factory=tuple)
    planned_topic_ids: tuple[int, ...] = field(default_factory=tuple)
    content_ids: tuple[int, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "topic": self.topic,
            "counts": {stage: self.counts.get(stage, 0) for stage in STAGES},
            "conversion_rates": {
                stage: self.conversion_rates.get(stage, 0.0) for stage in STAGES
            },
            "previous_stage_rates": {
                stage: self.previous_stage_rates.get(stage, 0.0)
                for stage in STAGES[1:]
            },
            "largest_dropoff": self.largest_dropoff.to_dict(),
            "idea_ids": list(self.idea_ids),
            "planned_topic_ids": list(self.planned_topic_ids),
            "content_ids": list(self.content_ids),
        }


@dataclass(frozen=True)
class ContentIdeaFunnelReport:
    """Grouped funnel report plus applied filters."""

    generated_at: str
    days: int
    group_by: str
    source: str | None
    topic: str | None
    resonance_score_threshold: float
    missing_optional_tables: tuple[str, ...]
    rows: tuple[ContentIdeaFunnelRow, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "days": self.days,
            "group_by": self.group_by,
            "source": self.source,
            "topic": self.topic,
            "resonance_score_threshold": self.resonance_score_threshold,
            "missing_optional_tables": list(self.missing_optional_tables),
            "row_count": len(self.rows),
            "totals": _total_counts(self.rows),
            "rows": [row.to_dict() for row in self.rows],
        }


def build_content_idea_funnel_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    group_by: str = DEFAULT_GROUP_BY,
    source: str | None = None,
    topic: str | None = None,
    resonance_score_threshold: float = DEFAULT_RESONANCE_SCORE_THRESHOLD,
    now: datetime | None = None,
) -> ContentIdeaFunnelReport:
    """Build a read-only grouped funnel for recent content ideas."""
    if days <= 0:
        raise ValueError("days must be positive")
    if group_by not in VALID_GROUP_BY:
        raise ValueError(f"invalid group_by: {group_by}")
    if resonance_score_threshold < 0:
        raise ValueError("resonance_score_threshold must be non-negative")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    now = _ensure_aware(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)

    ideas = _load_ideas(conn, schema, cutoff=cutoff, now=now, source=source, topic=topic)
    planned_by_idea = _planned_topics_by_idea(conn, schema)
    content_by_idea = _content_by_idea(conn, schema, ideas, planned_by_idea)
    outcomes = _content_outcomes(conn, schema, resonance_score_threshold)

    groups: dict[tuple[str | None, str | None], list[dict[str, Any]]] = {}
    for idea in ideas:
        key = _group_key(idea, group_by)
        groups.setdefault(key, []).append(idea)

    rows = [
        _build_row(group_source, group_topic, group_ideas, planned_by_idea, content_by_idea, outcomes)
        for (group_source, group_topic), group_ideas in groups.items()
    ]
    rows.sort(
        key=lambda row: (
            row.source or "",
            row.topic or "",
            -row.counts["created"],
            row.idea_ids,
        )
    )

    optional_tables = (
        "content_publications",
        "post_engagement",
        "linkedin_engagement",
        "bluesky_engagement",
        "mastodon_engagement",
        "newsletter_link_clicks",
    )
    return ContentIdeaFunnelReport(
        generated_at=now.isoformat(),
        days=days,
        group_by=group_by,
        source=_filter_label(source),
        topic=_filter_label(topic),
        resonance_score_threshold=resonance_score_threshold,
        missing_optional_tables=tuple(table for table in optional_tables if table not in schema),
        rows=tuple(rows),
    )


def format_content_idea_funnel_json(report: ContentIdeaFunnelReport) -> str:
    """Render the funnel report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_idea_funnel_text(report: ContentIdeaFunnelReport) -> str:
    """Render a stable operator-facing text report."""
    lines = [
        "Content Idea Conversion Funnel",
        f"Generated: {report.generated_at}",
        f"Window: {report.days} days",
        f"Group by: {report.group_by}",
    ]
    filters = []
    if report.source:
        filters.append(f"source={report.source}")
    if report.topic:
        filters.append(f"topic={report.topic}")
    if filters:
        lines.append(f"Filters: {', '.join(filters)}")
    if report.missing_optional_tables:
        lines.append(f"Missing optional tables: {', '.join(report.missing_optional_tables)}")
    lines.append("")

    if not report.rows:
        lines.append("No content ideas found.")
        return "\n".join(lines)

    header = (
        f"{'Source':<18} {'Topic':<18} "
        f"{'Create':>6} {'Promo':>6} {'Plan':>6} {'Gen':>6} {'Pub':>6} {'Res':>6} "
        f"{'Largest drop-off':<24}"
    )
    lines.extend([header, "-" * len(header)])
    for row in report.rows:
        dropoff = row.largest_dropoff
        if dropoff.from_stage and dropoff.to_stage:
            dropoff_text = (
                f"*{STAGE_LABELS[dropoff.from_stage]}->{STAGE_LABELS[dropoff.to_stage]} "
                f"-{dropoff.lost_count} ({dropoff.drop_rate:.0%})*"
            )
        else:
            dropoff_text = "*none*"
        lines.append(
            f"{_shorten(row.source or 'all', 18):<18} "
            f"{_shorten(row.topic or 'all', 18):<18} "
            f"{row.counts['created']:>6} "
            f"{row.counts['promoted']:>6} "
            f"{row.counts['planned']:>6} "
            f"{row.counts['generated']:>6} "
            f"{row.counts['published']:>6} "
            f"{row.counts['resonated']:>6} "
            f"{dropoff_text:<24}"
        )
    return "\n".join(lines)


def _build_row(
    source: str | None,
    topic: str | None,
    ideas: list[dict[str, Any]],
    planned_by_idea: dict[int, set[int]],
    content_by_idea: dict[int, set[int]],
    outcomes: dict[int, dict[str, Any]],
) -> ContentIdeaFunnelRow:
    idea_ids = tuple(sorted(int(idea["id"]) for idea in ideas))
    planned_ids = tuple(
        sorted({planned_id for idea_id in idea_ids for planned_id in planned_by_idea.get(idea_id, set())})
    )
    content_ids = tuple(
        sorted({content_id for idea_id in idea_ids for content_id in content_by_idea.get(idea_id, set())})
    )

    counts = {stage: 0 for stage in STAGES}
    counts["created"] = len(idea_ids)
    for idea in ideas:
        idea_id = int(idea["id"])
        idea_content_ids = content_by_idea.get(idea_id, set())
        has_planned = bool(planned_by_idea.get(idea_id))
        has_generated = bool(idea_content_ids)
        has_published = any(outcomes.get(content_id, {}).get("published") for content_id in idea_content_ids)
        has_resonated = any(outcomes.get(content_id, {}).get("resonated") for content_id in idea_content_ids)
        if _status(idea) == "promoted" or has_planned:
            counts["promoted"] += 1
        if has_planned:
            counts["planned"] += 1
        if has_generated:
            counts["generated"] += 1
        if has_published:
            counts["published"] += 1
        if has_resonated:
            counts["resonated"] += 1

    return ContentIdeaFunnelRow(
        source=source,
        topic=topic,
        counts=counts,
        conversion_rates=_conversion_rates(counts),
        previous_stage_rates=_previous_stage_rates(counts),
        largest_dropoff=_largest_dropoff(counts),
        idea_ids=idea_ids,
        planned_topic_ids=planned_ids,
        content_ids=content_ids,
    )


def _load_ideas(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    now: datetime,
    source: str | None,
    topic: str | None,
) -> list[dict[str, Any]]:
    if "content_ideas" not in schema:
        return []
    columns = schema["content_ideas"]
    select_columns = [
        _column_expr(columns, "id"),
        _column_expr(columns, "note"),
        _column_expr(columns, "topic"),
        _column_expr(columns, "status"),
        _column_expr(columns, "source"),
        _column_expr(columns, "source_metadata"),
        _column_expr(columns, "created_at"),
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
              FROM content_ideas
             ORDER BY created_at ASC, id ASC"""
    ).fetchall()
    ideas: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        created_at = _parse_timestamp(item.get("created_at")) or now
        if created_at < cutoff or created_at > now:
            continue
        if source is not None and _source_label(item.get("source")) != _source_label(source):
            continue
        if topic is not None and _topic_label(item.get("topic")) != _topic_label(topic):
            continue
        ideas.append(item)
    return ideas


def _planned_topics_by_idea(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, set[int]]:
    linked: dict[int, set[int]] = {}
    if "planned_topics" in schema:
        columns = schema["planned_topics"]
        rows = conn.execute(
            f"""SELECT {_column_expr(columns, 'id')}, {_column_expr(columns, 'source_material')}
                  FROM planned_topics
                 ORDER BY id ASC"""
        ).fetchall()
        for row in rows:
            planned_id = _int_or_none(row["id"])
            if planned_id is None:
                continue
            metadata = _decode_json_object(row["source_material"]) or {}
            idea_id = _int_or_none(metadata.get("content_idea_id"))
            if idea_id is not None:
                linked.setdefault(idea_id, set()).add(planned_id)

    if "content_ideas" in schema and "source_metadata" in schema["content_ideas"]:
        for row in conn.execute("SELECT id, source_metadata FROM content_ideas ORDER BY id ASC").fetchall():
            idea_id = _int_or_none(row["id"])
            metadata = _decode_json_object(row["source_metadata"]) or {}
            planned_id = _int_or_none(metadata.get("planned_topic_id"))
            if idea_id is not None and planned_id is not None:
                linked.setdefault(idea_id, set()).add(planned_id)
    return linked


def _content_by_idea(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    ideas: list[dict[str, Any]],
    planned_by_idea: dict[int, set[int]],
) -> dict[int, set[int]]:
    linked: dict[int, set[int]] = {int(idea["id"]): set() for idea in ideas}
    if "planned_topics" in schema:
        columns = schema["planned_topics"]
        if "content_id" in columns:
            rows = conn.execute(
                f"""SELECT {_column_expr(columns, 'id')}, {_column_expr(columns, 'content_id')}
                      FROM planned_topics
                     ORDER BY id ASC"""
            ).fetchall()
            content_by_planned = {
                int(row["id"]): int(row["content_id"])
                for row in rows
                if _int_or_none(row["id"]) is not None
                and _int_or_none(row["content_id"]) is not None
            }
            for idea_id, planned_ids in planned_by_idea.items():
                for planned_id in planned_ids:
                    content_id = content_by_planned.get(planned_id)
                    if content_id is not None and idea_id in linked:
                        linked[idea_id].add(content_id)

    for idea in ideas:
        idea_id = int(idea["id"])
        metadata = _decode_json_object(idea.get("source_metadata")) or {}
        for key in ("content_id", "generated_content_id", "source_content_id"):
            content_id = _int_or_none(metadata.get(key))
            if content_id is not None:
                linked[idea_id].add(content_id)

    if "generated_content" in schema:
        metadata_columns = [
            column for column in ("metadata", "source_metadata") if column in schema["generated_content"]
        ]
        if metadata_columns:
            rows = conn.execute(
                f"SELECT id, {', '.join(metadata_columns)} FROM generated_content ORDER BY id ASC"
            ).fetchall()
            for row in rows:
                content_id = _int_or_none(row["id"])
                if content_id is None:
                    continue
                for column in metadata_columns:
                    metadata = _decode_json_object(row[column]) or {}
                    idea_id = _int_or_none(metadata.get("content_idea_id"))
                    if idea_id in linked:
                        linked[idea_id].add(content_id)
    return linked


def _content_outcomes(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    resonance_score_threshold: float,
) -> dict[int, dict[str, Any]]:
    if "generated_content" not in schema:
        return {}
    columns = schema["generated_content"]
    rows = conn.execute(
        f"""SELECT {_column_expr(columns, 'id')},
                   {_column_expr(columns, 'published')},
                   {_column_expr(columns, 'published_at')},
                   {_column_expr(columns, 'auto_quality')}
              FROM generated_content
             ORDER BY id ASC"""
    ).fetchall()
    outcomes: dict[int, dict[str, Any]] = {}
    for row in rows:
        content_id = int(row["id"])
        outcomes[content_id] = {
            "published": bool(row["published"] == 1 or row["published_at"]),
            "resonated": str(row["auto_quality"] or "").lower() == "resonated",
            "engagement_score": 0.0,
        }

    if "content_publications" in schema and "content_id" in schema["content_publications"]:
        cp_columns = schema["content_publications"]
        status_expr = "status" if "status" in cp_columns else "NULL"
        rows = conn.execute(
            f"""SELECT content_id,
                       MAX(CASE WHEN {status_expr} = 'published' THEN 1 ELSE 0 END) AS published
                  FROM content_publications
                 GROUP BY content_id"""
        ).fetchall()
        for row in rows:
            content_id = int(row["content_id"])
            if content_id in outcomes and row["published"]:
                outcomes[content_id]["published"] = True

    scores: dict[int, float] = {}
    for table in ("post_engagement", "linkedin_engagement", "bluesky_engagement", "mastodon_engagement"):
        if table not in schema or "content_id" not in schema[table] or "engagement_score" not in schema[table]:
            continue
        for content_id, score in _latest_scores(conn, schema, table, "engagement_score").items():
            scores[content_id] = scores.get(content_id, 0.0) + score
    if "newsletter_link_clicks" in schema and "content_id" in schema["newsletter_link_clicks"]:
        click_column = "unique_clicks" if "unique_clicks" in schema["newsletter_link_clicks"] else "clicks"
        if click_column in schema["newsletter_link_clicks"]:
            for content_id, score in _latest_scores(conn, schema, "newsletter_link_clicks", click_column).items():
                scores[content_id] = scores.get(content_id, 0.0) + score

    for content_id, score in scores.items():
        if content_id in outcomes:
            outcomes[content_id]["engagement_score"] = round(score, 2)
            if score >= resonance_score_threshold:
                outcomes[content_id]["resonated"] = True
    return outcomes


def _latest_scores(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    table: str,
    score_column: str,
) -> dict[int, float]:
    order_column = "fetched_at" if "fetched_at" in schema[table] else "created_at"
    if order_column not in schema[table]:
        order_column = "id"
    rows = conn.execute(
        f"""SELECT content_id, {score_column} AS score
              FROM (
                    SELECT content_id, {score_column},
                           ROW_NUMBER() OVER (
                               PARTITION BY content_id ORDER BY {order_column} DESC, id DESC
                           ) AS rn
                      FROM {table}
                     WHERE {score_column} IS NOT NULL
                   )
             WHERE rn = 1"""
    ).fetchall()
    return {int(row["content_id"]): float(row["score"] or 0.0) for row in rows}


def _conversion_rates(counts: dict[str, int]) -> dict[str, float]:
    created = counts.get("created", 0)
    return {
        stage: (1.0 if stage == "created" and created else _rate(counts.get(stage, 0), created))
        for stage in STAGES
    }


def _previous_stage_rates(counts: dict[str, int]) -> dict[str, float]:
    rates: dict[str, float] = {}
    previous = "created"
    for stage in STAGES[1:]:
        rates[stage] = _rate(counts.get(stage, 0), counts.get(previous, 0))
        previous = stage
    return rates


def _largest_dropoff(counts: dict[str, int]) -> FunnelDropoff:
    largest = FunnelDropoff(None, None, 0, 0.0)
    previous = "created"
    for stage in STAGES[1:]:
        previous_count = counts.get(previous, 0)
        current_count = counts.get(stage, 0)
        lost = max(previous_count - current_count, 0)
        drop_rate = _rate(lost, previous_count)
        candidate = FunnelDropoff(previous, stage, lost, drop_rate)
        if (candidate.drop_rate, candidate.lost_count, candidate.to_stage or "") > (
            largest.drop_rate,
            largest.lost_count,
            largest.to_stage or "",
        ):
            largest = candidate
        previous = stage
    return largest


def _total_counts(rows: tuple[ContentIdeaFunnelRow, ...]) -> dict[str, int]:
    return {stage: sum(row.counts.get(stage, 0) for row in rows) for stage in STAGES}


def _rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 3)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if conn is None:
        raise ValueError("database connection is not available")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }
    return {
        table: {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
    }


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    return f"{column} AS {column}" if column in columns else f"{default} AS {column}"


def _decode_json_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if not value:
        return None
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return decoded if isinstance(decoded, dict) else None


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


def _source_label(value: Any) -> str:
    text = str(value or "").strip()
    return text or "unknown"


def _topic_label(value: Any) -> str:
    text = str(value or "").strip()
    return text or "untagged"


def _filter_label(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _group_key(idea: dict[str, Any], group_by: str) -> tuple[str | None, str | None]:
    source = _source_label(idea.get("source"))
    topic = _topic_label(idea.get("topic"))
    if group_by == "source":
        return source, None
    if group_by == "topic":
        return None, topic
    return source, topic


def _status(idea: dict[str, Any]) -> str:
    return str(idea.get("status") or "").strip().lower()


def _shorten(value: str, max_length: int) -> str:
    if len(value) <= max_length:
        return value
    return value[: max_length - 3] + "..."
