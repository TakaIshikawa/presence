"""Rank content idea sources by downstream generation and engagement."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_MIN_IDEAS = 1
RESONANCE_SCORE_THRESHOLD = 5.0


@dataclass(frozen=True)
class ContentIdeaSourceRoiRow:
    """One source-level ROI aggregate for content ideas."""

    source: str
    ideas_created: int
    promoted_generated: int
    published: int
    average_engagement: float
    resonance_rate: float
    recommendation: str
    idea_ids: tuple[int, ...] = field(default_factory=tuple)
    content_ids: tuple[int, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["idea_ids"] = list(self.idea_ids)
        payload["content_ids"] = list(self.content_ids)
        return payload


@dataclass(frozen=True)
class ContentIdeaSourceRoiReport:
    """Source ROI report plus applied filters."""

    days: int
    min_ideas: int
    generated_at: str
    resonance_score_threshold: float
    rows: tuple[ContentIdeaSourceRoiRow, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "days": self.days,
            "min_ideas": self.min_ideas,
            "generated_at": self.generated_at,
            "resonance_score_threshold": self.resonance_score_threshold,
            "source_count": len(self.rows),
            "rows": [row.to_dict() for row in self.rows],
        }


def build_content_idea_source_roi_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_ideas: int = DEFAULT_MIN_IDEAS,
    now: datetime | None = None,
) -> ContentIdeaSourceRoiReport:
    """Return source-level ROI rows for recent content ideas."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_ideas <= 0:
        raise ValueError("min_ideas must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    now = _ensure_aware(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)

    ideas = _load_ideas(conn, schema, cutoff=cutoff, now=now)
    if not ideas:
        return ContentIdeaSourceRoiReport(
            days=days,
            min_ideas=min_ideas,
            generated_at=now.isoformat(),
            resonance_score_threshold=RESONANCE_SCORE_THRESHOLD,
            rows=(),
        )

    linked = _linked_content_by_idea(conn, schema, ideas)
    content_outcomes = _content_outcomes(conn, schema)

    groups: dict[str, list[dict[str, Any]]] = {}
    for idea in ideas:
        groups.setdefault(_source_label(idea.get("source")), []).append(idea)

    rows: list[ContentIdeaSourceRoiRow] = []
    for source, source_ideas in groups.items():
        if len(source_ideas) < min_ideas:
            continue
        rows.append(_build_row(source, source_ideas, linked, content_outcomes, min_ideas))

    rows.sort(
        key=lambda row: (
            -row.average_engagement,
            -row.resonance_rate,
            -row.published,
            -row.promoted_generated,
            row.source,
        )
    )
    return ContentIdeaSourceRoiReport(
        days=days,
        min_ideas=min_ideas,
        generated_at=now.isoformat(),
        resonance_score_threshold=RESONANCE_SCORE_THRESHOLD,
        rows=tuple(rows),
    )


def format_content_idea_source_roi_json(report: ContentIdeaSourceRoiReport) -> str:
    """Render a source ROI report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_idea_source_roi_text(report: ContentIdeaSourceRoiReport) -> str:
    """Render a readable fixed-width source ROI table."""
    lines = [
        "Content Idea Source ROI",
        f"Generated: {report.generated_at}",
        f"Window: {report.days} days",
        f"Minimum ideas: {report.min_ideas}",
        "",
    ]
    if not report.rows:
        lines.append("No content idea sources found.")
        return "\n".join(lines)

    header = (
        f"{'Source':<24} {'Ideas':>5} {'Prom/gen':>8} {'Pub':>5} "
        f"{'Avg eng':>8} {'Res rate':>8} Recommendation"
    )
    lines.extend([header, "-" * len(header)])
    for row in report.rows:
        lines.append(
            f"{_shorten(row.source, 24):<24} "
            f"{row.ideas_created:>5} "
            f"{row.promoted_generated:>8} "
            f"{row.published:>5} "
            f"{row.average_engagement:>8.2f} "
            f"{row.resonance_rate:>7.0%} "
            f"{row.recommendation}"
        )
    return "\n".join(lines)


def _build_row(
    source: str,
    ideas: list[dict[str, Any]],
    linked: dict[int, set[int]],
    outcomes: dict[int, dict[str, Any]],
    min_ideas: int,
) -> ContentIdeaSourceRoiRow:
    idea_ids = tuple(sorted(int(idea["id"]) for idea in ideas))
    content_ids = tuple(
        sorted(
            {
                content_id
                for idea_id in idea_ids
                for content_id in linked.get(idea_id, set())
            }
        )
    )
    generated_idea_ids = {
        idea_id
        for idea_id in idea_ids
        if linked.get(idea_id)
        or _normalized_status(next(idea for idea in ideas if int(idea["id"]) == idea_id)) == "promoted"
    }
    published_ids = tuple(
        content_id
        for content_id in content_ids
        if outcomes.get(content_id, {}).get("published")
    )
    published_scores = [
        float(outcomes.get(content_id, {}).get("engagement_score") or 0.0)
        for content_id in published_ids
    ]
    resonant_count = sum(
        1
        for content_id in published_ids
        if outcomes.get(content_id, {}).get("auto_quality") == "resonated"
        or float(outcomes.get(content_id, {}).get("engagement_score") or 0.0)
        >= RESONANCE_SCORE_THRESHOLD
    )
    average_engagement = round(mean(published_scores), 2) if published_scores else 0.0
    resonance_rate = round(resonant_count / len(published_ids), 3) if published_ids else 0.0

    return ContentIdeaSourceRoiRow(
        source=source,
        ideas_created=len(ideas),
        promoted_generated=len(generated_idea_ids),
        published=len(published_ids),
        average_engagement=average_engagement,
        resonance_rate=resonance_rate,
        recommendation=_recommendation(
            ideas_created=len(ideas),
            promoted_generated=len(generated_idea_ids),
            published=len(published_ids),
            average_engagement=average_engagement,
            resonance_rate=resonance_rate,
            min_ideas=min_ideas,
        ),
        idea_ids=idea_ids,
        content_ids=content_ids,
    )


def _load_ideas(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    if "content_ideas" not in schema:
        return []
    rows = conn.execute(
        """SELECT id, note, topic, priority, status, source, source_metadata, created_at
           FROM content_ideas
           ORDER BY created_at ASC, id ASC"""
    ).fetchall()
    ideas = []
    for row in rows:
        item = dict(row)
        created_at = _parse_timestamp(item.get("created_at")) or now
        if cutoff <= created_at <= now:
            ideas.append(item)
    return ideas


def _linked_content_by_idea(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    ideas: list[dict[str, Any]],
) -> dict[int, set[int]]:
    linked: dict[int, set[int]] = {int(idea["id"]): set() for idea in ideas}
    planned_by_id: dict[int, dict[str, Any]] = {}

    if "planned_topics" in schema:
        for row in conn.execute(
            """SELECT id, status, content_id, source_material
               FROM planned_topics
               ORDER BY id ASC"""
        ).fetchall():
            planned = dict(row)
            planned_id = int(planned["id"])
            planned_by_id[planned_id] = planned
            metadata = _decode_json_object(planned.get("source_material"))
            idea_id = _int_or_none(metadata.get("content_idea_id")) if metadata else None
            content_id = _int_or_none(planned.get("content_id"))
            if idea_id in linked and content_id is not None:
                linked[idea_id].add(content_id)

    for idea in ideas:
        idea_id = int(idea["id"])
        metadata = _decode_json_object(idea.get("source_metadata")) or {}
        for key in ("content_id", "generated_content_id", "source_content_id"):
            content_id = _int_or_none(metadata.get(key))
            if content_id is not None:
                linked[idea_id].add(content_id)
        planned_id = _int_or_none(metadata.get("planned_topic_id"))
        planned = planned_by_id.get(planned_id) if planned_id is not None else None
        if planned:
            content_id = _int_or_none(planned.get("content_id"))
            if content_id is not None:
                linked[idea_id].add(content_id)

    _link_generated_metadata(conn, schema, linked)
    return linked


def _link_generated_metadata(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    linked: dict[int, set[int]],
) -> None:
    if "generated_content" not in schema:
        return
    metadata_columns = [
        column
        for column in ("metadata", "source_metadata")
        if column in schema["generated_content"]
    ]
    if not metadata_columns:
        return
    select_columns = ", ".join(["id", *metadata_columns])
    for row in conn.execute(f"SELECT {select_columns} FROM generated_content").fetchall():
        item = dict(row)
        content_id = _int_or_none(item.get("id"))
        if content_id is None:
            continue
        for column in metadata_columns:
            metadata = _decode_json_object(item.get(column)) or {}
            idea_id = _int_or_none(metadata.get("content_idea_id"))
            if idea_id in linked:
                linked[idea_id].add(content_id)


def _content_outcomes(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, dict[str, Any]]:
    if "generated_content" not in schema:
        return {}
    content: dict[int, dict[str, Any]] = {}
    for row in conn.execute(
        """SELECT id, published, published_at, auto_quality
           FROM generated_content
           ORDER BY id ASC"""
    ).fetchall():
        item = dict(row)
        content_id = int(item["id"])
        content[content_id] = {
            "published": bool(item.get("published") == 1 or item.get("published_at")),
            "auto_quality": item.get("auto_quality"),
            "engagement_score": 0.0,
        }

    if "content_publications" in schema:
        rows = conn.execute(
            """SELECT content_id, MAX(CASE WHEN status = 'published' THEN 1 ELSE 0 END) AS published
               FROM content_publications
               GROUP BY content_id"""
        ).fetchall()
        for row in rows:
            content_id = int(row["content_id"])
            if content_id in content and row["published"]:
                content[content_id]["published"] = True

    latest_scores: dict[int, float] = {}
    for table in ("post_engagement", "linkedin_engagement", "bluesky_engagement"):
        if table not in schema or "engagement_score" not in schema[table]:
            continue
        for content_id, score in _latest_scores(conn, table).items():
            latest_scores[content_id] = latest_scores.get(content_id, 0.0) + score
    for content_id, score in latest_scores.items():
        if content_id in content:
            content[content_id]["engagement_score"] = round(score, 2)
    return content


def _latest_scores(conn: sqlite3.Connection, table: str) -> dict[int, float]:
    rows = conn.execute(
        f"""SELECT content_id, engagement_score
            FROM (
                SELECT content_id, engagement_score,
                       ROW_NUMBER() OVER (
                           PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                       ) AS rn
                FROM {table}
                WHERE engagement_score IS NOT NULL
            )
            WHERE rn = 1"""
    ).fetchall()
    return {int(row["content_id"]): float(row["engagement_score"] or 0.0) for row in rows}


def _recommendation(
    *,
    ideas_created: int,
    promoted_generated: int,
    published: int,
    average_engagement: float,
    resonance_rate: float,
    min_ideas: int,
) -> str:
    if ideas_created < min_ideas:
        return "watch"
    if promoted_generated == 0 or (published == 0 and ideas_created >= max(min_ideas, 2)):
        return "deprioritize"
    if published and resonance_rate >= 0.5 and average_engagement >= RESONANCE_SCORE_THRESHOLD:
        return "double_down"
    if published >= 2 and average_engagement >= 3.0:
        return "double_down"
    if published and resonance_rate < 0.25 and average_engagement < 2.0:
        return "deprioritize"
    return "watch"


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


def _normalized_status(idea: dict[str, Any]) -> str:
    return str(idea.get("status") or "").strip().lower()


def _shorten(value: str, max_length: int) -> str:
    if len(value) <= max_length:
        return value
    return value[: max_length - 3] + "..."
