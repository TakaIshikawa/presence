"""Export content idea conversion by source type and funnel stage."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from io import StringIO
import json
import sqlite3
from typing import Any


DEFAULT_MIN_AGE_DAYS = 14
STAGES = (
    "created",
    "candidate_generated",
    "approved",
    "published",
    "abandoned",
    "stale",
)
ABANDONED_IDEA_STATUSES = {"abandoned", "dismissed", "rejected"}
ABANDONED_TOPIC_STATUSES = {"abandoned", "dismissed", "rejected", "skipped"}
APPROVED_QUALITY_VALUES = {"approved", "good"}


@dataclass(frozen=True)
class ContentIdeaConversionFunnelRow:
    """Aggregate funnel counts for one content idea source type."""

    source_type: str
    counts: dict[str, int]
    idea_ids: tuple[int, ...] = field(default_factory=tuple)
    stale_idea_ids: tuple[int, ...] = field(default_factory=tuple)
    abandoned_idea_ids: tuple[int, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "counts": {stage: self.counts.get(stage, 0) for stage in STAGES},
            "idea_ids": list(self.idea_ids),
            "stale_idea_ids": list(self.stale_idea_ids),
            "abandoned_idea_ids": list(self.abandoned_idea_ids),
        }


@dataclass(frozen=True)
class ContentIdeaConversionFunnelReport:
    """Content idea funnel export plus applied filters."""

    generated_at: str
    filters: dict[str, Any]
    row_count: int
    totals: dict[str, int]
    rows: tuple[ContentIdeaConversionFunnelRow, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "content_idea_conversion_funnel",
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "row_count": self.row_count,
            "totals": {stage: self.totals.get(stage, 0) for stage in STAGES},
            "rows": [row.to_dict() for row in self.rows],
            "missing_tables": list(self.missing_tables),
        }


def build_content_idea_conversion_funnel_report(
    db_or_conn: Any,
    *,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    source_type: str | None = None,
    min_age_days: int = DEFAULT_MIN_AGE_DAYS,
    now: datetime | None = None,
) -> ContentIdeaConversionFunnelReport:
    """Summarize content ideas by source type and downstream funnel stage."""
    if min_age_days < 0:
        raise ValueError("min_age_days must be non-negative")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_aware(now or datetime.now(timezone.utc))
    start = _parse_boundary(start_date, is_end=False)
    end = _parse_boundary(end_date, is_end=True)
    if start and end and start > end:
        raise ValueError("start_date must be on or before end_date")

    filters = {
        "start_date": _filter_value(start_date),
        "end_date": _filter_value(end_date),
        "source_type": _source_filter_value(source_type),
        "min_age_days": min_age_days,
    }
    if "content_ideas" not in schema:
        return ContentIdeaConversionFunnelReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            row_count=0,
            totals={stage: 0 for stage in STAGES},
            rows=(),
            missing_tables=("content_ideas",),
        )

    ideas = _load_ideas(
        conn,
        schema,
        start=start,
        end=end,
        source_type=source_type,
        now=generated_at,
    )
    planned_by_idea = _planned_topics_by_idea(conn, schema)
    content_by_idea = _content_by_idea(conn, schema, ideas, planned_by_idea)
    content_outcomes = _content_outcomes(conn, schema)

    grouped: dict[str, list[dict[str, Any]]] = {}
    for idea in ideas:
        grouped.setdefault(_source_label(idea.get("source")), []).append(idea)

    rows = tuple(
        sorted(
            (
                _build_row(
                    source,
                    source_ideas,
                    planned_by_idea,
                    content_by_idea,
                    content_outcomes,
                    min_age_days=min_age_days,
                    now=generated_at,
                )
                for source, source_ideas in grouped.items()
            ),
            key=lambda row: (row.source_type, row.idea_ids),
        )
    )
    return ContentIdeaConversionFunnelReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        row_count=len(rows),
        totals=_total_counts(rows),
        rows=rows,
        missing_tables=tuple(
            table
            for table in ("planned_topics", "generated_content", "content_publications")
            if table not in schema
        ),
    )


def format_content_idea_conversion_funnel_json(
    report: ContentIdeaConversionFunnelReport,
) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_idea_conversion_funnel_csv(
    report: ContentIdeaConversionFunnelReport,
) -> str:
    """Render deterministic CSV for spreadsheet import."""
    buffer = StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=[
            "source_type",
            *STAGES,
            "idea_ids",
            "stale_idea_ids",
            "abandoned_idea_ids",
        ],
        lineterminator="\n",
    )
    writer.writeheader()
    for row in report.rows:
        writer.writerow(
            {
                "source_type": row.source_type,
                **{stage: row.counts.get(stage, 0) for stage in STAGES},
                "idea_ids": _join_ids(row.idea_ids),
                "stale_idea_ids": _join_ids(row.stale_idea_ids),
                "abandoned_idea_ids": _join_ids(row.abandoned_idea_ids),
            }
        )
    return buffer.getvalue().rstrip("\n")


def _build_row(
    source_type: str,
    ideas: list[dict[str, Any]],
    planned_by_idea: dict[int, list[dict[str, Any]]],
    content_by_idea: dict[int, set[int]],
    content_outcomes: dict[int, dict[str, bool]],
    *,
    min_age_days: int,
    now: datetime,
) -> ContentIdeaConversionFunnelRow:
    counts = {stage: 0 for stage in STAGES}
    idea_ids: list[int] = []
    stale_ids: list[int] = []
    abandoned_ids: list[int] = []

    for idea in ideas:
        idea_id = int(idea["id"])
        idea_ids.append(idea_id)
        planned = planned_by_idea.get(idea_id, [])
        content_ids = content_by_idea.get(idea_id, set())
        outcomes = [content_outcomes.get(content_id, {}) for content_id in content_ids]

        candidate_generated = bool(content_ids)
        approved = any(outcome.get("approved") for outcome in outcomes)
        published = any(outcome.get("published") for outcome in outcomes)
        abandoned = _is_abandoned(idea, planned, outcomes)
        stale = _is_stale(
            idea,
            candidate_generated=candidate_generated,
            approved=approved,
            published=published,
            abandoned=abandoned,
            min_age_days=min_age_days,
            now=now,
        )

        counts["created"] += 1
        if candidate_generated:
            counts["candidate_generated"] += 1
        if approved:
            counts["approved"] += 1
        if published:
            counts["published"] += 1
        if abandoned:
            counts["abandoned"] += 1
            abandoned_ids.append(idea_id)
        if stale:
            counts["stale"] += 1
            stale_ids.append(idea_id)

    return ContentIdeaConversionFunnelRow(
        source_type=source_type,
        counts=counts,
        idea_ids=tuple(sorted(idea_ids)),
        stale_idea_ids=tuple(sorted(stale_ids)),
        abandoned_idea_ids=tuple(sorted(abandoned_ids)),
    )


def _load_ideas(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    start: datetime | None,
    end: datetime | None,
    source_type: str | None,
    now: datetime,
) -> list[dict[str, Any]]:
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
    expected_source = _source_label(source_type) if source_type is not None else None
    for row in rows:
        item = dict(row)
        created_at = _parse_timestamp(item.get("created_at")) or now
        if start and created_at < start:
            continue
        if end and created_at >= end:
            continue
        if expected_source is not None and _source_label(item.get("source")) != expected_source:
            continue
        item["_created_at_dt"] = created_at
        ideas.append(item)
    return ideas


def _planned_topics_by_idea(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, list[dict[str, Any]]]:
    linked: dict[int, list[dict[str, Any]]] = {}
    if "planned_topics" in schema:
        columns = schema["planned_topics"]
        rows = conn.execute(
            f"""SELECT {_column_expr(columns, 'id')},
                       {_column_expr(columns, 'status')},
                       {_column_expr(columns, 'content_id')},
                       {_column_expr(columns, 'source_material')}
                  FROM planned_topics
                 ORDER BY id ASC"""
        ).fetchall()
        for row in rows:
            planned = dict(row)
            metadata = _decode_json_object(planned.get("source_material")) or {}
            idea_id = _int_or_none(metadata.get("content_idea_id"))
            if idea_id is not None:
                linked.setdefault(idea_id, []).append(planned)

    if "content_ideas" in schema and "source_metadata" in schema["content_ideas"]:
        planned_by_id = {
            _int_or_none(row["id"]): dict(row)
            for row in conn.execute(
                f"""SELECT {_column_expr(schema.get('planned_topics', set()), 'id')},
                           {_column_expr(schema.get('planned_topics', set()), 'status')},
                           {_column_expr(schema.get('planned_topics', set()), 'content_id')}
                      FROM planned_topics
                     ORDER BY id ASC"""
            ).fetchall()
        } if "planned_topics" in schema else {}
        for row in conn.execute("SELECT id, source_metadata FROM content_ideas ORDER BY id ASC"):
            idea_id = _int_or_none(row["id"])
            metadata = _decode_json_object(row["source_metadata"]) or {}
            planned_id = _int_or_none(metadata.get("planned_topic_id"))
            planned = planned_by_id.get(planned_id)
            if idea_id is not None and planned:
                linked.setdefault(idea_id, []).append(planned)
    return linked


def _content_by_idea(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    ideas: list[dict[str, Any]],
    planned_by_idea: dict[int, list[dict[str, Any]]],
) -> dict[int, set[int]]:
    linked: dict[int, set[int]] = {int(idea["id"]): set() for idea in ideas}
    for idea_id, planned_rows in planned_by_idea.items():
        if idea_id not in linked:
            continue
        for planned in planned_rows:
            content_id = _int_or_none(planned.get("content_id"))
            if content_id is not None:
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
            column
            for column in ("metadata", "source_metadata")
            if column in schema["generated_content"]
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
) -> dict[int, dict[str, bool]]:
    if "generated_content" not in schema:
        return {}
    columns = schema["generated_content"]
    rows = conn.execute(
        f"""SELECT {_column_expr(columns, 'id')},
                   {_column_expr(columns, 'published')},
                   {_column_expr(columns, 'published_at')},
                   {_column_expr(columns, 'published_url')},
                   {_column_expr(columns, 'curation_quality')}
              FROM generated_content
             ORDER BY id ASC"""
    ).fetchall()
    outcomes: dict[int, dict[str, bool]] = {}
    for row in rows:
        content_id = int(row["id"])
        published = bool(row["published"] == 1 or row["published_at"] or row["published_url"])
        curation_quality = str(row["curation_quality"] or "").strip().lower()
        outcomes[content_id] = {
            "approved": published or curation_quality in APPROVED_QUALITY_VALUES,
            "published": published,
            "abandoned": row["published"] == -1,
        }

    if "content_publications" in schema and "content_id" in schema["content_publications"]:
        cp_columns = schema["content_publications"]
        rows = conn.execute(
            f"""SELECT {_column_expr(cp_columns, 'content_id')},
                       {_column_expr(cp_columns, 'status')},
                       {_column_expr(cp_columns, 'published_at')}
                  FROM content_publications
                 ORDER BY content_id ASC, id ASC"""
        ).fetchall()
        for row in rows:
            content_id = _int_or_none(row["content_id"])
            if content_id not in outcomes:
                continue
            status = str(row["status"] or "").strip().lower()
            if status in {"queued", "publishing", "published"}:
                outcomes[content_id]["approved"] = True
            if status == "published" or row["published_at"]:
                outcomes[content_id]["published"] = True
                outcomes[content_id]["approved"] = True
    return outcomes


def _is_abandoned(
    idea: dict[str, Any],
    planned: list[dict[str, Any]],
    outcomes: list[dict[str, bool]],
) -> bool:
    idea_status = str(idea.get("status") or "").strip().lower()
    if idea_status in ABANDONED_IDEA_STATUSES:
        return True
    if any(str(row.get("status") or "").strip().lower() in ABANDONED_TOPIC_STATUSES for row in planned):
        return True
    return any(outcome.get("abandoned") for outcome in outcomes)


def _is_stale(
    idea: dict[str, Any],
    *,
    candidate_generated: bool,
    approved: bool,
    published: bool,
    abandoned: bool,
    min_age_days: int,
    now: datetime,
) -> bool:
    if abandoned or candidate_generated or approved or published:
        return False
    created_at = idea.get("_created_at_dt") or _parse_timestamp(idea.get("created_at")) or now
    return now - created_at >= timedelta(days=min_age_days)


def _total_counts(rows: tuple[ContentIdeaConversionFunnelRow, ...]) -> dict[str, int]:
    return {stage: sum(row.counts.get(stage, 0) for row in rows) for stage in STAGES}


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


def _parse_boundary(value: str | date | datetime | None, *, is_end: bool) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_aware(value)
    if isinstance(value, date):
        base = datetime.combine(value, time.min, tzinfo=timezone.utc)
        return base + timedelta(days=1) if is_end else base
    text = str(value).strip()
    if not text:
        return None
    try:
        if len(text) == 10:
            base = datetime.combine(date.fromisoformat(text), time.min, tzinfo=timezone.utc)
            return base + timedelta(days=1) if is_end else base
        return _ensure_aware(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError as exc:
        name = "end_date" if is_end else "start_date"
        raise ValueError(f"{name} must be ISO-8601 date or datetime") from exc


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


def _source_filter_value(value: str | None) -> str | None:
    if value is None:
        return None
    return _source_label(value)


def _filter_value(value: str | date | datetime | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def _join_ids(values: tuple[int, ...]) -> str:
    return ",".join(str(value) for value in values)
