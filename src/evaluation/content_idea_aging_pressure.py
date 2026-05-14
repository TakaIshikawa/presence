"""Report open content ideas prioritized by aging pressure score."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_MIN_AGE_DAYS = 0
NOTE_PREVIEW_LENGTH = 96
REQUIRED_COLUMNS = {"id", "note", "status", "created_at"}

_PRIORITY_RANK = {"high": 0, "normal": 1, "low": 2}


@dataclass(frozen=True)
class ContentIdeaAgingPressureRow:
    """One open idea with aging pressure score."""

    id: int
    age_days: int
    priority: str
    pressure_score: float
    topic: str | None
    source: str | None
    note_preview: str
    created_at: str
    updated_at: str | None
    is_snoozed: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ContentIdeaAgingPressureReport:
    """Content idea aging pressure report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ContentIdeaAgingPressureRow, ...]
    grouped_by_topic: dict[str, int]
    grouped_by_source: dict[str, int]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "content_idea_aging_pressure",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "grouped_by_source": dict(sorted(self.grouped_by_source.items())),
            "grouped_by_topic": dict(sorted(self.grouped_by_topic.items())),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_content_idea_aging_pressure_report(
    db_or_conn: Any,
    *,
    min_age_days: int = DEFAULT_MIN_AGE_DAYS,
    include_snoozed: bool = False,
    topic: str | None = None,
    now: datetime | None = None,
) -> ContentIdeaAgingPressureReport:
    """Return open content ideas with pressure scores based on age and priority."""
    if min_age_days < 0:
        raise ValueError("min_age_days must be non-negative")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {
        "min_age_days": min_age_days,
        "include_snoozed": include_snoozed,
        "topic": topic,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    all_rows = (
        _build_row(row, now=generated_at)
        for row in _load_rows(
            conn,
            columns=schema["content_ideas"],
            include_snoozed=include_snoozed,
            topic=topic,
        )
    )
    rows = tuple(
        sorted(
            (row for row in all_rows if row.age_days >= min_age_days),
            key=_sort_key,
            reverse=True,
        )
    )

    grouped_by_topic: dict[str, int] = {}
    grouped_by_source: dict[str, int] = {}
    for row in rows:
        topic_key = row.topic or "(none)"
        source_key = row.source or "(none)"
        grouped_by_topic[topic_key] = grouped_by_topic.get(topic_key, 0) + 1
        grouped_by_source[source_key] = grouped_by_source.get(source_key, 0) + 1

    return ContentIdeaAgingPressureReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "idea_count": len(rows),
            "snoozed_count": sum(1 for row in rows if row.is_snoozed),
            "active_count": sum(1 for row in rows if not row.is_snoozed),
        },
        rows=rows,
        grouped_by_topic=grouped_by_topic,
        grouped_by_source=grouped_by_source,
        missing_tables=(),
        missing_columns={},
    )


def format_content_idea_aging_pressure_json(
    report: ContentIdeaAgingPressureReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_idea_aging_pressure_csv(
    report: ContentIdeaAgingPressureReport,
) -> str:
    """Render the aging pressure report as CSV."""
    lines = [
        "id,age_days,priority,pressure_score,topic,source,is_snoozed,note_preview"
    ]
    for row in report.rows:
        lines.append(
            f"{row.id},{row.age_days},{row.priority},{row.pressure_score:.2f},"
            f"{_csv_escape(row.topic or '')},"
            f"{_csv_escape(row.source or '')},"
            f"{row.is_snoozed},"
            f"{_csv_escape(row.note_preview)}"
        )
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    *,
    columns: set[str],
    include_snoozed: bool,
    topic: str | None,
) -> list[dict[str, Any]]:
    select = {
        "id": "ci.id",
        "note": "ci.note",
        "topic": _column_expr(columns, "topic", alias="ci"),
        "priority": _column_expr(columns, "priority", "'normal'", alias="ci"),
        "source": _column_expr(columns, "source", alias="ci"),
        "created_at": "ci.created_at",
        "updated_at": _column_expr(columns, "updated_at", alias="ci"),
        "snoozed_until": _column_expr(columns, "snoozed_until", alias="ci"),
    }
    filters = ["ci.status = 'open'"]
    params: list[Any] = []

    if not include_snoozed:
        filters.append("(ci.snoozed_until IS NULL OR datetime(ci.snoozed_until) IS NULL)")

    if topic:
        filters.append("ci.topic = ?")
        params.append(topic)

    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   {select['id']} AS id,
                   {select['note']} AS note,
                   {select['topic']} AS topic,
                   {select['priority']} AS priority,
                   {select['source']} AS source,
                   {select['created_at']} AS created_at,
                   {select['updated_at']} AS updated_at,
                   {select['snoozed_until']} AS snoozed_until
               FROM content_ideas ci
               WHERE {' AND '.join(filters)}
               ORDER BY ci.id ASC""",
            params,
        ).fetchall()
    ]


def _build_row(row: dict[str, Any], *, now: datetime) -> ContentIdeaAgingPressureRow:
    created_at = _parse_datetime(row["created_at"])
    age_days = max(0, int((now - created_at).total_seconds() // 86400))
    priority = _value(row.get("priority"), "normal")
    is_snoozed = bool(
        row.get("snoozed_until")
        and _parse_datetime(row["snoozed_until"]) is not None
    )
    pressure_score = _compute_pressure_score(age_days=age_days, priority=priority)

    return ContentIdeaAgingPressureRow(
        id=int(row["id"]),
        age_days=age_days,
        priority=priority,
        pressure_score=pressure_score,
        topic=_optional_value(row.get("topic")),
        source=_optional_value(row.get("source")),
        note_preview=_preview(row.get("note")),
        created_at=created_at.isoformat(),
        updated_at=row.get("updated_at"),
        is_snoozed=is_snoozed,
    )


def _compute_pressure_score(*, age_days: int, priority: str) -> float:
    """Compute pressure score from age and priority.

    High priority: 3.0 * age_days
    Normal priority: 1.0 * age_days
    Low priority: 0.3 * age_days
    """
    multipliers = {"high": 3.0, "normal": 1.0, "low": 0.3}
    multiplier = multipliers.get(priority, 1.0)
    return multiplier * age_days


def _sort_key(row: ContentIdeaAgingPressureRow) -> tuple[Any, ...]:
    return (
        row.pressure_score,
        _PRIORITY_RANK.get(row.priority, 99),
        row.age_days,
        row.id,
    )


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    if "content_ideas" not in schema:
        return ("content_ideas",), {}
    missing = REQUIRED_COLUMNS - schema["content_ideas"]
    missing_columns = (
        {"content_ideas": tuple(sorted(missing))}
        if missing
        else {}
    )
    return (), missing_columns


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ContentIdeaAgingPressureReport:
    return ContentIdeaAgingPressureReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "idea_count": 0,
            "snoozed_count": 0,
            "active_count": 0,
        },
        rows=(),
        grouped_by_topic={},
        grouped_by_source={},
        missing_tables=missing_tables,
        missing_columns=missing_columns or {},
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {
            str(info[1]) for info in conn.execute(f"PRAGMA table_info({table})")
        }
    return schema


def _column_expr(
    columns: set[str],
    column: str,
    default: str = "NULL",
    *,
    alias: str,
) -> str:
    if column in columns:
        return f"{alias}.{column}"
    return default


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if value is None:
        raise ValueError("Cannot parse None as datetime")
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return _ensure_utc(datetime.fromisoformat(text))


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _value(value: Any, default: str) -> str:
    text = str(value or "").strip()
    return text if text else default


def _optional_value(value: Any) -> str | None:
    text = str(value or "").strip()
    return text if text else None


def _preview(value: Any, width: int = NOTE_PREVIEW_LENGTH) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= width:
        return text
    return text[: max(width - 3, 0)] + "..."


def _csv_escape(value: str) -> str:
    """Escape CSV field values."""
    if not value:
        return ""
    if "," in value or '"' in value or "\n" in value:
        return f'"{value.replace(chr(34), chr(34) * 2)}"'
    return value
