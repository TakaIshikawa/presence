"""Report snoozed content ideas that should return to review soon."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS_AHEAD = 7
NOTE_PREVIEW_LENGTH = 96
REQUIRED_COLUMNS = {"id", "note", "status", "snoozed_until"}

_BUCKET_RANK = {"overdue": 0, "due_today": 1, "due_soon": 2, "later": 3}
_PRIORITY_RANK = {"high": 0, "normal": 1, "low": 2}


@dataclass(frozen=True)
class ContentIdeaSnoozeWakeupRow:
    """One snoozed idea that is ready or nearing readiness."""

    id: int
    bucket: str
    priority: str
    topic: str | None
    source: str | None
    snoozed_until: str
    snooze_reason: str | None
    note_preview: str
    created_at: str | None
    updated_at: str | None
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ContentIdeaSnoozeWakeupReport:
    """Content idea snooze wake-up report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ContentIdeaSnoozeWakeupRow, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "content_idea_snooze_wakeup",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_content_idea_snooze_wakeup_report(
    db_or_conn: Any,
    *,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    include_overdue: bool = True,
    now: datetime | None = None,
) -> ContentIdeaSnoozeWakeupReport:
    """Return open snoozed content ideas due now or within the lookahead window."""
    if days_ahead < 0:
        raise ValueError("days_ahead must be non-negative")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    due_before = generated_at + timedelta(days=days_ahead)
    filters = {
        "days_ahead": days_ahead,
        "due_before": due_before.isoformat(),
        "include_overdue": include_overdue,
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

    rows = tuple(
        sorted(
            (
                _build_row(row, now=generated_at)
                for row in _load_rows(
                    conn,
                    columns=schema["content_ideas"],
                    now=generated_at,
                    due_before=due_before,
                    include_overdue=include_overdue,
                )
            ),
            key=_sort_key,
        )
    )
    bucket_counts = {
        bucket: sum(1 for row in rows if row.bucket == bucket)
        for bucket in ("overdue", "due_today", "due_soon", "later")
    }
    return ContentIdeaSnoozeWakeupReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={"idea_count": len(rows), **bucket_counts},
        rows=rows,
        missing_tables=(),
        missing_columns={},
    )


def format_content_idea_snooze_wakeup_json(
    report: ContentIdeaSnoozeWakeupReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_idea_snooze_wakeup_text(
    report: ContentIdeaSnoozeWakeupReport,
) -> str:
    """Render the snooze wake-up report for operators."""
    totals = report.totals
    lines = [
        "Content Idea Snooze Wake-up",
        f"Generated: {report.generated_at}",
        (
            f"Window: days_ahead={report.filters['days_ahead']} "
            f"include_overdue={report.filters['include_overdue']} "
            f"due_before={report.filters['due_before']}"
        ),
        (
            "Totals: "
            f"ideas={totals['idea_count']} "
            f"overdue={totals['overdue']} "
            f"due_today={totals['due_today']} "
            f"due_soon={totals['due_soon']} "
            f"later={totals['later']}"
        ),
    ]
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in report.missing_columns.items()
        ]
        lines.append(f"Missing columns: {'; '.join(missing)}")
    lines.append("")

    if not report.rows:
        lines.append("No snoozed content ideas due for wake-up.")
        return "\n".join(lines)

    lines.append("Ideas:")
    for row in report.rows:
        lines.append(
            f"  - id={row.id} bucket={row.bucket} priority={row.priority} "
            f"topic={row.topic or '-'} snoozed_until={row.snoozed_until} "
            f"action={row.recommended_action}"
        )
        lines.append(f"    reason: {row.snooze_reason or '-'}")
        lines.append(f"    note: {row.note_preview or '-'}")
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    *,
    columns: set[str],
    now: datetime,
    due_before: datetime,
    include_overdue: bool,
) -> list[dict[str, Any]]:
    select = {
        "id": "ci.id",
        "note": "ci.note",
        "topic": _column_expr(columns, "topic", alias="ci"),
        "priority": _column_expr(columns, "priority", "'normal'", alias="ci"),
        "source": _column_expr(columns, "source", alias="ci"),
        "snoozed_until": "ci.snoozed_until",
        "snooze_reason": _column_expr(columns, "snooze_reason", alias="ci"),
        "created_at": _column_expr(columns, "created_at", alias="ci"),
        "updated_at": _column_expr(columns, "updated_at", alias="ci"),
    }
    filters = [
        "ci.status = 'open'",
        "ci.snoozed_until IS NOT NULL",
        "datetime(ci.snoozed_until) IS NOT NULL",
        "datetime(ci.snoozed_until) <= datetime(?)",
    ]
    params: list[Any] = [due_before.isoformat()]
    if not include_overdue:
        filters.append("datetime(ci.snoozed_until) >= datetime(?)")
        params.append(now.isoformat())

    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   {select['id']} AS id,
                   {select['note']} AS note,
                   {select['topic']} AS topic,
                   {select['priority']} AS priority,
                   {select['source']} AS source,
                   {select['snoozed_until']} AS snoozed_until,
                   {select['snooze_reason']} AS snooze_reason,
                   {select['created_at']} AS created_at,
                   {select['updated_at']} AS updated_at
               FROM content_ideas ci
               WHERE {' AND '.join(filters)}
               ORDER BY ci.id ASC""",
            params,
        ).fetchall()
    ]


def _build_row(row: dict[str, Any], *, now: datetime) -> ContentIdeaSnoozeWakeupRow:
    snoozed_until = _parse_datetime(row["snoozed_until"])
    bucket = _bucket(snoozed_until, now=now)
    priority = _value(row.get("priority"), "normal")
    return ContentIdeaSnoozeWakeupRow(
        id=int(row["id"]),
        bucket=bucket,
        priority=priority,
        topic=_optional_value(row.get("topic")),
        source=_optional_value(row.get("source")),
        snoozed_until=snoozed_until.isoformat(),
        snooze_reason=_optional_value(row.get("snooze_reason")),
        note_preview=_preview(row.get("note")),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
        recommended_action=_recommended_action(bucket=bucket, priority=priority),
    )


def _bucket(snoozed_until: datetime, *, now: datetime) -> str:
    if snoozed_until < now:
        return "overdue"
    if snoozed_until.date() == now.date():
        return "due_today"
    if snoozed_until <= now + timedelta(days=3):
        return "due_soon"
    return "later"


def _recommended_action(*, bucket: str, priority: str) -> str:
    if bucket == "overdue" and priority == "high":
        return "promote_to_active_queue"
    if bucket == "overdue":
        return "unsnooze_for_review"
    if bucket == "due_today":
        return "review_today"
    if bucket == "due_soon":
        return "prepare_for_review"
    return "monitor"


def _sort_key(row: ContentIdeaSnoozeWakeupRow) -> tuple[Any, ...]:
    return (
        _BUCKET_RANK.get(row.bucket, 99),
        _PRIORITY_RANK.get(row.priority, 99),
        row.snoozed_until,
        row.topic or "",
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
) -> ContentIdeaSnoozeWakeupReport:
    return ContentIdeaSnoozeWakeupReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "idea_count": 0,
            "overdue": 0,
            "due_today": 0,
            "due_soon": 0,
            "later": 0,
        },
        rows=(),
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
