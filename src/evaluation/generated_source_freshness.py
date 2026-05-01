"""Freshness audit for generated-content source material."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_AGING_DAYS = 14
DEFAULT_STALE_DAYS = 30
SOURCE_COLUMNS = ("source_commits", "source_messages", "source_activity_ids")
STATUSES = ("fresh", "aging", "stale", "missing_sources")


@dataclass(frozen=True)
class GeneratedSourceFreshnessRow:
    """Freshness status for one generated_content row."""

    content_id: int
    content_type: str | None
    created_at: str | None
    status: str
    source_count: int
    resolved_source_count: int
    missing_source_count: int
    oldest_source_timestamp: str | None
    newest_source_timestamp: str | None
    age_days: float | None
    source_refs: dict[str, list[str]]
    missing_refs: dict[str, list[str]]
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["warnings"] = list(self.warnings)
        return payload


@dataclass(frozen=True)
class GeneratedSourceFreshnessReport:
    """Aggregate generated-content source freshness report."""

    generated_at: str
    days: int | None
    content_type: str | None
    aging_days: int
    stale_days: int
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, list[str]]
    warnings: tuple[str, ...]
    rows: tuple[GeneratedSourceFreshnessRow, ...]

    @property
    def summary(self) -> dict[str, Any]:
        by_status = {status: 0 for status in STATUSES}
        for row in self.rows:
            by_status[row.status] = by_status.get(row.status, 0) + 1
        return {
            "total": len(self.rows),
            "fresh": by_status.get("fresh", 0),
            "aging": by_status.get("aging", 0),
            "stale": by_status.get("stale", 0),
            "missing_sources": by_status.get("missing_sources", 0),
            "by_status": by_status,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "aging_days": self.aging_days,
            "content_type": self.content_type,
            "days": self.days,
            "generated_at": self.generated_at,
            "missing_columns": self.missing_columns,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "stale_days": self.stale_days,
            "summary": self.summary,
            "warnings": list(self.warnings),
        }


def build_generated_source_freshness_report(
    db_or_conn: Any,
    *,
    days: int | None = None,
    stale_days: int = DEFAULT_STALE_DAYS,
    aging_days: int = DEFAULT_AGING_DAYS,
    content_type: str | None = None,
    now: datetime | None = None,
) -> GeneratedSourceFreshnessReport:
    """Return a read-only generated-content source freshness report."""
    if days is not None and days < 0:
        raise ValueError("days must be non-negative")
    if stale_days < 0:
        raise ValueError("stale_days must be non-negative")
    if aging_days < 0:
        raise ValueError("aging_days must be non-negative")
    if aging_days > stale_days:
        raise ValueError("aging_days must be less than or equal to stale_days")

    conn = _connection(db_or_conn)
    conn.row_factory = sqlite3.Row
    current_time = _ensure_utc(now or datetime.now(timezone.utc))
    schema = _schema(conn)
    missing_tables = _missing_tables(schema)
    missing_columns = _missing_columns(schema)
    warnings: list[str] = []

    content_rows = _load_generated_content(
        conn,
        schema,
        days=days,
        content_type=content_type,
        now=current_time,
    )
    indexes = _source_indexes(conn, schema)
    rows = tuple(
        _freshness_row(
            content,
            indexes=indexes,
            schema=schema,
            now=current_time,
            aging_days=aging_days,
            stale_days=stale_days,
            report_warnings=warnings,
        )
        for content in content_rows
    )

    return GeneratedSourceFreshnessReport(
        generated_at=current_time.isoformat(),
        days=days,
        content_type=content_type,
        aging_days=aging_days,
        stale_days=stale_days,
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns={key: missing_columns[key] for key in sorted(missing_columns)},
        warnings=tuple(dict.fromkeys(warnings)),
        rows=rows,
    )


def format_generated_source_freshness_json(
    report: GeneratedSourceFreshnessReport,
) -> str:
    """Render the freshness report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_generated_source_freshness_text(
    report: GeneratedSourceFreshnessReport,
) -> str:
    """Render the freshness report as stable terminal text."""
    summary = report.summary
    lines = [
        "GENERATED SOURCE FRESHNESS",
        f"Generated at: {report.generated_at}",
        f"Lookback days: {report.days if report.days is not None else 'all'}",
        f"Content type: {report.content_type or 'all'}",
        f"Thresholds: aging={report.aging_days} days stale={report.stale_days} days",
        (
            "Summary: "
            f"total={summary['total']} fresh={summary['fresh']} aging={summary['aging']} "
            f"stale={summary['stale']} missing_sources={summary['missing_sources']}"
        ),
    ]
    if report.missing_tables:
        lines.append(f"Missing source tables: {', '.join(report.missing_tables)}")
    if report.missing_columns:
        for table, columns in report.missing_columns.items():
            lines.append(f"Missing columns on {table}: {', '.join(columns)}")
    if report.warnings:
        lines.append("Warnings:")
        lines.extend(f"  - {warning}" for warning in report.warnings)

    focus = [row for row in report.rows if row.status in {"stale", "missing_sources"}]
    if focus:
        lines.append("")
        lines.append("Stale or missing-source content:")
        for row in focus:
            age = "n/a" if row.age_days is None else f"{row.age_days:g} days"
            newest = row.newest_source_timestamp or "n/a"
            lines.append(
                f"  - content_id={row.content_id} type={row.content_type or 'n/a'} "
                f"status={row.status} source_age={age} newest_source={newest}"
            )
            for warning in row.warnings:
                lines.append(f"    - {warning}")
    else:
        lines.append("")
        lines.append("No stale or missing-source generated content found.")

    return "\n".join(lines)


def _load_generated_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    days: int | None,
    content_type: str | None,
    now: datetime,
) -> list[dict[str, Any]]:
    columns = schema.get("generated_content")
    if not columns or "id" not in columns:
        return []

    select = {
        "id": "gc.id",
        "content_type": _column_expr(columns, "content_type", alias="gc"),
        "created_at": _column_expr(columns, "created_at", alias="gc"),
        "source_commits": _column_expr(columns, "source_commits", alias="gc"),
        "source_messages": _column_expr(columns, "source_messages", alias="gc"),
        "source_activity_ids": _column_expr(columns, "source_activity_ids", alias="gc"),
    }
    where: list[str] = []
    params: list[Any] = []
    if content_type and "content_type" in columns:
        where.append("gc.content_type = ?")
        params.append(content_type)
    if days is not None and "created_at" in columns:
        where.append("gc.created_at >= ?")
        params.append((now - timedelta(days=days)).isoformat())
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""SELECT
               {select['id']} AS id,
               {select['content_type']} AS content_type,
               {select['created_at']} AS created_at,
               {select['source_commits']} AS source_commits,
               {select['source_messages']} AS source_messages,
               {select['source_activity_ids']} AS source_activity_ids
           FROM generated_content gc
           {where_sql}
           ORDER BY gc.id ASC""",
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def _source_indexes(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[str, dict[str, str]]:
    return {
        "source_commits": _commit_index(conn, schema),
        "source_messages": _message_index(conn, schema),
        "source_activity_ids": _activity_index(conn, schema),
    }


def _commit_index(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[str, str]:
    columns = schema.get("github_commits", set())
    if not {"commit_sha", "timestamp"}.issubset(columns):
        return {}
    rows = conn.execute(
        "SELECT commit_sha, timestamp FROM github_commits ORDER BY commit_sha ASC"
    ).fetchall()
    return {str(row["commit_sha"]): row["timestamp"] for row in rows}


def _message_index(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[str, str]:
    columns = schema.get("claude_messages", set())
    if not {"message_uuid", "timestamp"}.issubset(columns):
        return {}
    rows = conn.execute(
        "SELECT message_uuid, timestamp FROM claude_messages ORDER BY message_uuid ASC"
    ).fetchall()
    return {str(row["message_uuid"]): row["timestamp"] for row in rows}


def _activity_index(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[str, str]:
    columns = schema.get("github_activity", set())
    required = {"id", "repo_name", "number", "activity_type", "updated_at"}
    if not required.issubset(columns):
        return {}
    rows = conn.execute(
        """SELECT id, repo_name, number, activity_type, updated_at
           FROM github_activity
           ORDER BY id ASC"""
    ).fetchall()
    index: dict[str, str] = {}
    for row in rows:
        timestamp = row["updated_at"]
        index[str(row["id"])] = timestamp
        index[_activity_id(row["repo_name"], row["number"], row["activity_type"])] = timestamp
    return index


def _freshness_row(
    content: dict[str, Any],
    *,
    indexes: dict[str, dict[str, str]],
    schema: dict[str, set[str]],
    now: datetime,
    aging_days: int,
    stale_days: int,
    report_warnings: list[str],
) -> GeneratedSourceFreshnessRow:
    content_id = int(content["id"])
    source_refs: dict[str, list[str]] = {}
    missing_refs: dict[str, list[str]] = {}
    timestamps: list[datetime] = []
    row_warnings: list[str] = []

    for field in SOURCE_COLUMNS:
        if field not in schema.get("generated_content", set()):
            row_warnings.append(f"generated_content.{field} column is missing")
            continue
        refs = _json_list(content.get(field), field, content_id, report_warnings, row_warnings)
        source_refs[field] = [str(ref) for ref in refs if ref is not None]
        field_index = indexes.get(field, {})
        missing: list[str] = []
        for ref in source_refs[field]:
            timestamp = field_index.get(ref)
            parsed = _parse_datetime(timestamp)
            if parsed is None:
                missing.append(ref)
                continue
            timestamps.append(parsed)
        if missing:
            missing_refs[field] = missing

    source_count = sum(len(refs) for refs in source_refs.values())
    resolved_source_count = len(timestamps)
    missing_source_count = source_count - resolved_source_count
    oldest = min(timestamps) if timestamps else None
    newest = max(timestamps) if timestamps else None
    age_days = _age_days(newest, now) if newest else None
    status = _status(age_days, source_count, resolved_source_count, aging_days, stale_days)

    if source_count == 0:
        row_warnings.append("no source references recorded")
    elif resolved_source_count == 0:
        row_warnings.append("no source references resolved to source timestamps")
    elif missing_source_count:
        row_warnings.append(f"{missing_source_count} source reference(s) did not resolve")

    return GeneratedSourceFreshnessRow(
        content_id=content_id,
        content_type=content.get("content_type"),
        created_at=content.get("created_at"),
        status=status,
        source_count=source_count,
        resolved_source_count=resolved_source_count,
        missing_source_count=missing_source_count,
        oldest_source_timestamp=oldest.isoformat() if oldest else None,
        newest_source_timestamp=newest.isoformat() if newest else None,
        age_days=age_days,
        source_refs=source_refs,
        missing_refs=missing_refs,
        warnings=tuple(row_warnings),
    )


def _json_list(
    value: Any,
    field: str,
    content_id: int,
    report_warnings: list[str],
    row_warnings: list[str],
) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        warning = f"generated_content {content_id} has malformed {field}: {exc}"
        report_warnings.append(warning)
        row_warnings.append(warning)
        return []
    if not isinstance(parsed, list):
        warning = f"generated_content {content_id} has non-list {field}: {type(parsed).__name__}"
        report_warnings.append(warning)
        row_warnings.append(warning)
        return []
    return parsed


def _status(
    age_days: float | None,
    source_count: int,
    resolved_source_count: int,
    aging_days: int,
    stale_days: int,
) -> str:
    if source_count == 0 or resolved_source_count == 0 or age_days is None:
        return "missing_sources"
    if age_days >= stale_days:
        return "stale"
    if age_days >= aging_days:
        return "aging"
    return "fresh"


def _missing_tables(schema: dict[str, set[str]]) -> list[str]:
    return [
        table
        for table in ("generated_content", "github_commits", "claude_messages", "github_activity")
        if table not in schema
    ]


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    required = {
        "generated_content": {"id", "content_type", "created_at", *SOURCE_COLUMNS},
        "github_commits": {"commit_sha", "timestamp"},
        "claude_messages": {"message_uuid", "timestamp"},
        "github_activity": {"id", "repo_name", "number", "activity_type", "updated_at"},
    }
    missing: dict[str, list[str]] = {}
    for table, columns in required.items():
        if table not in schema:
            continue
        absent = sorted(columns - schema[table])
        if absent:
            missing[table] = absent
    return missing


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {
            str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
            for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
        }
        for table in tables
    }


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str,
) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3 connection or database wrapper with .conn")
    return conn


def _activity_id(repo_name: Any, number: Any, activity_type: Any) -> str:
    return f"{repo_name}#{number}:{activity_type}"


def _age_days(source_time: datetime, now: datetime) -> float:
    return round((now - source_time).total_seconds() / 86400, 2)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
