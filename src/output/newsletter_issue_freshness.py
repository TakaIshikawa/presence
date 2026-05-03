"""Measure source artifact freshness for assembled newsletter issues."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, time, timezone
from io import StringIO
import json
import sqlite3
from statistics import median
from typing import Any


DEFAULT_STALE_DAYS = 14


@dataclass(frozen=True)
class NewsletterIssueFreshnessRow:
    """Freshness metrics for one assembled newsletter issue."""

    newsletter_send_id: int
    issue_id: str
    subject: str
    sent_at: str
    status: str
    source_content_ids: tuple[int, ...]
    section_count: int
    source_timestamp_count: int
    missing_source_count: int
    missing_source_timestamp_count: int
    newest_source_age_days: float | None
    oldest_source_age_days: float | None
    median_source_age_days: float | None
    stale_section_count: int
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["source_content_ids"] = list(self.source_content_ids)
        payload["warnings"] = list(self.warnings)
        return payload


@dataclass(frozen=True)
class NewsletterIssueFreshnessReport:
    """Issue-level source freshness report."""

    generated_at: str
    filters: dict[str, Any]
    summary: dict[str, int]
    rows: tuple[NewsletterIssueFreshnessRow, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_issue_freshness",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(sorted(self.summary.items())),
        }


def build_newsletter_issue_freshness_report(
    db_or_conn: Any,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    stale_days: int = DEFAULT_STALE_DAYS,
    now: datetime | None = None,
) -> NewsletterIssueFreshnessReport:
    """Return one freshness row per newsletter issue."""

    if stale_days < 0:
        raise ValueError("stale_days must be non-negative")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    start_bound = _date_bound(start_date, end_of_day=False)
    end_bound = _date_bound(end_date, end_of_day=True)
    filters = {
        "end_date": end_date,
        "end_bound": end_bound,
        "stale_days": stale_days,
        "start_date": start_date,
        "start_bound": start_bound,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return NewsletterIssueFreshnessReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            summary=_summary(()),
            rows=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    sends = _load_sends(conn, schema, start_bound=start_bound, end_bound=end_bound)
    source_ids = {
        content_id
        for send in sends
        for content_id in parse_source_content_ids(send.get("source_content_ids"))[0]
    }
    sources = _load_sources(conn, schema, source_ids)
    rows = tuple(
        _freshness_row(send, sources, stale_days=stale_days)
        for send in sends
    )
    return NewsletterIssueFreshnessReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        summary=_summary(rows),
        rows=rows,
        missing_columns={},
    )


def format_newsletter_issue_freshness_json(
    report: NewsletterIssueFreshnessReport,
) -> str:
    """Serialize a report as deterministic JSON."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_issue_freshness_csv(
    report: NewsletterIssueFreshnessReport,
) -> str:
    """Serialize freshness rows as CSV."""

    fieldnames = [
        "newsletter_send_id",
        "issue_id",
        "subject",
        "sent_at",
        "status",
        "section_count",
        "source_timestamp_count",
        "missing_source_count",
        "missing_source_timestamp_count",
        "newest_source_age_days",
        "oldest_source_age_days",
        "median_source_age_days",
        "stale_section_count",
        "source_content_ids",
        "warnings",
    ]
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in report.rows:
        payload = row.to_dict()
        payload["source_content_ids"] = json.dumps(payload["source_content_ids"])
        payload["warnings"] = json.dumps(payload["warnings"])
        writer.writerow({field: payload.get(field) for field in fieldnames})
    return output.getvalue().rstrip("\r\n")


def parse_source_content_ids(raw_value: Any) -> tuple[list[int], list[str]]:
    """Parse newsletter_sends.source_content_ids without raising on bad data."""

    if raw_value in (None, ""):
        return [], ["missing_source_content_ids"]
    try:
        parsed = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (TypeError, json.JSONDecodeError):
        return [], ["malformed_source_content_ids"]
    if not isinstance(parsed, list):
        return [], ["malformed_source_content_ids"]

    source_ids: list[int] = []
    malformed = False
    for item in parsed:
        try:
            content_id = int(item)
        except (TypeError, ValueError):
            malformed = True
            continue
        if content_id <= 0:
            malformed = True
            continue
        source_ids.append(content_id)
    warnings = ["malformed_source_content_ids"] if malformed else []
    if not source_ids and not warnings:
        warnings.append("missing_source_content_ids")
    return source_ids, warnings


def _freshness_row(
    send: dict[str, Any],
    sources: dict[int, dict[str, Any]],
    *,
    stale_days: int,
) -> NewsletterIssueFreshnessRow:
    source_ids, parse_warnings = parse_source_content_ids(send.get("source_content_ids"))
    sent_at = _parse_datetime(send.get("sent_at"))
    warnings = set(parse_warnings)
    ages: list[float] = []
    missing_source_count = 0
    missing_timestamp_count = 0
    stale_section_count = 0

    if sent_at is None and source_ids:
        warnings.add("missing_issue_timestamp")

    for content_id in source_ids:
        source = sources.get(content_id)
        if source is None:
            missing_source_count += 1
            warnings.add("missing_source_row")
            continue
        source_timestamp = _parse_datetime(source.get("source_timestamp"))
        if source_timestamp is None or sent_at is None:
            missing_timestamp_count += 1
            warnings.add("missing_source_timestamp")
            continue
        age_days = round((sent_at - source_timestamp).total_seconds() / 86400, 2)
        ages.append(age_days)
        if age_days > stale_days:
            stale_section_count += 1

    if stale_section_count:
        warnings.add("stale_sections")

    return NewsletterIssueFreshnessRow(
        newsletter_send_id=int(send["newsletter_send_id"]),
        issue_id=str(send.get("issue_id") or ""),
        subject=str(send.get("subject") or ""),
        sent_at=str(send.get("sent_at") or ""),
        status=str(send.get("status") or ""),
        source_content_ids=tuple(source_ids),
        section_count=len(source_ids),
        source_timestamp_count=len(ages),
        missing_source_count=missing_source_count,
        missing_source_timestamp_count=missing_timestamp_count,
        newest_source_age_days=min(ages) if ages else None,
        oldest_source_age_days=max(ages) if ages else None,
        median_source_age_days=round(float(median(ages)), 2) if ages else None,
        stale_section_count=stale_section_count,
        warnings=tuple(sorted(warnings)),
    )


def _load_sends(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    start_bound: str | None,
    end_bound: str | None,
) -> list[dict[str, Any]]:
    columns = schema["newsletter_sends"]
    select = {
        "newsletter_send_id": "ns.id",
        "issue_id": _column_expr(columns, "issue_id", "''", alias="ns"),
        "subject": _column_expr(columns, "subject", "''", alias="ns"),
        "source_content_ids": _column_expr(columns, "source_content_ids", "NULL", alias="ns"),
        "status": _column_expr(columns, "status", "''", alias="ns"),
        "sent_at": _column_expr(columns, "sent_at", "NULL", alias="ns"),
    }
    filters: list[str] = []
    params: list[Any] = []
    if start_bound and "sent_at" in columns:
        filters.append("ns.sent_at >= ?")
        params.append(start_bound)
    if end_bound and "sent_at" in columns:
        filters.append("ns.sent_at <= ?")
        params.append(end_bound)
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = conn.execute(
        f"""SELECT
               {select['newsletter_send_id']} AS newsletter_send_id,
               {select['issue_id']} AS issue_id,
               {select['subject']} AS subject,
               {select['source_content_ids']} AS source_content_ids,
               {select['status']} AS status,
               {select['sent_at']} AS sent_at
           FROM newsletter_sends ns
           {where_clause}
           ORDER BY {select['sent_at']} DESC, ns.id DESC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _load_sources(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    source_ids: set[int],
) -> dict[int, dict[str, Any]]:
    columns = schema.get("generated_content", set())
    if not source_ids or not columns or "id" not in columns:
        return {}
    placeholders = ",".join("?" for _ in sorted(source_ids))
    timestamp_expr = _source_timestamp_expr(columns)
    rows = conn.execute(
        f"""SELECT id, {timestamp_expr} AS source_timestamp
            FROM generated_content
            WHERE id IN ({placeholders})""",
        sorted(source_ids),
    ).fetchall()
    return {int(row["id"]): dict(row) for row in rows}


def _source_timestamp_expr(columns: set[str]) -> str:
    timestamp_columns = [
        column for column in ("published_at", "created_at") if column in columns
    ]
    if not timestamp_columns:
        return "NULL"
    if len(timestamp_columns) == 1:
        return timestamp_columns[0]
    return "COALESCE(published_at, created_at)"


def _summary(rows: tuple[NewsletterIssueFreshnessRow, ...]) -> dict[str, int]:
    return {
        "issue_count": len(rows),
        "missing_source_count": sum(row.missing_source_count for row in rows),
        "missing_source_timestamp_count": sum(
            row.missing_source_timestamp_count for row in rows
        ),
        "section_count": sum(row.section_count for row in rows),
        "stale_section_count": sum(row.stale_section_count for row in rows),
    }


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "newsletter_sends": {"id", "source_content_ids"},
        "generated_content": {"id"},
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _date_bound(value: str | None, *, end_of_day: bool) -> str | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if len(text) == 10:
        parsed = datetime.combine(
            datetime.strptime(text, "%Y-%m-%d").date(),
            time.max if end_of_day else time.min,
            tzinfo=timezone.utc,
        )
        return parsed.isoformat()
    parsed = _parse_datetime(text)
    if parsed is None:
        raise ValueError(f"invalid date: {value}")
    return parsed.isoformat()


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
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _ensure_utc(parsed)


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


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str,
) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
