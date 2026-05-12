"""Forecast failed publications approaching retry budget exhaustion."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any

from output.publish_errors import classify_publish_error, normalize_error_category
from storage.db import MAX_RETRIES


DEFAULT_LIMIT = 100


@dataclass(frozen=True)
class PublicationRetryBudgetRow:
    content_id: int
    publication_id: int
    platform: str
    status: str
    attempt_count: int
    remaining_attempts: int
    budget_status: str
    error_category: str
    normalized_error_category: str
    last_error_at: str | None
    next_retry_at: str | None
    due_status: str
    error: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PublicationRetryBudgetReport:
    generated_at: str
    max_retries: int
    rows: tuple[PublicationRetryBudgetRow, ...]
    summary_by_platform: dict[str, str]
    totals: dict[str, int]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "publication_retry_budget",
            "generated_at": self.generated_at,
            "max_retries": self.max_retries,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "summary_by_platform": dict(sorted(self.summary_by_platform.items())),
            "totals": dict(sorted(self.totals.items())),
        }


def build_publication_retry_budget_report(
    db_or_conn: Any,
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> PublicationRetryBudgetReport:
    """Return failed content_publications ranked by retry budget pressure."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = {}
    if "content_publications" not in schema:
        missing_tables = ("content_publications",)
        rows: tuple[PublicationRetryBudgetRow, ...] = ()
    else:
        required = ("id", "content_id", "platform", "status", "attempt_count")
        optional = ("error", "error_category", "last_error_at", "next_retry_at")
        columns = schema["content_publications"]
        missing_required = tuple(column for column in required if column not in columns)
        missing_optional = tuple(column for column in optional if column not in columns)
        if missing_required:
            missing_columns["content_publications"] = missing_required + missing_optional
            rows = ()
        else:
            if missing_optional:
                missing_columns["content_publications"] = missing_optional
            rows = tuple(
                sorted(
                    (
                        _row_from_record(record, columns=columns, now=generated_at)
                        for record in _failed_records(conn, columns)
                    ),
                    key=_sort_key,
                )[:limit]
            )

    return PublicationRetryBudgetReport(
        generated_at=generated_at.isoformat(),
        max_retries=MAX_RETRIES,
        rows=rows,
        summary_by_platform=_summary_by_platform(rows),
        totals={
            "row_count": len(rows),
            "exhausted_count": sum(row.budget_status == "exhausted" for row in rows),
            "not_due_count": sum(row.due_status == "not_due" for row in rows),
        },
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_publication_retry_budget_json(report: PublicationRetryBudgetReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publication_retry_budget_text(report: PublicationRetryBudgetReport) -> str:
    lines = [
        "Publication Retry Budget Forecast",
        f"Generated: {report.generated_at}",
        f"Max retries: {report.max_retries}",
        (
            "Totals: "
            f"rows={report.totals['row_count']} "
            f"exhausted={report.totals['exhausted_count']} "
            f"not_due={report.totals['not_due_count']}"
        ),
    ]
    if report.summary_by_platform:
        lines.append("")
        lines.append("Summary by platform:")
        for platform, summary in sorted(report.summary_by_platform.items()):
            lines.append(f"- {platform}: {summary}")
    if report.rows:
        lines.append("")
        for row in report.rows:
            lines.append(
                f"- content={row.content_id} platform={row.platform} "
                f"remaining={row.remaining_attempts} budget={row.budget_status} "
                f"due={row.due_status} category={row.normalized_error_category} "
                f"next_retry_at={row.next_retry_at or '-'}"
            )
    else:
        lines.append("")
        lines.append("No failed publications found.")
    return "\n".join(lines)


def _failed_records(conn: sqlite3.Connection, columns: set[str]) -> list[sqlite3.Row]:
    select_columns = [
        "id",
        "content_id",
        "platform",
        "status",
        "attempt_count",
        _column_expr(columns, "error"),
        _column_expr(columns, "error_category"),
        _column_expr(columns, "last_error_at"),
        _column_expr(columns, "next_retry_at"),
    ]
    return conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM content_publications
            WHERE lower(status) = 'failed'
            ORDER BY id ASC"""
    ).fetchall()


def _row_from_record(
    record: sqlite3.Row,
    *,
    columns: set[str],
    now: datetime,
) -> PublicationRetryBudgetRow:
    attempt_count = max(0, _int(record["attempt_count"]))
    remaining_attempts = max(MAX_RETRIES - attempt_count, 0)
    raw_category = _clean(record["error_category"])
    normalized = normalize_error_category(raw_category)
    if normalized == "unknown":
        normalized = classify_publish_error(record["error"], platform=str(record["platform"]))
    next_retry_at = _clean(record["next_retry_at"])
    next_retry_dt = _parse_dt(next_retry_at)
    return PublicationRetryBudgetRow(
        content_id=int(record["content_id"]),
        publication_id=int(record["id"]),
        platform=str(record["platform"]),
        status=str(record["status"]),
        attempt_count=attempt_count,
        remaining_attempts=remaining_attempts,
        budget_status="exhausted" if attempt_count >= MAX_RETRIES else "available",
        error_category=raw_category or "unknown",
        normalized_error_category=normalized,
        last_error_at=_clean(record["last_error_at"]),
        next_retry_at=next_retry_at,
        due_status="not_due" if next_retry_dt and next_retry_dt > now else "due",
        error=_clean(record["error"]),
    )


def _summary_by_platform(rows: tuple[PublicationRetryBudgetRow, ...]) -> dict[str, str]:
    platforms = sorted({row.platform for row in rows})
    summaries: dict[str, str] = {}
    for platform in platforms:
        platform_rows = [row for row in rows if row.platform == platform]
        exhausted = sum(row.budget_status == "exhausted" for row in platform_rows)
        not_due = sum(row.due_status == "not_due" for row in platform_rows)
        by_category: dict[str, int] = {}
        for row in platform_rows:
            by_category[row.normalized_error_category] = by_category.get(row.normalized_error_category, 0) + 1
        category_text = ", ".join(
            f"{category}={count}" for category, count in sorted(by_category.items())
        )
        summaries[platform] = (
            f"{len(platform_rows)} failed, {exhausted} exhausted, "
            f"{not_due} not due; {category_text}"
        )
    return summaries


def _sort_key(row: PublicationRetryBudgetRow) -> tuple[int, str, int]:
    return (row.remaining_attempts, row.last_error_at or "", row.publication_id)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[0]): {
            str(info[1]) for info in conn.execute(f"PRAGMA table_info({row['name'] if isinstance(row, sqlite3.Row) else row[0]})")
        }
        for row in rows
    }


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    return column if column in columns else f"{default} AS {column}"


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
