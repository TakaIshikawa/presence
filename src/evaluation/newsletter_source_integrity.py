"""Audit newsletter source_content_ids references for integrity issues."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30


@dataclass(frozen=True)
class NewsletterSourceIntegrityIssue:
    """One integrity issue found on a newsletter send source reference."""

    issue_type: str
    newsletter_send_id: int
    issue_id: str
    message: str
    source_content_id: int | None = None
    position: int | None = None
    raw_value: Any | None = None
    sent_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterSourceIntegrityReport:
    """Read-only audit result for recent newsletter source references."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    issues: tuple[NewsletterSourceIntegrityIssue, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    @property
    def has_issues(self) -> bool:
        return bool(self.issues)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_source_integrity",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "issue_count": len(self.issues),
            "issues": [issue.to_dict() for issue in self.issues],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_newsletter_source_integrity_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    now: datetime | None = None,
) -> NewsletterSourceIntegrityReport:
    """Return integrity issues for recent newsletter_sends.source_content_ids."""
    if days <= 0:
        raise ValueError("days must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "cutoff": cutoff.isoformat()}
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    sends = _load_sends(conn, schema, cutoff=cutoff)
    issues: list[NewsletterSourceIntegrityIssue] = []
    referenced_ids: set[int] = set()
    parsed_by_send: dict[int, list[int]] = {}

    for send in sends:
        send_issues, source_ids = _parse_send_source_ids(send)
        issues.extend(send_issues)
        parsed_by_send[int(send["newsletter_send_id"])] = source_ids
        referenced_ids.update(source_ids)

    content_by_id = _load_generated_content(conn, schema, referenced_ids)
    for send in sends:
        send_id = int(send["newsletter_send_id"])
        sent_at = _parse_datetime(send.get("sent_at"))
        for content_id in parsed_by_send.get(send_id, []):
            content = content_by_id.get(content_id)
            if content is None:
                issues.append(
                    _issue(
                        "missing_content_reference",
                        send,
                        f"source_content_id {content_id} does not exist in generated_content",
                        source_content_id=content_id,
                    )
                )
                continue

            published = _int_or_none(content.get("published"))
            published_at = _parse_datetime(content.get("published_at"))
            if published == -1:
                issues.append(
                    _issue(
                        "abandoned_content_reference",
                        send,
                        f"source_content_id {content_id} was abandoned",
                        source_content_id=content_id,
                    )
                )
            elif published == 0 or published is None:
                issues.append(
                    _issue(
                        "unpublished_content_reference",
                        send,
                        f"source_content_id {content_id} was unpublished at send time",
                        source_content_id=content_id,
                    )
                )
            elif sent_at is not None and published_at is not None and published_at > sent_at:
                issues.append(
                    _issue(
                        "not_yet_published_content_reference",
                        send,
                        f"source_content_id {content_id} was published after the newsletter send",
                        source_content_id=content_id,
                    )
                )

    issues = sorted(issues, key=_issue_sort_key)
    counts = Counter(issue.issue_type for issue in issues)
    return NewsletterSourceIntegrityReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "send_count": len(sends),
            "referenced_source_count": sum(len(ids) for ids in parsed_by_send.values()),
            "unique_referenced_source_count": len(referenced_ids),
            "issue_count": len(issues),
            "by_issue_type": dict(sorted(counts.items())),
        },
        issues=tuple(issues),
        missing_tables=(),
        missing_columns={},
    )


def format_newsletter_source_integrity_json(
    report: NewsletterSourceIntegrityReport,
) -> str:
    """Serialize the integrity report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_source_integrity_text(
    report: NewsletterSourceIntegrityReport,
) -> str:
    """Render the integrity report for command-line review."""
    lines = [
        "Newsletter Source Integrity",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['days']} days",
        (
            "Totals: "
            f"sends={report.totals['send_count']} "
            f"sources={report.totals['referenced_source_count']} "
            f"issues={report.totals['issue_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    missing = [
        f"{table}({', '.join(columns)})"
        for table, columns in report.missing_columns.items()
        if columns
    ]
    if missing:
        lines.append(f"Missing columns: {'; '.join(missing)}")
    lines.append("")

    if not report.issues:
        lines.append("No newsletter source integrity issues found.")
        return "\n".join(lines)

    lines.append("Issues:")
    for issue in report.issues:
        source = "-" if issue.source_content_id is None else str(issue.source_content_id)
        position = "-" if issue.position is None else str(issue.position)
        lines.append(
            f"  - {issue.issue_type} send={issue.newsletter_send_id} "
            f"issue={issue.issue_id or '-'} source={source} pos={position}: "
            f"{issue.message}"
        )
    return "\n".join(lines)


def _parse_send_source_ids(
    send: dict[str, Any],
) -> tuple[list[NewsletterSourceIntegrityIssue], list[int]]:
    raw_value = send.get("source_content_ids")
    if raw_value in (None, ""):
        return [
            _issue(
                "missing_source_content_ids",
                send,
                "source_content_ids is missing",
                raw_value=raw_value,
            )
        ], []

    try:
        parsed = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (TypeError, json.JSONDecodeError) as exc:
        return [
            _issue(
                "malformed_source_content_ids",
                send,
                f"source_content_ids is not valid JSON: {exc}",
                raw_value=raw_value,
            )
        ], []

    if not isinstance(parsed, list):
        return [
            _issue(
                "malformed_source_content_ids",
                send,
                f"source_content_ids must be a JSON array, got {type(parsed).__name__}",
                raw_value=raw_value,
            )
        ], []

    issues: list[NewsletterSourceIntegrityIssue] = []
    source_ids: list[int] = []
    seen: set[int] = set()
    duplicates: set[int] = set()
    for position, item in enumerate(parsed):
        if not _is_positive_int(item):
            issues.append(
                _issue(
                    "non_integer_source_content_id",
                    send,
                    "source_content_ids must contain only positive integers",
                    position=position,
                    raw_value=item,
                )
            )
            continue
        content_id = int(item)
        source_ids.append(content_id)
        if content_id in seen and content_id not in duplicates:
            duplicates.add(content_id)
            issues.append(
                _issue(
                    "duplicate_source_content_id",
                    send,
                    f"source_content_id {content_id} appears more than once in this send",
                    source_content_id=content_id,
                    position=position,
                )
            )
        seen.add(content_id)
    if not parsed:
        issues.append(
            _issue(
                "missing_source_content_ids",
                send,
                "source_content_ids is empty",
                raw_value=raw_value,
            )
        )
    return issues, source_ids


def _load_sends(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    columns = schema["newsletter_sends"]
    filters = []
    params: list[Any] = []
    if "sent_at" in columns:
        filters.append("ns.sent_at >= ?")
        params.append(cutoff.isoformat())
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   ns.id AS newsletter_send_id,
                   {_column_expr(columns, "issue_id", "''", alias="ns")} AS issue_id,
                   {_column_expr(columns, "sent_at", "NULL", alias="ns")} AS sent_at,
                   ns.source_content_ids AS source_content_ids
               FROM newsletter_sends ns
               {where_clause}
               ORDER BY {_column_expr(columns, "sent_at", "NULL", alias="ns")} DESC,
                        ns.id DESC""",
            params,
        ).fetchall()
    ]


def _load_generated_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: set[int],
) -> dict[int, dict[str, Any]]:
    columns = schema["generated_content"]
    ids = sorted(content_ids)
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"""SELECT
               gc.id AS id,
               {_column_expr(columns, "published", "0", alias="gc")} AS published,
               {_column_expr(columns, "published_at", "NULL", alias="gc")} AS published_at
           FROM generated_content gc
           WHERE gc.id IN ({placeholders})""",
        ids,
    ).fetchall()
    return {int(row["id"]): dict(row) for row in rows}


def _issue(
    issue_type: str,
    send: dict[str, Any],
    message: str,
    *,
    source_content_id: int | None = None,
    position: int | None = None,
    raw_value: Any | None = None,
) -> NewsletterSourceIntegrityIssue:
    return NewsletterSourceIntegrityIssue(
        issue_type=issue_type,
        newsletter_send_id=int(send["newsletter_send_id"]),
        issue_id=str(send.get("issue_id") or ""),
        message=message,
        source_content_id=source_content_id,
        position=position,
        raw_value=raw_value,
        sent_at=send.get("sent_at"),
    )


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
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> NewsletterSourceIntegrityReport:
    return NewsletterSourceIntegrityReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "send_count": 0,
            "referenced_source_count": 0,
            "unique_referenced_source_count": 0,
            "issue_count": 0,
            "by_issue_type": {},
        },
        issues=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


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


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _issue_sort_key(issue: NewsletterSourceIntegrityIssue) -> tuple[Any, ...]:
    return (
        issue.newsletter_send_id,
        issue.issue_type,
        issue.source_content_id or 0,
        -1 if issue.position is None else issue.position,
        str(issue.raw_value),
    )
