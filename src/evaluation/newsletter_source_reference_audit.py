"""Audit newsletter source references for broken generated_content links."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 50
ISSUE_CODES = (
    "malformed_json",
    "missing_content",
    "duplicate_source_id",
    "unpublished_source",
)


@dataclass(frozen=True)
class NewsletterSourceReferenceIssue:
    """One affected newsletter send and the source ids that need attention."""

    newsletter_send_id: int
    issue_id: str
    subject: str
    sent_at: str | None
    issue_codes: tuple[str, ...]
    affected_content_ids: tuple[int, ...]
    missing_content_ids: tuple[int, ...] = ()
    duplicate_source_ids: tuple[int, ...] = ()
    unpublished_source_ids: tuple[int, ...] = ()
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        for key in (
            "issue_codes",
            "affected_content_ids",
            "missing_content_ids",
            "duplicate_source_ids",
            "unpublished_source_ids",
        ):
            result[key] = list(result[key])
        return result


@dataclass(frozen=True)
class NewsletterSourceReferenceAuditReport:
    """Read-only audit result for newsletter source reference health."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    issues: tuple[NewsletterSourceReferenceIssue, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    @property
    def has_issues(self) -> bool:
        return bool(self.issues)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_source_reference_audit",
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
            "totals": dict(self.totals),
        }


def build_newsletter_source_reference_audit_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> NewsletterSourceReferenceAuditReport:
    """Return broken newsletter_sends.source_content_ids references."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit < 0:
        raise ValueError("limit must be non-negative")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "limit": limit,
        "source": "database",
    }
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    sends = _load_sends(conn, schema, cutoff=cutoff, limit=limit)
    content_by_id = _load_generated_content(conn, schema, _referenced_ids(sends))
    return _build_report_from_rows(
        sends,
        content_by_id,
        generated_at=generated_at,
        filters=filters,
        missing_tables=(),
        missing_columns={},
    )


def build_newsletter_source_reference_audit_report_from_fixture(
    fixture_path: str | Path,
    *,
    days: int = DEFAULT_DAYS,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> NewsletterSourceReferenceAuditReport:
    """Build the audit from fixture JSON containing sends and generated content."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit < 0:
        raise ValueError("limit must be non-negative")

    payload = json.loads(Path(fixture_path).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("fixture must be an object")
    sends = payload.get("newsletter_sends", payload.get("sends", []))
    generated_content = payload.get("generated_content", [])
    if not isinstance(sends, list) or not isinstance(generated_content, list):
        raise ValueError("fixture sends and generated_content must be lists")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    rows = [
        _fixture_send_row(row)
        for row in sends
        if isinstance(row, Mapping) and _within_window(row.get("sent_at"), cutoff)
    ]
    rows.sort(key=lambda row: (row.get("sent_at") or "", int(row["newsletter_send_id"])), reverse=True)
    if limit is not None:
        rows = rows[:limit]
    content_by_id = {
        int(row["id"]): dict(row)
        for row in generated_content
        if isinstance(row, Mapping) and _int_or_none(row.get("id")) is not None
    }
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "limit": limit,
        "source": "fixture",
    }
    return _build_report_from_rows(
        rows,
        content_by_id,
        generated_at=generated_at,
        filters=filters,
        missing_tables=(),
        missing_columns={},
    )


def format_newsletter_source_reference_audit_json(
    report: NewsletterSourceReferenceAuditReport,
) -> str:
    """Serialize the audit report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_source_reference_audit_text(
    report: NewsletterSourceReferenceAuditReport,
) -> str:
    """Render the audit report for command-line review."""
    totals = report.totals
    lines = [
        "Newsletter Source Reference Audit",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['days']} days "
            f"limit={report.filters['limit'] if report.filters['limit'] is not None else 'all'}"
        ),
        (
            "Totals: "
            f"sends={totals['send_count']} "
            f"affected={totals['affected_send_count']} "
            f"issues={totals['issue_count']}"
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

    if not report.issues:
        lines.append("No newsletter source reference issues found.")
        return "\n".join(lines)

    lines.append("Issues:")
    for issue in report.issues:
        label = issue.issue_id or issue.subject or "-"
        affected = (
            ",".join(str(content_id) for content_id in issue.affected_content_ids)
            if issue.affected_content_ids
            else "-"
        )
        lines.append(
            f"  - send={issue.newsletter_send_id} issue={label} "
            f"codes={','.join(issue.issue_codes)} affected={affected}"
        )
    return "\n".join(lines)


def _build_report_from_rows(
    sends: list[dict[str, Any]],
    content_by_id: dict[int, dict[str, Any]],
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> NewsletterSourceReferenceAuditReport:
    issues = [
        issue
        for send in sends
        if (issue := _audit_send(send, content_by_id)) is not None
    ]
    issues.sort(key=lambda item: (item.newsletter_send_id, item.issue_codes))
    code_counts: Counter[str] = Counter()
    for issue in issues:
        code_counts.update(issue.issue_codes)
    return NewsletterSourceReferenceAuditReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "send_count": len(sends),
            "affected_send_count": len(issues),
            "issue_count": sum(code_counts.values()),
            "by_issue_code": {code: code_counts.get(code, 0) for code in ISSUE_CODES},
        },
        issues=tuple(issues),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _audit_send(
    send: dict[str, Any],
    content_by_id: dict[int, dict[str, Any]],
) -> NewsletterSourceReferenceIssue | None:
    issue_codes: set[str] = set()
    source_ids, malformed = _parse_source_ids(send.get("source_content_ids"))
    if malformed:
        issue_codes.add("malformed_json")

    duplicates = tuple(sorted(_duplicate_ids(source_ids)))
    if duplicates:
        issue_codes.add("duplicate_source_id")

    unique_ids = sorted(set(source_ids))
    missing = tuple(content_id for content_id in unique_ids if content_id not in content_by_id)
    if missing:
        issue_codes.add("missing_content")

    unpublished = tuple(
        content_id
        for content_id in unique_ids
        if content_id in content_by_id and not _has_publication_url(content_by_id[content_id])
    )
    if unpublished:
        issue_codes.add("unpublished_source")

    if not issue_codes:
        return None

    affected = tuple(sorted(set(missing) | set(duplicates) | set(unpublished)))
    codes = tuple(code for code in ISSUE_CODES if code in issue_codes)
    return NewsletterSourceReferenceIssue(
        newsletter_send_id=int(send["newsletter_send_id"]),
        issue_id=str(send.get("issue_id") or ""),
        subject=str(send.get("subject") or ""),
        sent_at=send.get("sent_at"),
        issue_codes=codes,
        affected_content_ids=affected,
        missing_content_ids=missing,
        duplicate_source_ids=duplicates,
        unpublished_source_ids=unpublished,
        message=_message(codes),
    )


def _parse_source_ids(raw_value: Any) -> tuple[list[int], bool]:
    if raw_value in (None, ""):
        return [], True
    try:
        parsed = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (TypeError, json.JSONDecodeError):
        return [], True
    if not isinstance(parsed, list):
        return [], True

    malformed = False
    source_ids: list[int] = []
    for item in parsed:
        if isinstance(item, bool):
            malformed = True
            continue
        try:
            content_id = int(item)
        except (TypeError, ValueError):
            malformed = True
            continue
        if content_id <= 0:
            malformed = True
            continue
        source_ids.append(content_id)
    return source_ids, malformed


def _load_sends(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    limit: int | None,
) -> list[dict[str, Any]]:
    columns = schema["newsletter_sends"]
    filters = []
    params: list[Any] = []
    if "sent_at" in columns:
        filters.append("ns.sent_at >= ?")
        params.append(cutoff.isoformat())
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT ?"
        params.append(limit)
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = conn.execute(
        f"""SELECT
               ns.id AS newsletter_send_id,
               {_column_expr(columns, "issue_id", "''", alias="ns")} AS issue_id,
               {_column_expr(columns, "subject", "''", alias="ns")} AS subject,
               {_column_expr(columns, "sent_at", "NULL", alias="ns")} AS sent_at,
               ns.source_content_ids AS source_content_ids
           FROM newsletter_sends ns
           {where_clause}
           ORDER BY {_column_expr(columns, "sent_at", "NULL", alias="ns")} DESC,
                    ns.id DESC
           {limit_sql}""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _load_generated_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: set[int],
) -> dict[int, dict[str, Any]]:
    if not content_ids:
        return {}
    columns = schema["generated_content"]
    ids = sorted(content_ids)
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"""SELECT
               gc.id AS id,
               {_column_expr(columns, "published_url", "NULL", alias="gc")} AS published_url
           FROM generated_content gc
           WHERE gc.id IN ({placeholders})""",
        ids,
    ).fetchall()
    return {int(row["id"]): dict(row) for row in rows}


def _referenced_ids(sends: list[dict[str, Any]]) -> set[int]:
    ids: set[int] = set()
    for send in sends:
        parsed, _malformed = _parse_source_ids(send.get("source_content_ids"))
        ids.update(parsed)
    return ids


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "newsletter_sends": {"id", "source_content_ids"},
        "generated_content": {"id", "published_url"},
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
) -> NewsletterSourceReferenceAuditReport:
    return NewsletterSourceReferenceAuditReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "send_count": 0,
            "affected_send_count": 0,
            "issue_count": 0,
            "by_issue_code": {code: 0 for code in ISSUE_CODES},
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


def _fixture_send_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "newsletter_send_id": int(row.get("newsletter_send_id") or row.get("id")),
        "issue_id": row.get("issue_id") or "",
        "subject": row.get("subject") or "",
        "sent_at": row.get("sent_at"),
        "source_content_ids": row.get("source_content_ids"),
    }


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str,
) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _within_window(value: Any, cutoff: datetime) -> bool:
    parsed = _parse_datetime(value)
    return parsed is None or parsed >= cutoff


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    text = str(value).strip()
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


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _duplicate_ids(values: list[int]) -> set[int]:
    seen: set[int] = set()
    duplicates: set[int] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return duplicates


def _has_publication_url(row: Mapping[str, Any]) -> bool:
    return bool(str(row.get("published_url") or "").strip())


def _message(codes: tuple[str, ...]) -> str:
    labels = {
        "malformed_json": "source_content_ids is missing or malformed",
        "missing_content": "one or more source ids do not resolve to generated_content",
        "duplicate_source_id": "one or more source ids are duplicated in the send",
        "unpublished_source": "one or more source ids have no publication URL",
    }
    return "; ".join(labels[code] for code in codes)
