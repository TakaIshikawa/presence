"""Audit pending reply drafts for untracked follow-up promises."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 7
DEFAULT_LIMIT = 100
DEFAULT_STATUS = "pending"
SEVERITY = "high"

AUDIT_COLUMNS = (
    "id",
    "status",
    "platform",
    "inbound_tweet_id",
    "inbound_author_handle",
    "inbound_author_id",
    "inbound_url",
    "draft_text",
    "platform_metadata",
    "relationship_context",
    "detected_at",
)
METADATA_COLUMNS = (
    "platform_metadata",
    "relationship_context",
    "metadata",
    "draft_metadata",
    "followup_metadata",
)
DIRECT_DUE_COLUMNS = (
    "due_at",
    "followup_at",
    "follow_up_at",
    "followup_due_at",
    "follow_up_due_at",
)
DIRECT_ACTION_COLUMNS = (
    "action",
    "action_ref",
    "action_reference",
    "followup_action",
    "follow_up_action",
    "next_action",
)
PROMISE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("will follow up", re.compile(r"\b(?:i\s+)?will\s+follow\s+up\b", re.IGNORECASE)),
    ("circle back", re.compile(r"\bcircle\s+back\b", re.IGNORECASE)),
    ("tomorrow", re.compile(r"\btomorrow\b", re.IGNORECASE)),
    ("next week", re.compile(r"\bnext\s+week\b", re.IGNORECASE)),
    ("I'll send", re.compile(r"\bi(?:'|’)?ll\s+send\b|\bi\s+will\s+send\b", re.IGNORECASE)),
)


@dataclass(frozen=True)
class ReplyFollowupPromiseFinding:
    """One pending draft that promises a follow-up without tracking metadata."""

    reply_queue_id: int | None
    platform: str
    inbound_id: str | None
    inbound_url: str | None
    inbound_author_handle: str | None
    inbound_author_id: str | None
    detected_promise_phrases: tuple[str, ...]
    due_metadata_status: str
    severity: str
    status: str | None = None
    draft_preview: str = ""
    detected_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["detected_promise_phrases"] = list(self.detected_promise_phrases)
        return payload


@dataclass(frozen=True)
class ReplyFollowupPromiseAuditReport:
    """Aggregated audit report for follow-up promise tracking gaps."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    audited_count: int
    promised_count: int
    finding_count: int
    by_platform: dict[str, int]
    by_severity: dict[str, int]
    findings: tuple[ReplyFollowupPromiseFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return self.finding_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_followup_promise_audit",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "audited_count": self.audited_count,
            "promised_count": self.promised_count,
            "finding_count": self.finding_count,
            "blocking_issue_count": self.blocking_issue_count,
            "by_platform": dict(sorted(self.by_platform.items())),
            "by_severity": dict(sorted(self.by_severity.items())),
            "findings": [finding.to_dict() for finding in self.findings],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_reply_followup_promise_audit(
    source: Any | None = None,
    *,
    reply_records: Iterable[Mapping[str, Any]] | None = None,
    followup_records: Iterable[Mapping[str, Any]] | None = None,
    days: int = DEFAULT_DAYS,
    platform: str | Sequence[str] | None = None,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ReplyFollowupPromiseAuditReport:
    """Find pending reply drafts that promise future follow-up without tracking."""

    if reply_records is not None and source is not None:
        raise ValueError("provide either source or reply_records, not both")
    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    platforms = _normalise_platforms(platform)
    filters = {
        "days": days,
        "status": DEFAULT_STATUS,
        "platform": list(platforms),
        "limit": limit,
    }

    if reply_records is not None or _is_records(source):
        records = reply_records if reply_records is not None else source
        rows = [dict(row) for row in records or ()]
        rows = _filter_fixture_rows(rows, days=days, platforms=platforms, limit=limit, now=generated_at)
        followups = [dict(row) for row in followup_records or ()]
        return _report_from_rows(rows, followups, generated_at=generated_at, filters=filters)

    if source is None:
        raise ValueError("source or reply_records is required")

    conn = _connection(source)
    schema = _schema(conn)
    if "reply_queue" not in schema:
        return _empty_report(generated_at, filters, missing_tables=("reply_queue",))

    reply_columns = schema["reply_queue"]
    if "draft_text" not in reply_columns:
        return _empty_report(
            generated_at,
            filters,
            missing_columns={"reply_queue": ("draft_text",)},
        )

    rows = _reply_rows(
        conn,
        reply_columns,
        days=days,
        platforms=platforms,
        limit=limit,
        now=generated_at,
    )
    followups = _followup_rows(conn, schema)
    return _report_from_rows(rows, followups, generated_at=generated_at, filters=filters)


def detected_promise_phrases(value: Any) -> tuple[str, ...]:
    """Return stable promise phrase labels detected in draft text."""

    text = _clean_text(value)
    if not text:
        return ()
    return tuple(label for label, pattern in PROMISE_PATTERNS if pattern.search(text))


def inspect_reply_followup_promise_row(
    row: Mapping[str, Any],
    *,
    followup_records: Iterable[Mapping[str, Any]] = (),
) -> ReplyFollowupPromiseFinding | None:
    """Inspect one reply_queue-like row for an untracked follow-up promise."""

    phrases = detected_promise_phrases(row.get("draft_text"))
    if not phrases:
        return None
    status = due_metadata_status(row, followup_records=followup_records)
    if status != "missing":
        return None
    return _finding(row, phrases, status)


def due_metadata_status(
    row: Mapping[str, Any],
    *,
    followup_records: Iterable[Mapping[str, Any]] = (),
) -> str:
    """Return whether a promised follow-up has concrete tracking metadata."""

    metadata_status = _row_due_metadata_status(row)
    if metadata_status != "missing":
        return metadata_status
    reply_id = _int_or_none(row.get("id") or row.get("reply_queue_id"))
    if reply_id is not None:
        for followup in followup_records:
            if _followup_matches_reply(followup, reply_id) and _followup_has_tracking(followup):
                return "followup_record"
    return "missing"


def format_reply_followup_promise_audit_json(report: ReplyFollowupPromiseAuditReport) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_followup_promise_audit_text(report: ReplyFollowupPromiseAuditReport) -> str:
    """Render a compact human-readable promise audit."""

    filters = report.filters
    lines = [
        "Reply Follow-up Promise Audit",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"status={filters.get('status')} days={filters.get('days')} "
            f"platform={_display_platform_filter(filters.get('platform'))} "
            f"limit={filters.get('limit')}"
        ),
        (
            f"Audited: {report.audited_count} "
            f"promised={report.promised_count} findings={report.finding_count}"
        ),
    ]
    if report.by_platform:
        lines.append(
            "By platform: "
            + ", ".join(f"{key}={value}" for key, value in sorted(report.by_platform.items()))
        )
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in sorted(report.missing_columns.items())
                for column in columns
            )
        )
    if not report.findings:
        lines.append("No untracked follow-up promises found.")
        return "\n".join(lines)

    lines.append("")
    for finding in report.findings:
        handle = finding.inbound_author_handle or finding.inbound_author_id or "unknown"
        phrases = ", ".join(finding.detected_promise_phrases)
        lines.append(
            f"#{finding.reply_queue_id or '-'} {finding.severity} "
            f"platform={finding.platform or 'unknown'} author={handle} "
            f"due_metadata={finding.due_metadata_status} phrases={phrases}"
        )
        if finding.draft_preview:
            lines.append(f"  draft={finding.draft_preview!r}")
    return "\n".join(lines)


def _report_from_rows(
    rows: list[dict[str, Any]],
    followup_records: list[dict[str, Any]],
    *,
    generated_at: datetime,
    filters: dict[str, Any],
) -> ReplyFollowupPromiseAuditReport:
    promised_count = 0
    findings: list[ReplyFollowupPromiseFinding] = []
    for row in rows:
        phrases = detected_promise_phrases(row.get("draft_text"))
        if not phrases:
            continue
        promised_count += 1
        status = due_metadata_status(row, followup_records=followup_records)
        if status == "missing":
            findings.append(_finding(row, phrases, status))

    findings = sorted(findings, key=_finding_sort_key)
    return ReplyFollowupPromiseAuditReport(
        ok=not findings,
        generated_at=generated_at.isoformat(),
        filters=filters,
        audited_count=len(rows),
        promised_count=promised_count,
        finding_count=len(findings),
        by_platform=dict(Counter(finding.platform for finding in findings)),
        by_severity=dict(Counter(finding.severity for finding in findings)),
        findings=tuple(findings),
    )


def _finding(
    row: Mapping[str, Any],
    phrases: tuple[str, ...],
    status: str,
) -> ReplyFollowupPromiseFinding:
    return ReplyFollowupPromiseFinding(
        reply_queue_id=_int_or_none(row.get("id") or row.get("reply_queue_id")),
        platform=str(row.get("platform") or "x").strip().lower() or "x",
        inbound_id=_str_or_none(row.get("inbound_tweet_id") or row.get("inbound_id")),
        inbound_url=_str_or_none(row.get("inbound_url")),
        inbound_author_handle=_str_or_none(row.get("inbound_author_handle") or row.get("author_handle")),
        inbound_author_id=_str_or_none(row.get("inbound_author_id") or row.get("author_id")),
        detected_promise_phrases=phrases,
        due_metadata_status=status,
        severity=SEVERITY,
        status=_str_or_none(row.get("status")),
        draft_preview=_shorten(_clean_text(row.get("draft_text")), 120),
        detected_at=_str_or_none(row.get("detected_at")),
    )


def _row_due_metadata_status(row: Mapping[str, Any]) -> str:
    for column in DIRECT_DUE_COLUMNS:
        if _parse_timestamp(row.get(column)) is not None:
            return "due_at"
    for column in DIRECT_ACTION_COLUMNS:
        if _str_or_none(row.get(column)):
            return "action_reference"

    for column in METADATA_COLUMNS:
        metadata = _parse_json_object(row.get(column))
        if not metadata:
            continue
        status = _metadata_due_status(metadata)
        if status != "missing":
            return status
    return "missing"


def _metadata_due_status(value: Any) -> str:
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key).casefold()
            if key_text in {column.casefold() for column in DIRECT_DUE_COLUMNS}:
                if _parse_timestamp(item) is not None:
                    return "due_at"
            if key_text in {column.casefold() for column in DIRECT_ACTION_COLUMNS}:
                if _str_or_none(item):
                    return "action_reference"
            if key_text in {"followup", "follow_up", "reminder", "next_step"}:
                nested = _metadata_due_status(item)
                if nested != "missing":
                    return nested
            if isinstance(item, (Mapping, list, tuple)):
                nested = _metadata_due_status(item)
                if nested != "missing":
                    return nested
    elif isinstance(value, (list, tuple)):
        for item in value:
            nested = _metadata_due_status(item)
            if nested != "missing":
                return nested
    return "missing"


def _followup_matches_reply(row: Mapping[str, Any], reply_id: int) -> bool:
    source_reply_id = _int_or_none(row.get("source_reply_id"))
    if source_reply_id == reply_id:
        return True
    return (
        str(row.get("source_type") or "").strip().lower() == "reply_queue"
        and _int_or_none(row.get("source_id")) == reply_id
    )


def _followup_has_tracking(row: Mapping[str, Any]) -> bool:
    if _parse_timestamp(row.get("due_at") or row.get("followup_at")) is not None:
        return True
    return any(_str_or_none(row.get(column)) for column in DIRECT_ACTION_COLUMNS + ("reason",))


def _filter_fixture_rows(
    rows: list[dict[str, Any]],
    *,
    days: int,
    platforms: tuple[str, ...],
    limit: int | None,
    now: datetime,
) -> list[dict[str, Any]]:
    cutoff = now - timedelta(days=days)
    filtered = []
    for row in rows:
        status = str(row.get("status") or DEFAULT_STATUS).strip().lower()
        if status != DEFAULT_STATUS:
            continue
        platform = str(row.get("platform") or "x").strip().lower()
        if platforms and platform not in platforms:
            continue
        detected_at = _parse_timestamp(row.get("detected_at"))
        if detected_at is not None and detected_at < cutoff:
            continue
        filtered.append(row)
    filtered.sort(key=lambda row: (str(row.get("detected_at") or ""), _int_or_none(row.get("id")) or 0), reverse=True)
    return filtered[:limit] if limit is not None else filtered


def _reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    days: int,
    platforms: tuple[str, ...],
    limit: int | None,
    now: datetime,
) -> list[dict[str, Any]]:
    where = []
    params: list[Any] = []
    if "status" in columns:
        where.append("COALESCE(status, ?) = ?")
        params.extend([DEFAULT_STATUS, DEFAULT_STATUS])
    if "detected_at" in columns:
        cutoff = now - timedelta(days=days)
        where.append("(detected_at IS NULL OR datetime(detected_at) >= datetime(?))")
        params.append(cutoff.isoformat())
    if platforms and "platform" in columns:
        placeholders = ",".join("?" for _ in platforms)
        where.append(f"LOWER(COALESCE(platform, 'x')) IN ({placeholders})")
        params.extend(platforms)

    select_columns = [column for column in AUDIT_COLUMNS if column in columns]
    for column in DIRECT_DUE_COLUMNS + DIRECT_ACTION_COLUMNS + ("metadata", "draft_metadata", "followup_metadata"):
        if column in columns and column not in select_columns:
            select_columns.append(column)
    query = f"SELECT {', '.join(_quote_identifier(column) for column in select_columns)} FROM reply_queue"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY " + _order_clause(columns)
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _followup_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema.get("reply_followup_reminders")
    if not columns:
        return []
    wanted = [
        column
        for column in (
            "id",
            "target_handle",
            "source_type",
            "source_id",
            "source_reply_id",
            "due_at",
            "followup_at",
            "status",
            "reason",
            "action",
            "action_ref",
            "action_reference",
        )
        if column in columns
    ]
    if not wanted:
        return []
    query = (
        f"SELECT {', '.join(_quote_identifier(column) for column in wanted)} "
        "FROM reply_followup_reminders"
    )
    if "status" in columns:
        query += " WHERE COALESCE(status, 'pending') IN ('pending', 'done')"
    return [dict(row) for row in conn.execute(query).fetchall()]


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        table: _table_columns(conn, table)
        for table in ("reply_queue", "reply_followup_reminders")
        if _table_columns(conn, table)
    }


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ReplyFollowupPromiseAuditReport:
    return ReplyFollowupPromiseAuditReport(
        ok=True,
        generated_at=generated_at.isoformat(),
        filters=filters,
        audited_count=0,
        promised_count=0,
        finding_count=0,
        by_platform={},
        by_severity={},
        findings=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3 connection or database wrapper with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")}
    except sqlite3.Error:
        return set()


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) DESC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _finding_sort_key(finding: ReplyFollowupPromiseFinding) -> tuple[str, int, str]:
    return (
        finding.platform,
        finding.reply_queue_id or 0,
        ",".join(finding.detected_promise_phrases),
    )


def _normalise_platforms(platform: str | Sequence[str] | None) -> tuple[str, ...]:
    if platform is None:
        return ()
    raw = (platform,) if isinstance(platform, str) else tuple(platform)
    return tuple(sorted({item.strip().casefold() for item in raw if item and item.strip()}))


def _display_platform_filter(value: Any) -> str:
    if not value:
        return "all"
    return ",".join(str(item) for item in value)


def _parse_json_object(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, Mapping) else None


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _is_records(value: Any) -> bool:
    return isinstance(value, Iterable) and not isinstance(value, (str, bytes, sqlite3.Connection))


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _shorten(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3].rstrip() + "..."


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
