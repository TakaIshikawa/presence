"""Report reply draft source freshness by examining knowledge context age."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence


DEFAULT_DAYS = 7
DEFAULT_STALE_DAYS = 30
DEFAULT_STATUS = ("pending",)

WARNING_STALE_CONTEXT = "stale_context"
WARNING_MISSING_CONTEXT = "missing_context"
WARNING_MALFORMED_CONTEXT = "malformed_context"


@dataclass(frozen=True)
class ReplySourceFreshnessFinding:
    """One reply draft's source freshness analysis."""

    draft_id: int | None
    mention_id: str | None
    author_handle: str | None
    platform: str
    drafted_at: str | None
    context_item_count: int
    newest_context_age_days: float | None
    oldest_context_age_days: float | None
    warnings: tuple[str, ...]
    freshness_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReplySourceFreshnessReport:
    """Aggregated source freshness report for reply drafts."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    scanned_count: int
    stale_count: int
    missing_count: int
    malformed_count: int
    fresh_count: int
    findings: tuple[ReplySourceFreshnessFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return self.stale_count + self.missing_count + self.malformed_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_source_freshness",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "scanned_count": self.scanned_count,
            "stale_count": self.stale_count,
            "missing_count": self.missing_count,
            "malformed_count": self.malformed_count,
            "fresh_count": self.fresh_count,
            "blocking_issue_count": self.blocking_issue_count,
            "findings": [finding.to_dict() for finding in self.findings],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            }
            if self.missing_columns
            else {},
        }


def build_reply_source_freshness_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    stale_days: int = DEFAULT_STALE_DAYS,
    status: str | Sequence[str] | None = DEFAULT_STATUS,
    platform: str | Sequence[str] | None = None,
    now: datetime | None = None,
) -> ReplySourceFreshnessReport:
    """Build a source freshness report for reply drafts."""
    if days <= 0:
        raise ValueError("days must be positive")
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    statuses = _normalize_filter(status)
    platforms = _normalize_filter(platform)

    filters = {
        "days": days,
        "stale_days": stale_days,
        "status": list(statuses),
        "platform": list(platforms) if platforms else None,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)

    if "reply_queue" not in schema:
        return _empty_report(generated_at, filters, missing_tables=("reply_queue",))

    required = {"id", "inbound_tweet_id"}
    missing_required = tuple(sorted(required - schema["reply_queue"]))
    if missing_required:
        return _empty_report(
            generated_at,
            filters,
            missing_columns={"reply_queue": missing_required},
        )

    rows = _reply_rows(
        conn,
        schema,
        days=days,
        statuses=statuses,
        platforms=platforms,
        generated_at=generated_at,
    )

    findings = tuple(
        _analyze_draft_freshness(row, conn, schema, stale_days=stale_days, now=generated_at)
        for row in rows
    )

    stale_count = sum(
        1 for f in findings if WARNING_STALE_CONTEXT in f.warnings
    )
    missing_count = sum(
        1 for f in findings if WARNING_MISSING_CONTEXT in f.warnings
    )
    malformed_count = sum(
        1 for f in findings if WARNING_MALFORMED_CONTEXT in f.warnings
    )
    fresh_count = sum(1 for f in findings if not f.warnings)

    return ReplySourceFreshnessReport(
        ok=stale_count == 0 and missing_count == 0 and malformed_count == 0,
        generated_at=generated_at.isoformat(),
        filters=filters,
        scanned_count=len(findings),
        stale_count=stale_count,
        missing_count=missing_count,
        malformed_count=malformed_count,
        fresh_count=fresh_count,
        findings=tuple(sorted(findings, key=_finding_sort_key)),
    )


def format_reply_source_freshness_json(report: ReplySourceFreshnessReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_source_freshness_text(report: ReplySourceFreshnessReport) -> str:
    """Render human-readable text summary."""
    lines = [
        "Reply Source Freshness Report",
        f"Generated: {report.generated_at}",
        f"Filters: days={report.filters['days']}, stale_days={report.filters['stale_days']}, "
        f"status={report.filters['status']}",
        "",
        f"Scanned: {report.scanned_count}",
        f"Fresh: {report.fresh_count}",
        f"Stale: {report.stale_count}",
        f"Missing context: {report.missing_count}",
        f"Malformed: {report.malformed_count}",
        f"OK: {report.ok}",
    ]

    if report.missing_tables:
        lines.extend(["", f"Missing tables: {', '.join(report.missing_tables)}"])

    if report.missing_columns:
        lines.append("")
        lines.append("Missing columns:")
        for table, columns in sorted(report.missing_columns.items()):
            lines.append(f"  {table}: {', '.join(columns)}")

    if report.findings:
        lines.extend(["", "Findings:"])
        for finding in report.findings:
            warnings_str = ", ".join(finding.warnings) if finding.warnings else "fresh"
            age_str = ""
            if finding.newest_context_age_days is not None:
                age_str = f" (newest: {finding.newest_context_age_days:.1f}d"
                if finding.oldest_context_age_days != finding.newest_context_age_days:
                    age_str += f", oldest: {finding.oldest_context_age_days:.1f}d"
                age_str += ")"
            lines.append(
                f"  [{finding.freshness_id}] {finding.author_handle or 'unknown'} "
                f"({finding.context_item_count} items{age_str}): {warnings_str}"
            )

    return "\n".join(lines)


def _analyze_draft_freshness(
    row: Mapping[str, Any],
    conn: sqlite3.Connection,
    schema: Mapping[str, set[str]],
    *,
    stale_days: int,
    now: datetime,
) -> ReplySourceFreshnessFinding:
    """Analyze one reply draft's source freshness."""
    draft_id = _int_or_none(row.get("id"))
    mention_id = _clean(row.get("inbound_tweet_id") or row.get("mention_id"))
    author_handle = _clean(row.get("inbound_author_handle") or row.get("author_handle"))
    platform = _clean(row.get("platform")) or "x"
    drafted_at = _clean(row.get("detected_at") or row.get("created_at"))

    freshness_id = f"{platform}/{mention_id or draft_id or 'unknown'}"

    # Parse draft timestamp
    draft_timestamp = _parse_datetime(drafted_at)
    if draft_timestamp is None:
        draft_timestamp = now

    # Get knowledge context items
    if "reply_knowledge_links" not in schema or "knowledge" not in schema:
        return ReplySourceFreshnessFinding(
            draft_id=draft_id,
            mention_id=mention_id,
            author_handle=author_handle,
            platform=platform,
            drafted_at=drafted_at,
            context_item_count=0,
            newest_context_age_days=None,
            oldest_context_age_days=None,
            warnings=(WARNING_MISSING_CONTEXT,),
            freshness_id=freshness_id,
        )

    context_items = _get_knowledge_context(conn, draft_id)

    if not context_items:
        return ReplySourceFreshnessFinding(
            draft_id=draft_id,
            mention_id=mention_id,
            author_handle=author_handle,
            platform=platform,
            drafted_at=drafted_at,
            context_item_count=0,
            newest_context_age_days=None,
            oldest_context_age_days=None,
            warnings=(WARNING_MISSING_CONTEXT,),
            freshness_id=freshness_id,
        )

    # Compute context ages
    ages: list[float] = []
    malformed = False

    for item in context_items:
        timestamp_str = item.get("published_at") or item.get("created_at")
        timestamp = _parse_datetime(timestamp_str)
        if timestamp is None:
            malformed = True
            continue
        age_seconds = (draft_timestamp - timestamp).total_seconds()
        age_days = max(age_seconds / 86400, 0.0)
        ages.append(age_days)

    warnings: list[str] = []
    newest_age = None
    oldest_age = None

    if malformed:
        warnings.append(WARNING_MALFORMED_CONTEXT)

    if ages:
        newest_age = round(min(ages), 2)
        oldest_age = round(max(ages), 2)
        if oldest_age > stale_days:
            warnings.append(WARNING_STALE_CONTEXT)
    elif not malformed:
        # All timestamps were None but not malformed (empty strings)
        warnings.append(WARNING_MALFORMED_CONTEXT)

    return ReplySourceFreshnessFinding(
        draft_id=draft_id,
        mention_id=mention_id,
        author_handle=author_handle,
        platform=platform,
        drafted_at=drafted_at,
        context_item_count=len(context_items),
        newest_context_age_days=newest_age,
        oldest_context_age_days=oldest_age,
        warnings=tuple(warnings),
        freshness_id=freshness_id,
    )


def _get_knowledge_context(
    conn: sqlite3.Connection, draft_id: int | None
) -> list[dict[str, Any]]:
    """Get knowledge items linked to a reply draft."""
    if draft_id is None:
        return []

    query = """
        SELECT k.id, k.published_at, k.created_at, k.source_type
        FROM reply_knowledge_links rkl
        JOIN knowledge k ON k.id = rkl.knowledge_id
        WHERE rkl.reply_queue_id = ?
        ORDER BY k.published_at DESC, k.created_at DESC
    """
    try:
        rows = conn.execute(query, (draft_id,)).fetchall()
        return [dict(row) for row in rows]
    except sqlite3.OperationalError:
        return []


def _reply_rows(
    conn: sqlite3.Connection,
    schema: Mapping[str, set[str]],
    *,
    days: int,
    statuses: tuple[str, ...],
    platforms: tuple[str, ...],
    generated_at: datetime,
) -> list[dict[str, Any]]:
    """Load reply queue rows matching filters."""
    columns = schema.get("reply_queue", set())
    where: list[str] = []
    params: list[Any] = []

    if "detected_at" in columns:
        cutoff = generated_at - _timedelta_days(days)
        where.append("(detected_at IS NULL OR datetime(detected_at) >= datetime(?))")
        params.append(cutoff.isoformat())

    if "status" in columns and statuses:
        where.append(f"LOWER(COALESCE(status, 'pending')) IN ({_placeholders(statuses)})")
        params.extend(statuses)

    if "platform" in columns and platforms:
        where.append(f"LOWER(COALESCE(platform, 'x')) IN ({_placeholders(platforms)})")
        params.extend(platforms)

    query = "SELECT * FROM reply_queue"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY " + _order_clause(columns)

    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in tables}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
    }


def _empty_report(
    generated_at: datetime,
    filters: Mapping[str, Any],
    *,
    missing_tables: Sequence[str] = (),
    missing_columns: Mapping[str, Sequence[str]] | None = None,
) -> ReplySourceFreshnessReport:
    return ReplySourceFreshnessReport(
        ok=True,
        generated_at=generated_at.isoformat(),
        filters=dict(filters),
        scanned_count=0,
        stale_count=0,
        missing_count=0,
        malformed_count=0,
        fresh_count=0,
        findings=(),
        missing_tables=tuple(missing_tables),
        missing_columns={
            table: tuple(columns) for table, columns in (missing_columns or {}).items()
        }
        or None,
    )


def _normalize_filter(value: str | Sequence[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        values = (value,)
    else:
        values = tuple(value)
    return tuple(sorted({item.strip().casefold() for item in values if item and item.strip()}))


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    for parser in (
        lambda candidate: datetime.fromisoformat(candidate.replace("Z", "+00:00")),
        lambda candidate: datetime.strptime(candidate, "%Y-%m-%d %H:%M:%S"),
    ):
        try:
            return _as_utc(parser(text))
        except ValueError:
            continue
    return None


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _finding_sort_key(finding: ReplySourceFreshnessFinding) -> tuple[Any, ...]:
    warning_rank = {
        WARNING_MALFORMED_CONTEXT: 0,
        WARNING_MISSING_CONTEXT: 1,
        WARNING_STALE_CONTEXT: 2,
    }
    primary_warning = min(
        (warning_rank.get(w, 99) for w in finding.warnings),
        default=99,
    )
    return (
        primary_warning,
        -(finding.oldest_context_age_days or 0),
        finding.platform,
        finding.freshness_id,
    )


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) DESC")
    if "id" in columns:
        parts.append("id DESC")
    else:
        parts.append("rowid DESC")
    return ", ".join(parts)


def _placeholders(values: Sequence[Any]) -> str:
    return ",".join("?" for _ in values)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _timedelta_days(days: int) -> Any:
    from datetime import timedelta

    return timedelta(days=days)
