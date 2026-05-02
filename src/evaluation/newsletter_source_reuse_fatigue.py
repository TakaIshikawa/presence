"""Detect repeated newsletter source material across recent sends."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_REUSES = 2
RECOMMENDATION = "Replace or refresh heavily reused source material before the next issue."


@dataclass(frozen=True)
class NewsletterSourceReuseFinding:
    """One parse finding for a newsletter send source_content_ids value."""

    finding_type: str
    newsletter_send_id: int
    issue_id: str
    message: str
    sent_at: str | None = None
    position: int | None = None
    raw_value: Any | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterSourceReuseExample:
    """One newsletter send that reused a source content item."""

    newsletter_send_id: int
    issue_id: str
    sent_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterSourceReuseGroup:
    """A source content item reused across multiple newsletter sends."""

    content_id: int
    reuse_count: int
    issue_count: int
    first_seen: str | None
    last_seen: str | None
    send_ids: tuple[int, ...]
    issue_ids: tuple[str, ...]
    examples: tuple[NewsletterSourceReuseExample, ...]
    recommendation: str = RECOMMENDATION

    def to_dict(self) -> dict[str, Any]:
        return {
            "content_id": self.content_id,
            "examples": [example.to_dict() for example in self.examples],
            "first_seen": self.first_seen,
            "issue_count": self.issue_count,
            "issue_ids": list(self.issue_ids),
            "last_seen": self.last_seen,
            "recommendation": self.recommendation,
            "reuse_count": self.reuse_count,
            "send_ids": list(self.send_ids),
        }


@dataclass(frozen=True)
class NewsletterSourceReuseFatigueReport:
    """Source reuse fatigue report plus filter and schema metadata."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    reused_sources: tuple[NewsletterSourceReuseGroup, ...]
    findings: tuple[NewsletterSourceReuseFinding, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    @property
    def has_issues(self) -> bool:
        return bool(self.reused_sources or self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_source_reuse_fatigue",
            "filters": dict(self.filters),
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "reused_source_count": len(self.reused_sources),
            "reused_sources": [group.to_dict() for group in self.reused_sources],
            "totals": dict(sorted(self.totals.items())),
        }


def build_newsletter_source_reuse_fatigue_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_reuses: int = DEFAULT_MIN_REUSES,
    now: datetime | None = None,
) -> NewsletterSourceReuseFatigueReport:
    """Return source content reused across recent newsletter sends."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_reuses <= 0:
        raise ValueError("min_reuses must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "min_reuses": min_reuses,
    }
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    sends = _load_sends(conn, schema, cutoff=cutoff)
    buckets: dict[int, list[NewsletterSourceReuseExample]] = defaultdict(list)
    findings: list[NewsletterSourceReuseFinding] = []
    parsed_reference_count = 0

    for send in sends:
        send_findings, source_ids = _parse_send_source_ids(send)
        findings.extend(send_findings)
        parsed_reference_count += len(source_ids)
        for content_id in sorted(set(source_ids)):
            buckets[content_id].append(
                NewsletterSourceReuseExample(
                    newsletter_send_id=int(send["newsletter_send_id"]),
                    issue_id=str(send.get("issue_id") or ""),
                    sent_at=send.get("sent_at"),
                )
            )

    groups = [
        _reuse_group(content_id, examples)
        for content_id, examples in buckets.items()
        if len(examples) >= min_reuses
    ]
    groups.sort(
        key=lambda group: (
            -group.reuse_count,
            group.first_seen or "",
            group.content_id,
        )
    )
    findings.sort(key=_finding_sort_key)
    counts = Counter(finding.finding_type for finding in findings)
    return NewsletterSourceReuseFatigueReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "send_count": len(sends),
            "parsed_reference_count": parsed_reference_count,
            "unique_source_count": len(buckets),
            "reused_source_count": len(groups),
            "reused_send_count": sum(group.reuse_count for group in groups),
            "finding_count": len(findings),
            "by_finding_type": dict(sorted(counts.items())),
        },
        reused_sources=tuple(groups),
        findings=tuple(findings),
        missing_tables=(),
        missing_columns={},
    )


def format_newsletter_source_reuse_fatigue_json(
    report: NewsletterSourceReuseFatigueReport,
) -> str:
    """Serialize the source reuse fatigue report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_source_reuse_fatigue_text(
    report: NewsletterSourceReuseFatigueReport,
) -> str:
    """Render the source reuse fatigue report for command-line review."""
    totals = report.totals
    lines = [
        "Newsletter Source Reuse Fatigue",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['days']} days "
            f"min_reuses={report.filters['min_reuses']}"
        ),
        (
            "Totals: "
            f"sends={totals['send_count']} "
            f"sources={totals['parsed_reference_count']} "
            f"reused_sources={totals['reused_source_count']} "
            f"findings={totals['finding_count']}"
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

    if not report.reused_sources and not report.findings:
        lines.append("No newsletter source reuse fatigue issues found.")
        return "\n".join(lines)

    if report.reused_sources:
        lines.append("Reused source content:")
        for group in report.reused_sources:
            issue_ids = ", ".join(group.issue_ids) if group.issue_ids else "-"
            send_ids = ", ".join(str(send_id) for send_id in group.send_ids)
            lines.append(
                f"  - content_id={group.content_id} "
                f"reuses={group.reuse_count} issues={issue_ids} "
                f"sends={send_ids} first_seen={group.first_seen or '-'} "
                f"last_seen={group.last_seen or '-'}"
            )
            lines.append(f"      recommendation: {group.recommendation}")

    if report.findings:
        if report.reused_sources:
            lines.append("")
        lines.append("Parse findings:")
        for finding in report.findings:
            position = "-" if finding.position is None else str(finding.position)
            lines.append(
                f"  - {finding.finding_type} send={finding.newsletter_send_id} "
                f"issue={finding.issue_id or '-'} pos={position}: {finding.message}"
            )
    return "\n".join(lines)


def _parse_send_source_ids(
    send: dict[str, Any],
) -> tuple[list[NewsletterSourceReuseFinding], list[int]]:
    raw_value = send.get("source_content_ids")
    if raw_value is None or (isinstance(raw_value, str) and not raw_value.strip()):
        return [
            _finding(
                "blank_source_content_ids",
                send,
                "source_content_ids is blank",
                raw_value=raw_value,
            )
        ], []

    try:
        parsed = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (TypeError, json.JSONDecodeError) as exc:
        return [
            _finding(
                "malformed_source_content_ids",
                send,
                f"source_content_ids is not valid JSON: {exc}",
                raw_value=raw_value,
            )
        ], []

    if not isinstance(parsed, list):
        return [
            _finding(
                "malformed_source_content_ids",
                send,
                f"source_content_ids must be a JSON array, got {type(parsed).__name__}",
                raw_value=raw_value,
            )
        ], []

    findings: list[NewsletterSourceReuseFinding] = []
    source_ids: list[int] = []
    for position, item in enumerate(parsed):
        if not _is_positive_int(item):
            findings.append(
                _finding(
                    "invalid_source_content_id",
                    send,
                    "source_content_ids must contain only positive integers",
                    position=position,
                    raw_value=item,
                )
            )
            continue
        source_ids.append(int(item))
    if not parsed:
        findings.append(
            _finding(
                "blank_source_content_ids",
                send,
                "source_content_ids is empty",
                raw_value=raw_value,
            )
        )
    return findings, source_ids


def _reuse_group(
    content_id: int,
    examples: list[NewsletterSourceReuseExample],
) -> NewsletterSourceReuseGroup:
    ordered = sorted(
        examples,
        key=lambda example: (
            example.sent_at or "",
            example.newsletter_send_id,
        ),
    )
    issue_ids = tuple(dict.fromkeys(example.issue_id for example in ordered if example.issue_id))
    return NewsletterSourceReuseGroup(
        content_id=content_id,
        reuse_count=len(ordered),
        issue_count=len(issue_ids),
        first_seen=ordered[0].sent_at,
        last_seen=ordered[-1].sent_at,
        send_ids=tuple(example.newsletter_send_id for example in ordered),
        issue_ids=issue_ids,
        examples=tuple(ordered),
    )


def _load_sends(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    columns = schema["newsletter_sends"]
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   ns.id AS newsletter_send_id,
                   {_column_expr(columns, "issue_id", "''", alias="ns")} AS issue_id,
                   {_column_expr(columns, "sent_at", "NULL", alias="ns")} AS sent_at,
                   ns.source_content_ids AS source_content_ids
               FROM newsletter_sends ns
               WHERE ns.sent_at >= ?
               ORDER BY ns.sent_at DESC, ns.id DESC""",
            (cutoff.isoformat(),),
        ).fetchall()
    ]


def _finding(
    finding_type: str,
    send: dict[str, Any],
    message: str,
    *,
    position: int | None = None,
    raw_value: Any | None = None,
) -> NewsletterSourceReuseFinding:
    return NewsletterSourceReuseFinding(
        finding_type=finding_type,
        newsletter_send_id=int(send["newsletter_send_id"]),
        issue_id=str(send.get("issue_id") or ""),
        message=message,
        sent_at=send.get("sent_at"),
        position=position,
        raw_value=raw_value,
    )


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "newsletter_sends": {"id", "source_content_ids", "sent_at"},
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
) -> NewsletterSourceReuseFatigueReport:
    return NewsletterSourceReuseFatigueReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "send_count": 0,
            "parsed_reference_count": 0,
            "unique_source_count": 0,
            "reused_source_count": 0,
            "reused_send_count": 0,
            "finding_count": 0,
            "by_finding_type": {},
        },
        reused_sources=(),
        findings=(),
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


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _is_positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _finding_sort_key(finding: NewsletterSourceReuseFinding) -> tuple[Any, ...]:
    return (
        finding.newsletter_send_id,
        finding.finding_type,
        -1 if finding.position is None else finding.position,
        str(finding.raw_value),
    )
