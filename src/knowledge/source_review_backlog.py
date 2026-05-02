"""Report discovered curated sources that are still awaiting review."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_AGING_DAYS = 7
DEFAULT_OVERDUE_DAYS = 30

FRESH_BUCKET = "fresh"
AGING_BUCKET = "aging"
OVERDUE_BUCKET = "overdue"

PENDING_REVIEW_STATUSES = frozenset(
    {
        "candidate",
        "discovered",
        "needs_review",
        "pending",
        "pending_review",
        "review",
        "unreviewed",
    }
)
RESOLVED_STATUSES = frozenset(
    {
        "active",
        "approved",
        "inactive",
        "paused",
        "quarantined",
        "rejected",
        "retired",
    }
)

RECOMMENDED_ACTIONS = {
    FRESH_BUCKET: "Review candidate source metadata and approve or reject.",
    AGING_BUCKET: "Prioritize review before the source discovery signal goes stale.",
    OVERDUE_BUCKET: "Escalate source review; approve, reject, quarantine, or retire.",
}


@dataclass(frozen=True)
class SourceReviewBacklogFinding:
    """One curated source candidate awaiting review."""

    source_id: int
    identifier: str
    source_type: str
    discovery_source: str
    status: str
    created_at: str | None
    age_days: int
    age_bucket: str
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SourceReviewBacklogReport:
    """Read-only backlog report for unresolved curated source candidates."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    oldest_created_at: str | None
    findings: tuple[SourceReviewBacklogFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "source_review_backlog",
            "filters": dict(self.filters),
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "oldest_created_at": self.oldest_created_at,
            "totals": dict(self.totals),
        }


def build_source_review_backlog_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    source_type: str | None = None,
    aging_days: int = DEFAULT_AGING_DAYS,
    overdue_days: int = DEFAULT_OVERDUE_DAYS,
    now: datetime | None = None,
) -> SourceReviewBacklogReport:
    """Return unresolved curated source candidates grouped by review age."""
    if days <= 0:
        raise ValueError("days must be positive")
    if aging_days <= 0:
        raise ValueError("aging_days must be positive")
    if overdue_days <= aging_days:
        raise ValueError("overdue_days must be greater than aging_days")
    source_type_filter = _clean_optional(source_type, "source_type")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "aging_days": aging_days,
        "cutoff": cutoff.isoformat(),
        "days": days,
        "overdue_days": overdue_days,
        "pending_statuses": sorted(PENDING_REVIEW_STATUSES),
        "source_type": source_type_filter,
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

    rows = _load_rows(
        conn,
        source_type=source_type_filter,
        cutoff=cutoff,
        statuses=sorted(PENDING_REVIEW_STATUSES),
    )
    findings = tuple(
        _finding(
            row,
            generated_at=generated_at,
            aging_days=aging_days,
            overdue_days=overdue_days,
        )
        for row in rows
    )
    oldest_created_at = _oldest_created_at(findings)
    return SourceReviewBacklogReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(findings),
        oldest_created_at=oldest_created_at,
        findings=findings,
        missing_tables=(),
        missing_columns={},
    )


def format_source_review_backlog_json(report: SourceReviewBacklogReport) -> str:
    """Serialize a source review backlog report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_source_review_backlog_text(report: SourceReviewBacklogReport) -> str:
    """Render a source review backlog report for terminal review."""
    totals = report.totals
    lines = [
        "Curated Source Review Backlog",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={report.filters['days']} "
            f"source_type={report.filters['source_type'] or 'all'} "
            f"aging_days={report.filters['aging_days']} "
            f"overdue_days={report.filters['overdue_days']}"
        ),
        (
            "Totals: "
            f"findings={totals['finding_count']} "
            f"oldest_created_at={report.oldest_created_at or '-'}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.append("Missing columns: " + "; ".join(missing))
    if not report.findings:
        lines.append("No unresolved curated source candidates found.")
        return "\n".join(lines)

    lines.append("By source_type: " + _format_counts(totals["by_source_type"]))
    lines.append("By discovery_source: " + _format_counts(totals["by_discovery_source"]))
    lines.append("By age_bucket: " + _format_counts(totals["by_age_bucket"]))
    lines.append("")
    lines.append("Findings:")
    for finding in report.findings:
        lines.append(
            f"  - #{finding.source_id} {finding.source_type}:{finding.identifier} "
            f"source={finding.discovery_source} status={finding.status} "
            f"created_at={finding.created_at or '-'} age_days={finding.age_days} "
            f"bucket={finding.age_bucket}"
        )
        lines.append(f"      action: {finding.recommended_action}")
    return "\n".join(lines)


def _finding(
    row: dict[str, Any],
    *,
    generated_at: datetime,
    aging_days: int,
    overdue_days: int,
) -> SourceReviewBacklogFinding:
    created_at = _parse_datetime(row.get("created_at"))
    age_days = _age_days(created_at, generated_at)
    age_bucket = _age_bucket(age_days, aging_days=aging_days, overdue_days=overdue_days)
    return SourceReviewBacklogFinding(
        source_id=int(row.get("id") or 0),
        identifier=_clean(row.get("identifier")) or "",
        source_type=_clean(row.get("source_type")) or "unknown",
        discovery_source=_clean(row.get("discovery_source")) or "unknown",
        status=_clean(row.get("status")) or "pending",
        created_at=_clean(row.get("created_at")),
        age_days=age_days,
        age_bucket=age_bucket,
        recommended_action=RECOMMENDED_ACTIONS[age_bucket],
    )


def _load_rows(
    conn: sqlite3.Connection,
    *,
    source_type: str | None,
    cutoff: datetime,
    statuses: list[str],
) -> list[dict[str, Any]]:
    where = [
        f"LOWER(COALESCE(status, 'pending')) IN ({', '.join('?' for _ in statuses)})",
        "(created_at IS NULL OR created_at >= ?)",
    ]
    params: list[Any] = [*statuses, cutoff.isoformat()]
    if source_type is not None:
        where.append("source_type = ?")
        params.append(source_type)
    rows = conn.execute(
        f"""SELECT id, identifier, source_type, discovery_source, status, created_at
            FROM curated_sources
            WHERE {' AND '.join(where)}
            ORDER BY
                CASE WHEN created_at IS NULL THEN 1 ELSE 0 END ASC,
                datetime(created_at) ASC,
                source_type ASC,
                discovery_source ASC,
                identifier ASC,
                id ASC""",
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def _totals(findings: tuple[SourceReviewBacklogFinding, ...]) -> dict[str, Any]:
    return {
        "finding_count": len(findings),
        "by_age_bucket": _ordered_counts(
            Counter(finding.age_bucket for finding in findings),
            (FRESH_BUCKET, AGING_BUCKET, OVERDUE_BUCKET),
        ),
        "by_discovery_source": dict(
            sorted(Counter(finding.discovery_source for finding in findings).items())
        ),
        "by_source_type": dict(
            sorted(Counter(finding.source_type for finding in findings).items())
        ),
    }


def _ordered_counts(counter: Counter[str], order: tuple[str, ...]) -> dict[str, int]:
    return {key: counter[key] for key in order if counter[key]}


def _oldest_created_at(findings: tuple[SourceReviewBacklogFinding, ...]) -> str | None:
    values = [finding.created_at for finding in findings if finding.created_at]
    return min(values) if values else None


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "curated_sources": (
            "id",
            "identifier",
            "source_type",
            "discovery_source",
            "status",
            "created_at",
        )
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in required.items()
        if table in schema
        and any(column not in schema.get(table, set()) for column in columns)
    }
    return missing_tables, missing_columns


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> SourceReviewBacklogReport:
    return SourceReviewBacklogReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(()),
        oldest_created_at=None,
        findings=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_conn must be a sqlite3.Connection or Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        name = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(name)] = {
            str(column[1])
            for column in conn.execute(f"PRAGMA table_info({name})").fetchall()
        }
    return schema


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _age_days(created_at: datetime | None, generated_at: datetime) -> int:
    if created_at is None:
        return 0
    return max(0, int((generated_at - created_at).total_seconds() // 86400))


def _age_bucket(age_days: int, *, aging_days: int, overdue_days: int) -> str:
    if age_days >= overdue_days:
        return OVERDUE_BUCKET
    if age_days >= aging_days:
        return AGING_BUCKET
    return FRESH_BUCKET


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_optional(value: str | None, name: str) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{name} must not be blank")
    return cleaned


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))
