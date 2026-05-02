"""Audit newsletter subject candidate pools for low pre-send diversity."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any, Mapping


DEFAULT_DAYS = 14

DUPLICATE_NORMALIZED_SUBJECT = "duplicate_normalized_subject"
REPEATED_OPENING_TOKEN = "repeated_opening_token"
SINGLE_SOURCE_POOL = "single_source_pool"

_TOKEN_RE = re.compile(r"[a-z0-9]+")


@dataclass(frozen=True)
class DuplicateSubjectGroup:
    """A set of candidates that normalize to the same subject."""

    normalized_subject: str
    count: int
    candidate_ids: tuple[int, ...]
    subjects: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DominantOpening:
    """The most repeated first token in a send candidate pool."""

    token: str
    count: int
    share: float
    candidate_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterSubjectCandidateDiversityFinding:
    """Low-diversity findings for one newsletter send candidate pool."""

    newsletter_send_id: int
    issue_id: str | None
    candidate_count: int
    issue_codes: tuple[str, ...]
    duplicate_groups: tuple[DuplicateSubjectGroup, ...]
    dominant_opening: DominantOpening | None
    source_counts: dict[str, int]
    recommended_action: str
    latest_candidate_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_count": self.candidate_count,
            "dominant_opening": (
                self.dominant_opening.to_dict() if self.dominant_opening else None
            ),
            "duplicate_groups": [group.to_dict() for group in self.duplicate_groups],
            "issue_codes": list(self.issue_codes),
            "issue_id": self.issue_id,
            "latest_candidate_at": self.latest_candidate_at,
            "newsletter_send_id": self.newsletter_send_id,
            "recommended_action": self.recommended_action,
            "source_counts": dict(sorted(self.source_counts.items())),
        }


@dataclass(frozen=True)
class NewsletterSubjectCandidateDiversityReport:
    """Subject candidate diversity audit for a reporting window."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    findings: tuple[NewsletterSubjectCandidateDiversityFinding, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    @property
    def has_issues(self) -> bool:
        return bool(self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_subject_candidate_diversity",
            "filters": dict(self.filters),
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_newsletter_subject_candidate_diversity_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    newsletter_send_id: int | None = None,
    now: datetime | None = None,
) -> NewsletterSubjectCandidateDiversityReport:
    """Return sends whose subject candidate pools are too repetitive."""
    if days <= 0:
        raise ValueError("days must be positive")
    if newsletter_send_id is not None and newsletter_send_id <= 0:
        raise ValueError("newsletter_send_id must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "cutoff": cutoff.isoformat(),
        "days": days,
        "newsletter_send_id": newsletter_send_id,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return NewsletterSubjectCandidateDiversityReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals={"candidate_count": 0, "issue_send_count": 0, "send_count": 0},
            findings=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_candidate_rows(
        conn,
        cutoff=cutoff,
        newsletter_send_id=newsletter_send_id,
    )
    grouped = _group_by_send(rows)
    findings = [
        finding
        for send_rows in grouped.values()
        if (finding := _finding_for_send(send_rows)) is not None
    ]
    findings.sort(key=lambda item: (item.newsletter_send_id, item.issue_id or ""))

    return NewsletterSubjectCandidateDiversityReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "candidate_count": len(rows),
            "issue_send_count": len(findings),
            "send_count": len(grouped),
        },
        findings=tuple(findings),
        missing_tables=(),
        missing_columns={},
    )


def format_newsletter_subject_candidate_diversity_json(
    report: NewsletterSubjectCandidateDiversityReport,
) -> str:
    """Format a subject candidate diversity report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_subject_candidate_diversity_text(
    report: NewsletterSubjectCandidateDiversityReport,
) -> str:
    """Format a subject candidate diversity report for terminal review."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Newsletter Subject Candidate Diversity Report",
        f"Generated: {report.generated_at}",
        (
            f"Window: {filters['days']} days cutoff={filters['cutoff']} "
            f"newsletter_send_id={filters['newsletter_send_id'] or '-'}"
        ),
        (
            f"Totals: sends={totals['send_count']} candidates={totals['candidate_count']} "
            f"issue_sends={totals['issue_send_count']}"
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
    lines.append("")

    if not report.findings:
        lines.append("No low-diversity newsletter subject candidate pools found.")
        return "\n".join(lines)

    lines.append("Findings:")
    for finding in report.findings:
        lines.append(
            f"- send={finding.newsletter_send_id} issue={finding.issue_id or '-'} "
            f"candidates={finding.candidate_count} "
            f"issues={', '.join(finding.issue_codes)}"
        )
        if finding.duplicate_groups:
            groups = [
                f"{group.normalized_subject}({group.count})"
                for group in finding.duplicate_groups
            ]
            lines.append("  duplicate_groups=" + ", ".join(groups))
        if finding.dominant_opening:
            opening = finding.dominant_opening
            lines.append(
                f"  dominant_opening={opening.token} "
                f"count={opening.count} share={opening.share:.2f}"
            )
        lines.append("  sources=" + _format_counts(finding.source_counts))
        lines.append(f"  recommended_action={finding.recommended_action}")
    return "\n".join(lines)


def _load_candidate_rows(
    conn: sqlite3.Connection,
    *,
    cutoff: datetime,
    newsletter_send_id: int | None,
) -> list[dict[str, Any]]:
    clauses = [
        "c.newsletter_send_id IS NOT NULL",
        "datetime(COALESCE(c.created_at, ns.sent_at)) >= datetime(?)",
    ]
    params: list[Any] = [cutoff.isoformat()]
    if newsletter_send_id is not None:
        clauses.append("c.newsletter_send_id = ?")
        params.append(newsletter_send_id)

    query = f"""SELECT
                   c.id AS candidate_id,
                   c.newsletter_send_id,
                   COALESCE(c.issue_id, ns.issue_id) AS issue_id,
                   c.subject,
                   COALESCE(c.source, 'unknown') AS source,
                   c.created_at,
                   ns.sent_at
               FROM newsletter_subject_candidates c
               LEFT JOIN newsletter_sends ns
                 ON ns.id = c.newsletter_send_id
               WHERE {" AND ".join(clauses)}
               ORDER BY c.newsletter_send_id ASC, c.rank ASC, c.id ASC"""
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _finding_for_send(
    rows: list[Mapping[str, Any]],
) -> NewsletterSubjectCandidateDiversityFinding | None:
    candidate_count = len(rows)
    if candidate_count == 0:
        return None

    duplicate_groups = _duplicate_groups(rows)
    dominant_opening = _dominant_opening(rows)
    source_counts = dict(sorted(Counter(_source(row) for row in rows).items()))

    issue_codes = []
    if duplicate_groups:
        issue_codes.append(DUPLICATE_NORMALIZED_SUBJECT)
    if dominant_opening:
        issue_codes.append(REPEATED_OPENING_TOKEN)
    if candidate_count > 1 and len(source_counts) == 1:
        issue_codes.append(SINGLE_SOURCE_POOL)
    if not issue_codes:
        return None

    return NewsletterSubjectCandidateDiversityFinding(
        newsletter_send_id=int(rows[0]["newsletter_send_id"]),
        issue_id=rows[0].get("issue_id"),
        candidate_count=candidate_count,
        issue_codes=tuple(issue_codes),
        duplicate_groups=tuple(duplicate_groups),
        dominant_opening=dominant_opening,
        source_counts=source_counts,
        recommended_action=_recommended_action(issue_codes),
        latest_candidate_at=_latest_candidate_at(rows),
    )


def _duplicate_groups(rows: list[Mapping[str, Any]]) -> list[DuplicateSubjectGroup]:
    buckets: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        normalized = _normalize_subject(row.get("subject"))
        if normalized:
            buckets.setdefault(normalized, []).append(row)

    groups = []
    for normalized, items in buckets.items():
        if len(items) <= 1:
            continue
        groups.append(
            DuplicateSubjectGroup(
                normalized_subject=normalized,
                count=len(items),
                candidate_ids=tuple(int(item["candidate_id"]) for item in items),
                subjects=tuple(str(item.get("subject") or "") for item in items),
            )
        )
    groups.sort(key=lambda group: (-group.count, group.normalized_subject))
    return groups


def _dominant_opening(rows: list[Mapping[str, Any]]) -> DominantOpening | None:
    buckets: dict[str, list[int]] = {}
    for row in rows:
        token = _opening_token(row.get("subject"))
        if token:
            buckets.setdefault(token, []).append(int(row["candidate_id"]))
    if not buckets:
        return None
    token, candidate_ids = sorted(
        buckets.items(),
        key=lambda item: (-len(item[1]), item[0]),
    )[0]
    if len(candidate_ids) <= 1:
        return None
    return DominantOpening(
        token=token,
        count=len(candidate_ids),
        share=round(len(candidate_ids) / len(rows), 4),
        candidate_ids=tuple(candidate_ids),
    )


def _group_by_send(rows: list[Mapping[str, Any]]) -> dict[int, list[Mapping[str, Any]]]:
    grouped: dict[int, list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(int(row["newsletter_send_id"]), []).append(row)
    return grouped


def _recommended_action(issue_codes: list[str]) -> str:
    if DUPLICATE_NORMALIZED_SUBJECT in issue_codes:
        return (
            "Rewrite duplicate subjects before selection, then add candidates from a "
            "different source or opening angle."
        )
    if REPEATED_OPENING_TOKEN in issue_codes:
        return "Replace repeated first-word hooks with subjects that start from distinct nouns or outcomes."
    return "Add candidates from another source before selecting the newsletter subject."


def _latest_candidate_at(rows: list[Mapping[str, Any]]) -> str | None:
    values = [row.get("created_at") or row.get("sent_at") for row in rows]
    values = [str(value) for value in values if value]
    return max(values) if values else None


def _normalize_subject(value: Any) -> str:
    return " ".join(_tokens(value))


def _opening_token(value: Any) -> str | None:
    tokens = _tokens(value)
    return tokens[0] if tokens else None


def _tokens(value: Any) -> list[str]:
    return _TOKEN_RE.findall(str(value or "").lower())


def _source(row: Mapping[str, Any]) -> str:
    return str(row.get("source") or "unknown")


def _format_counts(counts: Mapping[str, int]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        row[0]: {
            column[1]
            for column in conn.execute(f"PRAGMA table_info({row[0]})").fetchall()
        }
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }


def _schema_gaps(
    schema: Mapping[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "newsletter_subject_candidates": {
            "id",
            "newsletter_send_id",
            "issue_id",
            "subject",
            "source",
            "rank",
            "created_at",
        },
        "newsletter_sends": {"id", "issue_id", "sent_at"},
    }
    missing_tables = tuple(
        table for table in required if table not in schema
    )
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
