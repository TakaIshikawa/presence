"""Report repetitive patterns in recently sent newsletter subject lines."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 60
DEFAULT_THRESHOLD = 3

REPEATED_OPENING = "repeated_opening"
FREQUENT_TERM = "frequent_term"
SIMILAR_STRUCTURE = "similar_structure"

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_PUNCTUATION_RE = re.compile(r"[.!?;:,]|--|[-\u2013\u2014]")
_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "by",
        "for",
        "from",
        "how",
        "in",
        "inside",
        "into",
        "is",
        "it",
        "of",
        "on",
        "or",
        "our",
        "that",
        "the",
        "this",
        "to",
        "with",
        "your",
    }
)


@dataclass(frozen=True)
class NewsletterSubjectFatigueExample:
    """One sent newsletter subject contributing to a fatigue finding."""

    newsletter_send_id: int
    issue_id: str
    subject: str
    sent_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterSubjectFatigueFinding:
    """A repeated subject pattern found in the reporting window."""

    finding_type: str
    pattern: str
    occurrence_count: int
    example_subjects: tuple[str, ...]
    examples: tuple[NewsletterSubjectFatigueExample, ...]
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "example_subjects": list(self.example_subjects),
            "examples": [example.to_dict() for example in self.examples],
            "finding_type": self.finding_type,
            "occurrence_count": self.occurrence_count,
            "pattern": self.pattern,
            "recommendation": self.recommendation,
        }


@dataclass(frozen=True)
class NewsletterSubjectFatigueReport:
    """Subject-line fatigue report plus filters and schema metadata."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    findings: tuple[NewsletterSubjectFatigueFinding, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    @property
    def has_findings(self) -> bool:
        return bool(self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_subject_fatigue",
            "filters": dict(self.filters),
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "has_findings": self.has_findings,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "subject_count": self.totals["subject_count"],
            "subject_finding_count": len(self.findings),
            "totals": dict(self.totals),
        }


def build_newsletter_subject_fatigue_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    threshold: int = DEFAULT_THRESHOLD,
    now: datetime | None = None,
) -> NewsletterSubjectFatigueReport:
    """Return repeated subject openings, terms, and structures for recent sends."""
    if days <= 0:
        raise ValueError("days must be positive")
    if threshold <= 0:
        raise ValueError("threshold must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "threshold": threshold,
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
    return _build_report_from_rows(
        sends,
        generated_at=generated_at,
        filters=filters,
        threshold=threshold,
        missing_tables=(),
        missing_columns={},
    )


def format_newsletter_subject_fatigue_json(
    report: NewsletterSubjectFatigueReport,
) -> str:
    """Serialize the subject fatigue report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_subject_fatigue_text(
    report: NewsletterSubjectFatigueReport,
) -> str:
    """Render the subject fatigue report for command-line review."""
    totals = report.totals
    lines = [
        "Newsletter Subject Fatigue",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['days']} days "
            f"threshold={report.filters['threshold']}"
        ),
        (
            "Totals: "
            f"subjects={totals['subject_count']} "
            f"findings={totals['finding_count']} "
            f"repeated_subjects={totals['repeated_subject_count']}"
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

    if not report.findings:
        lines.append("No newsletter subject fatigue patterns found.")
        return "\n".join(lines)

    lines.append("Subject fatigue findings:")
    for finding in report.findings:
        lines.append(
            f"  - {finding.finding_type}={finding.pattern!r} "
            f"count={finding.occurrence_count}"
        )
        lines.append(f"      recommendation: {finding.recommendation}")
        for example in finding.examples:
            lines.append(
                f"      send={example.newsletter_send_id} "
                f"issue={example.issue_id or '-'} "
                f"sent_at={example.sent_at or '-'} "
                f"subject={example.subject!r}"
            )
    return "\n".join(lines)


def _build_report_from_rows(
    sends: list[dict[str, Any]],
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    threshold: int,
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> NewsletterSubjectFatigueReport:
    examples = [
        NewsletterSubjectFatigueExample(
            newsletter_send_id=int(send["newsletter_send_id"]),
            issue_id=str(send.get("issue_id") or ""),
            subject=_collapse_spaces(send.get("subject") or ""),
            sent_at=send.get("sent_at"),
        )
        for send in sends
        if _collapse_spaces(send.get("subject") or "")
    ]

    findings: list[NewsletterSubjectFatigueFinding] = []
    findings.extend(
        _bucket_findings(
            examples,
            threshold=threshold,
            finding_type=REPEATED_OPENING,
            key_fn=_opening_phrase,
        )
    )
    findings.extend(_term_findings(examples, threshold=threshold))
    findings.extend(
        _bucket_findings(
            examples,
            threshold=threshold,
            finding_type=SIMILAR_STRUCTURE,
            key_fn=_structure_pattern,
        )
    )
    findings.sort(
        key=lambda finding: (
            -finding.occurrence_count,
            finding.finding_type,
            finding.pattern,
        )
    )
    repeated_send_ids = {
        example.newsletter_send_id
        for finding in findings
        for example in finding.examples
    }
    by_type: dict[str, int] = defaultdict(int)
    for finding in findings:
        by_type[finding.finding_type] += 1

    return NewsletterSubjectFatigueReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "subject_count": len(examples),
            "finding_count": len(findings),
            "repeated_subject_count": len(repeated_send_ids),
            "by_finding_type": dict(sorted(by_type.items())),
        },
        findings=tuple(findings),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _bucket_findings(
    examples: list[NewsletterSubjectFatigueExample],
    *,
    threshold: int,
    finding_type: str,
    key_fn: Any,
) -> list[NewsletterSubjectFatigueFinding]:
    buckets: dict[str, list[NewsletterSubjectFatigueExample]] = defaultdict(list)
    for example in examples:
        key = key_fn(example.subject)
        if key:
            buckets[key].append(example)
    return [
        _finding(finding_type, pattern, bucket)
        for pattern, bucket in buckets.items()
        if len(bucket) >= threshold
    ]


def _term_findings(
    examples: list[NewsletterSubjectFatigueExample],
    *,
    threshold: int,
) -> list[NewsletterSubjectFatigueFinding]:
    buckets: dict[str, list[NewsletterSubjectFatigueExample]] = defaultdict(list)
    for example in examples:
        for term in sorted(set(_terms(example.subject))):
            buckets[term].append(example)
    return [
        _finding(FREQUENT_TERM, term, bucket)
        for term, bucket in buckets.items()
        if len(bucket) >= threshold
    ]


def _finding(
    finding_type: str,
    pattern: str,
    examples: list[NewsletterSubjectFatigueExample],
) -> NewsletterSubjectFatigueFinding:
    ordered = tuple(
        sorted(
            examples,
            key=lambda example: (
                example.sent_at or "",
                example.newsletter_send_id,
            ),
            reverse=True,
        )
    )
    return NewsletterSubjectFatigueFinding(
        finding_type=finding_type,
        pattern=pattern,
        occurrence_count=len(ordered),
        example_subjects=tuple(_sample_subjects(ordered)),
        examples=ordered[:5],
        recommendation=_recommendation(finding_type, pattern),
    )


def _load_sends(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    columns = schema["newsletter_sends"]
    status_filter = "AND ns.status = 'sent'" if "status" in columns else ""
    rows = conn.execute(
        f"""SELECT
               ns.id AS newsletter_send_id,
               {_column_expr(columns, "issue_id", "''", alias="ns")} AS issue_id,
               ns.subject AS subject,
               ns.sent_at AS sent_at
           FROM newsletter_sends ns
           WHERE ns.sent_at >= ?
             {status_filter}
           ORDER BY ns.sent_at DESC, ns.id DESC""",
        (cutoff.isoformat(),),
    ).fetchall()
    return [dict(row) for row in rows]


def _opening_phrase(subject: str) -> str:
    text = _collapse_spaces(subject)
    match = _PUNCTUATION_RE.search(text)
    if match and match.start() > 0:
        opening = text[: match.start()]
    else:
        opening = " ".join(text.split()[:5])
    tokens = [token for token in _tokens(opening) if token not in _STOPWORDS]
    return " ".join(tokens[:3])


def _terms(subject: str) -> list[str]:
    return [
        token
        for token in _tokens(subject)
        if token not in _STOPWORDS and len(token) >= 3
    ]


def _structure_pattern(subject: str) -> str:
    words = _tokens(subject)
    punctuation = _punctuation_pattern(subject)
    if not punctuation or len(words) < 3:
        return ""
    if len(words) <= 5:
        length_bucket = "short"
    elif len(words) <= 9:
        length_bucket = "medium"
    else:
        length_bucket = "long"
    return f"{length_bucket} subject with punctuation {punctuation!r}"


def _punctuation_pattern(value: str) -> str:
    chars: list[str] = []
    for char in value:
        if char in ".!?;:,":
            chars.append(char)
        elif char in "-\u2013\u2014":
            chars.append("-")
    return "".join(chars[:8])


def _tokens(value: str) -> list[str]:
    return _TOKEN_RE.findall(str(value or "").lower().replace("\u2019", "'"))


def _sample_subjects(
    examples: tuple[NewsletterSubjectFatigueExample, ...],
) -> list[str]:
    subjects: list[str] = []
    seen: set[str] = set()
    for example in examples:
        if example.subject in seen:
            continue
        seen.add(example.subject)
        subjects.append(example.subject)
        if len(subjects) == 3:
            break
    return subjects


def _recommendation(finding_type: str, pattern: str) -> str:
    if finding_type == REPEATED_OPENING:
        return (
            f"Retire the opening '{pattern}' temporarily and lead the next subject "
            "with a more specific outcome, audience, or concrete noun."
        )
    if finding_type == FREQUENT_TERM:
        return (
            f"Replace or narrow the repeated term '{pattern}' so the subject line "
            "does not keep selling the same promise."
        )
    return (
        f"Break the repeated structure '{pattern}' with a different rhythm, word "
        "order, or punctuation-free subject."
    )


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {"newsletter_sends": {"id", "subject", "sent_at"}}
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
) -> NewsletterSubjectFatigueReport:
    return NewsletterSubjectFatigueReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "subject_count": 0,
            "finding_count": 0,
            "repeated_subject_count": 0,
            "by_finding_type": {},
        },
        findings=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


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


def _collapse_spaces(value: Any) -> str:
    return " ".join(str(value or "").split())
