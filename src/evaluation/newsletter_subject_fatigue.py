"""Detect fatigue in recent newsletter subject-line patterns."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
import json
import re
import sqlite3
from typing import Any, Mapping


DEFAULT_DAYS = 90
DEFAULT_THRESHOLD = 3
NEAR_DUPLICATE_RATIO = 0.86

OPENING = "opening"
PUNCTUATION = "punctuation"
NEAR_DUPLICATE = "near_duplicate"

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_PUNCT_RE = re.compile(r"[!?]+|[:;]+|[-]+|[.]{2,}")
_STOPWORDS = {
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
    "in",
    "inside",
    "into",
    "is",
    "of",
    "on",
    "or",
    "our",
    "the",
    "this",
    "to",
    "with",
    "your",
}


@dataclass(frozen=True)
class SubjectFatigueExample:
    """One subject contributing to a fatigue finding."""

    subject: str
    issue_id: str | None = None
    newsletter_send_id: int | None = None
    candidate_id: int | None = None
    selected: bool = False
    sent_at: str | None = None
    open_rate: float | None = None
    click_rate: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "click_rate": self.click_rate,
            "issue_id": self.issue_id,
            "newsletter_send_id": self.newsletter_send_id,
            "open_rate": self.open_rate,
            "selected": self.selected,
            "sent_at": self.sent_at,
            "subject": self.subject,
        }


@dataclass(frozen=True)
class SubjectFatigueFinding:
    """A repeated subject-line pattern and its engagement context."""

    pattern_type: str
    pattern: str
    occurrences: int
    selected_occurrences: int
    average_open_rate: float | None
    average_click_rate: float | None
    open_rate_delta: float | None
    click_rate_delta: float | None
    fatigue_score: float
    guidance: str
    examples: list[SubjectFatigueExample] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "average_click_rate": self.average_click_rate,
            "average_open_rate": self.average_open_rate,
            "click_rate_delta": self.click_rate_delta,
            "examples": [example.to_dict() for example in self.examples],
            "fatigue_score": self.fatigue_score,
            "guidance": self.guidance,
            "occurrences": self.occurrences,
            "open_rate_delta": self.open_rate_delta,
            "pattern": self.pattern,
            "pattern_type": self.pattern_type,
            "selected_occurrences": self.selected_occurrences,
        }


@dataclass(frozen=True)
class SubjectFatigueReport:
    """Subject fatigue findings for a reporting window."""

    period_days: int
    threshold: int
    candidate_count: int
    selected_send_count: int
    finding_count: int
    findings: list[SubjectFatigueFinding]

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_count": self.candidate_count,
            "finding_count": self.finding_count,
            "findings": [finding.to_dict() for finding in self.findings],
            "period_days": self.period_days,
            "selected_send_count": self.selected_send_count,
            "threshold": self.threshold,
        }


def build_newsletter_subject_fatigue_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    threshold: int = DEFAULT_THRESHOLD,
    now: datetime | None = None,
) -> SubjectFatigueReport:
    """Return subject-line fatigue findings for recent candidates and sends."""
    if days <= 0:
        raise ValueError("days must be positive")
    if threshold <= 1:
        raise ValueError("threshold must be greater than 1")

    conn = _connection(db_or_conn)
    if not _has_tables(conn, "newsletter_subject_candidates", "newsletter_sends"):
        return SubjectFatigueReport(days, threshold, 0, 0, 0, [])

    cutoff = _as_utc(now or datetime.now(timezone.utc)) - timedelta(days=days)
    rows = _fetch_subject_rows(conn, cutoff=cutoff)
    examples = [_example_from_row(row) for row in rows]
    selected = [example for example in examples if example.selected]

    findings = []
    findings.extend(_opening_findings(examples, threshold))
    findings.extend(_punctuation_findings(examples, threshold))
    findings.extend(_near_duplicate_findings(examples))
    findings.sort(
        key=lambda finding: (
            -finding.fatigue_score,
            finding.pattern_type,
            finding.pattern,
        )
    )

    return SubjectFatigueReport(
        period_days=days,
        threshold=threshold,
        candidate_count=len(examples),
        selected_send_count=len({item.newsletter_send_id for item in selected}),
        finding_count=len(findings),
        findings=findings,
    )


def format_newsletter_subject_fatigue_json(report: SubjectFatigueReport) -> str:
    """Format a fatigue report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_subject_fatigue_text(report: SubjectFatigueReport) -> str:
    """Format a fatigue report for terminal review."""
    lines = [
        "Newsletter Subject Fatigue Report",
        f"Window: {report.period_days} days; threshold: {report.threshold}",
        (
            f"Candidates: {report.candidate_count}; "
            f"selected sends: {report.selected_send_count}; "
            f"findings: {report.finding_count}"
        ),
        "",
    ]
    if not report.findings:
        lines.append("No repeated subject patterns met the fatigue criteria.")
        return "\n".join(lines)

    for index, finding in enumerate(report.findings, start=1):
        lines.append(
            f"{index}. {finding.pattern_type}: {finding.pattern} "
            f"({finding.occurrences} uses, {finding.selected_occurrences} selected, "
            f"score {finding.fatigue_score:.2f})"
        )
        lines.append(
            "   Engagement: "
            f"opens {_format_rate(finding.average_open_rate)} "
            f"({_format_delta(finding.open_rate_delta)}), "
            f"clicks {_format_rate(finding.average_click_rate)} "
            f"({_format_delta(finding.click_rate_delta)})"
        )
        lines.append(f"   Guidance: {finding.guidance}")
        for example in finding.examples[:3]:
            metrics = (
                f"open {_format_rate(example.open_rate)}, "
                f"click {_format_rate(example.click_rate)}"
                if example.selected
                else "candidate only"
            )
            lines.append(f"   - {example.subject} ({metrics})")
        lines.append("")
    return "\n".join(lines).rstrip()


def _opening_findings(
    examples: list[SubjectFatigueExample], threshold: int
) -> list[SubjectFatigueFinding]:
    buckets: dict[str, list[SubjectFatigueExample]] = {}
    for example in examples:
        opening = _opening_pattern(example.subject)
        if opening:
            buckets.setdefault(opening, []).append(example)
    return [
        _finding(OPENING, pattern, items)
        for pattern, items in buckets.items()
        if len(items) >= threshold
    ]


def _punctuation_findings(
    examples: list[SubjectFatigueExample], threshold: int
) -> list[SubjectFatigueFinding]:
    buckets: dict[str, list[SubjectFatigueExample]] = {}
    for example in examples:
        pattern = _punctuation_pattern(example.subject)
        if pattern:
            buckets.setdefault(pattern, []).append(example)
    return [
        _finding(PUNCTUATION, pattern, items)
        for pattern, items in buckets.items()
        if len(items) >= threshold
    ]


def _near_duplicate_findings(
    examples: list[SubjectFatigueExample],
) -> list[SubjectFatigueFinding]:
    normalized: list[tuple[SubjectFatigueExample, str]] = [
        (example, _normalize_subject(example.subject)) for example in examples
    ]
    used: set[int] = set()
    findings: list[SubjectFatigueFinding] = []
    for index, (example, normalized_subject) in enumerate(normalized):
        if index in used or not normalized_subject:
            continue
        cluster = [example]
        cluster_indexes = {index}
        for other_index, (other, other_subject) in enumerate(
            normalized[index + 1 :],
            start=index + 1,
        ):
            if other_index in used or not other_subject:
                continue
            ratio = SequenceMatcher(None, normalized_subject, other_subject).ratio()
            if ratio >= NEAR_DUPLICATE_RATIO:
                cluster.append(other)
                cluster_indexes.add(other_index)
        if len(cluster) > 1:
            used.update(cluster_indexes)
            findings.append(_finding(NEAR_DUPLICATE, cluster[0].subject, cluster))
    return findings


def _finding(
    pattern_type: str,
    pattern: str,
    examples: list[SubjectFatigueExample],
) -> SubjectFatigueFinding:
    ordered = sorted(
        examples,
        key=lambda item: (
            item.sent_at or "",
            item.newsletter_send_id or 0,
            item.candidate_id or 0,
        ),
    )
    selected = [item for item in ordered if item.selected]
    open_rates = [item.open_rate for item in selected if item.open_rate is not None]
    click_rates = [item.click_rate for item in selected if item.click_rate is not None]
    open_delta = _rate_delta(selected, "open_rate")
    click_delta = _rate_delta(selected, "click_rate")
    score = len(ordered) + (len(selected) * 0.5)
    if open_delta is not None and open_delta < 0:
        score += abs(open_delta) * 100
    if click_delta is not None and click_delta < 0:
        score += abs(click_delta) * 300

    return SubjectFatigueFinding(
        pattern_type=pattern_type,
        pattern=pattern,
        occurrences=len(ordered),
        selected_occurrences=len(selected),
        average_open_rate=_average(open_rates),
        average_click_rate=_average(click_rates),
        open_rate_delta=open_delta,
        click_rate_delta=click_delta,
        fatigue_score=round(score, 2),
        guidance=_guidance(pattern_type, pattern),
        examples=list(reversed(ordered))[:5],
    )


def _fetch_subject_rows(
    conn: sqlite3.Connection, *, cutoff: datetime
) -> list[dict[str, Any]]:
    engagement_join = ""
    engagement_select = (
        "NULL AS opens, NULL AS clicks, NULL AS unsubscribes, NULL AS fetched_at"
    )
    if _has_tables(conn, "newsletter_engagement"):
        engagement_select = "ne.opens, ne.clicks, ne.unsubscribes, ne.fetched_at"
        engagement_join = """
               LEFT JOIN latest_engagement ne
                 ON ne.newsletter_send_id = ns.id"""
        cte = """WITH latest_engagement AS (
                   SELECT ne.*
                   FROM newsletter_engagement ne
                   WHERE ne.id = (
                       SELECT latest.id
                       FROM newsletter_engagement latest
                       WHERE latest.newsletter_send_id = ne.newsletter_send_id
                       ORDER BY datetime(latest.fetched_at) DESC, latest.id DESC
                       LIMIT 1
                   )
               )"""
    else:
        cte = ""

    query = f"""{cte}
               SELECT c.id AS candidate_id,
                      c.newsletter_send_id,
                      c.issue_id,
                      c.subject,
                      c.selected,
                      c.created_at,
                      ns.subscriber_count,
                      ns.sent_at,
                      {engagement_select}
               FROM newsletter_subject_candidates c
               LEFT JOIN newsletter_sends ns
                 ON ns.id = c.newsletter_send_id
               {engagement_join}
               WHERE datetime(COALESCE(ns.sent_at, c.created_at)) >= datetime(?)
               ORDER BY datetime(COALESCE(ns.sent_at, c.created_at)) DESC, c.id DESC"""
    cursor = conn.execute(query, (cutoff.isoformat(),))
    return [dict(row) for row in cursor.fetchall()]


def _example_from_row(row: Mapping[str, Any]) -> SubjectFatigueExample:
    subscriber_count = int(row.get("subscriber_count") or 0)
    opens = row.get("opens")
    clicks = row.get("clicks")
    return SubjectFatigueExample(
        subject=str(row.get("subject") or ""),
        issue_id=row.get("issue_id"),
        newsletter_send_id=_optional_int(row.get("newsletter_send_id")),
        candidate_id=_optional_int(row.get("candidate_id")),
        selected=bool(row.get("selected")),
        sent_at=row.get("sent_at") or row.get("created_at"),
        open_rate=_rate(_optional_int(opens), subscriber_count),
        click_rate=_rate(_optional_int(clicks), subscriber_count),
    )


def _opening_pattern(subject: str) -> str | None:
    tokens = [token for token in _tokens(subject) if token not in _STOPWORDS]
    if len(tokens) >= 2:
        return " ".join(tokens[:2])
    return tokens[0] if tokens else None


def _punctuation_pattern(subject: str) -> str | None:
    pieces = _PUNCT_RE.findall(subject)
    if not pieces:
        return None
    return " ".join(pieces)


def _normalize_subject(subject: str) -> str:
    return " ".join(_tokens(subject))


def _tokens(value: str) -> list[str]:
    return _TOKEN_RE.findall((value or "").lower())


def _rate(numerator: int | None, denominator: int) -> float | None:
    if numerator is None or denominator <= 0:
        return None
    return numerator / denominator


def _rate_delta(examples: list[SubjectFatigueExample], attr: str) -> float | None:
    rates = [
        getattr(example, attr)
        for example in examples
        if getattr(example, attr) is not None
    ]
    if len(rates) < 2:
        return None
    midpoint = len(rates) // 2
    early = rates[:midpoint]
    recent = rates[midpoint:]
    early_average = _average(early)
    recent_average = _average(recent)
    if early_average is None or recent_average is None:
        return None
    return round(recent_average - early_average, 4)


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _guidance(pattern_type: str, pattern: str) -> str:
    if pattern_type == OPENING:
        return (
            f"Retire the opening '{pattern}' for the next send; lead with a concrete "
            "outcome, audience, or fresh noun instead."
        )
    if pattern_type == PUNCTUATION:
        return (
            f"Replace the repeated punctuation shape '{pattern}' with a plain subject "
            "or a different rhythm that does not rely on the same hook."
        )
    return (
        "Rewrite one of the near-duplicate candidates around a different promise, "
        "specific detail, or reader benefit before selecting it."
    )


def _format_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _format_delta(value: float | None) -> str:
    if value is None:
        return "delta n/a"
    return f"delta {value * 100:+.1f}pp"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _has_tables(conn: sqlite3.Connection, *tables: str) -> bool:
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return all(table in existing for table in tables)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
