"""Identify newsletter issues with weak click-through opportunity."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Mapping


DEFAULT_DAYS = 60
DEFAULT_LIMIT = 20
TARGET_CLICK_RATE = 0.03
TARGET_CLICK_TO_OPEN_RATE = 0.12
LOW_CLICK_RATE = 0.02
LOW_CLICK_TO_OPEN_RATE = 0.08
LOW_OPEN_RATE = 0.20

SUBJECT_BODY_CTA_REVIEW = "subject_body_cta_review"
LINK_PLACEMENT_REVIEW = "link_placement_review"
AUDIENCE_FIT_REVIEW = "audience_fit_review"
MISSING_METRICS = "missing_metrics"


@dataclass(frozen=True)
class NewsletterClickOpportunityIssue:
    """One newsletter send with computed click-through diagnostics."""

    newsletter_send_id: int
    issue_id: str
    subject: str
    sent_at: str | None
    subscriber_count: int | None
    opens: int | None
    clicks: int | None
    open_rate: float | None
    click_rate: float | None
    click_to_open_rate: float | None
    opportunity_score: float
    issue_codes: tuple[str, ...]
    recommendation: str
    fetched_at: str | None = None

    @property
    def flagged(self) -> bool:
        return bool(self.issue_codes and self.issue_codes != (MISSING_METRICS,))

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["issue_codes"] = list(self.issue_codes)
        payload["flagged"] = self.flagged
        return payload


@dataclass(frozen=True)
class NewsletterClickOpportunityReport:
    """Ranked click-through opportunity report for newsletter sends."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    issues: tuple[NewsletterClickOpportunityIssue, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def has_issues(self) -> bool:
        return any(issue.flagged for issue in self.issues)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_click_opportunity",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "issues": [issue.to_dict() for issue in self.issues],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def analyze_newsletter_click_opportunities(
    rows: list[Mapping[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
) -> tuple[NewsletterClickOpportunityIssue, ...]:
    """Return newsletter rows ranked by click-through review opportunity."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    issues = [_issue_from_row(row) for row in rows]
    issues.sort(
        key=lambda issue: (
            issue.flagged,
            issue.opportunity_score,
            issue.sent_at or "",
            issue.newsletter_send_id,
        ),
        reverse=True,
    )
    return tuple(issues[:limit])


def build_newsletter_click_opportunity_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> NewsletterClickOpportunityReport:
    """Build a local, read-only report from newsletter send engagement data."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"cutoff": cutoff.isoformat(), "days": days, "limit": limit}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)

    if missing_tables or missing_columns:
        return NewsletterClickOpportunityReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals={"flagged_issue_count": 0, "measured_issue_count": 0, "send_count": 0},
            issues=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_newsletter_rows(conn, schema, cutoff=cutoff)
    all_issues = analyze_newsletter_click_opportunities(
        rows,
        limit=max(len(rows), 1),
    )
    issues = all_issues[:limit]
    return NewsletterClickOpportunityReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "flagged_issue_count": sum(1 for issue in all_issues if issue.flagged),
            "measured_issue_count": sum(
                1 for issue in all_issues if issue.opens is not None
            ),
            "send_count": len(rows),
        },
        issues=issues,
        missing_tables=(),
        missing_columns={},
    )


def format_newsletter_click_opportunity_json(
    report: NewsletterClickOpportunityReport,
) -> str:
    """Format a click opportunity report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_click_opportunity_text(
    report: NewsletterClickOpportunityReport,
) -> str:
    """Format a concise markdown report for operators."""
    totals = report.totals
    filters = report.filters
    lines = [
        "# Newsletter Click-Through Opportunity",
        f"Generated: {report.generated_at}",
        f"Window: {filters['days']} days cutoff={filters['cutoff']} limit={filters['limit']}",
        (
            f"Totals: sends={totals['send_count']} measured={totals['measured_issue_count']} "
            f"flagged={totals['flagged_issue_count']}"
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

    flagged = [issue for issue in report.issues if issue.flagged]
    if not flagged:
        lines.append("No low-click newsletter issues found.")
        return "\n".join(lines)

    lines.append("## Prioritized Issues")
    for issue in flagged:
        lines.append(
            f"- send={issue.newsletter_send_id} issue={issue.issue_id or '-'} "
            f"score={issue.opportunity_score:.2f} subject={issue.subject or '-'}"
        )
        lines.append(
            "  metrics: "
            f"subscribers={issue.subscriber_count if issue.subscriber_count is not None else '-'} "
            f"opens={issue.opens if issue.opens is not None else '-'} "
            f"clicks={issue.clicks if issue.clicks is not None else '-'} "
            f"open_rate={_format_rate(issue.open_rate)} "
            f"click_rate={_format_rate(issue.click_rate)} "
            f"click_to_open={_format_rate(issue.click_to_open_rate)}"
        )
        lines.append(f"  issues={', '.join(issue.issue_codes)}")
        lines.append(f"  recommendation={issue.recommendation}")
    return "\n".join(lines)


def _issue_from_row(row: Mapping[str, Any]) -> NewsletterClickOpportunityIssue:
    subscribers = _optional_int(row.get("subscriber_count"))
    opens = _optional_int(row.get("opens"))
    clicks = _optional_int(row.get("clicks"))
    open_rate = _rate(opens, subscribers)
    click_rate = _rate(clicks, subscribers)
    click_to_open_rate = _rate(clicks, opens)
    issue_codes = _issue_codes(
        subscribers=subscribers,
        opens=opens,
        clicks=clicks,
        open_rate=open_rate,
        click_rate=click_rate,
        click_to_open_rate=click_to_open_rate,
    )
    score = _opportunity_score(
        subscribers=subscribers,
        opens=opens,
        clicks=clicks,
        open_rate=open_rate,
        issue_codes=issue_codes,
    )
    return NewsletterClickOpportunityIssue(
        newsletter_send_id=int(row.get("newsletter_send_id") or row.get("id")),
        issue_id=str(row.get("issue_id") or ""),
        subject=str(row.get("subject") or ""),
        sent_at=_text_or_none(row.get("sent_at")),
        subscriber_count=subscribers,
        opens=opens,
        clicks=clicks,
        open_rate=open_rate,
        click_rate=click_rate,
        click_to_open_rate=click_to_open_rate,
        opportunity_score=score,
        issue_codes=issue_codes,
        recommendation=_recommendation(issue_codes),
        fetched_at=_text_or_none(row.get("fetched_at")),
    )


def _issue_codes(
    *,
    subscribers: int | None,
    opens: int | None,
    clicks: int | None,
    open_rate: float | None,
    click_rate: float | None,
    click_to_open_rate: float | None,
) -> tuple[str, ...]:
    if subscribers is None or subscribers <= 0 or opens is None or clicks is None:
        return (MISSING_METRICS,)
    if opens <= 0:
        return (MISSING_METRICS,)

    codes = []
    if click_to_open_rate is not None and click_to_open_rate < LOW_CLICK_TO_OPEN_RATE:
        codes.append(SUBJECT_BODY_CTA_REVIEW)
        if clicks == 0 or (open_rate is not None and open_rate >= LOW_OPEN_RATE):
            codes.append(LINK_PLACEMENT_REVIEW)
    if click_rate is not None and click_rate < LOW_CLICK_RATE:
        if open_rate is not None and open_rate < LOW_OPEN_RATE:
            codes.append(AUDIENCE_FIT_REVIEW)
        elif LINK_PLACEMENT_REVIEW not in codes:
            codes.append(LINK_PLACEMENT_REVIEW)
    return tuple(dict.fromkeys(codes))


def _opportunity_score(
    *,
    subscribers: int | None,
    opens: int | None,
    clicks: int | None,
    open_rate: float | None,
    issue_codes: tuple[str, ...],
) -> float:
    if issue_codes == (MISSING_METRICS,) or not issue_codes:
        return 0.0
    subscriber_count = max(subscribers or 0, 0)
    open_count = max(opens or 0, 0)
    click_count = max(clicks or 0, 0)
    missed_from_opens = max(0.0, open_count * TARGET_CLICK_TO_OPEN_RATE - click_count)
    missed_from_subscribers = max(0.0, subscriber_count * TARGET_CLICK_RATE - click_count)
    open_interest = (open_rate or 0.0) * 10.0
    zero_click_bonus = 5.0 if click_count == 0 and open_count > 0 else 0.0
    return round(missed_from_opens + missed_from_subscribers + open_interest + zero_click_bonus, 2)


def _recommendation(issue_codes: tuple[str, ...]) -> str:
    if issue_codes == (MISSING_METRICS,):
        return "Refresh newsletter engagement metrics before diagnosing click-through performance."
    if AUDIENCE_FIT_REVIEW in issue_codes:
        return "Run an audience fit review before the next send, then tighten the subject/body CTA promise."
    if LINK_PLACEMENT_REVIEW in issue_codes:
        return "Run a link placement review and make the primary CTA visible earlier in the body."
    return "Run a subject/body CTA review so the opened issue clearly points readers to the next click."


def _load_newsletter_rows(
    conn: sqlite3.Connection,
    schema: Mapping[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    status_filter = ""
    if "status" in schema["newsletter_sends"]:
        status_filter = "AND COALESCE(ns.status, 'sent') NOT IN ('draft', 'queued')"
    query = f"""WITH latest_engagement AS (
                   SELECT ne.*
                   FROM newsletter_engagement ne
                   WHERE ne.id = (
                       SELECT latest.id
                       FROM newsletter_engagement latest
                       WHERE latest.newsletter_send_id = ne.newsletter_send_id
                          OR (
                              latest.newsletter_send_id IS NULL
                              AND ne.newsletter_send_id IS NULL
                              AND latest.issue_id = ne.issue_id
                          )
                       ORDER BY datetime(latest.fetched_at) DESC, latest.id DESC
                       LIMIT 1
                   )
               )
               SELECT ns.id AS newsletter_send_id,
                      ns.issue_id,
                      ns.subject,
                      ns.subscriber_count,
                      ns.sent_at,
                      le.opens,
                      le.clicks,
                      le.fetched_at
               FROM newsletter_sends ns
               LEFT JOIN latest_engagement le
                 ON le.newsletter_send_id = ns.id
               WHERE datetime(ns.sent_at) >= datetime(?)
                 {status_filter}
               ORDER BY datetime(ns.sent_at) DESC, ns.id DESC"""
    return [dict(row) for row in conn.execute(query, (cutoff.isoformat(),)).fetchall()]


def _schema_gaps(
    schema: Mapping[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "newsletter_sends": {"id", "issue_id", "subject", "subscriber_count", "sent_at"},
        "newsletter_engagement": {
            "id",
            "newsletter_send_id",
            "issue_id",
            "opens",
            "clicks",
            "fetched_at",
        },
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _rate(numerator: int | None, denominator: int | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text_or_none(value: Any) -> str | None:
    return str(value) if value is not None else None


def _format_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


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


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
