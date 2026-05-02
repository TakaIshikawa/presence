"""Triage recent newsletter sends for churn risk before the next send."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_BASELINE_DAYS = 60
DEFAULT_MIN_SENDS = 3
UNSUBSCRIBE_RATE_MULTIPLIER = 2.0
LOW_CLICK_RATE_MULTIPLIER = 0.5


@dataclass(frozen=True)
class NewsletterChurnTriageWindow:
    """One reporting window used by the triage report."""

    start: str
    end: str
    duration_days: int
    send_count: int
    measured_send_count: int


@dataclass(frozen=True)
class NewsletterChurnTriageBaseline:
    """Baseline engagement metrics for prior newsletter sends."""

    send_count: int
    measured_send_count: int
    min_sends: int
    sufficient: bool
    subscriber_count: int
    opens: int
    clicks: int
    unsubscribes: int
    complaints: int | None
    average_click_rate: float | None
    average_unsubscribe_rate: float | None
    average_complaint_rate: float | None


@dataclass(frozen=True)
class NewsletterChurnTriageReason:
    """A concrete reason an operator should review a send."""

    label: str
    severity: str
    detail: str
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterChurnTriageSend:
    """A recent newsletter send with one or more churn-risk signals."""

    newsletter_send_id: int
    issue_id: str
    subject: str
    sent_at: str
    subscriber_count: int
    opens: int | None
    clicks: int | None
    unsubscribes: int | None
    complaints: int | None
    click_rate: float | None
    unsubscribe_rate: float | None
    complaint_rate: float | None
    fetched_at: str | None
    reasons: tuple[NewsletterChurnTriageReason, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reasons"] = [reason.to_dict() for reason in self.reasons]
        return payload


@dataclass(frozen=True)
class NewsletterChurnTriageReport:
    """Read-only newsletter churn triage report."""

    generated_at: str
    filters: dict[str, Any]
    windows: dict[str, dict[str, Any]]
    baseline_metrics: dict[str, Any]
    totals: dict[str, Any]
    flagged_sends: tuple[NewsletterChurnTriageSend, ...]
    recommended_review_reasons: tuple[str, ...]
    availability: dict[str, bool]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "windows": self.windows,
            "baseline_metrics": dict(self.baseline_metrics),
            "totals": dict(self.totals),
            "flagged_sends": [send.to_dict() for send in self.flagged_sends],
            "recommended_review_reasons": list(self.recommended_review_reasons),
            "availability": dict(sorted(self.availability.items())),
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_newsletter_churn_triage_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    baseline_days: int = DEFAULT_BASELINE_DAYS,
    min_sends: int = DEFAULT_MIN_SENDS,
    now: datetime | None = None,
) -> NewsletterChurnTriageReport:
    """Build a deterministic churn triage report from stored Buttondown metrics."""
    if days <= 0:
        raise ValueError("days must be positive")
    if baseline_days <= 0:
        raise ValueError("baseline_days must be positive")
    if min_sends <= 0:
        raise ValueError("min_sends must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    recent_start = generated_at - timedelta(days=days)
    baseline_start = recent_start - timedelta(days=baseline_days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: set[str] = set()
    missing_columns: dict[str, tuple[str, ...]] = {}

    complaint_column = _complaint_column(schema.get("newsletter_engagement", set()))
    rows = _load_send_rows(
        conn,
        schema,
        start=baseline_start,
        end=generated_at,
        complaint_column=complaint_column,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    baseline_rows = [
        row for row in rows if baseline_start <= row["sent_at_dt"] < recent_start
    ]
    recent_rows = [
        row for row in rows if recent_start <= row["sent_at_dt"] <= generated_at
    ]
    baseline = _baseline(baseline_rows, min_sends=min_sends)
    flagged_sends = tuple(
        _flagged_sends(
            recent_rows,
            baseline=baseline,
            complaints_available=complaint_column is not None,
        )
    )
    reasons = tuple(
        sorted({reason.label for send in flagged_sends for reason in send.reasons})
    )
    baseline_window = NewsletterChurnTriageWindow(
        start=baseline_start.isoformat(),
        end=recent_start.isoformat(),
        duration_days=baseline_days,
        send_count=len(baseline_rows),
        measured_send_count=sum(1 for row in baseline_rows if row["has_metrics"]),
    )
    recent_window = NewsletterChurnTriageWindow(
        start=recent_start.isoformat(),
        end=generated_at.isoformat(),
        duration_days=days,
        send_count=len(recent_rows),
        measured_send_count=sum(1 for row in recent_rows if row["has_metrics"]),
    )

    return NewsletterChurnTriageReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "baseline_days": baseline_days,
            "min_sends": min_sends,
        },
        windows={
            "baseline": asdict(baseline_window),
            "recent": asdict(recent_window),
        },
        baseline_metrics=asdict(baseline),
        totals={
            "send_count": len(rows),
            "baseline_send_count": len(baseline_rows),
            "recent_send_count": len(recent_rows),
            "recent_measured_send_count": recent_window.measured_send_count,
            "flagged_send_count": len(flagged_sends),
        },
        flagged_sends=flagged_sends,
        recommended_review_reasons=reasons,
        availability={
            "newsletter_sends": "newsletter_sends" in schema,
            "newsletter_engagement": "newsletter_engagement" in schema,
            "complaints": complaint_column is not None,
        },
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns=missing_columns,
    )


def format_newsletter_churn_triage_json(report: NewsletterChurnTriageReport) -> str:
    """Serialize a churn triage report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_churn_triage_text(report: NewsletterChurnTriageReport) -> str:
    """Render a human-readable churn triage report."""
    baseline = report.baseline_metrics
    recent = report.windows["recent"]
    baseline_window = report.windows["baseline"]
    lines = [
        "Newsletter Churn Triage",
        f"Generated: {report.generated_at}",
        (
            f"Recent window: {recent['start']} -> {recent['end']} "
            f"({report.filters['days']} days)"
        ),
        (
            f"Baseline window: {baseline_window['start']} -> {baseline_window['end']} "
            f"({report.filters['baseline_days']} days)"
        ),
        f"Minimum baseline sends: {report.filters['min_sends']}",
        (
            "Availability: "
            + ", ".join(
                f"{name}={'yes' if value else 'no'}"
                for name, value in sorted(report.availability.items())
            )
        ),
        (
            "Totals: "
            f"recent_sends={report.totals['recent_send_count']} "
            f"recent_measured={report.totals['recent_measured_send_count']} "
            f"flagged={report.totals['flagged_send_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + details)

    lines.extend(
        [
            (
                "Baseline: "
                f"sends={baseline['send_count']} "
                f"measured={baseline['measured_send_count']} "
                f"sufficient={'yes' if baseline['sufficient'] else 'no'} "
                f"click_rate={_format_rate(baseline['average_click_rate'])} "
                f"unsubscribe_rate={_format_rate(baseline['average_unsubscribe_rate'])} "
                f"complaint_rate={_format_rate(baseline['average_complaint_rate'])}"
            ),
            "",
        ]
    )
    if report.totals["recent_send_count"] == 0:
        lines.append("No recent newsletter sends found for the selected window.")
        return "\n".join(lines)
    if report.totals["recent_measured_send_count"] == 0:
        lines.append("Recent newsletter sends have no engagement snapshots yet.")
        return "\n".join(lines)
    if not report.flagged_sends:
        lines.append("No newsletter churn triage signals detected.")
        return "\n".join(lines)

    lines.append("Flagged sends:")
    for send in report.flagged_sends:
        lines.append(
            f"- send={send.newsletter_send_id} issue={send.issue_id or '-'} "
            f"sent_at={send.sent_at} subject={send.subject or '-'}"
        )
        lines.append(
            "  metrics: "
            f"click_rate={_format_rate(send.click_rate)} "
            f"unsubscribe_rate={_format_rate(send.unsubscribe_rate)} "
            f"complaint_rate={_format_rate(send.complaint_rate)} "
            f"fetched_at={send.fetched_at or '-'}"
        )
        for reason in send.reasons:
            lines.append(
                f"  reason={reason.label} severity={reason.severity}: "
                f"{reason.detail}; recommendation={reason.recommendation}"
            )
    return "\n".join(lines)


def _load_send_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    start: datetime,
    end: datetime,
    complaint_column: str | None,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    send_table = "newsletter_sends"
    if send_table not in schema:
        missing_tables.add(send_table)
        return []
    send_required = ("id", "issue_id", "subject", "subscriber_count", "sent_at")
    send_missing = tuple(
        column for column in send_required if column not in schema[send_table]
    )
    if send_missing:
        missing_columns[send_table] = send_missing
        return []

    engagement_available = "newsletter_engagement" in schema
    engagement_required = (
        "id",
        "newsletter_send_id",
        "issue_id",
        "opens",
        "clicks",
        "unsubscribes",
        "fetched_at",
    )
    engagement_missing: tuple[str, ...] = ()
    if not engagement_available:
        missing_tables.add("newsletter_engagement")
    else:
        engagement_missing = tuple(
            column
            for column in engagement_required
            if column not in schema["newsletter_engagement"]
        )
        if engagement_missing:
            missing_columns["newsletter_engagement"] = engagement_missing

    if not engagement_available or engagement_missing:
        rows = _fetch_dicts(
            conn,
            """SELECT id AS newsletter_send_id, issue_id, subject,
                      subscriber_count, sent_at
               FROM newsletter_sends
               ORDER BY datetime(sent_at) ASC, id ASC""",
            (),
        )
        normalized = []
        for row in rows:
            item = _send_row(row, None, start=start, end=end)
            if item:
                normalized.append(item)
        return normalized

    complaint_select = (
        f"ne.{complaint_column} AS complaints" if complaint_column else "NULL AS complaints"
    )
    rows = _fetch_dicts(
        conn,
        f"""WITH latest_engagement AS (
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
                  ne.opens,
                  ne.clicks,
                  ne.unsubscribes,
                  {complaint_select},
                  ne.fetched_at
           FROM newsletter_sends ns
           LEFT JOIN latest_engagement ne
             ON ne.newsletter_send_id = ns.id
           ORDER BY datetime(ns.sent_at) ASC, ns.id ASC""",
        (),
    )
    normalized = []
    for row in rows:
        item = _send_row(row, row, start=start, end=end)
        if item:
            normalized.append(item)
    return normalized


def _send_row(
    send: dict[str, Any],
    metrics: dict[str, Any] | None,
    *,
    start: datetime,
    end: datetime,
) -> dict[str, Any] | None:
    sent_at = _parse_timestamp(send.get("sent_at"))
    if sent_at is None or sent_at < start or sent_at > end:
        return None
    subscriber_count = _optional_int(send.get("subscriber_count")) or 0
    has_metrics = bool(metrics and metrics.get("fetched_at") is not None)
    opens = _optional_int(metrics.get("opens")) if metrics else None
    clicks = _optional_int(metrics.get("clicks")) if metrics else None
    unsubscribes = _optional_int(metrics.get("unsubscribes")) if metrics else None
    complaints = _optional_int(metrics.get("complaints")) if metrics else None
    return {
        "newsletter_send_id": int(send["newsletter_send_id"]),
        "issue_id": str(send.get("issue_id") or ""),
        "subject": str(send.get("subject") or ""),
        "subscriber_count": subscriber_count,
        "sent_at": sent_at.isoformat(),
        "sent_at_dt": sent_at,
        "opens": opens,
        "clicks": clicks,
        "unsubscribes": unsubscribes,
        "complaints": complaints,
        "click_rate": _rate(clicks, subscriber_count),
        "unsubscribe_rate": _rate(unsubscribes, subscriber_count),
        "complaint_rate": _rate(complaints, subscriber_count),
        "fetched_at": _timestamp_text(metrics.get("fetched_at")) if metrics else None,
        "has_metrics": has_metrics,
    }


def _baseline(
    rows: list[dict[str, Any]],
    *,
    min_sends: int,
) -> NewsletterChurnTriageBaseline:
    measured = [row for row in rows if row["has_metrics"]]
    subscriber_count = sum(row["subscriber_count"] for row in measured)
    clicks = sum(row["clicks"] or 0 for row in measured)
    unsubscribes = sum(row["unsubscribes"] or 0 for row in measured)
    complaint_values = [
        row["complaints"] for row in measured if row["complaints"] is not None
    ]
    complaints = sum(complaint_values) if complaint_values else None
    click_rates = [row["click_rate"] for row in measured if row["click_rate"] is not None]
    unsubscribe_rates = [
        row["unsubscribe_rate"]
        for row in measured
        if row["unsubscribe_rate"] is not None
    ]
    complaint_rates = [
        row["complaint_rate"]
        for row in measured
        if row["complaint_rate"] is not None
    ]
    return NewsletterChurnTriageBaseline(
        send_count=len(rows),
        measured_send_count=len(measured),
        min_sends=min_sends,
        sufficient=len(measured) >= min_sends,
        subscriber_count=subscriber_count,
        opens=sum(row["opens"] or 0 for row in measured),
        clicks=clicks,
        unsubscribes=unsubscribes,
        complaints=complaints,
        average_click_rate=_average(click_rates),
        average_unsubscribe_rate=_average(unsubscribe_rates),
        average_complaint_rate=_average(complaint_rates),
    )


def _flagged_sends(
    rows: list[dict[str, Any]],
    *,
    baseline: NewsletterChurnTriageBaseline,
    complaints_available: bool,
) -> list[NewsletterChurnTriageSend]:
    flagged = []
    for row in rows:
        if not row["has_metrics"]:
            continue
        reasons = _reasons(row, baseline, complaints_available=complaints_available)
        if not reasons:
            continue
        flagged.append(
            NewsletterChurnTriageSend(
                newsletter_send_id=row["newsletter_send_id"],
                issue_id=row["issue_id"],
                subject=row["subject"],
                sent_at=row["sent_at"],
                subscriber_count=row["subscriber_count"],
                opens=row["opens"],
                clicks=row["clicks"],
                unsubscribes=row["unsubscribes"],
                complaints=row["complaints"],
                click_rate=row["click_rate"],
                unsubscribe_rate=row["unsubscribe_rate"],
                complaint_rate=row["complaint_rate"],
                fetched_at=row["fetched_at"],
                reasons=tuple(reasons),
            )
        )
    return sorted(
        flagged,
        key=lambda send: (
            -max(_severity_rank(reason.severity) for reason in send.reasons),
            send.sent_at,
            send.newsletter_send_id,
        ),
    )


def _reasons(
    row: dict[str, Any],
    baseline: NewsletterChurnTriageBaseline,
    *,
    complaints_available: bool,
) -> list[NewsletterChurnTriageReason]:
    reasons = []
    baseline_unsubscribe = baseline.average_unsubscribe_rate
    unsubscribe_rate = row["unsubscribe_rate"]
    unsubscribes = row["unsubscribes"] or 0
    if unsubscribe_rate is not None and unsubscribes > 0:
        threshold = None
        if baseline.sufficient and baseline_unsubscribe is not None:
            threshold = baseline_unsubscribe * UNSUBSCRIBE_RATE_MULTIPLIER
        if (
            (threshold is not None and unsubscribe_rate >= threshold)
            or (threshold is None and unsubscribes >= 2)
            or (baseline_unsubscribe == 0 and unsubscribes > 0)
        ):
            severity = (
                "high"
                if unsubscribes >= 3 or unsubscribe_rate >= 0.02
                else "medium"
            )
            detail = (
                f"unsubscribe rate {_format_rate(unsubscribe_rate)} "
                f"exceeds baseline {_format_rate(baseline_unsubscribe)}"
            )
            reasons.append(
                NewsletterChurnTriageReason(
                    label="unsubscribe_spike",
                    severity=severity,
                    detail=detail,
                    recommendation="review_audience_fit_and_pause_if_repeated",
                )
            )

    complaints = row["complaints"] or 0
    if complaints_available and complaints > 0:
        reasons.append(
            NewsletterChurnTriageReason(
                label="complaint_signal",
                severity="high",
                detail=(
                    f"{complaints} complaint(s), complaint rate "
                    f"{_format_rate(row['complaint_rate'])}"
                ),
                recommendation="review_compliance_and_suppression_before_next_send",
            )
        )

    click_rate = row["click_rate"]
    baseline_click = baseline.average_click_rate
    clicks = row["clicks"] or 0
    if click_rate is not None:
        low_click_threshold = (
            baseline_click * LOW_CLICK_RATE_MULTIPLIER
            if baseline.sufficient and baseline_click is not None
            else None
        )
        if (
            (low_click_threshold is not None and click_rate <= low_click_threshold)
            or (low_click_threshold is None and clicks == 0)
        ):
            reasons.append(
                NewsletterChurnTriageReason(
                    label="low_click_rate",
                    severity="medium",
                    detail=(
                        f"click rate {_format_rate(click_rate)} is below baseline "
                        f"{_format_rate(baseline_click)}"
                    ),
                    recommendation="review_subject_content_match_and_link_prominence",
                )
            )
    return reasons


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


def _fetch_dicts(
    conn: sqlite3.Connection,
    query: str,
    params: tuple[Any, ...] | list[Any],
) -> list[dict[str, Any]]:
    cursor = conn.execute(query, params)
    names = [column[0] for column in cursor.description or ()]
    return [
        {
            name: row[name] if hasattr(row, "keys") else row[index]
            for index, name in enumerate(names)
        }
        for row in cursor.fetchall()
    ]


def _complaint_column(columns: set[str]) -> str | None:
    for column in ("complaints", "spam_complaints", "abuse_reports"):
        if column in columns:
            return column
    return None


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _as_utc(parsed)


def _timestamp_text(value: Any) -> str | None:
    parsed = _parse_timestamp(value)
    return parsed.isoformat() if parsed else None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _rate(numerator: int | None, denominator: int) -> float | None:
    if numerator is None or denominator <= 0:
        return None
    return round(numerator / denominator, 6)


def _average(values: list[float | None]) -> float | None:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return round(sum(filtered) / len(filtered), 6)


def _severity_rank(value: str) -> int:
    return {"low": 1, "medium": 2, "high": 3}.get(value, 0)


def _format_rate(value: float | None) -> str:
    return "-" if value is None else f"{value * 100:.2f}%"
