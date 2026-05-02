"""Detect abnormal newsletter unsubscribe and churn spikes."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from statistics import mean
from typing import Any


DEFAULT_DAYS = 7
DEFAULT_BASELINE_DAYS = 28
DEFAULT_MIN_UNSUBSCRIBES = 3
SPIKE_MULTIPLIER = 2.0
HIGH_SPIKE_MULTIPLIER = 4.0
MEDIUM_SPIKE_MULTIPLIER = 3.0


@dataclass(frozen=True)
class NewsletterChurnWindow:
    """Aggregate subscriber metrics for one comparison window."""

    start: str
    end: str
    duration_days: int
    snapshot_count: int
    first_snapshot_at: str | None
    latest_snapshot_at: str | None
    first_unsubscribes: int | None
    latest_unsubscribes: int | None
    unsubscribe_total: int | None
    daily_unsubscribes: float | None
    average_churn_rate: float | None
    first_subscriber_count: int | None
    latest_subscriber_count: int | None
    subscriber_delta: int | None
    first_active_subscriber_count: int | None
    latest_active_subscriber_count: int | None
    active_subscriber_delta: int | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterChurnContribution:
    """Newsletter send that contributed unsubscribe data in the recent window."""

    newsletter_send_id: int | None
    issue_id: str | None
    unsubscribes: int
    fetched_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterChurnSpike:
    """Detected spike with a concrete baseline comparison."""

    label: str
    severity: str
    recent_unsubscribes: int
    baseline_unsubscribes: int
    recent_daily_unsubscribes: float
    baseline_daily_unsubscribes: float
    daily_unsubscribe_delta: float
    daily_unsubscribe_ratio: float | None
    recent_average_churn_rate: float | None
    baseline_average_churn_rate: float | None
    churn_rate_delta: float | None
    comparison: str
    contributing_sends: tuple[NewsletterChurnContribution, ...]
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["contributing_sends"] = [
            send.to_dict() for send in self.contributing_sends
        ]
        return payload


@dataclass(frozen=True)
class NewsletterChurnSpikeReport:
    """Read-only newsletter churn spike report."""

    generated_at: str
    filters: dict[str, Any]
    windows: dict[str, dict[str, Any]]
    totals: dict[str, Any]
    spikes: tuple[NewsletterChurnSpike, ...]
    recommendations: tuple[str, ...]
    availability: dict[str, bool]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None
    empty_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": self.filters,
            "windows": self.windows,
            "totals": self.totals,
            "spikes": [spike.to_dict() for spike in self.spikes],
            "recommendations": list(self.recommendations),
            "availability": dict(sorted(self.availability.items())),
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "empty_reason": self.empty_reason,
        }


def build_newsletter_churn_spike_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    baseline_days: int = DEFAULT_BASELINE_DAYS,
    min_unsubscribes: int = DEFAULT_MIN_UNSUBSCRIBES,
    now: datetime | None = None,
) -> NewsletterChurnSpikeReport:
    """Compare recent unsubscribe/churn metrics against a prior baseline."""
    if days <= 0:
        raise ValueError("days must be positive")
    if baseline_days <= 0:
        raise ValueError("baseline_days must be positive")
    if min_unsubscribes <= 0:
        raise ValueError("min_unsubscribes must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    recent_start = generated_at - timedelta(days=days)
    baseline_start = recent_start - timedelta(days=baseline_days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: set[str] = set()
    missing_columns: dict[str, tuple[str, ...]] = {}

    rows = _load_subscriber_snapshots(
        conn,
        schema,
        start=baseline_start,
        end=generated_at,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    baseline_rows = [
        row for row in rows if baseline_start <= row["fetched_at"] < recent_start
    ]
    recent_rows = [
        row for row in rows if recent_start <= row["fetched_at"] <= generated_at
    ]
    baseline_window = _build_window(
        baseline_rows,
        start=baseline_start,
        end=recent_start,
        duration_days=baseline_days,
    )
    recent_window = _build_window(
        recent_rows,
        start=recent_start,
        end=generated_at,
        duration_days=days,
    )
    contributions = _load_recent_contributions(
        conn,
        schema,
        start=recent_start,
        end=generated_at,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    empty_reason = _empty_reason(
        schema=schema,
        missing_columns=missing_columns,
        baseline_window=baseline_window,
        recent_window=recent_window,
    )
    spikes = (
        ()
        if empty_reason
        else tuple(
            _detect_spikes(
                baseline_window=baseline_window,
                recent_window=recent_window,
                min_unsubscribes=min_unsubscribes,
                contributions=contributions,
            )
        )
    )
    recommendations = tuple(spike.recommendation for spike in spikes)

    return NewsletterChurnSpikeReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "baseline_days": baseline_days,
            "min_unsubscribes": min_unsubscribes,
        },
        windows={
            "baseline": baseline_window.to_dict(),
            "recent": recent_window.to_dict(),
        },
        totals={
            "snapshot_count": len(rows),
            "baseline_snapshot_count": baseline_window.snapshot_count,
            "recent_snapshot_count": recent_window.snapshot_count,
            "baseline_unsubscribes": baseline_window.unsubscribe_total,
            "recent_unsubscribes": recent_window.unsubscribe_total,
            "spike_count": len(spikes),
            "contributing_send_count": len(contributions),
        },
        spikes=spikes,
        recommendations=recommendations,
        availability={
            "newsletter_subscriber_metrics": "newsletter_subscriber_metrics" in schema,
            "newsletter_engagement": "newsletter_engagement" in schema,
        },
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns=missing_columns,
        empty_reason=empty_reason,
    )


def format_newsletter_churn_spike_json(report: NewsletterChurnSpikeReport) -> str:
    """Serialize a churn spike report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_churn_spike_text(report: NewsletterChurnSpikeReport) -> str:
    """Format a churn spike report for cron logs."""
    baseline = report.windows["baseline"]
    recent = report.windows["recent"]
    lines = [
        "Newsletter Churn Spikes",
        f"Generated: {report.generated_at}",
        (
            f"Recent window: {recent['start']} -> {recent['end']} "
            f"({report.filters['days']} days)"
        ),
        (
            f"Baseline window: {baseline['start']} -> {baseline['end']} "
            f"({report.filters['baseline_days']} days)"
        ),
        f"Minimum unsubscribes: {report.filters['min_unsubscribes']}",
        (
            "Availability: "
            + ", ".join(
                f"{name}={'yes' if value else 'no'}"
                for name, value in sorted(report.availability.items())
            )
        ),
        (
            "Totals: "
            f"snapshots={report.totals['snapshot_count']} "
            f"recent_unsubscribes={_format_int(report.totals['recent_unsubscribes'])} "
            f"baseline_unsubscribes={_format_int(report.totals['baseline_unsubscribes'])} "
            f"spikes={report.totals['spike_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing optional tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + details)
    if report.empty_reason:
        lines.append(f"Empty report: {report.empty_reason}")
        return "\n".join(lines)

    lines.extend(
        [
            (
                "Baseline: "
                f"snapshots={baseline['snapshot_count']} "
                f"unsubscribes={_format_int(baseline['unsubscribe_total'])} "
                f"daily={_format_float(baseline['daily_unsubscribes'])} "
                f"avg_churn={_format_rate(baseline['average_churn_rate'])}"
            ),
            (
                "Recent: "
                f"snapshots={recent['snapshot_count']} "
                f"unsubscribes={_format_int(recent['unsubscribe_total'])} "
                f"daily={_format_float(recent['daily_unsubscribes'])} "
                f"avg_churn={_format_rate(recent['average_churn_rate'])}"
            ),
        ]
    )
    if not report.spikes:
        lines.append("No newsletter churn spikes detected.")
        return "\n".join(lines)

    lines.append("Spikes:")
    for spike in report.spikes:
        lines.append(
            f"- {spike.severity} {spike.label}: {spike.comparison}; "
            f"recommendation={spike.recommendation}"
        )
        for send in spike.contributing_sends:
            lines.append(
                "  send="
                f"{send.newsletter_send_id if send.newsletter_send_id is not None else '-'} "
                f"issue={send.issue_id or '-'} "
                f"unsubscribes={send.unsubscribes} fetched_at={send.fetched_at}"
            )
    return "\n".join(lines)


def _load_subscriber_snapshots(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    start: datetime,
    end: datetime,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    table = "newsletter_subscriber_metrics"
    if table not in schema:
        missing_tables.add(table)
        return []
    required = (
        "id",
        "subscriber_count",
        "active_subscriber_count",
        "unsubscribes",
        "churn_rate",
        "fetched_at",
    )
    missing = tuple(column for column in required if column not in schema[table])
    if missing:
        missing_columns[table] = missing
        return []
    rows = _fetch_dicts(
        conn,
        """SELECT id, subscriber_count, active_subscriber_count, unsubscribes,
                  churn_rate, fetched_at
           FROM newsletter_subscriber_metrics
           ORDER BY datetime(fetched_at) ASC, id ASC""",
        (),
    )
    snapshots = []
    for row in rows:
        fetched_at = _parse_timestamp(row.get("fetched_at"))
        if fetched_at is None or fetched_at < start or fetched_at > end:
            continue
        snapshots.append(
            {
                "id": int(row["id"]),
                "subscriber_count": _optional_int(row.get("subscriber_count")),
                "active_subscriber_count": _optional_int(
                    row.get("active_subscriber_count")
                ),
                "unsubscribes": _optional_int(row.get("unsubscribes")),
                "churn_rate": _optional_float(row.get("churn_rate")),
                "fetched_at": fetched_at,
            }
        )
    return snapshots


def _load_recent_contributions(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    start: datetime,
    end: datetime,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> tuple[NewsletterChurnContribution, ...]:
    table = "newsletter_engagement"
    if table not in schema:
        missing_tables.add(table)
        return ()
    required = ("id", "newsletter_send_id", "issue_id", "unsubscribes", "fetched_at")
    missing = tuple(column for column in required if column not in schema[table])
    if missing:
        missing_columns[table] = missing
        return ()
    rows = _fetch_dicts(
        conn,
        """SELECT ne.newsletter_send_id, ne.issue_id, ne.unsubscribes, ne.fetched_at
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
           ORDER BY datetime(ne.fetched_at) ASC, ne.newsletter_send_id ASC, ne.issue_id ASC""",
        (),
    )
    contributions = []
    for row in rows:
        fetched_at = _parse_timestamp(row.get("fetched_at"))
        unsubscribes = _optional_int(row.get("unsubscribes")) or 0
        if fetched_at is None or fetched_at < start or fetched_at > end:
            continue
        if unsubscribes <= 0:
            continue
        send_id = _optional_int(row.get("newsletter_send_id"))
        contributions.append(
            NewsletterChurnContribution(
                newsletter_send_id=send_id,
                issue_id=str(row["issue_id"]) if row.get("issue_id") is not None else None,
                unsubscribes=unsubscribes,
                fetched_at=fetched_at.isoformat(),
            )
        )
    return tuple(contributions)


def _build_window(
    rows: list[dict[str, Any]],
    *,
    start: datetime,
    end: datetime,
    duration_days: int,
) -> NewsletterChurnWindow:
    first = rows[0] if rows else None
    latest = rows[-1] if rows else None
    unsubscribe_total = None
    if first and latest:
        unsubscribe_total = _delta(first["unsubscribes"], latest["unsubscribes"])
        if unsubscribe_total is not None:
            unsubscribe_total = max(unsubscribe_total, 0)
    churn_values = [
        row["churn_rate"] for row in rows if row.get("churn_rate") is not None
    ]
    average_churn_rate = (
        round(mean(churn_values), 6) if churn_values else None
    )
    daily_unsubscribes = (
        round(unsubscribe_total / duration_days, 6)
        if unsubscribe_total is not None
        else None
    )
    return NewsletterChurnWindow(
        start=start.isoformat(),
        end=end.isoformat(),
        duration_days=duration_days,
        snapshot_count=len(rows),
        first_snapshot_at=first["fetched_at"].isoformat() if first else None,
        latest_snapshot_at=latest["fetched_at"].isoformat() if latest else None,
        first_unsubscribes=first["unsubscribes"] if first else None,
        latest_unsubscribes=latest["unsubscribes"] if latest else None,
        unsubscribe_total=unsubscribe_total,
        daily_unsubscribes=daily_unsubscribes,
        average_churn_rate=average_churn_rate,
        first_subscriber_count=first["subscriber_count"] if first else None,
        latest_subscriber_count=latest["subscriber_count"] if latest else None,
        subscriber_delta=_delta(
            first["subscriber_count"] if first else None,
            latest["subscriber_count"] if latest else None,
        ),
        first_active_subscriber_count=(
            first["active_subscriber_count"] if first else None
        ),
        latest_active_subscriber_count=(
            latest["active_subscriber_count"] if latest else None
        ),
        active_subscriber_delta=_delta(
            first["active_subscriber_count"] if first else None,
            latest["active_subscriber_count"] if latest else None,
        ),
    )


def _detect_spikes(
    *,
    baseline_window: NewsletterChurnWindow,
    recent_window: NewsletterChurnWindow,
    min_unsubscribes: int,
    contributions: tuple[NewsletterChurnContribution, ...],
) -> list[NewsletterChurnSpike]:
    recent_unsubscribes = recent_window.unsubscribe_total or 0
    baseline_unsubscribes = baseline_window.unsubscribe_total or 0
    recent_daily = recent_window.daily_unsubscribes or 0.0
    baseline_daily = baseline_window.daily_unsubscribes or 0.0
    ratio = None
    if baseline_unsubscribes > 0:
        ratio = round(
            (recent_unsubscribes / recent_window.duration_days)
            / (baseline_unsubscribes / baseline_window.duration_days),
            4,
        )
    churn_delta = None
    if (
        recent_window.average_churn_rate is not None
        and baseline_window.average_churn_rate is not None
    ):
        churn_delta = round(
            recent_window.average_churn_rate - baseline_window.average_churn_rate,
            6,
        )
    if recent_unsubscribes < min_unsubscribes:
        return []
    if baseline_daily > 0 and recent_daily < baseline_daily * SPIKE_MULTIPLIER:
        return []
    if baseline_daily == 0 and recent_unsubscribes == 0:
        return []

    severity = _severity(
        recent_unsubscribes=recent_unsubscribes,
        min_unsubscribes=min_unsubscribes,
        daily_ratio=ratio,
        recent_churn=recent_window.average_churn_rate,
    )
    comparison = _comparison(
        recent_unsubscribes=recent_unsubscribes,
        baseline_unsubscribes=baseline_unsubscribes,
        recent_daily=recent_daily,
        baseline_daily=baseline_daily,
        ratio=ratio,
        churn_delta=churn_delta,
    )
    recommendation = _recommendation(severity, contributions)
    return [
        NewsletterChurnSpike(
            label="unsubscribe_spike",
            severity=severity,
            recent_unsubscribes=recent_unsubscribes,
            baseline_unsubscribes=baseline_unsubscribes,
            recent_daily_unsubscribes=recent_daily,
            baseline_daily_unsubscribes=baseline_daily,
            daily_unsubscribe_delta=round(recent_daily - baseline_daily, 6),
            daily_unsubscribe_ratio=ratio,
            recent_average_churn_rate=recent_window.average_churn_rate,
            baseline_average_churn_rate=baseline_window.average_churn_rate,
            churn_rate_delta=churn_delta,
            comparison=comparison,
            contributing_sends=contributions,
            recommendation=recommendation,
        )
    ]


def _empty_reason(
    *,
    schema: dict[str, set[str]],
    missing_columns: dict[str, tuple[str, ...]],
    baseline_window: NewsletterChurnWindow,
    recent_window: NewsletterChurnWindow,
) -> str | None:
    if "newsletter_subscriber_metrics" not in schema:
        return "newsletter_subscriber_metrics table is not available"
    if "newsletter_subscriber_metrics" in missing_columns:
        columns = ", ".join(missing_columns["newsletter_subscriber_metrics"])
        return f"newsletter_subscriber_metrics is missing required columns: {columns}"
    if recent_window.snapshot_count < 2:
        return "recent window has fewer than 2 subscriber metric snapshots"
    if baseline_window.snapshot_count < 2:
        return "baseline window has fewer than 2 subscriber metric snapshots"
    if recent_window.unsubscribe_total is None:
        return "recent window lacks unsubscribe totals"
    if baseline_window.unsubscribe_total is None:
        return "baseline window lacks unsubscribe totals"
    return None


def _severity(
    *,
    recent_unsubscribes: int,
    min_unsubscribes: int,
    daily_ratio: float | None,
    recent_churn: float | None,
) -> str:
    if (
        (daily_ratio is not None and daily_ratio >= HIGH_SPIKE_MULTIPLIER)
        or recent_unsubscribes >= min_unsubscribes * 3
        or (recent_churn is not None and recent_churn >= 0.05)
    ):
        return "high"
    if (
        (daily_ratio is not None and daily_ratio >= MEDIUM_SPIKE_MULTIPLIER)
        or recent_unsubscribes >= min_unsubscribes * 2
        or (recent_churn is not None and recent_churn >= 0.03)
    ):
        return "medium"
    return "low"


def _recommendation(
    severity: str,
    contributions: tuple[NewsletterChurnContribution, ...],
) -> str:
    if severity == "high":
        return "pause_or_review_next_send"
    if contributions:
        return "review_contributing_sends"
    return "monitor_next_snapshot"


def _comparison(
    *,
    recent_unsubscribes: int,
    baseline_unsubscribes: int,
    recent_daily: float,
    baseline_daily: float,
    ratio: float | None,
    churn_delta: float | None,
) -> str:
    ratio_text = (
        "baseline had zero daily unsubscribes"
        if ratio is None
        else f"{ratio:.2f}x baseline"
    )
    churn_text = (
        ""
        if churn_delta is None
        else f"; avg churn delta {_format_signed_rate(churn_delta)}"
    )
    return (
        f"recent {recent_unsubscribes} unsubscribes "
        f"({_format_float(recent_daily)}/day) vs baseline {baseline_unsubscribes} "
        f"({_format_float(baseline_daily)}/day), {ratio_text}{churn_text}"
    )


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


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _delta(first: int | None, latest: int | None) -> int | None:
    if first is None or latest is None:
        return None
    return latest - first


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _format_int(value: int | None) -> str:
    return "-" if value is None else str(value)


def _format_float(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}"


def _format_rate(value: float | None) -> str:
    return "-" if value is None else f"{value * 100:.2f}%"


def _format_signed_rate(value: float) -> str:
    return f"{value * 100:+.2f}%"
