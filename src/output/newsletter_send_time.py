"""Recommend newsletter send windows from historical engagement."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


WEEKDAY_NAMES = (
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
)
SCORE_FORMULA = "open_rate*100 + click_rate*300 - unsubscribe_rate*100"


@dataclass
class NewsletterSendTimeWindow:
    """Aggregated performance for one weekday/hour send window."""

    weekday: int
    weekday_name: str
    hour: int
    sends: int = 0
    metric_sends: int = 0
    missing_engagement_sends: int = 0
    subscriber_count: int = 0
    opens: int = 0
    clicks: int = 0
    unsubscribes: int = 0
    open_rate: float | None = None
    click_rate: float | None = None
    unsubscribe_rate: float | None = None
    score: float | None = None
    recommendation: str = "insufficient_sample"


@dataclass
class NewsletterSendTimeReport:
    """Newsletter send-time recommendation report."""

    period_days: int
    min_sample: int
    limit: int
    score_formula: str
    total_sends: int
    total_windows: int
    recommended_count: int
    missing_engagement_sends: int
    notes: list[str] = field(default_factory=list)
    windows: list[NewsletterSendTimeWindow] = field(default_factory=list)
    recommendations: list[NewsletterSendTimeWindow] = field(default_factory=list)


class NewsletterSendTimeRecommender:
    """Build send-time recommendations from newsletter_sends and engagement."""

    def __init__(self, db) -> None:
        self.db = db
        self.conn = db.conn if hasattr(db, "conn") else db

    def recommend(
        self, days: int = 90, min_sample: int = 3, limit: int = 10
    ) -> NewsletterSendTimeReport:
        """Return ranked weekday/hour windows for the requested lookback."""
        days = max(int(days), 1)
        min_sample = max(int(min_sample), 1)
        limit = max(int(limit), 1)
        notes: list[str] = []
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        sends = self._load_sends(cutoff, notes)
        engagement = self._load_latest_engagement(notes)
        windows = self._group_windows(sends, engagement)

        recommendations = [
            window
            for window in windows
            if window.metric_sends >= min_sample and window.score is not None
        ]
        recommendations.sort(key=self._recommendation_sort_key)
        recommendations = recommendations[:limit]
        for window in recommendations:
            window.recommendation = "recommended"

        windows.sort(key=self._window_sort_key)
        missing = sum(window.missing_engagement_sends for window in windows)
        if sends and not recommendations:
            notes.append(
                f"No weekday/hour window met the minimum sample of {min_sample} "
                "sends with engagement."
            )

        return NewsletterSendTimeReport(
            period_days=days,
            min_sample=min_sample,
            limit=limit,
            score_formula=SCORE_FORMULA,
            total_sends=len(sends),
            total_windows=len(windows),
            recommended_count=len(recommendations),
            missing_engagement_sends=missing,
            notes=notes,
            windows=windows,
            recommendations=recommendations,
        )

    def _load_sends(
        self, cutoff: datetime, notes: list[str]
    ) -> list[dict[str, Any]]:
        if not _has_table(self.conn, "newsletter_sends"):
            notes.append("newsletter_sends table is not available.")
            return []
        columns = _columns(self.conn, "newsletter_sends")
        if not {"id", "sent_at"}.issubset(columns):
            notes.append("newsletter_sends is missing id or sent_at columns.")
            return []
        selected = [
            _select_column(columns, "id"),
            _select_column(columns, "issue_id"),
            _select_column(columns, "subject"),
            _select_column(columns, "subscriber_count"),
            _select_column(columns, "sent_at"),
        ]
        rows = _fetch_dicts(
            self.conn,
            f"SELECT {', '.join(selected)} FROM newsletter_sends",
        )
        return [
            row
            for row in rows
            if (parsed := _parse_datetime(row.get("sent_at"))) is not None
            and parsed >= cutoff
        ]

    def _load_latest_engagement(
        self, notes: list[str]
    ) -> dict[int, dict[str, Any]]:
        if not _has_table(self.conn, "newsletter_engagement"):
            notes.append("newsletter_engagement table is not available.")
            return {}
        columns = _columns(self.conn, "newsletter_engagement")
        required = {"newsletter_send_id", "opens", "clicks", "unsubscribes", "fetched_at"}
        if not required.issubset(columns):
            missing = ", ".join(sorted(required - columns))
            notes.append(f"newsletter_engagement is missing columns: {missing}.")
            return {}

        rows = _fetch_dicts(
            self.conn,
            """SELECT newsletter_send_id, issue_id, opens, clicks, unsubscribes,
                      fetched_at, id
               FROM newsletter_engagement
               ORDER BY datetime(fetched_at) DESC, id DESC""",
        )
        latest: dict[int, dict[str, Any]] = {}
        for row in rows:
            send_id = _optional_int(row.get("newsletter_send_id"))
            if send_id is None or send_id in latest:
                continue
            latest[send_id] = row
        return latest

    def _group_windows(
        self, sends: list[dict[str, Any]], engagement: dict[int, dict[str, Any]]
    ) -> list[NewsletterSendTimeWindow]:
        windows: dict[tuple[int, int], NewsletterSendTimeWindow] = {}
        for send in sends:
            sent_at = _parse_datetime(send.get("sent_at"))
            if sent_at is None:
                continue
            key = (sent_at.weekday(), sent_at.hour)
            window = windows.setdefault(
                key,
                NewsletterSendTimeWindow(
                    weekday=key[0],
                    weekday_name=WEEKDAY_NAMES[key[0]],
                    hour=key[1],
                ),
            )
            window.sends += 1
            metrics = engagement.get(int(send["id"]))
            if not metrics:
                window.missing_engagement_sends += 1
                continue
            subscriber_count = _optional_int(send.get("subscriber_count")) or 0
            opens = _optional_int(metrics.get("opens")) or 0
            clicks = _optional_int(metrics.get("clicks")) or 0
            unsubscribes = _optional_int(metrics.get("unsubscribes")) or 0
            window.metric_sends += 1
            window.subscriber_count += subscriber_count
            window.opens += opens
            window.clicks += clicks
            window.unsubscribes += unsubscribes

        for window in windows.values():
            window.open_rate = _rate(window.opens, window.subscriber_count)
            window.click_rate = _rate(window.clicks, window.subscriber_count)
            window.unsubscribe_rate = _rate(window.unsubscribes, window.subscriber_count)
            if window.subscriber_count > 0:
                window.score = score_send_time_window(
                    window.open_rate,
                    window.click_rate,
                    window.unsubscribe_rate,
                )
        return list(windows.values())

    @staticmethod
    def _recommendation_sort_key(
        window: NewsletterSendTimeWindow,
    ) -> tuple[float, float, float, int, int, int]:
        return (
            -(window.score or 0.0),
            -(window.click_rate or 0.0),
            -(window.open_rate or 0.0),
            -window.metric_sends,
            window.weekday,
            window.hour,
        )

    @staticmethod
    def _window_sort_key(window: NewsletterSendTimeWindow) -> tuple[int, int]:
        return (window.weekday, window.hour)


def score_send_time_window(
    open_rate: float | None,
    click_rate: float | None,
    unsubscribe_rate: float | None,
) -> float:
    """Score a window as open_rate*100 + click_rate*300 - unsubscribe_rate*100."""
    return round(
        ((open_rate or 0.0) * 100.0)
        + ((click_rate or 0.0) * 300.0)
        - ((unsubscribe_rate or 0.0) * 100.0),
        2,
    )


def format_json_report(report: NewsletterSendTimeReport) -> str:
    """Format send-time recommendations as stable JSON."""
    from dataclasses import asdict

    return json.dumps(asdict(report), indent=2, sort_keys=True)


def format_text_report(report: NewsletterSendTimeReport) -> str:
    """Format send-time recommendations for CLI use."""
    lines = [
        "",
        "=" * 70,
        f"Newsletter Send-Time Recommendations (last {report.period_days} days)",
        "=" * 70,
        "",
        (
            f"Sends: {report.total_sends}; windows: {report.total_windows}; "
            f"minimum {report.min_sample} metric send(s)"
        ),
        f"Score: {report.score_formula}",
        "",
    ]
    if report.notes:
        lines.extend(f"Note: {note}" for note in report.notes)
        lines.append("")
    if not report.recommendations:
        lines.append("No send windows met the recommendation criteria.")
        return "\n".join(lines).rstrip()

    for index, window in enumerate(report.recommendations, start=1):
        lines.append(
            f"{index}. {window.weekday_name} {window.hour:02d}:00 "
            f"(score {window.score:.2f}, {window.metric_sends} metric sends)"
        )
        lines.append(
            f"   Opens {_format_rate(window.open_rate)}, "
            f"clicks {_format_rate(window.click_rate)}, "
            f"unsubscribes {_format_rate(window.unsubscribe_rate)}"
        )
        lines.append(
            f"   Totals: {window.opens} opens, {window.clicks} clicks, "
            f"{window.unsubscribes} unsubscribes, "
            f"{window.subscriber_count} subscribers"
        )
        if window.missing_engagement_sends:
            lines.append(
                f"   Missing engagement: {window.missing_engagement_sends} send(s)"
            )
        lines.append("")

    return "\n".join(lines).rstrip()


def _format_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _has_table(conn, table: str) -> bool:
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
    except sqlite3.DatabaseError:
        return False
    return row is not None


def _columns(conn, table: str) -> set[str]:
    try:
        return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.DatabaseError:
        return set()


def _select_column(columns: set[str], name: str) -> str:
    if name in columns:
        return name
    return f"NULL AS {name}"


def _fetch_dicts(conn, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    try:
        cursor = conn.execute(sql, params or [])
    except sqlite3.DatabaseError:
        return []
    return [dict(row) for row in cursor.fetchall()]


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed
