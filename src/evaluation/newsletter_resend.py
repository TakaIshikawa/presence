"""Find newsletter content worth resending or subject retesting."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional


RECOMMEND_RESEND = "resend"
RECOMMEND_SUBJECT_RETEST = "subject_retest"
RECOMMEND_NO_ACTION = "no_action"


@dataclass
class NewsletterResendCandidate:
    """One newsletter resend/retest recommendation row."""

    content_id: Optional[int]
    subject: str
    sent_at: str
    published_at: str
    open_rate: Optional[float]
    click_rate: Optional[float]
    opens: Optional[int]
    clicks: Optional[int]
    subscriber_count: Optional[int]
    recommendation: str
    reasons: list[str]
    newsletter_send_id: Optional[int] = None
    issue_id: str = ""
    content_type: str = ""
    engagement_score: Optional[float] = None
    unsubscribes: Optional[int] = None
    fetched_at: str = ""


@dataclass
class NewsletterResendReport:
    """Newsletter resend finder output."""

    period_days: int
    min_open_rate: float
    max_click_rate: float
    rows: list[NewsletterResendCandidate] = field(default_factory=list)

    @property
    def resend_count(self) -> int:
        return sum(1 for row in self.rows if row.recommendation == RECOMMEND_RESEND)

    @property
    def subject_retest_count(self) -> int:
        return sum(
            1 for row in self.rows if row.recommendation == RECOMMEND_SUBJECT_RETEST
        )

    @property
    def no_action_count(self) -> int:
        return sum(1 for row in self.rows if row.recommendation == RECOMMEND_NO_ACTION)


class NewsletterResendFinder:
    """Build resend/retest recommendations from newsletter response metrics."""

    def __init__(self, db) -> None:
        self.db = db
        self.conn = db.conn if hasattr(db, "conn") else db

    def find(
        self,
        days: int = 90,
        min_open_rate: float = 0.40,
        max_click_rate: float = 0.04,
        limit: int = 20,
    ) -> NewsletterResendReport:
        """Return recent newsletter-like content with resend recommendations."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=max(int(days), 0))
        content_by_id = self._load_generated_content(cutoff)
        candidates: dict[tuple[Optional[int], Optional[int]], NewsletterResendCandidate] = {}
        content_ids_with_sends: set[int] = set()

        for send in self._load_newsletter_sends(cutoff):
            content_ids = _parse_content_ids(send.get("source_content_ids"))
            if not content_ids:
                content_ids = [None]
            else:
                content_ids_with_sends.update(content_ids)
            metrics = self._latest_newsletter_metrics(
                send_id=send.get("id"),
                issue_id=send.get("issue_id"),
            )
            for content_id in content_ids:
                content = content_by_id.get(content_id or -1, {})
                row = self._build_row(
                    content=content,
                    send=send,
                    metrics=metrics,
                    min_open_rate=min_open_rate,
                    max_click_rate=max_click_rate,
                )
                candidates[(row.content_id, row.newsletter_send_id)] = row

        for content in content_by_id.values():
            if not self._is_newsletter_like(content):
                continue
            if content.get("id") in content_ids_with_sends:
                continue
            key = (content.get("id"), None)
            if key in candidates:
                continue
            row = self._build_row(
                content=content,
                send={},
                metrics={},
                min_open_rate=min_open_rate,
                max_click_rate=max_click_rate,
            )
            candidates[key] = row

        rows = sorted(candidates.values(), key=self._sort_key)[: max(int(limit), 0)]
        return NewsletterResendReport(
            period_days=days,
            min_open_rate=min_open_rate,
            max_click_rate=max_click_rate,
            rows=rows,
        )

    def _build_row(
        self,
        content: dict[str, Any],
        send: dict[str, Any],
        metrics: dict[str, Any],
        min_open_rate: float,
        max_click_rate: float,
    ) -> NewsletterResendCandidate:
        subscriber_count = _optional_int(send.get("subscriber_count"))
        opens = _optional_int(metrics.get("opens"))
        clicks = _optional_int(metrics.get("clicks"))
        open_rate = _rate(opens, subscriber_count)
        click_rate = _rate(clicks, subscriber_count)
        recommendation, reasons = _recommend(
            open_rate=open_rate,
            click_rate=click_rate,
            min_open_rate=min_open_rate,
            max_click_rate=max_click_rate,
            has_send=bool(send),
            has_metrics=bool(metrics),
        )
        return NewsletterResendCandidate(
            content_id=_optional_int(content.get("id")),
            newsletter_send_id=_optional_int(send.get("id")),
            issue_id=str(send.get("issue_id") or ""),
            subject=str(send.get("subject") or self._subject_from_content(content)),
            content_type=str(content.get("content_type") or ""),
            sent_at=str(send.get("sent_at") or ""),
            published_at=str(content.get("published_at") or ""),
            open_rate=open_rate,
            click_rate=click_rate,
            opens=opens,
            clicks=clicks,
            unsubscribes=_optional_int(metrics.get("unsubscribes")),
            subscriber_count=subscriber_count,
            engagement_score=self._content_engagement_score(content),
            fetched_at=str(metrics.get("fetched_at") or ""),
            recommendation=recommendation,
            reasons=reasons,
        )

    def _load_newsletter_sends(self, cutoff: datetime) -> list[dict[str, Any]]:
        if not _has_table(self.conn, "newsletter_sends"):
            return []
        columns = _columns(self.conn, "newsletter_sends")
        required = {"id", "sent_at"}
        if not required.issubset(columns):
            return []
        selected = [
            _select_column(columns, "id"),
            _select_column(columns, "issue_id"),
            _select_column(columns, "subject"),
            _select_column(columns, "source_content_ids"),
            _select_column(columns, "subscriber_count"),
            _select_column(columns, "sent_at"),
        ]
        sql = f"SELECT {', '.join(selected)} FROM newsletter_sends"
        rows = _fetch_dicts(self.conn, sql)
        return [
            row
            for row in rows
            if _timestamp_in_window(row.get("sent_at"), cutoff)
        ]

    def _load_generated_content(self, cutoff: datetime) -> dict[int, dict[str, Any]]:
        if not _has_table(self.conn, "generated_content"):
            return {}
        columns = _columns(self.conn, "generated_content")
        if "id" not in columns:
            return {}
        selected = [
            _select_column(columns, "id"),
            _select_column(columns, "content_type"),
            _select_column(columns, "content"),
            _select_column(columns, "published"),
            _select_column(columns, "published_at"),
            _select_column(columns, "created_at"),
            _select_column(columns, "auto_quality"),
            _select_column(columns, "eval_score"),
        ]
        rows = _fetch_dicts(
            self.conn,
            f"SELECT {', '.join(selected)} FROM generated_content",
        )
        result: dict[int, dict[str, Any]] = {}
        for row in rows:
            content_id = _optional_int(row.get("id"))
            if content_id is None:
                continue
            published = row.get("published")
            timestamp = row.get("published_at") or row.get("created_at")
            if published not in (1, "1", True) or not _timestamp_in_window(
                timestamp, cutoff
            ):
                continue
            result[content_id] = row
        return result

    def _latest_newsletter_metrics(
        self, send_id: Any, issue_id: Any
    ) -> dict[str, Any]:
        if not _has_table(self.conn, "newsletter_engagement"):
            return {}
        columns = _columns(self.conn, "newsletter_engagement")
        needed = {"opens", "clicks", "fetched_at"}
        if not needed.issubset(columns):
            return {}
        selected = [
            _select_column(columns, "newsletter_send_id"),
            _select_column(columns, "issue_id"),
            _select_column(columns, "opens"),
            _select_column(columns, "clicks"),
            _select_column(columns, "unsubscribes"),
            _select_column(columns, "fetched_at"),
        ]
        clauses = []
        params: list[Any] = []
        if "newsletter_send_id" in columns and send_id is not None:
            clauses.append("newsletter_send_id = ?")
            params.append(send_id)
        if "issue_id" in columns and issue_id:
            clauses.append("issue_id = ?")
            params.append(issue_id)
        if not clauses:
            return {}
        sql = (
            f"SELECT {', '.join(selected)} FROM newsletter_engagement "
            f"WHERE {' OR '.join(clauses)} "
            "ORDER BY fetched_at DESC LIMIT 1"
        )
        rows = _fetch_dicts(self.conn, sql, params)
        return rows[0] if rows else {}

    def _content_engagement_score(self, content: dict[str, Any]) -> Optional[float]:
        content_id = content.get("id")
        scores = []
        for table in ("post_engagement", "bluesky_engagement"):
            if not _has_table(self.conn, table):
                continue
            columns = _columns(self.conn, table)
            if {"content_id", "engagement_score", "fetched_at"}.issubset(columns):
                rows = _fetch_dicts(
                    self.conn,
                    f"""SELECT engagement_score
                        FROM {table}
                        WHERE content_id = ?
                        ORDER BY fetched_at DESC
                        LIMIT 1""",
                    [content_id],
                )
                if rows and rows[0].get("engagement_score") is not None:
                    scores.append(float(rows[0]["engagement_score"]))
        if scores:
            return max(scores)
        if content.get("eval_score") is not None:
            return float(content["eval_score"])
        return None

    @staticmethod
    def _is_newsletter_like(content: dict[str, Any]) -> bool:
        content_type = str(content.get("content_type") or "").lower()
        return "newsletter" in content_type

    @staticmethod
    def _subject_from_content(content: dict[str, Any]) -> str:
        text = str(content.get("content") or "").strip()
        if not text:
            return ""
        return text.splitlines()[0][:120]

    @staticmethod
    def _sort_key(row: NewsletterResendCandidate) -> tuple[int, float, float, int]:
        priority = {
            RECOMMEND_RESEND: 0,
            RECOMMEND_SUBJECT_RETEST: 1,
            RECOMMEND_NO_ACTION: 2,
        }.get(row.recommendation, 3)
        timestamp = _parse_timestamp(row.sent_at or row.published_at)
        return (
            priority,
            -(row.open_rate or 0.0),
            row.click_rate if row.click_rate is not None else 1.0,
            -int(timestamp),
        )


def _recommend(
    open_rate: Optional[float],
    click_rate: Optional[float],
    min_open_rate: float,
    max_click_rate: float,
    has_send: bool,
    has_metrics: bool,
) -> tuple[str, list[str]]:
    if not has_send:
        return (
            RECOMMEND_NO_ACTION,
            ["newsletter send record not found for this content"],
        )
    if not has_metrics:
        return RECOMMEND_NO_ACTION, ["newsletter metrics are not available"]
    if open_rate is None or click_rate is None:
        return (
            RECOMMEND_NO_ACTION,
            ["subscriber count or open/click metrics are missing"],
        )
    if open_rate >= min_open_rate and click_rate <= max_click_rate:
        return (
            RECOMMEND_RESEND,
            [
                f"open rate {open_rate:.3f} meets threshold {min_open_rate:.3f}",
                f"click rate {click_rate:.3f} is at or below {max_click_rate:.3f}",
            ],
        )
    if open_rate < min_open_rate and click_rate <= max_click_rate:
        return (
            RECOMMEND_SUBJECT_RETEST,
            [
                f"open rate {open_rate:.3f} is below threshold {min_open_rate:.3f}",
                f"click rate {click_rate:.3f} is at or below {max_click_rate:.3f}",
            ],
        )
    return (
        RECOMMEND_NO_ACTION,
        [f"click rate {click_rate:.3f} is above {max_click_rate:.3f}"],
    )


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


def _optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _rate(count: Optional[int], denominator: Optional[int]) -> Optional[float]:
    if count is None or not denominator:
        return None
    return count / denominator


def _parse_content_ids(value: Any) -> list[int]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        raw = value
    else:
        try:
            raw = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return []
    ids = []
    for item in raw:
        parsed = _optional_int(item)
        if parsed is not None:
            ids.append(parsed)
    return ids


def _timestamp_in_window(value: Any, cutoff: datetime) -> bool:
    parsed = _parse_datetime(value)
    if parsed is None:
        return True
    return parsed >= cutoff


def _parse_timestamp(value: Any) -> float:
    parsed = _parse_datetime(value)
    return parsed.timestamp() if parsed else 0.0


def _parse_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed
