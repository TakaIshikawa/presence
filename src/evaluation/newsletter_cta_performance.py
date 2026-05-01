"""Aggregate newsletter CTA performance from send metadata and metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from output.newsletter_cta import extract_cta_id_from_send


UNKNOWN_CTA_ID = "unknown"


@dataclass
class NewsletterCtaExample:
    """One delivered newsletter example for a CTA."""

    newsletter_send_id: int
    issue_id: str
    subject: str
    sent_at: str
    subscriber_count: int
    opens: int
    clicks: int
    link_clicks: int
    unsubscribes: int
    open_rate: float | None
    click_rate: float | None
    unsubscribe_rate: float | None
    performance_score: float


@dataclass
class NewsletterCtaTotal:
    """Aggregated performance for one CTA bucket."""

    cta_id: str
    sends: int = 0
    subscriber_count: int = 0
    opens: int = 0
    clicks: int = 0
    link_clicks: int = 0
    unsubscribes: int = 0
    open_rate: float | None = None
    click_rate: float | None = None
    unsubscribe_rate: float | None = None
    best_examples: list[NewsletterCtaExample] = field(default_factory=list)
    worst_examples: list[NewsletterCtaExample] = field(default_factory=list)


@dataclass
class NewsletterCtaPerformanceReport:
    """CTA performance over a reporting window."""

    period_days: int
    min_sends: int
    limit: int
    total_sends: int
    included_sends: int
    unknown_sends: int
    cta_count: int
    ctas: list[NewsletterCtaTotal]


class NewsletterCtaPerformance:
    """Build CTA performance reports from newsletter send metadata."""

    def __init__(self, db) -> None:
        self.db = db

    def summarize(
        self, days: int = 90, min_sends: int = 1, limit: int = 10
    ) -> NewsletterCtaPerformanceReport:
        """Return CTA metrics grouped by selected CTA id."""
        days = max(int(days), 1)
        min_sends = max(int(min_sends), 1)
        limit = max(int(limit), 1)
        rows = self._load_send_metrics(days)

        totals: dict[str, NewsletterCtaTotal] = {}
        examples: dict[str, list[NewsletterCtaExample]] = {}
        for row in rows:
            cta_id = self._extract_cta_id(row)
            total = totals.setdefault(cta_id, NewsletterCtaTotal(cta_id=cta_id))
            subscriber_count = int(row.get("subscriber_count") or 0)
            opens = int(row.get("opens") or 0)
            clicks = int(row.get("clicks") or 0)
            link_clicks = int(row.get("link_clicks") or 0)
            unsubscribes = int(row.get("unsubscribes") or 0)

            total.sends += 1
            total.subscriber_count += subscriber_count
            total.opens += opens
            total.clicks += clicks
            total.link_clicks += link_clicks
            total.unsubscribes += unsubscribes

            example = NewsletterCtaExample(
                newsletter_send_id=int(row["newsletter_send_id"]),
                issue_id=row.get("issue_id") or "",
                subject=row.get("subject") or "",
                sent_at=row.get("sent_at") or "",
                subscriber_count=subscriber_count,
                opens=opens,
                clicks=clicks,
                link_clicks=link_clicks,
                unsubscribes=unsubscribes,
                open_rate=self._rate(opens, subscriber_count),
                click_rate=self._rate(clicks, subscriber_count),
                unsubscribe_rate=self._rate(unsubscribes, subscriber_count),
                performance_score=self._score(
                    opens=opens,
                    clicks=clicks,
                    unsubscribes=unsubscribes,
                    subscriber_count=subscriber_count,
                ),
            )
            examples.setdefault(cta_id, []).append(example)

        included = [total for total in totals.values() if total.sends >= min_sends]
        for total in included:
            total.open_rate = self._rate(total.opens, total.subscriber_count)
            total.click_rate = self._rate(total.clicks, total.subscriber_count)
            total.unsubscribe_rate = self._rate(
                total.unsubscribes, total.subscriber_count
            )
            ranked_examples = sorted(
                examples.get(total.cta_id, []),
                key=lambda item: (
                    item.performance_score,
                    item.open_rate or 0.0,
                    item.click_rate or 0.0,
                    item.sent_at,
                    item.newsletter_send_id,
                ),
                reverse=True,
            )
            total.best_examples = ranked_examples[:limit]
            total.worst_examples = list(reversed(ranked_examples[-limit:]))

        ctas = sorted(
            included,
            key=lambda item: (
                item.cta_id == UNKNOWN_CTA_ID,
                -(item.click_rate or 0.0),
                -(item.open_rate or 0.0),
                -item.sends,
                item.cta_id,
            ),
        )[:limit]

        return NewsletterCtaPerformanceReport(
            period_days=days,
            min_sends=min_sends,
            limit=limit,
            total_sends=len(rows),
            included_sends=sum(total.sends for total in ctas),
            unknown_sends=totals.get(
                UNKNOWN_CTA_ID, NewsletterCtaTotal(UNKNOWN_CTA_ID)
            ).sends,
            cta_count=len(ctas),
            ctas=ctas,
        )

    def _load_send_metrics(self, days: int) -> list[dict[str, Any]]:
        cursor = self.db.conn.execute(
            """WITH latest_engagement AS (
                   SELECT ne.*
                   FROM newsletter_engagement ne
                   WHERE ne.id = (
                       SELECT latest.id
                       FROM newsletter_engagement latest
                       WHERE latest.newsletter_send_id = ne.newsletter_send_id
                       ORDER BY datetime(latest.fetched_at) DESC, latest.id DESC
                       LIMIT 1
                   )
               ),
               latest_link_clicks AS (
                   SELECT nlc.*
                   FROM newsletter_link_clicks nlc
                   WHERE nlc.id = (
                       SELECT latest.id
                       FROM newsletter_link_clicks latest
                       WHERE latest.newsletter_send_id = nlc.newsletter_send_id
                         AND latest.link_url = nlc.link_url
                       ORDER BY datetime(latest.fetched_at) DESC, latest.id DESC
                       LIMIT 1
                   )
               ),
               link_totals AS (
                   SELECT newsletter_send_id, SUM(clicks) AS link_clicks
                   FROM latest_link_clicks
                   GROUP BY newsletter_send_id
               )
               SELECT ns.id AS newsletter_send_id,
                      ns.issue_id,
                      ns.subject,
                      ns.subscriber_count,
                      ns.metadata,
                      ns.sent_at,
                      COALESCE(ne.opens, 0) AS opens,
                      COALESCE(ne.clicks, 0) AS clicks,
                      COALESCE(ne.unsubscribes, 0) AS unsubscribes,
                      COALESCE(lt.link_clicks, 0) AS link_clicks
               FROM newsletter_sends ns
               LEFT JOIN latest_engagement ne
                 ON ne.newsletter_send_id = ns.id
               LEFT JOIN link_totals lt
                 ON lt.newsletter_send_id = ns.id
               WHERE datetime(ns.sent_at) >= datetime('now', ?)
               ORDER BY datetime(ns.sent_at) DESC, ns.id DESC""",
            (f"-{int(days)} days",),
        )
        return [dict(row) for row in cursor.fetchall()]

    @staticmethod
    def _extract_cta_id(row: dict[str, Any]) -> str:
        send = {"metadata": row.get("metadata")}
        cta_id = extract_cta_id_from_send(send)
        return cta_id or UNKNOWN_CTA_ID

    @staticmethod
    def _rate(numerator: int, denominator: int) -> float | None:
        if denominator <= 0:
            return None
        return numerator / denominator

    @classmethod
    def _score(
        cls, opens: int, clicks: int, unsubscribes: int, subscriber_count: int
    ) -> float:
        open_rate = cls._rate(opens, subscriber_count) or 0.0
        click_rate = cls._rate(clicks, subscriber_count) or 0.0
        unsubscribe_rate = cls._rate(unsubscribes, subscriber_count) or 0.0
        return round(
            (open_rate * 100.0)
            + (click_rate * 300.0)
            - (unsubscribe_rate * 100.0),
            2,
        )
