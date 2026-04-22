"""Score newsletter subject candidates against Buttondown engagement metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class NewsletterSubjectAlternative:
    """A non-selected subject candidate recorded for the same issue."""

    candidate_id: int
    subject: str
    candidate_score: float
    rationale: str = ""
    source: str = "heuristic"
    rank: Optional[int] = None


@dataclass
class NewsletterSubjectScore:
    """Performance score for one sent newsletter subject."""

    candidate_id: int
    newsletter_send_id: int
    issue_id: str
    subject: str
    candidate_score: float
    open_rate: Optional[float]
    click_rate: Optional[float]
    opens: int
    clicks: int
    unsubscribes: int
    subscriber_count: int
    performance_score: float
    sent_at: str
    fetched_at: str
    rationale: str = ""
    source: str = "heuristic"
    alternatives: list[NewsletterSubjectAlternative] = field(default_factory=list)


@dataclass
class NewsletterSubjectPerformanceSummary:
    """Ranked subject performance over a reporting window."""

    period_days: int
    subject_count: int
    ranked_subjects: list[NewsletterSubjectScore]
    average_open_rate: Optional[float] = None
    average_click_rate: Optional[float] = None
    best_subject: Optional[NewsletterSubjectScore] = None


class NewsletterSubjectPerformance:
    """Compute subject-line performance from stored sends and metrics."""

    def __init__(self, db) -> None:
        self.db = db

    def summarize(self, days: int = 90) -> NewsletterSubjectPerformanceSummary:
        """Return selected subject performance ranked by engagement score."""
        rows = self.db.get_newsletter_subject_performance(days=days)
        scores = [self._score_row(row) for row in rows]
        scores.sort(
            key=lambda score: (
                score.performance_score,
                score.open_rate or 0.0,
                score.click_rate or 0.0,
                self._parse_timestamp(score.sent_at),
            ),
            reverse=True,
        )

        open_rates = [score.open_rate for score in scores if score.open_rate is not None]
        click_rates = [
            score.click_rate for score in scores if score.click_rate is not None
        ]
        return NewsletterSubjectPerformanceSummary(
            period_days=days,
            subject_count=len(scores),
            ranked_subjects=scores,
            average_open_rate=self._average(open_rates),
            average_click_rate=self._average(click_rates),
            best_subject=scores[0] if scores else None,
        )

    def _score_row(self, row: dict) -> NewsletterSubjectScore:
        alternatives = [
            NewsletterSubjectAlternative(
                candidate_id=int(item["id"]),
                subject=item["subject"],
                candidate_score=float(item.get("score") or 0.0),
                rationale=item.get("rationale") or "",
                source=item.get("source") or "heuristic",
                rank=item.get("rank"),
            )
            for item in self.db.get_newsletter_subject_alternatives(
                newsletter_send_id=int(row["newsletter_send_id"]),
                issue_id=row.get("issue_id"),
            )
        ]
        open_rate = row.get("open_rate")
        click_rate = row.get("click_rate")
        open_rate = float(open_rate) if open_rate is not None else None
        click_rate = float(click_rate) if click_rate is not None else None
        return NewsletterSubjectScore(
            candidate_id=int(row["candidate_id"]),
            newsletter_send_id=int(row["newsletter_send_id"]),
            issue_id=row.get("issue_id") or "",
            subject=row["subject"],
            candidate_score=float(row.get("candidate_score") or 0.0),
            open_rate=open_rate,
            click_rate=click_rate,
            opens=int(row.get("opens") or 0),
            clicks=int(row.get("clicks") or 0),
            unsubscribes=int(row.get("unsubscribes") or 0),
            subscriber_count=int(row.get("subscriber_count") or 0),
            performance_score=score_subject_performance(
                open_rate=open_rate,
                click_rate=click_rate,
                unsubscribes=int(row.get("unsubscribes") or 0),
                subscriber_count=int(row.get("subscriber_count") or 0),
            ),
            sent_at=row.get("sent_at") or "",
            fetched_at=row.get("fetched_at") or "",
            rationale=row.get("rationale") or "",
            source=row.get("source") or "heuristic",
            alternatives=alternatives,
        )

    @staticmethod
    def _average(values: list[float]) -> Optional[float]:
        if not values:
            return None
        return sum(values) / len(values)

    @staticmethod
    def _parse_timestamp(value: str) -> float:
        try:
            parsed = datetime.fromisoformat(value)
        except (TypeError, ValueError):
            return 0.0
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()


def score_subject_performance(
    open_rate: Optional[float],
    click_rate: Optional[float],
    unsubscribes: int = 0,
    subscriber_count: int = 0,
) -> float:
    """Score delivered subject performance with extra weight on clicks."""
    open_component = (open_rate or 0.0) * 100.0
    click_component = (click_rate or 0.0) * 300.0
    unsubscribe_rate = (
        unsubscribes / subscriber_count if subscriber_count and unsubscribes else 0.0
    )
    unsubscribe_penalty = unsubscribe_rate * 100.0
    return round(open_component + click_component - unsubscribe_penalty, 2)
