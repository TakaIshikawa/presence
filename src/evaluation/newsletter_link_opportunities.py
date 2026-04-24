"""Rank newsletter links that deserve follow-up social posts."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass
class NewsletterLinkOpportunity:
    """A clicked newsletter link with follow-up potential."""

    newsletter_send_id: int
    issue_id: str
    url: str
    title: Optional[str]
    clicks: int
    ctr: Optional[float]
    score: float
    suggested_follow_up_angle: str
    score_components: dict[str, float] = field(default_factory=dict)
    source_content_id: Optional[int] = None
    content_type: Optional[str] = None
    content_age_days: Optional[int] = None
    has_follow_up_posts: bool = False
    sent_at: str = ""
    fetched_at: str = ""


@dataclass
class NewsletterLinkOpportunitySummary:
    """Ranked newsletter link opportunities for a reporting window."""

    period_days: int
    min_clicks: int
    opportunity_count: int
    opportunities: list[NewsletterLinkOpportunity]


class NewsletterLinkOpportunityAnalyzer:
    """Analyze stored newsletter sends and Buttondown link metrics."""

    def __init__(self, db) -> None:
        self.db = db

    def summarize(
        self,
        days: int = 90,
        limit: int = 20,
        min_clicks: int = 1,
    ) -> NewsletterLinkOpportunitySummary:
        """Return clicked links ranked by follow-up opportunity score."""
        rows = self._load_latest_link_rows(days=days, min_clicks=min_clicks)
        content_by_id, content_by_url, follow_up_counts = self._load_content_context(rows)

        opportunities = []
        for row in rows:
            source_content = self._match_source_content(
                row=row,
                content_by_id=content_by_id,
                content_by_url=content_by_url,
            )
            has_follow_up_posts = False
            if source_content:
                source_id = int(source_content["id"])
                has_follow_up_posts = follow_up_counts.get(source_id, 0) > 0
            opportunity = self._build_opportunity(row, source_content, has_follow_up_posts)
            opportunities.append(opportunity)

        opportunities.sort(
            key=lambda item: (
                item.score,
                item.clicks,
                item.ctr or 0.0,
                self._parse_timestamp(item.sent_at),
            ),
            reverse=True,
        )
        opportunities = opportunities[: max(0, int(limit))]

        return NewsletterLinkOpportunitySummary(
            period_days=days,
            min_clicks=min_clicks,
            opportunity_count=len(opportunities),
            opportunities=opportunities,
        )

    def _load_latest_link_rows(self, days: int, min_clicks: int) -> list[dict[str, Any]]:
        cursor = self.db.conn.execute(
            """WITH latest_links AS (
                   SELECT nlc.*
                   FROM newsletter_link_clicks nlc
                   WHERE nlc.id = (
                       SELECT latest.id
                       FROM newsletter_link_clicks latest
                       WHERE latest.newsletter_send_id = nlc.newsletter_send_id
                         AND latest.link_url = nlc.link_url
                       ORDER BY latest.fetched_at DESC, latest.id DESC
                       LIMIT 1
                   )
               ),
               latest_engagement AS (
                   SELECT ne.*
                   FROM newsletter_engagement ne
                   WHERE ne.id = (
                       SELECT latest.id
                       FROM newsletter_engagement latest
                       WHERE latest.newsletter_send_id = ne.newsletter_send_id
                       ORDER BY latest.fetched_at DESC, latest.id DESC
                       LIMIT 1
                   )
               )
               SELECT ll.id,
                      ll.newsletter_send_id,
                      ll.issue_id,
                      ll.link_url,
                      ll.raw_url,
                      ll.clicks,
                      ll.unique_clicks,
                      ll.fetched_at,
                      ns.subject,
                      ns.source_content_ids,
                      ns.subscriber_count,
                      ns.sent_at,
                      le.clicks AS send_clicks
               FROM latest_links ll
               JOIN newsletter_sends ns
                 ON ns.id = ll.newsletter_send_id
               LEFT JOIN latest_engagement le
                 ON le.newsletter_send_id = ns.id
               WHERE ll.clicks >= ?
                 AND datetime(ns.sent_at) >= datetime('now', ?)
               ORDER BY ll.clicks DESC, ns.sent_at DESC, ll.id DESC""",
            (int(min_clicks), f"-{int(days)} days"),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _load_content_context(
        self,
        rows: list[dict[str, Any]],
    ) -> tuple[dict[int, dict[str, Any]], dict[str, dict[str, Any]], dict[int, int]]:
        source_ids: set[int] = set()
        urls: set[str] = set()
        for row in rows:
            source_ids.update(self._parse_source_ids(row.get("source_content_ids")))
            if row.get("link_url"):
                urls.add(str(row["link_url"]))
            if row.get("raw_url"):
                urls.add(str(row["raw_url"]))

        content_by_id: dict[int, dict[str, Any]] = {}
        content_by_url: dict[str, dict[str, Any]] = {}

        if source_ids:
            placeholders = ",".join("?" for _ in source_ids)
            cursor = self.db.conn.execute(
                f"""SELECT id, content, content_type, published_url, published_at,
                           created_at, repurposed_from
                    FROM generated_content
                    WHERE id IN ({placeholders})""",
                tuple(source_ids),
            )
            for row in cursor.fetchall():
                item = dict(row)
                content_by_id[int(item["id"])] = item
                if item.get("published_url"):
                    content_by_url[str(item["published_url"])] = item

        if urls:
            placeholders = ",".join("?" for _ in urls)
            cursor = self.db.conn.execute(
                f"""SELECT id, content, content_type, published_url, published_at,
                           created_at, repurposed_from
                    FROM generated_content
                    WHERE published_url IN ({placeholders})""",
                tuple(urls),
            )
            for row in cursor.fetchall():
                item = dict(row)
                content_id = int(item["id"])
                content_by_id[content_id] = item
                if item.get("published_url"):
                    content_by_url[str(item["published_url"])] = item

        follow_up_counts = self._load_follow_up_counts(set(content_by_id))
        return content_by_id, content_by_url, follow_up_counts

    def _load_follow_up_counts(self, content_ids: set[int]) -> dict[int, int]:
        if not content_ids:
            return {}
        placeholders = ",".join("?" for _ in content_ids)
        cursor = self.db.conn.execute(
            f"""SELECT repurposed_from, COUNT(*) AS count
                FROM generated_content
                WHERE repurposed_from IN ({placeholders})
                GROUP BY repurposed_from""",
            tuple(content_ids),
        )
        return {int(row["repurposed_from"]): int(row["count"]) for row in cursor.fetchall()}

    def _match_source_content(
        self,
        row: dict[str, Any],
        content_by_id: dict[int, dict[str, Any]],
        content_by_url: dict[str, dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        for url_key in (row.get("link_url"), row.get("raw_url")):
            if url_key and str(url_key) in content_by_url:
                return content_by_url[str(url_key)]

        source_ids = self._parse_source_ids(row.get("source_content_ids"))
        if len(source_ids) == 1:
            return content_by_id.get(source_ids[0])

        return None

    def _build_opportunity(
        self,
        row: dict[str, Any],
        source_content: Optional[dict[str, Any]],
        has_follow_up_posts: bool,
    ) -> NewsletterLinkOpportunity:
        subscriber_count = int(row.get("subscriber_count") or 0)
        clicks = int(row.get("clicks") or 0)
        ctr = clicks / subscriber_count if subscriber_count > 0 else None
        content_age_days = self._content_age_days(row, source_content)
        components = score_link_opportunity_components(
            clicks=clicks,
            ctr=ctr,
            content_age_days=content_age_days,
            has_follow_up_posts=has_follow_up_posts,
        )
        title = self._title_for(source_content)
        return NewsletterLinkOpportunity(
            newsletter_send_id=int(row["newsletter_send_id"]),
            issue_id=row.get("issue_id") or "",
            url=row.get("link_url") or "",
            title=title,
            clicks=clicks,
            ctr=ctr,
            score=round(sum(components.values()), 2),
            suggested_follow_up_angle=self._suggest_angle(
                title=title,
                url=row.get("link_url") or "",
                content_type=source_content.get("content_type") if source_content else None,
                has_follow_up_posts=has_follow_up_posts,
            ),
            score_components=components,
            source_content_id=int(source_content["id"]) if source_content else None,
            content_type=source_content.get("content_type") if source_content else None,
            content_age_days=content_age_days,
            has_follow_up_posts=has_follow_up_posts,
            sent_at=row.get("sent_at") or "",
            fetched_at=row.get("fetched_at") or "",
        )

    @staticmethod
    def _parse_source_ids(value: Any) -> list[int]:
        try:
            parsed = json.loads(value or "[]")
        except (TypeError, json.JSONDecodeError):
            return []

        source_ids = []
        for item in parsed if isinstance(parsed, list) else []:
            try:
                source_ids.append(int(item))
            except (TypeError, ValueError):
                continue
        return source_ids

    @staticmethod
    def _title_for(content: Optional[dict[str, Any]]) -> Optional[str]:
        if not content:
            return None
        text = (content.get("content") or "").strip()
        if not text:
            return None
        first_line = text.splitlines()[0].strip()
        if first_line.upper().startswith("TITLE:"):
            first_line = first_line[6:].strip()
        return first_line[:120] if first_line else None

    @staticmethod
    def _content_age_days(
        row: dict[str, Any],
        content: Optional[dict[str, Any]],
    ) -> Optional[int]:
        timestamp = None
        if content:
            timestamp = content.get("published_at") or content.get("created_at")
        timestamp = timestamp or row.get("sent_at")
        parsed = NewsletterLinkOpportunityAnalyzer._parse_datetime(timestamp)
        if not parsed:
            return None
        now = datetime.now(timezone.utc)
        return max(0, (now - parsed).days)

    @staticmethod
    def _suggest_angle(
        title: Optional[str],
        url: str,
        content_type: Optional[str],
        has_follow_up_posts: bool,
    ) -> str:
        subject = title or url
        if has_follow_up_posts:
            return f"Extend the prior follow-up on {subject} with a newer outcome or sharper lesson."
        if content_type == "blog_post":
            return f"Turn {subject} into a concise social takeaway with one concrete example."
        if content_type == "x_thread":
            return f"Revisit {subject} as a single stronger post that adds what changed since publishing."
        if content_type == "x_post":
            return f"Expand {subject} into a short follow-up with the detail readers clicked to investigate."
        return f"Use the click interest around {subject} to frame a practical follow-up post."

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(str(value))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    @staticmethod
    def _parse_timestamp(value: Any) -> float:
        parsed = NewsletterLinkOpportunityAnalyzer._parse_datetime(value)
        return parsed.timestamp() if parsed else 0.0


def score_link_opportunity_components(
    clicks: int,
    ctr: Optional[float],
    content_age_days: Optional[int],
    has_follow_up_posts: bool,
) -> dict[str, float]:
    """Return transparent weighted score components for a newsletter link."""
    click_component = min(max(clicks, 0), 25) / 25 * 40
    ctr_component = min(max(ctr or 0.0, 0.0), 0.20) / 0.20 * 30
    age_component = min(max(content_age_days or 0, 0), 180) / 180 * 15
    follow_up_component = 0.0 if has_follow_up_posts else 15.0
    return {
        "clicks": round(click_component, 2),
        "ctr": round(ctr_component, 2),
        "content_age": round(age_component, 2),
        "no_existing_follow_up": round(follow_up_component, 2),
    }
