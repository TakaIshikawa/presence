"""Summarize Buttondown link clicks attributed to source content."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field

from storage.db import _normalize_newsletter_attribution_url


@dataclass
class NewsletterClickLinkSummary:
    """Aggregated clicks for one normalized link."""

    normalized_url: str
    clicks: int
    unique_clicks: int
    send_count: int
    source_kind: str | None = None
    latest_fetched_at: str = ""


@dataclass
class NewsletterClickContentSummary:
    """Aggregated newsletter clicks for one generated content item."""

    content_id: int
    content_type: str
    topic: str | None
    clicks: int
    unique_clicks: int
    links: list[NewsletterClickLinkSummary] = field(default_factory=list)


@dataclass
class NewsletterClickAttributionSummary:
    """Newsletter click attribution report for a lookback window."""

    period_days: int
    total_clicks: int
    total_unique_clicks: int
    attributed_clicks: int
    unattributed_clicks: int
    by_content: list[NewsletterClickContentSummary] = field(default_factory=list)
    unattributed_links: list[NewsletterClickLinkSummary] = field(default_factory=list)


class NewsletterClickAttribution:
    """Build a stable report from latest newsletter link-click snapshots."""

    def __init__(self, db) -> None:
        self.db = db

    def summarize(self, days: int = 90) -> NewsletterClickAttributionSummary:
        rows = self._latest_click_rows(days=days)
        by_content: dict[int, dict] = {}
        unattributed: dict[str, dict] = {}

        for row in rows:
            normalized_url = _normalize_newsletter_attribution_url(row["link_url"])
            clicks = int(row.get("clicks") or 0)
            unique_clicks = int(row.get("unique_clicks") or 0)
            if row.get("content_id") is None:
                bucket = unattributed.setdefault(
                    normalized_url,
                    {
                        "normalized_url": normalized_url,
                        "clicks": 0,
                        "unique_clicks": 0,
                        "send_ids": set(),
                        "source_kind": None,
                        "latest_fetched_at": "",
                    },
                )
                self._add_link_row(bucket, row, clicks, unique_clicks)
                continue

            content_id = int(row["content_id"])
            content_bucket = by_content.setdefault(
                content_id,
                {
                    "content_id": content_id,
                    "content_type": row.get("content_type") or "",
                    "topic": row.get("topic"),
                    "clicks": 0,
                    "unique_clicks": 0,
                    "links": {},
                },
            )
            content_bucket["clicks"] += clicks
            content_bucket["unique_clicks"] += unique_clicks
            link_bucket = content_bucket["links"].setdefault(
                normalized_url,
                {
                    "normalized_url": normalized_url,
                    "clicks": 0,
                    "unique_clicks": 0,
                    "send_ids": set(),
                    "source_kind": row.get("source_kind"),
                    "latest_fetched_at": "",
                },
            )
            self._add_link_row(link_bucket, row, clicks, unique_clicks)

        content_summaries = [
            NewsletterClickContentSummary(
                content_id=item["content_id"],
                content_type=item["content_type"],
                topic=item["topic"],
                clicks=item["clicks"],
                unique_clicks=item["unique_clicks"],
                links=self._format_link_buckets(item["links"].values()),
            )
            for item in by_content.values()
        ]
        content_summaries.sort(key=lambda item: (-item.clicks, item.content_id))
        unattributed_links = self._format_link_buckets(unattributed.values())

        attributed_clicks = sum(item.clicks for item in content_summaries)
        unattributed_clicks = sum(item.clicks for item in unattributed_links)
        attributed_unique = sum(item.unique_clicks for item in content_summaries)
        unattributed_unique = sum(item.unique_clicks for item in unattributed_links)
        return NewsletterClickAttributionSummary(
            period_days=days,
            total_clicks=attributed_clicks + unattributed_clicks,
            total_unique_clicks=attributed_unique + unattributed_unique,
            attributed_clicks=attributed_clicks,
            unattributed_clicks=unattributed_clicks,
            by_content=content_summaries,
            unattributed_links=unattributed_links,
        )

    def _latest_click_rows(self, days: int) -> list[dict]:
        cursor = self.db.conn.execute(
            """WITH primary_topics AS (
                   SELECT ct.content_id, ct.topic
                   FROM content_topics ct
                   WHERE ct.id = (
                       SELECT latest.id
                       FROM content_topics latest
                       WHERE latest.content_id = ct.content_id
                       ORDER BY latest.confidence DESC, latest.topic ASC, latest.id ASC
                       LIMIT 1
                   )
               ),
               latest_clicks AS (
                   SELECT nlc.*
                   FROM newsletter_link_clicks nlc
                   JOIN newsletter_sends ns ON ns.id = nlc.newsletter_send_id
                   WHERE datetime(ns.sent_at) >= datetime('now', ?)
                     AND nlc.id = (
                         SELECT latest.id
                         FROM newsletter_link_clicks latest
                         WHERE latest.newsletter_send_id = nlc.newsletter_send_id
                           AND latest.issue_id = nlc.issue_id
                           AND latest.link_url = nlc.link_url
                         ORDER BY latest.fetched_at DESC, latest.id DESC
                         LIMIT 1
                     )
               )
               SELECT lc.id,
                      lc.newsletter_send_id,
                      lc.issue_id,
                      lc.content_id,
                      lc.source_kind,
                      lc.link_url,
                      lc.clicks,
                      lc.unique_clicks,
                      lc.fetched_at,
                      gc.content_type,
                      pt.topic
               FROM latest_clicks lc
               LEFT JOIN generated_content gc ON gc.id = lc.content_id
               LEFT JOIN primary_topics pt ON pt.content_id = lc.content_id
               ORDER BY lc.content_id IS NULL,
                        lc.content_id ASC,
                        lc.link_url ASC,
                        lc.newsletter_send_id ASC""",
            (f"-{int(days)} days",),
        )
        return [dict(row) for row in cursor.fetchall()]

    @staticmethod
    def _add_link_row(
        bucket: dict,
        row: dict,
        clicks: int,
        unique_clicks: int,
    ) -> None:
        bucket["clicks"] += clicks
        bucket["unique_clicks"] += unique_clicks
        bucket["send_ids"].add(int(row["newsletter_send_id"]))
        bucket["latest_fetched_at"] = max(
            bucket["latest_fetched_at"],
            row.get("fetched_at") or "",
        )
        if not bucket.get("source_kind"):
            bucket["source_kind"] = row.get("source_kind")

    @staticmethod
    def _format_link_buckets(buckets) -> list[NewsletterClickLinkSummary]:
        links = [
            NewsletterClickLinkSummary(
                normalized_url=item["normalized_url"],
                clicks=item["clicks"],
                unique_clicks=item["unique_clicks"],
                send_count=len(item["send_ids"]),
                source_kind=item.get("source_kind"),
                latest_fetched_at=item.get("latest_fetched_at") or "",
            )
            for item in buckets
        ]
        links.sort(key=lambda item: (-item.clicks, item.normalized_url))
        return links


def format_newsletter_click_attribution_json(
    summary: NewsletterClickAttributionSummary,
) -> str:
    """Return stable artifact-friendly JSON."""
    return json.dumps(asdict(summary), indent=2, sort_keys=True)


def format_newsletter_click_attribution_text(
    summary: NewsletterClickAttributionSummary,
) -> str:
    """Return a compact human-readable report."""
    lines = [
        f"Newsletter Click Attribution (last {summary.period_days} days)",
        f"Clicks: {summary.total_clicks} total, {summary.attributed_clicks} attributed, "
        f"{summary.unattributed_clicks} unattributed",
        "",
    ]
    if summary.by_content:
        lines.append("Attributed content:")
        for item in summary.by_content:
            topic = f", topic {item.topic}" if item.topic else ""
            lines.append(
                f"- content_id {item.content_id} ({item.content_type}{topic}): "
                f"{item.clicks} clicks"
            )
            for link in item.links:
                lines.append(f"  {link.normalized_url}: {link.clicks} clicks")
    else:
        lines.append("Attributed content: none")

    lines.append("")
    if summary.unattributed_links:
        lines.append("Unattributed links:")
        for link in summary.unattributed_links:
            lines.append(f"- {link.normalized_url}: {link.clicks} clicks")
    else:
        lines.append("Unattributed links: none")
    return "\n".join(lines)
