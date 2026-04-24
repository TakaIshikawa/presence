"""Attribute Buttondown newsletter link clicks to generated content."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import parse_qsl, urlparse

from output.newsletter import normalize_newsletter_link_url


SECTION_BY_CONTENT_TYPE = {
    "blog_post": "This Week's Post",
    "x_thread": "Threads",
    "x_post": "Posts",
}


@dataclass
class NewsletterLinkIssueTotal:
    issue_id: str
    newsletter_send_id: int
    subject: str
    sent_at: str
    clicks: int
    unique_clicks: Optional[int]


@dataclass
class NewsletterLinkUrlTotal:
    url: str
    clicks: int = 0
    unique_clicks: Optional[int] = None
    content_id: Optional[int] = None
    content_type: Optional[str] = None
    section: Optional[str] = None
    attribution_status: str = "unmapped"
    issue_count: int = 0
    issues: list[NewsletterLinkIssueTotal] = field(default_factory=list)


@dataclass
class NewsletterContentTotal:
    content_id: int
    content_type: str
    section: str
    clicks: int = 0
    unique_clicks: Optional[int] = None
    url_count: int = 0
    issue_count: int = 0


@dataclass
class NewsletterContentTypeTotal:
    content_type: str
    clicks: int = 0
    unique_clicks: Optional[int] = None
    content_count: int = 0
    url_count: int = 0


@dataclass
class NewsletterIssueTotal:
    issue_id: str
    newsletter_send_id: int
    subject: str
    sent_at: str
    clicks: int = 0
    unique_clicks: Optional[int] = None
    mapped_clicks: int = 0
    unmapped_clicks: int = 0
    ambiguous_clicks: int = 0
    url_count: int = 0


@dataclass
class NewsletterLinkPerformanceReport:
    period_days: int
    issue_id: Optional[str]
    content_id: Optional[int]
    limit: int
    total_clicks: int
    total_unique_clicks: Optional[int]
    mapped_clicks: int
    unmapped_clicks: int
    ambiguous_clicks: int
    unmapped_link_count: int
    ambiguous_link_count: int
    malformed_send_count: int
    ranked_urls: list[NewsletterLinkUrlTotal]
    by_content: list[NewsletterContentTotal]
    by_content_type: list[NewsletterContentTypeTotal]
    by_issue: list[NewsletterIssueTotal]


class NewsletterLinkPerformance:
    """Build link-level newsletter click attribution reports."""

    def __init__(self, db) -> None:
        self.db = db

    def summarize(
        self,
        days: int = 90,
        issue_id: Optional[str] = None,
        content_id: Optional[int] = None,
        limit: int = 20,
    ) -> NewsletterLinkPerformanceReport:
        """Return latest link-click snapshots ranked and attributed."""
        limit = max(int(limit), 1)
        rows = self._load_latest_link_clicks(days=days, issue_id=issue_id)
        send_ids = {int(row["newsletter_send_id"]) for row in rows}
        source_map, malformed_send_ids = self._load_send_source_map(send_ids)
        content_map = self._load_content_map(source_map)

        url_totals: dict[str, NewsletterLinkUrlTotal] = {}
        content_totals: dict[int, NewsletterContentTotal] = {}
        type_totals: dict[str, NewsletterContentTypeTotal] = {}
        issue_totals: dict[tuple[int, str], NewsletterIssueTotal] = {}
        malformed_counted: set[int] = set()
        total_clicks = 0
        total_unique_clicks: Optional[int] = None
        mapped_clicks = 0
        unmapped_clicks = 0
        ambiguous_clicks = 0
        unmapped_links: set[tuple[int, str]] = set()
        ambiguous_links: set[tuple[int, str]] = set()

        for row in rows:
            send_id = int(row["newsletter_send_id"])
            url = normalize_newsletter_link_url(row.get("link_url") or "")
            if not url:
                continue
            clicks = int(row.get("clicks") or 0)
            unique_clicks = self._optional_int(row.get("unique_clicks"))
            source_ids = source_map.get(send_id, [])
            if send_id in malformed_send_ids:
                malformed_counted.add(send_id)

            attribution = self._attribute_url(
                url=url,
                raw_url=row.get("raw_url") or "",
                source_ids=source_ids,
                content_map=content_map,
            )
            if content_id is not None and attribution.content_id != content_id:
                continue

            total_clicks += clicks
            total_unique_clicks = self._sum_optional(total_unique_clicks, unique_clicks)
            issue_key = (send_id, row.get("issue_id") or "")
            issue_total = issue_totals.setdefault(
                issue_key,
                NewsletterIssueTotal(
                    issue_id=row.get("issue_id") or "",
                    newsletter_send_id=send_id,
                    subject=row.get("subject") or "",
                    sent_at=row.get("sent_at") or "",
                ),
            )
            issue_total.clicks += clicks
            issue_total.unique_clicks = self._sum_optional(
                issue_total.unique_clicks, unique_clicks
            )
            issue_total.url_count += 1

            if attribution.status == "mapped":
                mapped_clicks += clicks
                issue_total.mapped_clicks += clicks
                self._add_content_total(
                    content_totals,
                    type_totals,
                    attribution,
                    clicks,
                    unique_clicks,
                    url,
                    send_id,
                )
            elif attribution.status == "ambiguous":
                ambiguous_clicks += clicks
                issue_total.ambiguous_clicks += clicks
                ambiguous_links.add((send_id, url))
            else:
                unmapped_clicks += clicks
                issue_total.unmapped_clicks += clicks
                unmapped_links.add((send_id, url))

            url_total = url_totals.setdefault(
                url,
                NewsletterLinkUrlTotal(
                    url=url,
                    content_id=attribution.content_id,
                    content_type=attribution.content_type,
                    section=attribution.section,
                    attribution_status=attribution.status,
                ),
            )
            self._update_url_attribution(url_total, attribution)
            url_total.clicks += clicks
            url_total.unique_clicks = self._sum_optional(
                url_total.unique_clicks, unique_clicks
            )
            url_total.issues.append(
                NewsletterLinkIssueTotal(
                    issue_id=row.get("issue_id") or "",
                    newsletter_send_id=send_id,
                    subject=row.get("subject") or "",
                    sent_at=row.get("sent_at") or "",
                    clicks=clicks,
                    unique_clicks=unique_clicks,
                )
            )

        for total in url_totals.values():
            total.issue_count = len({issue.newsletter_send_id for issue in total.issues})
            total.issues.sort(
                key=lambda item: (item.sent_at, item.newsletter_send_id, item.issue_id),
                reverse=True,
            )

        ranked_urls = sorted(
            url_totals.values(),
            key=lambda item: (-item.clicks, item.url),
        )[:limit]
        by_content = sorted(
            content_totals.values(),
            key=lambda item: (-item.clicks, item.content_id),
        )[:limit]
        by_content_type = sorted(
            type_totals.values(),
            key=lambda item: (-item.clicks, item.content_type),
        )
        by_issue = sorted(
            issue_totals.values(),
            key=lambda item: (item.sent_at, item.newsletter_send_id, item.issue_id),
            reverse=True,
        )

        return NewsletterLinkPerformanceReport(
            period_days=int(days),
            issue_id=issue_id,
            content_id=content_id,
            limit=limit,
            total_clicks=total_clicks,
            total_unique_clicks=total_unique_clicks,
            mapped_clicks=mapped_clicks,
            unmapped_clicks=unmapped_clicks,
            ambiguous_clicks=ambiguous_clicks,
            unmapped_link_count=len(unmapped_links),
            ambiguous_link_count=len(ambiguous_links),
            malformed_send_count=len(malformed_counted),
            ranked_urls=ranked_urls,
            by_content=by_content,
            by_content_type=by_content_type,
            by_issue=by_issue,
        )

    def _load_latest_link_clicks(
        self, days: int, issue_id: Optional[str]
    ) -> list[dict]:
        filters = ["datetime(ns.sent_at) >= datetime('now', ?)"]
        params: list[object] = [f"-{int(days)} days"]
        if issue_id:
            filters.append("ns.issue_id = ?")
            params.append(issue_id)
        where_clause = " AND ".join(filters)
        cursor = self.db.conn.execute(
            f"""WITH latest_link_clicks AS (
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
               )
               SELECT llc.newsletter_send_id, llc.issue_id, llc.link_url,
                      llc.raw_url, llc.clicks, llc.unique_clicks, llc.fetched_at,
                      ns.subject, ns.sent_at
               FROM latest_link_clicks llc
               JOIN newsletter_sends ns ON ns.id = llc.newsletter_send_id
               WHERE {where_clause}
               ORDER BY llc.clicks DESC, llc.link_url ASC""",
            params,
        )
        return [dict(row) for row in cursor.fetchall()]

    def _load_send_source_map(
        self, send_ids: set[int]
    ) -> tuple[dict[int, list[int]], set[int]]:
        if not send_ids:
            return {}, set()
        placeholders = ",".join("?" for _ in send_ids)
        cursor = self.db.conn.execute(
            f"""SELECT id, source_content_ids
                FROM newsletter_sends
                WHERE id IN ({placeholders})""",
            tuple(sorted(send_ids)),
        )
        source_map: dict[int, list[int]] = {}
        malformed: set[int] = set()
        for row in cursor.fetchall():
            send_id = int(row["id"])
            try:
                parsed = json.loads(row["source_content_ids"] or "[]")
            except (TypeError, json.JSONDecodeError):
                source_map[send_id] = []
                malformed.add(send_id)
                continue
            if not isinstance(parsed, list):
                source_map[send_id] = []
                malformed.add(send_id)
                continue
            source_map[send_id] = [
                int(item)
                for item in parsed
                if isinstance(item, int)
                or (isinstance(item, str) and item.strip().isdigit())
            ]
        return source_map, malformed

    def _load_content_map(
        self, source_map: dict[int, list[int]]
    ) -> dict[int, dict]:
        content_ids = {content_id for ids in source_map.values() for content_id in ids}
        if not content_ids:
            return {}
        placeholders = ",".join("?" for _ in content_ids)
        cursor = self.db.conn.execute(
            f"""SELECT id, content_type, published_url
                FROM generated_content
                WHERE id IN ({placeholders})""",
            tuple(sorted(content_ids)),
        )
        content_map = {}
        for row in cursor.fetchall():
            item = dict(row)
            item["normalized_url"] = normalize_newsletter_link_url(
                item.get("published_url") or ""
            )
            item["section"] = SECTION_BY_CONTENT_TYPE.get(
                item.get("content_type") or "", item.get("content_type") or "Unknown"
            )
            content_map[int(item["id"])] = item
        return content_map

    def _attribute_url(
        self,
        url: str,
        raw_url: str,
        source_ids: list[int],
        content_map: dict[int, dict],
    ) -> "_Attribution":
        content_id_param = self._extract_content_id(raw_url)
        if content_id_param in source_ids and content_id_param in content_map:
            return self._mapped(content_map[content_id_param])

        matches = [
            content_map[content_id]
            for content_id in source_ids
            if content_id in content_map
            and content_map[content_id].get("normalized_url")
            and content_map[content_id]["normalized_url"] == url
        ]
        unique_ids = {int(item["id"]) for item in matches}
        if len(unique_ids) == 1:
            return self._mapped(matches[0])
        if len(unique_ids) > 1:
            return _Attribution(status="ambiguous")
        return _Attribution(status="unmapped")

    def _add_content_total(
        self,
        content_totals: dict[int, NewsletterContentTotal],
        type_totals: dict[str, NewsletterContentTypeTotal],
        attribution: "_Attribution",
        clicks: int,
        unique_clicks: Optional[int],
        url: str,
        send_id: int,
    ) -> None:
        if attribution.content_id is None or attribution.content_type is None:
            return
        content_total = content_totals.setdefault(
            attribution.content_id,
            NewsletterContentTotal(
                content_id=attribution.content_id,
                content_type=attribution.content_type,
                section=attribution.section or attribution.content_type,
            ),
        )
        content_total.clicks += clicks
        content_total.unique_clicks = self._sum_optional(
            content_total.unique_clicks, unique_clicks
        )
        seen_urls = getattr(content_total, "_seen_urls", set())
        seen_issues = getattr(content_total, "_seen_issues", set())
        seen_urls.add(url)
        seen_issues.add(send_id)
        setattr(content_total, "_seen_urls", seen_urls)
        setattr(content_total, "_seen_issues", seen_issues)
        content_total.url_count = len(seen_urls)
        content_total.issue_count = len(seen_issues)

        type_total = type_totals.setdefault(
            attribution.content_type,
            NewsletterContentTypeTotal(content_type=attribution.content_type),
        )
        type_total.clicks += clicks
        type_total.unique_clicks = self._sum_optional(
            type_total.unique_clicks, unique_clicks
        )
        type_urls = getattr(type_total, "_seen_urls", defaultdict(set))
        type_urls[attribution.content_id].add(url)
        setattr(type_total, "_seen_urls", type_urls)
        type_total.content_count = len(type_urls)
        type_total.url_count = sum(len(urls) for urls in type_urls.values())

    @staticmethod
    def _update_url_attribution(
        url_total: NewsletterLinkUrlTotal, attribution: "_Attribution"
    ) -> None:
        mapped_ids = getattr(url_total, "_mapped_content_ids", set())
        saw_unmapped = getattr(url_total, "_saw_unmapped", False)
        saw_ambiguous = getattr(url_total, "_saw_ambiguous", False)

        if attribution.status == "mapped" and attribution.content_id is not None:
            mapped_ids.add(attribution.content_id)
        elif attribution.status == "ambiguous":
            saw_ambiguous = True
        else:
            saw_unmapped = True

        setattr(url_total, "_mapped_content_ids", mapped_ids)
        setattr(url_total, "_saw_unmapped", saw_unmapped)
        setattr(url_total, "_saw_ambiguous", saw_ambiguous)

        if len(mapped_ids) == 1 and not saw_ambiguous:
            url_total.attribution_status = "mapped"
            url_total.content_id = next(iter(mapped_ids))
            if attribution.content_id == url_total.content_id:
                url_total.content_type = attribution.content_type
                url_total.section = attribution.section
        elif len(mapped_ids) > 1 or saw_ambiguous:
            url_total.attribution_status = "ambiguous"
            url_total.content_id = None
            url_total.content_type = None
            url_total.section = None
        elif saw_unmapped:
            url_total.attribution_status = "unmapped"

    @staticmethod
    def _mapped(content: dict) -> "_Attribution":
        return _Attribution(
            status="mapped",
            content_id=int(content["id"]),
            content_type=content.get("content_type"),
            section=content.get("section"),
        )

    @staticmethod
    def _extract_content_id(url: str) -> Optional[int]:
        parsed = urlparse(url or "")
        for key, value in parse_qsl(parsed.query, keep_blank_values=True):
            if key == "content_id" and value.isdigit():
                return int(value)
        return None

    @staticmethod
    def _optional_int(value: object) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _sum_optional(
        current: Optional[int], value: Optional[int]
    ) -> Optional[int]:
        if value is None:
            return current
        return (current or 0) + value


@dataclass
class _Attribution:
    status: str
    content_id: Optional[int] = None
    content_type: Optional[str] = None
    section: Optional[str] = None
