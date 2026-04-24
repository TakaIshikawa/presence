"""Attribute newsletter link clicks back to generated content."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from output.newsletter import normalize_newsletter_link_url


@dataclass
class NewsletterContentClickAttribution:
    """Click totals attributed to one content item in one newsletter issue."""

    content_id: int
    content_type: str
    url: str
    issue_id: str
    clicks: int
    unique_clicks: int | None = None
    newsletter_send_id: int | None = None
    subject: str = ""
    sent_at: str = ""


@dataclass
class NewsletterUnmatchedLink:
    """Newsletter link-click row that could not be tied to source content."""

    newsletter_send_id: int
    issue_id: str
    url: str
    raw_url: str | None
    clicks: int
    unique_clicks: int | None = None
    fetched_at: str = ""


@dataclass
class NewsletterLinkAttributionReport:
    """Attributed and unattributed newsletter link-click totals."""

    period_days: int
    issue_id: str | None
    attributed_content: list[NewsletterContentClickAttribution] = field(default_factory=list)
    unmatched_links: list[NewsletterUnmatchedLink] = field(default_factory=list)


class NewsletterLinkAttribution:
    """Connect Buttondown link-click metrics to generated content rows."""

    def __init__(self, db) -> None:
        self.db = db

    def summarize(
        self,
        days: int = 90,
        issue_id: str | None = None,
    ) -> NewsletterLinkAttributionReport:
        """Return per-content click attribution for recent newsletter sends."""
        sends = self._load_sends(days=days, issue_id=issue_id)
        if not sends:
            return NewsletterLinkAttributionReport(period_days=days, issue_id=issue_id)

        content_ids = sorted(
            {
                content_id
                for send in sends
                for content_id in send["source_content_ids"]
            }
        )
        content_by_id = self._load_content(content_ids)

        attributed: dict[tuple[int, str], NewsletterContentClickAttribution] = {}
        unmatched: list[NewsletterUnmatchedLink] = []

        for send in sends:
            url_to_content_ids = self._url_to_source_content_ids(send, content_by_id)
            links = self._load_latest_link_clicks(int(send["id"]))
            for link in links:
                normalized_url = normalize_newsletter_link_url(link.get("link_url") or "")
                matched_ids = url_to_content_ids.get(normalized_url, set())
                if not matched_ids:
                    unmatched.append(
                        NewsletterUnmatchedLink(
                            newsletter_send_id=int(send["id"]),
                            issue_id=send.get("issue_id") or "",
                            url=link.get("link_url") or "",
                            raw_url=link.get("raw_url"),
                            clicks=int(link.get("clicks") or 0),
                            unique_clicks=self._optional_int(link.get("unique_clicks")),
                            fetched_at=link.get("fetched_at") or "",
                        )
                    )
                    continue

                for content_id in sorted(matched_ids):
                    content = content_by_id.get(content_id)
                    if not content:
                        continue
                    key = (content_id, send.get("issue_id") or "")
                    current = attributed.get(key)
                    if current is None:
                        current = NewsletterContentClickAttribution(
                            content_id=content_id,
                            content_type=content.get("content_type") or "",
                            url=content.get("published_url") or normalized_url,
                            issue_id=send.get("issue_id") or "",
                            clicks=0,
                            unique_clicks=None,
                            newsletter_send_id=int(send["id"]),
                            subject=send.get("subject") or "",
                            sent_at=send.get("sent_at") or "",
                        )
                        attributed[key] = current
                    current.clicks += int(link.get("clicks") or 0)
                    current.unique_clicks = self._sum_optional(
                        current.unique_clicks,
                        self._optional_int(link.get("unique_clicks")),
                    )

        ranked = sorted(
            attributed.values(),
            key=lambda item: (item.clicks, item.unique_clicks or 0, item.sent_at),
            reverse=True,
        )
        unmatched.sort(
            key=lambda item: (item.clicks, item.unique_clicks or 0, item.fetched_at),
            reverse=True,
        )
        return NewsletterLinkAttributionReport(
            period_days=days,
            issue_id=issue_id,
            attributed_content=ranked,
            unmatched_links=unmatched,
        )

    def _load_sends(self, days: int, issue_id: str | None) -> list[dict]:
        clauses = [
            "datetime(sent_at) >= datetime('now', ?)",
            "source_content_ids IS NOT NULL",
            "source_content_ids != ''",
        ]
        params: list[Any] = [f"-{int(days)} days"]
        if issue_id:
            clauses.append("issue_id = ?")
            params.append(issue_id)
        cursor = self.db.conn.execute(
            f"""SELECT id, issue_id, subject, source_content_ids, metadata, sent_at
                FROM newsletter_sends
                WHERE {' AND '.join(clauses)}
                ORDER BY sent_at DESC, id DESC""",
            params,
        )
        sends = []
        for row in cursor.fetchall():
            item = dict(row)
            item["source_content_ids"] = self._parse_int_list(
                item.get("source_content_ids")
            )
            item["metadata"] = self._parse_json_object(item.get("metadata"))
            if item["source_content_ids"]:
                sends.append(item)
        return sends

    def _load_content(self, content_ids: list[int]) -> dict[int, dict]:
        if not content_ids:
            return {}
        placeholders = ",".join("?" for _ in content_ids)
        cursor = self.db.conn.execute(
            f"""SELECT id, content_type, published_url
                FROM generated_content
                WHERE id IN ({placeholders})""",
            content_ids,
        )
        return {int(row["id"]): dict(row) for row in cursor.fetchall()}

    def _load_latest_link_clicks(self, newsletter_send_id: int) -> list[dict]:
        cursor = self.db.conn.execute(
            """SELECT nlc.id, nlc.newsletter_send_id, nlc.issue_id, nlc.link_url,
                      nlc.raw_url, nlc.clicks, nlc.unique_clicks, nlc.fetched_at
               FROM newsletter_link_clicks nlc
               WHERE nlc.newsletter_send_id = ?
                 AND nlc.fetched_at = (
                     SELECT MAX(latest.fetched_at)
                     FROM newsletter_link_clicks latest
                     WHERE latest.newsletter_send_id = nlc.newsletter_send_id
                 )
               ORDER BY nlc.clicks DESC, nlc.id DESC""",
            (newsletter_send_id,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _url_to_source_content_ids(
        self,
        send: dict,
        content_by_id: dict[int, dict],
    ) -> dict[str, set[int]]:
        mapping: dict[str, set[int]] = {}
        source_ids = [int(item) for item in send["source_content_ids"]]
        for content_id in source_ids:
            content = content_by_id.get(content_id)
            if not content:
                continue
            self._add_url_mapping(mapping, content.get("published_url"), content_id)

        for content_id, url in self._metadata_content_urls(
            send.get("metadata") or {},
            source_ids,
        ):
            if content_id in source_ids:
                self._add_url_mapping(mapping, url, content_id)

        return mapping

    def _metadata_content_urls(
        self,
        metadata: dict,
        source_ids: list[int],
    ) -> list[tuple[int, str]]:
        urls: list[tuple[int, str]] = []
        for key in ("body_urls", "body_links", "content_urls", "newsletter_urls", "links"):
            value = metadata.get(key)
            urls.extend(self._metadata_url_pairs(value, source_ids))
        return urls

    def _metadata_url_pairs(
        self,
        value: Any,
        source_ids: list[int],
    ) -> list[tuple[int, str]]:
        if not value:
            return []
        if isinstance(value, dict):
            pairs: list[tuple[int, str]] = []
            for key, item in value.items():
                content_id = self._optional_int(key)
                if content_id is not None and isinstance(item, str):
                    pairs.append((content_id, item))
                elif content_id is not None and isinstance(item, dict):
                    url = self._first_text(item, "url", "link_url", "href")
                    if url:
                        pairs.append((content_id, url))
                elif isinstance(key, str):
                    item_id = self._optional_int(item)
                    if item_id is not None:
                        pairs.append((item_id, key))
            return pairs
        if isinstance(value, list):
            if all(isinstance(item, str) for item in value):
                if len(value) == len(source_ids):
                    return list(zip(source_ids, value))
                if len(source_ids) == 1:
                    return [(source_ids[0], item) for item in value]
                return []
            pairs = []
            for item in value:
                if not isinstance(item, dict):
                    continue
                content_id = self._optional_int(
                    item.get("content_id")
                    or item.get("source_content_id")
                    or item.get("generated_content_id")
                    or item.get("id")
                )
                url = self._first_text(item, "url", "link_url", "href")
                if content_id is not None and url:
                    pairs.append((content_id, url))
            return pairs
        return []

    @staticmethod
    def _add_url_mapping(
        mapping: dict[str, set[int]],
        url: str | None,
        content_id: int,
    ) -> None:
        normalized = normalize_newsletter_link_url(url or "")
        if normalized:
            mapping.setdefault(normalized, set()).add(content_id)

    @staticmethod
    def _parse_int_list(value: Any) -> list[int]:
        if isinstance(value, str):
            try:
                value = json.loads(value or "[]")
            except json.JSONDecodeError:
                return []
        if not isinstance(value, list):
            return []
        items = []
        for item in value:
            parsed = NewsletterLinkAttribution._optional_int(item)
            if parsed is not None:
                items.append(parsed)
        return items

    @staticmethod
    def _parse_json_object(value: Any) -> dict:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value or "{}")
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    @staticmethod
    def _optional_int(value: Any) -> int | None:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _sum_optional(first: int | None, second: int | None) -> int | None:
        if first is None:
            return second
        if second is None:
            return first
        return first + second

    @staticmethod
    def _first_text(item: dict, *keys: str) -> str:
        for key in keys:
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""
