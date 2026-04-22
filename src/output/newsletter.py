"""Newsletter assembly and Buttondown delivery."""

import logging
import re
import requests
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from storage.db import Database

logger = logging.getLogger(__name__)

if not hasattr(requests, "RequestException"):
    requests.RequestException = Exception

if not hasattr(requests, "Session"):
    class _MissingRequestsSession:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("requests.Session is unavailable")

    requests.Session = _MissingRequestsSession


@dataclass
class NewsletterResult:
    success: bool
    issue_id: Optional[str] = None
    url: Optional[str] = None
    error: Optional[str] = None


@dataclass
class NewsletterContent:
    subject: str
    body_markdown: str
    source_content_ids: list[int] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


@dataclass
class NewsletterMetrics:
    issue_id: str
    opens: int = 0
    clicks: int = 0
    unsubscribes: int = 0


class NewsletterAssembler:
    """Assembles newsletter content from the week's published posts."""

    def __init__(
        self,
        db: Database,
        site_url: str = "https://takaishikawa.com",
        utm_source: str = "",
        utm_medium: str = "",
        utm_campaign_template: str = "",
    ):
        self.db = db
        self.site_url = site_url.rstrip("/")
        self.utm_source = utm_source
        self.utm_medium = utm_medium
        self.utm_campaign_template = utm_campaign_template

    def assemble(
        self, week_start: datetime, week_end: datetime
    ) -> NewsletterContent:
        """Gather this week's blog post + top threads + top posts into a newsletter."""
        content_ids = []
        sections = []
        source_preferences = self._load_resonant_source_preferences()
        utm_campaign = self._build_utm_campaign(week_start, week_end)

        # 1. Blog post (if published this week)
        blog_posts = self.db.get_published_content_in_range(
            "blog_post", week_start, week_end
        )
        blog_posts = self._sort_by_source_preferences(blog_posts, source_preferences)
        if blog_posts:
            post = blog_posts[0]
            content_ids.append(post["id"])
            title = self._extract_blog_title(post["content"])
            excerpt = self._extract_blog_excerpt(post["content"], max_lines=3)
            url = post.get("published_url", "")
            url = self._rewrite_internal_link(url, post["id"], utm_campaign)
            sections.append((
                "blog_post",
                f"## This Week's Post\n\n"
                f"**[{title}]({url})**\n\n"
                f"{excerpt}\n\n"
                f"[Read the full post]({url})",
            ))

        # 2. Top threads (up to 2)
        threads = self.db.get_published_content_in_range(
            "x_thread", week_start, week_end
        )
        threads = self._sort_by_source_preferences(threads, source_preferences)
        if threads:
            thread_items = []
            for thread in threads[:2]:
                content_ids.append(thread["id"])
                first_tweet = self._extract_first_tweet(thread["content"])
                url = thread.get("published_url", "")
                url = self._rewrite_internal_link(url, thread["id"], utm_campaign)
                link = f" ([thread]({url}))" if url else ""
                thread_items.append(f"- {first_tweet}{link}")
            sections.append(
                ("x_thread", "## Threads\n\n" + "\n\n".join(thread_items))
            )

        # 3. Top posts by engagement (up to 3)
        posts = self.db.get_published_content_in_range(
            "x_post", week_start, week_end
        )
        posts = self._sort_by_source_preferences(posts, source_preferences)
        if posts:
            post_items = []
            for post in posts[:3]:
                content_ids.append(post["id"])
                url = post.get("published_url", "")
                url = self._rewrite_internal_link(url, post["id"], utm_campaign)
                link = f" ([link]({url}))" if url else ""
                post_items.append(f"> {post['content']}{link}")
            sections.append(
                ("x_post", "## Posts\n\n" + "\n\n".join(post_items))
            )

        if not sections:
            return NewsletterContent(
                subject="", body_markdown="", source_content_ids=[]
            )

        # Compose the full newsletter
        date_str = week_end.strftime("%B %d, %Y")
        subject = f"Building with AI — Week of {week_start.strftime('%b %d')}"

        body = f"# Weekly Digest\n\n{date_str}\n\n"
        ordered_sections = self._order_sections(sections, source_preferences)
        body += "\n\n---\n\n".join(section for _, section in ordered_sections)
        body += (
            "\n\n---\n\n"
            f"*Shipped from [takaishikawa.com]({self.site_url})*"
        )

        return NewsletterContent(
            subject=subject,
            body_markdown=body,
            source_content_ids=content_ids,
            metadata={"utm_campaign": utm_campaign} if utm_campaign else {},
        )

    def _build_utm_campaign(
        self, week_start: datetime, week_end: datetime
    ) -> str:
        """Render configured UTM campaign template for this issue."""
        if not (
            self.utm_source
            and self.utm_medium
            and self.utm_campaign_template
        ):
            return ""
        context = {
            "week_start": week_start.strftime("%Y-%m-%d"),
            "week_end": week_end.strftime("%Y-%m-%d"),
            "week_start_compact": week_start.strftime("%Y%m%d"),
            "week_end_compact": week_end.strftime("%Y%m%d"),
        }
        try:
            return self.utm_campaign_template.format(**context)
        except (KeyError, ValueError) as e:
            logger.debug(f"Newsletter UTM campaign template failed: {e}")
            return self.utm_campaign_template

    def _rewrite_internal_link(
        self, url: str, content_id: int, utm_campaign: str
    ) -> str:
        """Add UTM/click attribution to internal links only."""
        if not url or not utm_campaign:
            return url

        parsed_url = urlparse(url)
        parsed_site = urlparse(self.site_url)
        if parsed_url.netloc and parsed_url.netloc != parsed_site.netloc:
            return url
        if parsed_url.scheme and parsed_url.scheme not in ("http", "https"):
            return url

        query = dict(parse_qsl(parsed_url.query, keep_blank_values=True))
        query.update({
            "utm_source": self.utm_source,
            "utm_medium": self.utm_medium,
            "utm_campaign": utm_campaign,
            "content_id": str(content_id),
        })
        return urlunparse(parsed_url._replace(query=urlencode(query)))

    def _load_resonant_source_preferences(self) -> list[dict]:
        """Load content type/format patterns from prior resonant sends."""
        getter = getattr(self.db, "get_resonant_newsletter_source_patterns", None)
        if getter is None:
            return []
        try:
            return getter()
        except Exception as e:
            logger.debug(f"Newsletter source preference lookup failed: {e}")
            return []

    @staticmethod
    def _sort_by_source_preferences(
        items: list[dict], preferences: list[dict]
    ) -> list[dict]:
        """Prefer current content matching prior resonant type/format patterns."""
        if not preferences:
            return items

        format_rank = {}
        type_rank = {}
        for index, pref in enumerate(preferences):
            content_type = pref.get("content_type")
            content_format = pref.get("content_format")
            if content_type and content_type not in type_rank:
                type_rank[content_type] = index
            if content_type and content_format:
                format_rank[(content_type, content_format)] = index

        def preference_key(item: dict) -> tuple[int, int]:
            content_type = item.get("content_type")
            content_format = item.get("content_format")
            return (
                format_rank.get((content_type, content_format), len(preferences)),
                type_rank.get(content_type, len(preferences)),
            )

        return sorted(items, key=preference_key)

    @staticmethod
    def _order_sections(
        sections: list[tuple[str, str]], preferences: list[dict]
    ) -> list[tuple[str, str]]:
        """Order newsletter sections by prior resonant content-type mix."""
        if not preferences:
            return sections

        default_order = {"blog_post": 0, "x_thread": 1, "x_post": 2}
        type_rank = {}
        for index, pref in enumerate(preferences):
            content_type = pref.get("content_type")
            if content_type and content_type not in type_rank:
                type_rank[content_type] = index

        return sorted(
            sections,
            key=lambda section: (
                type_rank.get(section[0], len(preferences)),
                default_order.get(section[0], len(default_order)),
            ),
        )

    @staticmethod
    def _extract_blog_title(content: str) -> str:
        """Extract title from blog post content."""
        match = re.search(r"^TITLE:\s*(.+)$", content, re.MULTILINE)
        return match.group(1).strip() if match else "This Week's Post"

    @staticmethod
    def _extract_blog_excerpt(content: str, max_lines: int = 3) -> str:
        """Extract first few non-title, non-header lines as excerpt."""
        lines = content.split("\n")
        excerpt_lines = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("TITLE:") or stripped.startswith("#"):
                continue
            excerpt_lines.append(stripped)
            if len(excerpt_lines) >= max_lines:
                break
        return " ".join(excerpt_lines)

    @staticmethod
    def _extract_first_tweet(content: str) -> str:
        """Extract the first tweet from a thread."""
        match = re.search(
            r"TWEET\s+1:\s*\n(.+?)(?:\n\s*\nTWEET\s+\d+:|$)",
            content, re.DOTALL
        )
        if match:
            return match.group(1).strip()
        # Fallback: first non-empty line
        for line in content.split("\n"):
            stripped = line.strip()
            if stripped and not stripped.startswith("TWEET"):
                return stripped
        return content[:100]


class ButtondownClient:
    """Buttondown API client for sending newsletter emails."""

    BASE_URL = "https://api.buttondown.com/v1"

    def __init__(self, api_key: str, timeout: int = 30):
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Token {api_key}"

    def send(
        self, subject: str, body: str, publish: bool = True
    ) -> NewsletterResult:
        """Send a newsletter issue via Buttondown API."""
        status = "published" if publish else "draft"
        try:
            response = self.session.post(
                f"{self.BASE_URL}/emails",
                json={
                    "subject": subject,
                    "body": body,
                    "status": status,
                },
                timeout=self.timeout,
            )
            if response.status_code in (200, 201):
                data = response.json()
                return NewsletterResult(
                    success=True,
                    issue_id=data.get("id", ""),
                    url=data.get("absolute_url", ""),
                )
            else:
                return NewsletterResult(
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text[:200]}",
                )
        except requests.RequestException as e:
            return NewsletterResult(success=False, error=str(e))

    def get_subscriber_count(self) -> int:
        """Get current subscriber count."""
        try:
            response = self.session.get(
                f"{self.BASE_URL}/subscribers",
                params={"type": "regular"},
                timeout=self.timeout,
            )
            if response.status_code == 200:
                return response.json().get("count", 0)
        except requests.RequestException as e:
            logger.debug(f"Subscriber count fetch failed: {e}")
        return 0

    def get_email_analytics(self, issue_id: str) -> Optional[NewsletterMetrics]:
        """Fetch aggregate analytics for a Buttondown email issue."""
        try:
            response = self.session.get(
                f"{self.BASE_URL}/emails/{issue_id}/analytics",
                timeout=self.timeout,
            )
            if response.status_code == 200:
                data = response.json()
                return NewsletterMetrics(
                    issue_id=issue_id,
                    opens=int(data.get("opens") or 0),
                    clicks=int(data.get("clicks") or 0),
                    unsubscribes=int(data.get("unsubscriptions") or 0),
                )
        except (ValueError, TypeError, requests.RequestException):
            pass
        return None
