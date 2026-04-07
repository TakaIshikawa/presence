"""Newsletter assembly and Buttondown delivery."""

import re
import requests
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from storage.db import Database


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


class NewsletterAssembler:
    """Assembles newsletter content from the week's published posts."""

    def __init__(self, db: Database, site_url: str = "https://takaishikawa.com"):
        self.db = db
        self.site_url = site_url

    def assemble(
        self, week_start: datetime, week_end: datetime
    ) -> NewsletterContent:
        """Gather this week's blog post + top threads + top posts into a newsletter."""
        content_ids = []
        sections = []

        # 1. Blog post (if published this week)
        blog_posts = self.db.get_published_content_in_range(
            "blog_post", week_start, week_end
        )
        if blog_posts:
            post = blog_posts[0]
            content_ids.append(post["id"])
            title = self._extract_blog_title(post["content"])
            excerpt = self._extract_blog_excerpt(post["content"], max_lines=3)
            url = post.get("published_url", "")
            sections.append(
                f"## This Week's Post\n\n"
                f"**[{title}]({url})**\n\n"
                f"{excerpt}\n\n"
                f"[Read the full post]({url})"
            )

        # 2. Top threads (up to 2)
        threads = self.db.get_published_content_in_range(
            "x_thread", week_start, week_end
        )
        if threads:
            thread_items = []
            for thread in threads[:2]:
                content_ids.append(thread["id"])
                first_tweet = self._extract_first_tweet(thread["content"])
                url = thread.get("published_url", "")
                link = f" ([thread]({url}))" if url else ""
                thread_items.append(f"- {first_tweet}{link}")
            sections.append(
                "## Threads\n\n" + "\n\n".join(thread_items)
            )

        # 3. Top posts by engagement (up to 3)
        posts = self.db.get_published_content_in_range(
            "x_post", week_start, week_end
        )
        if posts:
            post_items = []
            for post in posts[:3]:
                content_ids.append(post["id"])
                url = post.get("published_url", "")
                link = f" ([link]({url}))" if url else ""
                post_items.append(f"> {post['content']}{link}")
            sections.append(
                "## Posts\n\n" + "\n\n".join(post_items)
            )

        if not sections:
            return NewsletterContent(
                subject="", body_markdown="", source_content_ids=[]
            )

        # Compose the full newsletter
        date_str = week_end.strftime("%B %d, %Y")
        subject = f"Building with AI — Week of {week_start.strftime('%b %d')}"

        body = f"# Weekly Digest\n\n{date_str}\n\n"
        body += "\n\n---\n\n".join(sections)
        body += (
            "\n\n---\n\n"
            f"*Shipped from [takaishikawa.com]({self.site_url})*"
        )

        return NewsletterContent(
            subject=subject,
            body_markdown=body,
            source_content_ids=content_ids,
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
        except requests.RequestException:
            pass
        return 0
