"""Generate and commit blog posts to static site."""

import re
import json
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Any
from dataclasses import dataclass


@dataclass
class BlogResult:
    success: bool
    file_path: Optional[str] = None
    url: Optional[str] = None
    error: Optional[str] = None


BLOG_TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title} - Taka Ishikawa</title>
  <meta name="description" content="{description}">
  <link rel="stylesheet" href="../style.css">
</head>
<body>
  <main>
    <a href="/" class="back-link">&larr; Back</a>

    <article>
      <header class="post-header">
        <h1>{title}</h1>
        <span class="date">{date}</span>
      </header>

      <div class="post-content">
{content}
      </div>
    </article>
  </main>
</body>
</html>
'''


def _json_frontmatter_value(value: Any) -> str:
    """Serialize frontmatter values as YAML-compatible JSON scalars."""
    return json.dumps(value, ensure_ascii=False)


class BlogWriter:
    def __init__(self, site_path: str, base_url: str = "https://takaishikawa.com") -> None:
        self.site_path = Path(site_path).expanduser()
        self.blog_path = self.site_path / "blog"
        self.drafts_path = self.site_path / "drafts"
        self.base_url = base_url

    def _slugify(self, title: str) -> str:
        """Convert title to URL-friendly slug."""
        slug = title.lower()
        slug = re.sub(r"[^a-z0-9\s-]", "", slug)
        slug = re.sub(r"[\s_]+", "-", slug)
        slug = re.sub(r"-+", "-", slug)
        return slug.strip("-")

    def _markdown_to_html(self, markdown: str) -> str:
        """Convert markdown content to HTML."""
        html_lines = []

        for line in markdown.split("\n"):
            line = line.strip()

            if not line:
                continue
            elif line.startswith("## "):
                html_lines.append(f"        <h2>{line[3:]}</h2>")
            elif line.startswith("### "):
                html_lines.append(f"        <h3>{line[4:]}</h3>")
            elif line.startswith("- "):
                html_lines.append(f"        <li>{line[2:]}</li>")
            elif line.startswith("**") and line.endswith("**"):
                html_lines.append(f"        <p><strong>{line[2:-2]}</strong></p>")
            else:
                # Handle inline bold
                line = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", line)
                html_lines.append(f"        <p>{line}</p>")

        return "\n".join(html_lines)

    def _extract_description(self, content: str) -> str:
        """Extract first paragraph as description."""
        lines = [l.strip() for l in content.split("\n") if l.strip()]
        for line in lines:
            if not line.startswith("#") and not line.startswith("-"):
                # Truncate to ~160 chars
                if len(line) > 160:
                    return line[:157] + "..."
                return line
        return ""

    def _extract_title_and_body(self, content: str) -> tuple[Optional[str], str]:
        """Extract TITLE: metadata and the remaining markdown body."""
        title_match = re.search(r"^TITLE:\s*(.+)$", content, re.MULTILINE)
        if not title_match:
            return None, content.strip()

        title = title_match.group(1).strip()
        body = content[title_match.end():].strip()
        return title, body

    def _normalize_tags(self, tags: list[str] | None) -> list[str]:
        """Normalize tags to stable, taxonomy-style slugs."""
        normalized = []
        seen = set()
        for tag in tags or []:
            text = str(tag).strip().lower()
            if not text:
                continue
            text = re.sub(r"[^a-z0-9\s-]", "", text)
            text = re.sub(r"[\s_]+", "-", text)
            text = re.sub(r"-+", "-", text).strip("-")
            if text and text not in seen:
                normalized.append(text)
                seen.add(text)
        return normalized

    def _tags_from_topics(self, topics: list[Any] | None) -> list[str]:
        """Convert topic extraction results into blog tags."""
        tags = []
        for topic in topics or []:
            if isinstance(topic, dict):
                label = topic.get("topic")
            elif isinstance(topic, (list, tuple)) and topic:
                label = topic[0]
            else:
                label = topic
            if label and label != "other":
                tags.append(str(label))
        return self._normalize_tags(tags)

    def _derive_tags(
        self,
        body: str,
        tags: list[str] | None = None,
        topics: list[Any] | None = None,
    ) -> list[str]:
        """Derive tags from explicit tags, topic extraction rows, or taxonomy keywords."""
        explicit_tags = self._normalize_tags(tags)
        if explicit_tags:
            return explicit_tags

        topic_tags = self._tags_from_topics(topics)
        if topic_tags:
            return topic_tags

        try:
            from synthesis.content_gaps import classify_source_topics

            return self._normalize_tags(classify_source_topics(body))
        except ImportError:
            return []

    def _frontmatter(
        self,
        *,
        title: str,
        summary: str,
        source_commits: list[str] | None = None,
        source_sessions: list[str] | None = None,
        generated_content_id: int | None = None,
        canonical_social_post_url: str | None = None,
        tags: list[str] | None = None,
    ) -> str:
        """Build structured frontmatter for generated static-site posts."""
        fields = [
            ("title", title),
            ("summary", summary),
            ("source_commits", source_commits or []),
            ("source_sessions", source_sessions or []),
            ("generated_content_id", generated_content_id),
            ("canonical_social_post_url", canonical_social_post_url),
            ("tags", tags or []),
        ]
        lines = ["---"]
        lines.extend(f"{key}: {_json_frontmatter_value(value)}" for key, value in fields)
        lines.extend(["---", ""])
        return "\n".join(lines)

    def _update_index(self, slug: str, title: str, date_str: str) -> None:
        """Add new post to index.html."""
        index_path = self.site_path / "index.html"
        content = index_path.read_text()

        new_entry = f'          <li><a href="/blog/{slug}.html">{title}</a><span class="date">{date_str}</span></li>'

        # Find the posts list and insert at top
        pattern = r'(<ul class="posts">)\s*\n(\s*<li>)'
        replacement = f'\\1\n{new_entry}\n\\2'
        updated = re.sub(pattern, replacement, content)

        if updated != content:
            index_path.write_text(updated)

    def write_post(
        self,
        content: str,
        *,
        source_commits: list[str] | None = None,
        source_sessions: list[str] | None = None,
        generated_content_id: int | None = None,
        canonical_social_post_url: str | None = None,
        tags: list[str] | None = None,
        topics: list[Any] | None = None,
        summary: str | None = None,
    ) -> BlogResult:
        """Parse generated content and write blog post."""
        title, body = self._extract_title_and_body(content)
        if not title:
            return BlogResult(success=False, error="No title found in content")

        # Generate slug and paths
        slug = self._slugify(title)
        file_path = self.blog_path / f"{slug}.html"

        # Convert to HTML
        html_content = self._markdown_to_html(body)
        description = summary or self._extract_description(body)
        date_str = datetime.now().strftime("%B %Y")
        post_tags = self._derive_tags(body, tags=tags, topics=topics)

        # Generate full HTML
        html = self._frontmatter(
            title=title,
            summary=description,
            source_commits=source_commits,
            source_sessions=source_sessions,
            generated_content_id=generated_content_id,
            canonical_social_post_url=canonical_social_post_url,
            tags=post_tags,
        ) + BLOG_TEMPLATE.format(
            title=title,
            description=description,
            date=date_str,
            content=html_content
        )

        # Write file
        file_path.write_text(html)

        # Update index
        self._update_index(slug, title, date_str)

        return BlogResult(
            success=True,
            file_path=str(file_path),
            url=f"{self.base_url}/blog/{slug}.html"
        )

    def write_draft(
        self,
        content: str,
        source_content_id: int,
        generated_content_id: int,
    ) -> BlogResult:
        """Write generated markdown as a reviewable static-site draft."""
        title, body = self._extract_title_and_body(content)
        if not title:
            return BlogResult(success=False, error="No title found in content")

        slug = self._slugify(title)
        file_path = self.drafts_path / f"{slug}.md"
        created_at = datetime.now(timezone.utc).isoformat()

        frontmatter = "\n".join([
            "---",
            f"title: {json.dumps(title)}",
            f"source_content_id: {source_content_id}",
            f"generated_content_id: {generated_content_id}",
            "status: draft",
            f"created_at: {json.dumps(created_at)}",
            "---",
            "",
        ])

        self.drafts_path.mkdir(parents=True, exist_ok=True)
        file_path.write_text(frontmatter + body + "\n")

        return BlogResult(
            success=True,
            file_path=str(file_path),
        )

    def commit_and_push(self, title: str) -> bool:
        """Commit and push blog changes to trigger GitHub Pages deploy."""
        try:
            # Stage only blog and index files, not unrelated changes
            subprocess.run(
                ["git", "add", "blog/", "index.html"],
                cwd=self.site_path,
                check=True,
                capture_output=True
            )
            subprocess.run(
                ["git", "commit", "-m", f"Add blog post: {title}"],
                cwd=self.site_path,
                check=True,
                capture_output=True
            )
            subprocess.run(
                ["git", "push"],
                cwd=self.site_path,
                check=True,
                capture_output=True
            )
            return True
        except subprocess.CalledProcessError:
            return False
