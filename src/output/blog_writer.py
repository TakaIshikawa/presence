"""Generate and commit blog posts to static site."""

import re
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Optional
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


class BlogWriter:
    def __init__(self, site_path: str, base_url: str = "https://takaishikawa.com"):
        self.site_path = Path(site_path).expanduser()
        self.blog_path = self.site_path / "blog"
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

    def write_post(self, content: str) -> BlogResult:
        """Parse generated content and write blog post."""
        # Extract title
        title_match = re.search(r"^TITLE:\s*(.+)$", content, re.MULTILINE)
        if not title_match:
            return BlogResult(success=False, error="No title found in content")

        title = title_match.group(1).strip()
        body = content[title_match.end():].strip()

        # Generate slug and paths
        slug = self._slugify(title)
        file_path = self.blog_path / f"{slug}.html"

        # Convert to HTML
        html_content = self._markdown_to_html(body)
        description = self._extract_description(body)
        date_str = datetime.now().strftime("%B %Y")

        # Generate full HTML
        html = BLOG_TEMPLATE.format(
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

    def commit_and_push(self, title: str) -> bool:
        """Commit and push changes to trigger GitHub Pages deploy."""
        try:
            subprocess.run(
                ["git", "add", "."],
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
