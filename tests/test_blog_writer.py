"""Tests for blog post generation (src/output/blog_writer.py)."""

import json
import subprocess
from unittest.mock import patch, call

import pytest

from output.blog_writer import BlogWriter, BlogResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

INDEX_TEMPLATE = """\
<html>
<body>
  <ul class="posts">
    <li><a href="/blog/existing.html">Existing Post</a><span class="date">January 2025</span></li>
  </ul>
</body>
</html>"""


def _setup_site(tmp_path):
    """Create a minimal site directory with blog/ and index.html."""
    blog_dir = tmp_path / "blog"
    blog_dir.mkdir()
    index_path = tmp_path / "index.html"
    index_path.write_text(INDEX_TEMPLATE)
    return tmp_path


# ---------------------------------------------------------------------------
# BlogWriter._slugify
# ---------------------------------------------------------------------------


class TestSlugify:
    def setup_method(self):
        self.writer = BlogWriter("/tmp/fake-site")

    def test_spaces_to_hyphens(self):
        assert self.writer._slugify("hello world") == "hello-world"

    def test_special_chars_removed(self):
        assert self.writer._slugify("hello! @world#") == "hello-world"

    def test_uppercase_lowered(self):
        assert self.writer._slugify("Hello World") == "hello-world"

    def test_consecutive_hyphens_collapsed(self):
        assert self.writer._slugify("hello---world") == "hello-world"

    def test_leading_trailing_hyphens_stripped(self):
        assert self.writer._slugify("--hello--") == "hello"

    def test_underscores_removed(self):
        """Underscores are stripped by the special-char pass before the whitespace pass."""
        assert self.writer._slugify("hello_world") == "helloworld"

    def test_mixed_special_chars(self):
        assert self.writer._slugify("My Post: A 2025 Review!") == "my-post-a-2025-review"


# ---------------------------------------------------------------------------
# BlogWriter._markdown_to_html
# ---------------------------------------------------------------------------


class TestMarkdownToHtml:
    def setup_method(self):
        self.writer = BlogWriter("/tmp/fake-site")

    def test_h2_heading(self):
        result = self.writer._markdown_to_html("## Heading Two")
        assert "<h2>Heading Two</h2>" in result

    def test_h3_heading(self):
        result = self.writer._markdown_to_html("### Heading Three")
        assert "<h3>Heading Three</h3>" in result

    def test_list_item(self):
        result = self.writer._markdown_to_html("- item one")
        assert "<li>item one</li>" in result

    def test_bold_line(self):
        result = self.writer._markdown_to_html("**bold text**")
        assert "<strong>bold text</strong>" in result
        assert "<p>" in result

    def test_plain_paragraph(self):
        result = self.writer._markdown_to_html("Just a paragraph.")
        assert "<p>Just a paragraph.</p>" in result

    def test_inline_bold_in_paragraph(self):
        result = self.writer._markdown_to_html("This is **important** text.")
        assert "<p>This is <strong>important</strong> text.</p>" in result

    def test_empty_lines_skipped(self):
        result = self.writer._markdown_to_html("line one\n\n\nline two")
        assert result.count("<p>") == 2

    def test_multiple_elements(self):
        md = "## Title\n\nSome text.\n\n- item"
        result = self.writer._markdown_to_html(md)
        assert "<h2>Title</h2>" in result
        assert "<p>Some text.</p>" in result
        assert "<li>item</li>" in result


# ---------------------------------------------------------------------------
# BlogWriter._extract_description
# ---------------------------------------------------------------------------


class TestExtractDescription:
    def setup_method(self):
        self.writer = BlogWriter("/tmp/fake-site")

    def test_extracts_first_paragraph(self):
        content = "## Header\n\nFirst paragraph here.\n\nSecond paragraph."
        assert self.writer._extract_description(content) == "First paragraph here."

    def test_skips_headers_and_lists(self):
        content = "## Header\n- list item\n\nActual paragraph."
        assert self.writer._extract_description(content) == "Actual paragraph."

    def test_truncates_at_160_chars(self):
        long_line = "a" * 200
        content = f"## Header\n\n{long_line}"
        result = self.writer._extract_description(content)
        assert len(result) == 160
        assert result.endswith("...")

    def test_only_headers_returns_empty(self):
        content = "## Header One\n### Header Two"
        assert self.writer._extract_description(content) == ""

    def test_only_lists_returns_empty(self):
        content = "- item one\n- item two"
        assert self.writer._extract_description(content) == ""


# ---------------------------------------------------------------------------
# BlogWriter.write_post
# ---------------------------------------------------------------------------


class TestWritePost:
    def test_creates_html_file(self, tmp_path):
        site = _setup_site(tmp_path)
        writer = BlogWriter(str(site))

        content = "TITLE: My Test Post\n\n## Intro\n\nHello world."
        result = writer.write_post(content)

        assert result.success is True
        assert result.file_path is not None
        html_file = site / "blog" / "my-test-post.html"
        assert html_file.exists()

    def test_html_contains_title_and_date(self, tmp_path):
        site = _setup_site(tmp_path)
        writer = BlogWriter(str(site))

        content = "TITLE: My Test Post\n\nSome body text."
        writer.write_post(content)

        html_file = site / "blog" / "my-test-post.html"
        html = html_file.read_text()
        assert "<h1>My Test Post</h1>" in html
        assert '<span class="date">' in html

    def test_html_contains_structured_frontmatter(self, tmp_path):
        site = _setup_site(tmp_path)
        writer = BlogWriter(str(site))

        content = "TITLE: My Test Post\n\nTesting the publishing workflow."
        writer.write_post(
            content,
            source_commits=["abc123"],
            source_sessions=["session-1"],
            generated_content_id=42,
            canonical_social_post_url="https://x.com/taka/status/123",
            tags=["Testing"],
            summary="Custom summary.",
        )

        html = (site / "blog" / "my-test-post.html").read_text()
        assert html.startswith("---\n")
        assert 'title: "My Test Post"' in html
        assert 'summary: "Custom summary."' in html
        assert 'source_commits: ["abc123"]' in html
        assert 'source_sessions: ["session-1"]' in html
        assert "generated_content_id: 42" in html
        assert 'canonical_social_post_url: "https://x.com/taka/status/123"' in html
        assert 'tags: ["testing"]' in html

    def test_derives_tags_from_topic_keywords(self, tmp_path):
        site = _setup_site(tmp_path)
        writer = BlogWriter(str(site))

        content = "TITLE: Topic Tags\n\nPytest fixtures improved coverage for integration tests."
        writer.write_post(content)

        html = (site / "blog" / "topic-tags.html").read_text()
        assert 'tags: ["testing"]' in html

    def test_html_contains_converted_content(self, tmp_path):
        site = _setup_site(tmp_path)
        writer = BlogWriter(str(site))

        content = "TITLE: Post\n\n## Section\n\nA paragraph."
        writer.write_post(content)

        html = (site / "blog" / "post.html").read_text()
        assert "<h2>Section</h2>" in html
        assert "<p>A paragraph.</p>" in html

    def test_updates_index_html(self, tmp_path):
        site = _setup_site(tmp_path)
        writer = BlogWriter(str(site))

        content = "TITLE: New Post\n\nBody."
        writer.write_post(content)

        index = (site / "index.html").read_text()
        assert "new-post.html" in index
        assert "New Post" in index

    def test_returns_url(self, tmp_path):
        site = _setup_site(tmp_path)
        writer = BlogWriter(str(site))

        content = "TITLE: URL Test\n\nBody."
        result = writer.write_post(content)

        assert result.url == "https://takaishikawa.com/blog/url-test.html"

    def test_missing_title_returns_error(self, tmp_path):
        site = _setup_site(tmp_path)
        writer = BlogWriter(str(site))

        result = writer.write_post("No title line here.\n\nJust body.")

        assert result.success is False
        assert "No title" in result.error


# ---------------------------------------------------------------------------
# BlogWriter.write_draft
# ---------------------------------------------------------------------------


class TestWriteDraft:
    def test_creates_markdown_draft_with_frontmatter(self, tmp_path):
        site = tmp_path
        writer = BlogWriter(str(site))

        content = "TITLE: My Draft Post\n\n## Outline\n\nDraft body."
        result = writer.write_draft(
            content,
            source_content_id=123,
            generated_content_id=42,
        )

        draft_file = site / "drafts" / "my-draft-post.md"
        assert result.success is True
        assert result.file_path == str(draft_file)
        assert draft_file.exists()

        draft = draft_file.read_text()
        assert draft.startswith("---\n")
        assert 'title: "My Draft Post"' in draft
        assert "source_content_id: 123" in draft
        assert "generated_content_id: 42" in draft
        assert "status: draft" in draft
        assert "created_at: " in draft
        assert "TITLE:" not in draft
        assert "## Outline\n\nDraft body." in draft

    def test_updates_default_draft_manifest(self, tmp_path):
        writer = BlogWriter(str(tmp_path))

        result = writer.write_draft(
            "TITLE: Manifest Draft\n\nPytest fixtures made this draft easier to review.",
            source_content_id=123,
            generated_content_id=42,
            topics=[("architecture", "module boundaries", 0.9)],
        )

        manifest_path = tmp_path / "drafts" / "manifest.json"
        assert result.success is True
        assert manifest_path.exists()

        manifest = json.loads(manifest_path.read_text())
        assert len(manifest["drafts"]) == 1
        entry = manifest["drafts"][0]
        assert entry["slug"] == "manifest-draft"
        assert entry["title"] == "Manifest Draft"
        assert entry["source_content_id"] == 123
        assert entry["generated_content_id"] == 42
        assert entry["created_at"]
        assert entry["tags"] == ["architecture"]
        assert entry["topics"] == [["architecture", "module boundaries", 0.9]]
        assert entry["draft_path"] == "drafts/manifest-draft.md"

    def test_custom_manifest_path_is_relative_to_site(self, tmp_path):
        writer = BlogWriter(str(tmp_path), manifest_path="data/blog-drafts.json")

        writer.write_draft(
            "TITLE: Custom Manifest\n\nDraft body.",
            source_content_id=11,
            generated_content_id=22,
            tags=["Writing Notes"],
        )

        manifest = json.loads((tmp_path / "data" / "blog-drafts.json").read_text())
        assert manifest["drafts"][0]["slug"] == "custom-manifest"
        assert manifest["drafts"][0]["tags"] == ["writing-notes"]

    def test_manifest_entry_is_replaced_for_same_generated_content(self, tmp_path):
        writer = BlogWriter(str(tmp_path))

        writer.write_draft(
            "TITLE: Old Title\n\nFirst draft.",
            source_content_id=1,
            generated_content_id=99,
        )
        writer.write_draft(
            "TITLE: New Title\n\nUpdated draft.",
            source_content_id=1,
            generated_content_id=99,
        )

        manifest = json.loads((tmp_path / "drafts" / "manifest.json").read_text())
        assert [entry["slug"] for entry in manifest["drafts"]] == ["new-title"]

    def test_missing_title_returns_error(self, tmp_path):
        writer = BlogWriter(str(tmp_path))

        result = writer.write_draft(
            "No title line here.\n\nJust body.",
            source_content_id=123,
            generated_content_id=42,
        )

        assert result.success is False
        assert "No title" in result.error
        assert not (tmp_path / "drafts").exists()
        assert not (tmp_path / "drafts" / "manifest.json").exists()


# ---------------------------------------------------------------------------
# BlogWriter._update_index
# ---------------------------------------------------------------------------


class TestUpdateIndex:
    def test_inserts_at_top_of_posts_list(self, tmp_path):
        site = _setup_site(tmp_path)
        writer = BlogWriter(str(site))

        writer._update_index("new-post", "New Post", "March 2025")

        index = (site / "index.html").read_text()
        # New entry should appear before the existing one
        new_pos = index.index("new-post.html")
        existing_pos = index.index("existing.html")
        assert new_pos < existing_pos

    def test_entry_contains_title_and_date(self, tmp_path):
        site = _setup_site(tmp_path)
        writer = BlogWriter(str(site))

        writer._update_index("test-slug", "Test Title", "April 2025")

        index = (site / "index.html").read_text()
        assert "test-slug.html" in index
        assert "Test Title" in index
        assert "April 2025" in index

    def test_no_change_when_pattern_missing(self, tmp_path):
        """Index without matching pattern is left unchanged."""
        site = tmp_path
        site.mkdir(exist_ok=True)
        index_path = site / "index.html"
        original = "<html><body>No posts list</body></html>"
        index_path.write_text(original)

        writer = BlogWriter(str(site))
        writer._update_index("slug", "Title", "Jan 2025")

        assert index_path.read_text() == original


# ---------------------------------------------------------------------------
# BlogWriter.commit_and_push
# ---------------------------------------------------------------------------


class TestCommitAndPush:
    @patch("output.blog_writer.subprocess.run")
    def test_calls_git_add_commit_push(self, mock_run):
        writer = BlogWriter("/tmp/fake-site")
        result = writer.commit_and_push("My Post")

        assert result is True
        assert mock_run.call_count == 3

        add_call, commit_call, push_call = mock_run.call_args_list

        assert add_call == call(
            ["git", "add", "blog/", "index.html"],
            cwd=writer.site_path,
            check=True,
            capture_output=True,
        )
        assert commit_call == call(
            ["git", "commit", "-m", "Add blog post: My Post"],
            cwd=writer.site_path,
            check=True,
            capture_output=True,
        )
        assert push_call == call(
            ["git", "push"],
            cwd=writer.site_path,
            check=True,
            capture_output=True,
        )

    @patch("output.blog_writer.subprocess.run")
    def test_returns_false_on_called_process_error(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(1, "git")
        writer = BlogWriter("/tmp/fake-site")

        assert writer.commit_and_push("Failing Post") is False
