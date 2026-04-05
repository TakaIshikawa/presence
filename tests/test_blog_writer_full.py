"""Comprehensive unit tests for BlogWriter."""

import subprocess
from unittest.mock import patch, call

import pytest

from output.blog_writer import BlogWriter, BlogResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def site(tmp_path):
    """Minimal site structure with blog/ dir and index.html."""
    blog_dir = tmp_path / "blog"
    blog_dir.mkdir()

    index_html = tmp_path / "index.html"
    index_html.write_text(
        '<ul class="posts">\n'
        "  <li>existing post</li>\n"
        "</ul>\n"
    )

    return tmp_path


@pytest.fixture
def writer(site):
    return BlogWriter(site_path=str(site), base_url="https://example.com")


# ---------------------------------------------------------------------------
# _slugify
# ---------------------------------------------------------------------------

class TestSlugify:
    def test_basic(self, writer):
        assert writer._slugify("Hello World") == "hello-world"

    def test_special_chars_removed(self, writer):
        assert writer._slugify("What's Up?! (2024)") == "whats-up-2024"

    def test_spaces_to_hyphens(self, writer):
        assert writer._slugify("a  b   c") == "a-b-c"

    def test_multiple_hyphens_collapsed(self, writer):
        assert writer._slugify("a---b") == "a-b"

    def test_leading_trailing_hyphens_stripped(self, writer):
        assert writer._slugify("--hello--") == "hello"

    def test_case_lowered(self, writer):
        assert writer._slugify("UPPER Case") == "upper-case"

    def test_underscores_stripped(self, writer):
        # Underscores are removed by the special-char regex before space→hyphen conversion
        assert writer._slugify("hello_world") == "helloworld"

    def test_empty_string(self, writer):
        assert writer._slugify("") == ""


# ---------------------------------------------------------------------------
# _markdown_to_html
# ---------------------------------------------------------------------------

class TestMarkdownToHtml:
    def test_h2(self, writer):
        result = writer._markdown_to_html("## Section")
        assert "<h2>Section</h2>" in result

    def test_h3(self, writer):
        result = writer._markdown_to_html("### Subsection")
        assert "<h3>Subsection</h3>" in result

    def test_list_item(self, writer):
        result = writer._markdown_to_html("- item one")
        assert "<li>item one</li>" in result

    def test_full_line_bold(self, writer):
        result = writer._markdown_to_html("**Important**")
        assert "<p><strong>Important</strong></p>" in result

    def test_inline_bold(self, writer):
        result = writer._markdown_to_html("This is **very** important")
        assert "<p>This is <strong>very</strong> important</p>" in result

    def test_plain_paragraph(self, writer):
        result = writer._markdown_to_html("Plain text here")
        assert "<p>Plain text here</p>" in result

    def test_empty_lines_skipped(self, writer):
        result = writer._markdown_to_html("line one\n\nline two")
        lines = [l.strip() for l in result.split("\n") if l.strip()]
        assert len(lines) == 2

    def test_mixed_content(self, writer):
        md = "## Title\n\nSome paragraph\n\n- bullet\n\n### Sub\n\n**bold line**"
        result = writer._markdown_to_html(md)
        assert "<h2>Title</h2>" in result
        assert "<p>Some paragraph</p>" in result
        assert "<li>bullet</li>" in result
        assert "<h3>Sub</h3>" in result
        assert "<strong>bold line</strong>" in result


# ---------------------------------------------------------------------------
# _extract_description
# ---------------------------------------------------------------------------

class TestExtractDescription:
    def test_returns_first_paragraph(self, writer):
        content = "## Header\n\nFirst paragraph here.\n\nSecond paragraph."
        assert writer._extract_description(content) == "First paragraph here."

    def test_skips_headers(self, writer):
        content = "## Header\n### Sub\nActual text"
        assert writer._extract_description(content) == "Actual text"

    def test_skips_list_items(self, writer):
        content = "- item\n- item2\nReal paragraph"
        assert writer._extract_description(content) == "Real paragraph"

    def test_truncates_at_160(self, writer):
        long_line = "a" * 200
        result = writer._extract_description(long_line)
        assert len(result) == 160
        assert result.endswith("...")

    def test_empty_when_no_suitable_line(self, writer):
        content = "## Only\n### Headers\n- and\n- lists"
        assert writer._extract_description(content) == ""

    def test_empty_input(self, writer):
        assert writer._extract_description("") == ""


# ---------------------------------------------------------------------------
# write_post
# ---------------------------------------------------------------------------

class TestWritePost:
    def test_success(self, writer, site):
        content = "TITLE: My Test Post\n\nSome body content here."
        result = writer.write_post(content)

        assert result.success is True
        assert result.file_path == str(site / "blog" / "my-test-post.html")
        assert result.url == "https://example.com/blog/my-test-post.html"
        assert result.error is None

        # File was written
        html = (site / "blog" / "my-test-post.html").read_text()
        assert "<title>My Test Post - Taka Ishikawa</title>" in html
        assert "<p>Some body content here.</p>" in html

    def test_no_title_returns_failure(self, writer):
        result = writer.write_post("No title line here\nJust body.")
        assert result.success is False
        assert "No title" in result.error

    def test_index_updated(self, writer, site):
        content = "TITLE: Index Test\n\nBody."
        writer.write_post(content)

        index = (site / "index.html").read_text()
        assert "index-test.html" in index
        assert "Index Test" in index

    def test_html_structure(self, writer, site):
        content = "TITLE: Structure\n\n## Heading\n\nParagraph\n\n- Bullet"
        writer.write_post(content)

        html = (site / "blog" / "structure.html").read_text()
        assert "<!DOCTYPE html>" in html
        assert "<h2>Heading</h2>" in html
        assert "<p>Paragraph</p>" in html
        assert "<li>Bullet</li>" in html

    def test_description_in_meta(self, writer, site):
        content = "TITLE: Meta\n\nDescription paragraph."
        writer.write_post(content)

        html = (site / "blog" / "meta.html").read_text()
        assert 'content="Description paragraph."' in html


# ---------------------------------------------------------------------------
# _update_index
# ---------------------------------------------------------------------------

class TestUpdateIndex:
    def test_inserts_entry(self, writer, site):
        writer._update_index("my-post", "My Post", "April 2026")

        index = (site / "index.html").read_text()
        assert "/blog/my-post.html" in index
        assert "My Post" in index
        assert "April 2026" in index

    def test_preserves_existing_entries(self, writer, site):
        writer._update_index("new-post", "New Post", "April 2026")

        index = (site / "index.html").read_text()
        assert "existing post" in index
        assert "new-post" in index

    def test_new_entry_before_existing(self, writer, site):
        writer._update_index("new-post", "New Post", "April 2026")

        index = (site / "index.html").read_text()
        new_pos = index.index("new-post")
        old_pos = index.index("existing post")
        assert new_pos < old_pos

    def test_no_match_leaves_file_unchanged(self, writer, site):
        # Overwrite index with content that doesn't match the regex
        index_path = site / "index.html"
        original = "<div>no posts list</div>"
        index_path.write_text(original)

        writer._update_index("slug", "Title", "April 2026")
        assert index_path.read_text() == original


# ---------------------------------------------------------------------------
# commit_and_push
# ---------------------------------------------------------------------------

class TestCommitAndPush:
    @patch("output.blog_writer.subprocess.run")
    def test_calls_git_commands_in_order(self, mock_run, writer):
        result = writer.commit_and_push("My Post")

        assert result is True
        assert mock_run.call_count == 3

        calls = mock_run.call_args_list
        assert calls[0] == call(
            ["git", "add", "blog/", "index.html"],
            cwd=writer.site_path,
            check=True,
            capture_output=True,
        )
        assert calls[1] == call(
            ["git", "commit", "-m", "Add blog post: My Post"],
            cwd=writer.site_path,
            check=True,
            capture_output=True,
        )
        assert calls[2] == call(
            ["git", "push"],
            cwd=writer.site_path,
            check=True,
            capture_output=True,
        )

    @patch("output.blog_writer.subprocess.run", side_effect=subprocess.CalledProcessError(1, "git"))
    def test_returns_false_on_failure(self, mock_run, writer):
        assert writer.commit_and_push("Fail") is False

    @patch("output.blog_writer.subprocess.run")
    def test_commit_message_includes_title(self, mock_run, writer):
        writer.commit_and_push("Special Title!")
        commit_call = mock_run.call_args_list[1]
        assert "Special Title!" in commit_call[0][0][3]
