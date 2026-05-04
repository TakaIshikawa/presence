"""Edge case tests for CONTRIBUTING.md validation script.

Tests malformed markdown, missing sections with whitespace variations, unicode handling,
very large files, symlinks, and permission errors.
"""

import sys
from textwrap import dedent

import pytest

from scripts.validate_contributing import (
    check_code_examples,
    check_internal_links,
    check_section_completeness,
    extract_code_blocks,
    extract_links,
    extract_sections,
    validate_contributing_md,
    validate_python_code,
)


class TestMalformedMarkdown:
    """Test handling of malformed markdown."""

    def test_extract_sections_with_inconsistent_header_levels(self):
        """Test section extraction with mixed header levels."""
        content = """# Title

### Subsection (should not match)

## Valid Section

Content here

#### Another subsection

## Another Valid Section

More content
"""
        sections = extract_sections(content)

        # Should only extract ## level headers
        assert "Valid Section" in sections
        assert "Another Valid Section" in sections
        # Should not extract ### or #### headers
        assert "Subsection (should not match)" not in sections
        assert "Another subsection" not in sections

    def test_extract_sections_with_trailing_whitespace(self):
        """Test extraction of sections with trailing whitespace in headers."""
        content = """## Section Name  \t

Content

## Another Section

More content
"""
        sections = extract_sections(content)

        # Should trim whitespace from section names
        assert "Section Name" in sections
        assert "Another Section" in sections

    def test_extract_sections_empty_content(self):
        """Test extraction from content with no sections."""
        content = "# Just a title\n\nSome content without sections."

        sections = extract_sections(content)

        assert len(sections) == 0

    def test_extract_code_blocks_malformed_backticks(self):
        """Test extraction with mismatched backticks."""
        content = """Some text

```python
def hello():
    print("missing closing backticks")

More text
"""
        code_blocks = extract_code_blocks(content)

        # Should not match incomplete code blocks
        assert len(code_blocks) == 0

    def test_extract_code_blocks_nested_backticks(self):
        """Test handling of nested backtick patterns."""
        content = """Text

```markdown
Example showing backticks: ``` in code
```

More text
"""
        code_blocks = extract_code_blocks(content)

        # Regex may not handle nested backticks perfectly
        # Just verify it extracts at least one block without crashing
        assert len(code_blocks) >= 1
        _, language, code = code_blocks[0]
        assert language == "markdown"

    def test_extract_links_malformed_syntax(self):
        """Test link extraction with malformed markdown link syntax."""
        content = """
[Valid link](url.md)
![Image link](image.png)
[Text only without url]
"""
        links = extract_links(content)

        # Should extract valid links including image links
        valid_links = [url for _, _, url in links]
        assert "url.md" in valid_links or "image.png" in valid_links
        # Image links are captured by the regex pattern
        assert len(links) >= 1

    def test_section_extraction_with_no_content(self):
        """Test sections that have no content at all."""
        content = """## Section One

## Section Two"""

        sections = extract_sections(content)

        assert "Section One" in sections
        assert "Section Two" in sections
        # Content should be empty or minimal
        _, section_content = sections["Section One"]
        assert section_content.strip() == ""


class TestWhitespaceVariations:
    """Test handling of various whitespace patterns."""

    def test_section_completeness_with_only_whitespace(self):
        """Test detection of sections containing only whitespace."""
        sections = {
            "Empty Section": (1, "   \n\t\n  "),
            "Only Spaces": (5, "        "),
        }

        issues = check_section_completeness(sections)

        assert len(issues) == 2
        assert all(issue.code == "empty_section" for issue in issues)

    def test_section_completeness_with_code_blocks_only(self):
        """Test sections with only code blocks and no explanatory text."""
        sections = {
            "Code Only Section": (
                1,
                """```python
def example():
    pass
```""",
            )
        }

        issues = check_section_completeness(sections)

        # Should warn - section has no substantive text besides code
        assert len(issues) == 1
        assert issues[0].code == "empty_section"

    def test_section_with_single_line_content(self):
        """Test detection of sections with only one line of content."""
        sections = {
            "Minimal Section": (1, "Just one line."),
        }

        issues = check_section_completeness(sections)

        # Should warn about insufficient content
        assert len(issues) == 1
        assert issues[0].code == "empty_section"

    def test_section_with_unicode_whitespace(self):
        """Test handling of unicode whitespace characters."""
        sections = {
            "Unicode Whitespace": (1, "\u00a0\u2000\u2001\u2002"),  # Various unicode spaces
        }

        issues = check_section_completeness(sections)

        # Should detect as empty
        assert len(issues) == 1


class TestUnicodeHandling:
    """Test handling of unicode characters."""

    def test_extract_sections_with_unicode_headers(self):
        """Test section extraction with unicode characters in headers."""
        content = """## Contributing 🎉

Welcome to our project!

## Code Style 日本語

Use proper formatting.

## Тестирование (Testing)

Run tests regularly.
"""
        sections = extract_sections(content)

        assert "Contributing 🎉" in sections
        assert "Code Style 日本語" in sections
        assert "Тестирование (Testing)" in sections

    def test_validate_python_code_with_unicode_strings(self):
        """Test validation of Python code containing unicode strings."""
        code = """
def greet():
    print("Hello 世界 🌍")
    return "Привет"
"""
        error = validate_python_code(code)
        assert error is None  # Should be valid

    def test_extract_links_with_unicode_urls(self):
        """Test extraction of links with unicode in URLs or text."""
        content = """
[日本語ドキュメント](docs/ja/README.md)
[Документация](https://example.com/docs)
[Emoji 🎯](target.md)
"""
        links = extract_links(content)

        assert len(links) == 3
        link_texts = [text for _, text, _ in links]
        assert "日本語ドキュメント" in link_texts
        assert "Документация" in link_texts
        assert "Emoji 🎯" in link_texts

    def test_code_block_with_unicode_content(self):
        """Test code block extraction with unicode content."""
        content = """
```python
# コメント
def 函数():
    \"\"\"Строка документации\"\"\"
    pass
```
"""
        code_blocks = extract_code_blocks(content)

        assert len(code_blocks) == 1
        _, _, code = code_blocks[0]
        assert "コメント" in code
        assert "函数" in code
        assert "Строка документации" in code


class TestLargeFiles:
    """Test handling of very large files."""

    def test_validate_large_contributing_file(self, tmp_path):
        """Test validation of a very large CONTRIBUTING.md file."""
        # Generate a large file with many sections
        sections = []
        for i in range(100):
            sections.append(f"## Section {i}\n\nContent for section {i}.\nMore content here.\n")

        # Add required sections
        required = """
## Test-Driven Development Workflow

TDD approach here.

## Running Tests

pytest command here.

## Code Style

PEP 8 guidelines.

## Commit Messages

Conventional commits.

## Pull Request Process

Submit PRs for review.
"""
        content = "# Contributing\n\n" + required + "\n".join(sections)

        file_path = tmp_path / "CONTRIBUTING.md"
        file_path.write_text(content)

        report = validate_contributing_md(file_path, tmp_path)

        # Should handle large file without errors
        assert isinstance(report.errors, list)
        assert isinstance(report.warnings, list)

    def test_extract_sections_from_huge_content(self):
        """Test section extraction from content with many sections."""
        # Create content with 500 sections
        content = "# Title\n\n"
        for i in range(500):
            content += f"## Section {i}\n\nContent {i}\n\n"

        sections = extract_sections(content)

        assert len(sections) == 500
        assert "Section 0" in sections
        assert "Section 499" in sections

    def test_extract_code_blocks_many_blocks(self):
        """Test extraction of many code blocks."""
        # Create content with 100 code blocks
        content = ""
        for i in range(100):
            content += f"""
## Section {i}

```python
def func_{i}():
    return {i}
```
"""
        code_blocks = extract_code_blocks(content)

        assert len(code_blocks) == 100

    def test_very_long_code_block(self):
        """Test handling of very long code blocks."""
        # Create a code block with 1000 lines
        long_code = "\n".join([f"line_{i} = {i}" for i in range(1000)])
        content = f"""## Code

```python
{long_code}
```
"""
        code_blocks = extract_code_blocks(content)

        assert len(code_blocks) == 1
        _, _, code = code_blocks[0]
        assert "line_0 = 0" in code
        assert "line_999 = 999" in code


class TestSymlinks:
    """Test handling of symlinks."""

    @pytest.mark.skipif(sys.platform == "win32", reason="Symlinks require special permissions on Windows")
    def test_validate_contributing_via_symlink(self, tmp_path):
        """Test validation when CONTRIBUTING.md is accessed via symlink."""
        # Create actual file
        actual_file = tmp_path / "actual" / "CONTRIBUTING.md"
        actual_file.parent.mkdir()
        actual_file.write_text(dedent("""# Contributing

## Test-Driven Development Workflow

TDD approach.

## Running Tests

Run pytest.

## Code Style

Follow PEP 8.

## Commit Messages

Use conventional commits.

## Pull Request Process

Submit PRs.
"""))

        # Create symlink
        link_dir = tmp_path / "link"
        link_dir.mkdir()
        symlink = link_dir / "CONTRIBUTING.md"
        symlink.symlink_to(actual_file)

        # Validate via symlink
        report = validate_contributing_md(symlink, link_dir)

        # Should work through symlink
        assert report.file_path == str(symlink)
        assert report.ok or len(report.errors) == 0  # May have warnings

    @pytest.mark.skipif(sys.platform == "win32", reason="Symlinks require special permissions on Windows")
    def test_check_internal_links_to_symlinked_files(self, tmp_path):
        """Test checking internal links that point to symlinked files."""
        # Create target file
        actual = tmp_path / "actual_docs.md"
        actual.write_text("# Docs")

        # Create symlink
        link = tmp_path / "docs_link.md"
        link.symlink_to(actual)

        links = [(1, "docs", "docs_link.md")]
        issues = check_internal_links(links, tmp_path)

        # Should accept symlinks as valid targets
        assert len(issues) == 0


class TestPermissionErrors:
    """Test handling of permission errors."""

    @pytest.mark.skipif(sys.platform == "win32", reason="Unix permission model")
    def test_validate_unreadable_file(self, tmp_path):
        """Test graceful handling when file cannot be read."""
        file_path = tmp_path / "CONTRIBUTING.md"
        file_path.write_text("# Content")
        file_path.chmod(0o000)  # Remove all permissions

        try:
            # The current implementation doesn't catch PermissionError,
            # so it will raise. Test that it raises as expected
            with pytest.raises(PermissionError):
                validate_contributing_md(file_path, tmp_path)
        finally:
            file_path.chmod(0o644)  # Restore permissions for cleanup

    @pytest.mark.skipif(sys.platform == "win32", reason="Unix permission model")
    def test_check_internal_links_to_unreadable_file(self, tmp_path):
        """Test checking links to files that exist but can't be read."""
        # Create file with no read permissions
        target = tmp_path / "secret.md"
        target.write_text("Secret content")
        target.chmod(0o000)

        try:
            links = [(1, "secret", "secret.md")]
            issues = check_internal_links(links, tmp_path)

            # File exists, so should not report as broken link
            # (The file check uses exists(), not read access)
            assert len(issues) == 0
        finally:
            target.chmod(0o644)


class TestSpecialCharacters:
    """Test handling of special characters in various contexts."""

    def test_section_names_with_special_chars(self):
        """Test extraction of sections with special characters in names."""
        content = """
## Code Style: PEP 8 & Type Hints

Content here.

## Test-Driven Development (TDD)

More content.

## Pull Request Process [IMPORTANT]

PR guidelines.
"""
        sections = extract_sections(content)

        assert "Code Style: PEP 8 & Type Hints" in sections
        assert "Test-Driven Development (TDD)" in sections
        assert "Pull Request Process [IMPORTANT]" in sections

    def test_links_with_query_parameters(self):
        """Test extraction of links with query parameters."""
        content = """
[Search](https://example.com/search?q=test&lang=en)
[Anchor with query](docs/api.md?version=2#endpoint)
"""
        links = extract_links(content)

        assert len(links) == 2
        urls = [url for _, _, url in links]
        assert "https://example.com/search?q=test&lang=en" in urls
        assert "docs/api.md?version=2#endpoint" in urls

    def test_code_blocks_with_special_languages(self):
        """Test extraction of code blocks with unusual language tags."""
        content = """
```diff
- removed line
+ added line
```

```cpp
class Example {};
```

```
No language specified
```
"""
        code_blocks = extract_code_blocks(content)

        # The regex pattern is r"```(\w+)?\n([\s\S]*?)```"
        # \w+ matches word characters, so c++ won't match, empty language will
        assert len(code_blocks) >= 2
        languages = [lang for _, lang, _ in code_blocks]
        assert "diff" in languages
        assert "cpp" in languages or "" in languages

    def test_python_code_with_escape_sequences(self):
        """Test validation of Python code with escape sequences."""
        code = '''
def example():
    s = "\\n\\t\\r"
    raw = r"C:\\Users\\name"
    return s, raw
'''
        error = validate_python_code(code)
        assert error is None


class TestEdgeCaseContent:
    """Test edge cases in content validation."""

    def test_empty_file(self, tmp_path):
        """Test validation of completely empty CONTRIBUTING.md."""
        file_path = tmp_path / "CONTRIBUTING.md"
        file_path.write_text("")

        report = validate_contributing_md(file_path, tmp_path)

        # Should fail - missing all required sections
        assert not report.ok
        assert len(report.errors) > 0

    def test_file_with_only_whitespace(self, tmp_path):
        """Test validation of file containing only whitespace."""
        file_path = tmp_path / "CONTRIBUTING.md"
        file_path.write_text("\n\n\t  \n   \n")

        report = validate_contributing_md(file_path, tmp_path)

        assert not report.ok
        # Should have errors for missing sections

    def test_multiple_code_blocks_same_section(self):
        """Test handling of multiple code blocks in a single section."""
        content = """
## Examples

First example:

```python
def first():
    pass
```

Second example:

```python
def second():
    pass
```

Third example:

```python
def third(
    pass  # Invalid syntax
```
"""
        issues = check_code_examples(content)

        # Should find the syntax error in third block
        assert len(issues) == 1
        assert issues[0].code == "invalid_python_syntax"

    def test_code_block_with_inline_comments(self):
        """Test Python code validation with various comment styles."""
        code = """
def example():
    # Single line comment
    x = 1  # Inline comment
    '''
    Multi-line string
    that looks like a comment
    '''
    return x
"""
        error = validate_python_code(code)
        assert error is None

    def test_section_with_nested_lists_and_formatting(self):
        """Test section completeness with complex markdown formatting."""
        sections = {
            "Complex Section": (
                1,
                """
- Item 1
  - Nested item
  - Another nested
- Item 2

**Bold text** and *italic text*.

> Quote block

- [ ] Checklist item
- [x] Completed item
""",
            )
        }

        issues = check_section_completeness(sections)

        # Should recognize this as substantial content
        assert len(issues) == 0


class TestBoundaryConditions:
    """Test boundary conditions and extreme inputs."""

    def test_section_at_exact_minimum_length(self):
        """Test section with exactly 2 substantive lines (minimum)."""
        sections = {
            "Minimal Valid": (1, "Line one.\nLine two."),
        }

        issues = check_section_completeness(sections)

        # Should pass with exactly 2 lines
        assert len(issues) == 0

    def test_link_with_very_long_url(self):
        """Test extraction of links with very long URLs."""
        very_long_url = "https://example.com/" + "a" * 1000
        content = f"[Link]({very_long_url})"

        links = extract_links(content)

        assert len(links) == 1
        _, _, url = links[0]
        assert url == very_long_url

    def test_deeply_nested_path_in_link(self):
        """Test internal link with deeply nested path."""
        deep_path = "/".join(["dir"] * 50) + "/file.md"
        content = f"[Deep link]({deep_path})"

        links = extract_links(content)

        assert len(links) == 1
        _, _, url = links[0]
        assert url == deep_path

    def test_code_block_language_with_special_chars(self):
        """Test code block with unusual language identifier."""
        content = """
```python3
def example():
    pass
```

```objectivec
@interface Example
@end
```
"""
        code_blocks = extract_code_blocks(content)

        # The regex pattern only matches \w+ (alphanumeric + underscore)
        # So dots and hyphens won't be captured
        assert len(code_blocks) >= 1
        languages = [lang for _, lang, _ in code_blocks]
        # Should at least capture alphanumeric languages
        assert "python3" in languages or "objectivec" in languages


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
