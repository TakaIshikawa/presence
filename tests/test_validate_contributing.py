"""Tests for scripts/validate_contributing.py validation logic.

Tests cover section validation, formatting checks, code validation, link validation,
command validation, and the complete validation workflow.
"""

import tempfile
from pathlib import Path
from textwrap import dedent

import pytest

from scripts.validate_contributing import (
    RECOMMENDED_SECTIONS,
    REQUIRED_SECTIONS,
    ContributingValidationReport,
    ValidationIssue,
    check_code_examples,
    check_command_executability,
    check_internal_links,
    check_project_consistency,
    check_required_sections,
    check_section_completeness,
    extract_code_blocks,
    extract_commands,
    extract_links,
    extract_sections,
    main,
    validate_contributing_md,
    validate_python_code,
)


class TestValidationIssue:
    """Test ValidationIssue dataclass."""

    def test_create_error_issue(self):
        """Test creating an error issue."""
        issue = ValidationIssue(
            level="error",
            code="test_error",
            message="Test error message",
            section="Test Section",
            line=10,
        )

        assert issue.level == "error"
        assert issue.code == "test_error"
        assert issue.message == "Test error message"
        assert issue.section == "Test Section"
        assert issue.line == 10

    def test_create_warning_issue(self):
        """Test creating a warning issue."""
        issue = ValidationIssue(
            level="warning", code="test_warning", message="Test warning"
        )

        assert issue.level == "warning"
        assert issue.code == "test_warning"
        assert issue.message == "Test warning"
        assert issue.section is None
        assert issue.line is None


class TestContributingValidationReport:
    """Test ContributingValidationReport dataclass."""

    def test_create_success_report(self):
        """Test creating a successful validation report."""
        report = ContributingValidationReport(ok=True, file_path="CONTRIBUTING.md")

        assert report.ok is True
        assert report.file_path == "CONTRIBUTING.md"
        assert len(report.errors) == 0
        assert len(report.warnings) == 0

    def test_create_failure_report(self):
        """Test creating a failure report with errors."""
        error = ValidationIssue(level="error", code="test", message="test error")
        report = ContributingValidationReport(
            ok=False, file_path="CONTRIBUTING.md", errors=[error]
        )

        assert report.ok is False
        assert len(report.errors) == 1
        assert report.errors[0].level == "error"

    def test_report_to_dict(self):
        """Test converting report to dictionary."""
        error = ValidationIssue(
            level="error", code="E001", message="Error", section="Intro", line=5
        )
        warning = ValidationIssue(
            level="warning", code="W001", message="Warning", section=None, line=None
        )

        report = ContributingValidationReport(
            ok=False, file_path="test.md", errors=[error], warnings=[warning]
        )

        result = report.to_dict()

        assert result["ok"] is False
        assert result["file_path"] == "test.md"
        assert result["error_count"] == 1
        assert result["warning_count"] == 1
        assert len(result["errors"]) == 1
        assert result["errors"][0]["code"] == "E001"
        assert len(result["warnings"]) == 1
        assert result["warnings"][0]["code"] == "W001"


class TestExtractSections:
    """Test section extraction from markdown."""

    def test_extract_single_section(self):
        """Test extracting a single section."""
        content = """## Test Section
This is test content.
"""
        sections = extract_sections(content)

        assert "Test Section" in sections
        assert len(sections) == 1
        line_num, content = sections["Test Section"]
        assert line_num == 1
        assert "This is test content" in content

    def test_extract_multiple_sections(self):
        """Test extracting multiple sections."""
        content = """## First Section
Content for first section.

## Second Section
Content for second section.

## Third Section
Content for third section.
"""
        sections = extract_sections(content)

        assert len(sections) == 3
        assert "First Section" in sections
        assert "Second Section" in sections
        assert "Third Section" in sections

    def test_section_line_numbers(self):
        """Test that line numbers are correctly tracked."""
        content = """Line 1
## Section A
Line 3
Line 4
## Section B
Line 6
"""
        sections = extract_sections(content)

        assert sections["Section A"][0] == 2
        assert sections["Section B"][0] == 5

    def test_empty_content_returns_empty_dict(self):
        """Test that empty content returns empty dict."""
        sections = extract_sections("")
        assert sections == {}

    def test_content_without_sections(self):
        """Test content with no markdown sections."""
        content = "Just regular text\nwithout any sections\n"
        sections = extract_sections(content)
        assert len(sections) == 0


class TestCheckRequiredSections:
    """Test required section validation."""

    def test_all_required_sections_present(self):
        """Test when all required sections are present."""
        sections = {name: (1, "content") for name in REQUIRED_SECTIONS}
        issues = check_required_sections(sections)

        # Should only have warnings for missing recommended sections
        errors = [i for i in issues if i.level == "error"]
        assert len(errors) == 0

    def test_missing_required_section(self):
        """Test when a required section is missing."""
        sections = {
            name: (1, "content")
            for name in REQUIRED_SECTIONS
            if name != "Test-Driven Development Workflow"
        }
        issues = check_required_sections(sections)

        errors = [i for i in issues if i.level == "error"]
        assert len(errors) == 1
        assert "Test-Driven Development Workflow" in errors[0].message

    def test_missing_recommended_section(self):
        """Test when a recommended section is missing."""
        sections = {name: (1, "content") for name in REQUIRED_SECTIONS}
        issues = check_required_sections(sections)

        warnings = [i for i in issues if i.level == "warning"]
        # Should have warnings for all missing recommended sections
        assert len(warnings) == len(RECOMMENDED_SECTIONS)

    def test_all_sections_present(self):
        """Test when all required and recommended sections are present."""
        all_sections = REQUIRED_SECTIONS + RECOMMENDED_SECTIONS
        sections = {name: (1, "content") for name in all_sections}
        issues = check_required_sections(sections)

        assert len(issues) == 0


class TestCheckSectionCompleteness:
    """Test section content completeness validation."""

    def test_section_with_sufficient_content(self):
        """Test that sections with enough content pass."""
        sections = {
            "Test Section": (
                1,
                """This is the first line of content.
This is the second line of content.
And here is more detailed content.
""",
            )
        }
        issues = check_section_completeness(sections)

        assert len(issues) == 0

    def test_section_with_insufficient_content(self):
        """Test that sections with insufficient content trigger warning."""
        sections = {"Test Section": (1, "Only one line.\n")}
        issues = check_section_completeness(sections)

        assert len(issues) == 1
        assert issues[0].level == "warning"
        assert "insufficient content" in issues[0].message.lower()

    def test_empty_section(self):
        """Test that empty sections trigger warning."""
        sections = {"Empty Section": (1, "\n\n")}
        issues = check_section_completeness(sections)

        assert len(issues) == 1
        assert issues[0].code == "empty_section"

    def test_section_with_only_code_block(self):
        """Test section that only contains code block."""
        sections = {
            "Code Only": (
                1,
                """```python
def example():
    pass
```
""",
            )
        }
        issues = check_section_completeness(sections)

        # Should warn because substantive text is minimal
        assert len(issues) == 1


class TestExtractCodeBlocks:
    """Test code block extraction."""

    def test_extract_python_code_block(self):
        """Test extracting Python code block."""
        content = """Some text

```python
def hello():
    print("Hello")
```

More text
"""
        blocks = extract_code_blocks(content)

        assert len(blocks) == 1
        line_num, language, code = blocks[0]
        assert language == "python"
        assert "def hello():" in code

    def test_extract_multiple_code_blocks(self):
        """Test extracting multiple code blocks."""
        content = """
```bash
echo "test"
```

Some text

```python
x = 1
```
"""
        blocks = extract_code_blocks(content)

        assert len(blocks) == 2
        assert blocks[0][1] == "bash"
        assert blocks[1][1] == "python"

    def test_extract_code_block_without_language(self):
        """Test code block without language specifier."""
        content = """
```
generic code
```
"""
        blocks = extract_code_blocks(content)

        assert len(blocks) == 1
        assert blocks[0][1] == ""

    def test_code_block_line_numbers(self):
        """Test that code block line numbers are correct."""
        content = """Line 1
Line 2
```python
code
```
Line 6
"""
        blocks = extract_code_blocks(content)

        assert blocks[0][0] == 3  # Code block starts at line 3


class TestValidatePythonCode:
    """Test Python syntax validation."""

    def test_valid_python_code(self):
        """Test that valid Python code passes."""
        code = """
def example():
    return 42

x = example()
"""
        result = validate_python_code(code)
        assert result is None

    def test_invalid_python_syntax(self):
        """Test that invalid Python code fails."""
        code = "def broken(\n    pass"
        result = validate_python_code(code)

        assert result is not None
        assert "syntax" in result.lower()

    def test_python_with_syntax_error(self):
        """Test Python code with syntax error."""
        code = "if True\n    print('missing colon')"
        result = validate_python_code(code)

        assert result is not None


class TestCheckCodeExamples:
    """Test code example validation."""

    def test_valid_code_examples(self):
        """Test content with valid code examples."""
        content = """
```python
def valid():
    return True
```
"""
        issues = check_code_examples(content)
        assert len(issues) == 0

    def test_invalid_python_example(self):
        """Test content with invalid Python example."""
        content = """
```python
def broken(
    missing closing paren
```
"""
        issues = check_code_examples(content)

        assert len(issues) == 1
        assert issues[0].level == "error"
        assert issues[0].code == "invalid_python_syntax"

    def test_non_python_code_not_validated(self):
        """Test that non-Python code blocks are not validated."""
        content = """
```bash
invalid python but valid bash
def broken():
```
"""
        issues = check_code_examples(content)
        # Bash code should not be validated as Python
        assert len(issues) == 0


class TestExtractLinks:
    """Test link extraction."""

    def test_extract_simple_link(self):
        """Test extracting a simple markdown link."""
        content = "Check out [the docs](https://example.com)"
        links = extract_links(content)

        assert len(links) == 1
        line_num, text, url = links[0]
        assert text == "the docs"
        assert url == "https://example.com"

    def test_extract_multiple_links_same_line(self):
        """Test multiple links on same line."""
        content = "See [link1](url1) and [link2](url2)"
        links = extract_links(content)

        assert len(links) == 2

    def test_extract_internal_file_link(self):
        """Test internal file path link."""
        content = "See [README](README.md) for details"
        links = extract_links(content)

        assert len(links) == 1
        assert links[0][2] == "README.md"

    def test_line_numbers_for_links(self):
        """Test that line numbers are correct for links."""
        content = """Line 1
Line 2 with [a link](url)
Line 3
Line 4 with [another](url2)
"""
        links = extract_links(content)

        assert links[0][0] == 2
        assert links[1][0] == 4


class TestCheckInternalLinks:
    """Test internal link validation."""

    def test_valid_internal_links(self, tmp_path):
        """Test that existing files pass validation."""
        readme = tmp_path / "README.md"
        readme.write_text("# README")

        links = [(1, "README", "README.md")]
        issues = check_internal_links(links, tmp_path)

        assert len(issues) == 0

    def test_broken_internal_link(self, tmp_path):
        """Test that non-existent files fail validation."""
        links = [(1, "missing", "nonexistent.md")]
        issues = check_internal_links(links, tmp_path)

        assert len(issues) == 1
        assert issues[0].level == "error"
        assert issues[0].code == "broken_internal_link"

    def test_external_links_not_validated(self, tmp_path):
        """Test that external URLs are not validated."""
        links = [
            (1, "http link", "http://example.com"),
            (2, "https link", "https://example.com"),
        ]
        issues = check_internal_links(links, tmp_path)

        # External links should be skipped
        assert len(issues) == 0

    def test_anchor_links_not_validated(self, tmp_path):
        """Test that anchor links are not validated."""
        links = [(1, "anchor", "#section-name")]
        issues = check_internal_links(links, tmp_path)

        assert len(issues) == 0


class TestExtractCommands:
    """Test command extraction from code blocks."""

    def test_extract_bash_commands(self):
        """Test extracting bash commands."""
        content = """
```bash
echo "hello"
ls -la
```
"""
        commands = extract_commands(content)

        assert len(commands) == 2
        assert commands[0][1] == 'echo "hello"'
        assert commands[1][1] == "ls -la"

    def test_skip_comments_in_commands(self):
        """Test that comments are skipped."""
        content = """
```bash
# This is a comment
echo "actual command"
# Another comment
```
"""
        commands = extract_commands(content)

        assert len(commands) == 1
        assert "actual command" in commands[0][1]

    def test_extract_shell_code_blocks(self):
        """Test that shell/sh blocks are also extracted."""
        content = """
```shell
pwd
```

```sh
cd /tmp
```
"""
        commands = extract_commands(content)

        assert len(commands) == 2


class TestCheckCommandExecutability:
    """Test command executability checks."""

    def test_skip_shell_builtins(self, tmp_path):
        """Test that shell builtins are skipped."""
        commands = [
            (1, "cd /tmp"),
            (2, "export VAR=value"),
            (3, "source script.sh"),
        ]
        issues = check_command_executability(commands, tmp_path)

        # Shell builtins should not trigger errors
        assert len(issues) == 0

    def test_missing_python_script(self, tmp_path):
        """Test that missing Python scripts are detected."""
        commands = [(1, "python scripts/missing.py")]
        issues = check_command_executability(commands, tmp_path)

        assert len(issues) == 1
        assert issues[0].code == "missing_script"

    def test_existing_python_script(self, tmp_path):
        """Test that existing scripts pass validation."""
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        script = scripts_dir / "exists.py"
        script.write_text("print('hello')")

        commands = [(1, "python scripts/exists.py")]
        issues = check_command_executability(commands, tmp_path)

        assert len(issues) == 0


class TestCheckProjectConsistency:
    """Test project consistency validation."""

    def test_pytest_mentioned_with_pyproject(self, tmp_path):
        """Test that pytest mention is OK when pyproject.toml exists."""
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("[tool.pytest.ini_options]")

        content = "Run tests with pytest"
        issues = check_project_consistency(content, tmp_path)

        # Should not have error about pytest not configured
        errors = [i for i in issues if "pytest" in i.message]
        assert len(errors) == 0

    def test_pytest_mentioned_without_config(self, tmp_path):
        """Test warning when pytest mentioned but not configured."""
        content = "Run tests with pytest"
        issues = check_project_consistency(content, tmp_path)

        warnings = [i for i in issues if i.level == "warning" and "pytest" in i.message]
        assert len(warnings) == 1

    def test_mentioned_file_exists(self, tmp_path):
        """Test that mentioned files are validated."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        module = src_dir / "module.py"
        module.write_text("# module")

        content = "See src/module.py for implementation"
        issues = check_project_consistency(content, tmp_path)

        # Should not warn about existing file
        file_issues = [i for i in issues if "module.py" in i.message]
        assert len(file_issues) == 0

    def test_mentioned_file_missing(self, tmp_path):
        """Test warning for missing mentioned file."""
        content = "See src/nonexistent.py for details"
        issues = check_project_consistency(content, tmp_path)

        warnings = [i for i in issues if "nonexistent.py" in i.message]
        assert len(warnings) == 1


class TestValidateContributingMd:
    """Test complete validation workflow."""

    def test_validate_nonexistent_file(self, tmp_path):
        """Test validation of non-existent file."""
        file_path = tmp_path / "CONTRIBUTING.md"
        report = validate_contributing_md(file_path, tmp_path)

        assert report.ok is False
        assert len(report.errors) == 1
        assert report.errors[0].code == "file_not_found"

    def test_validate_minimal_valid_file(self, tmp_path):
        """Test validation of minimal valid file."""
        file_path = tmp_path / "CONTRIBUTING.md"
        content = "\n".join([f"## {section}\n\nContent here.\n" for section in REQUIRED_SECTIONS])
        file_path.write_text(content)

        report = validate_contributing_md(file_path, tmp_path)

        # Should pass with only warnings about recommended sections
        assert report.ok is True
        assert len(report.errors) == 0

    def test_validate_file_with_errors(self, tmp_path):
        """Test validation catches errors."""
        file_path = tmp_path / "CONTRIBUTING.md"
        content = """## Test Section

```python
def broken(
    syntax error
```
"""
        file_path.write_text(content)

        report = validate_contributing_md(file_path, tmp_path)

        assert report.ok is False
        assert len(report.errors) > 0

    def test_validate_with_warnings_only(self, tmp_path):
        """Test that warnings don't fail validation."""
        file_path = tmp_path / "CONTRIBUTING.md"
        content = "\n".join([f"## {section}\n\nContent.\n" for section in REQUIRED_SECTIONS])
        file_path.write_text(content)

        report = validate_contributing_md(file_path, tmp_path)

        # Warnings about recommended sections shouldn't fail
        assert report.ok is True
        assert len(report.warnings) > 0

    def test_report_file_path(self, tmp_path):
        """Test that report includes file path."""
        file_path = tmp_path / "CONTRIBUTING.md"
        content = "\n".join([f"## {section}\n\nContent.\n" for section in REQUIRED_SECTIONS])
        file_path.write_text(content)

        report = validate_contributing_md(file_path, tmp_path)

        assert str(file_path) in report.file_path


class TestMainFunction:
    """Test main CLI function."""

    def test_main_with_valid_file(self, tmp_path, capsys):
        """Test main function with valid file."""
        file_path = tmp_path / "CONTRIBUTING.md"
        content = "\n".join([f"## {section}\n\nContent here.\n" for section in REQUIRED_SECTIONS])
        file_path.write_text(content)

        result = main(["--file", str(file_path), "--project-root", str(tmp_path)])

        assert result == 0

        captured = capsys.readouterr()
        assert "0 error" in captured.out

    def test_main_with_errors(self, tmp_path, capsys):
        """Test main function with errors."""
        file_path = tmp_path / "CONTRIBUTING.md"
        file_path.write_text("## Incomplete")

        result = main(["--file", str(file_path)])

        assert result == 1

    def test_main_json_output(self, tmp_path, capsys):
        """Test main function with JSON output."""
        file_path = tmp_path / "CONTRIBUTING.md"
        content = "\n".join([f"## {section}\n\nContent.\n" for section in REQUIRED_SECTIONS])
        file_path.write_text(content)

        result = main(["--file", str(file_path), "--json"])

        assert result == 0

        captured = capsys.readouterr()
        # Should contain JSON output
        assert '"ok"' in captured.out
        assert '"error_count"' in captured.out

    def test_main_default_file_path(self, tmp_path, monkeypatch):
        """Test main function with default file path."""
        # Change to temp directory
        monkeypatch.chdir(tmp_path)

        file_path = tmp_path / "CONTRIBUTING.md"
        content = "\n".join([f"## {section}\n\nContent.\n" for section in REQUIRED_SECTIONS])
        file_path.write_text(content)

        # Should use default CONTRIBUTING.md
        result = main([])

        # Should succeed since we created the file
        assert result == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
