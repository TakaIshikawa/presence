"""Tests for CONTRIBUTING.md validation."""

import json

import pytest

from scripts.validate_contributing import (
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
    validate_contributing_md,
    validate_python_code,
)


@pytest.fixture
def sample_contributing_complete(tmp_path):
    """Create a complete CONTRIBUTING.md for testing."""
    content = """# Contributing to Project

## Test-Driven Development Workflow

We follow TDD practices for all features.

Write tests first, then implement.

```python
def test_example():
    assert True
```

## Running Tests

Run tests with pytest:

```bash
pytest
pytest tests/test_example.py
```

## Code Style

Follow PEP 8 guidelines.

Use type hints for all functions.

## Commit Messages

Format:
```
<type>: <message>
```

## Pull Request Process

1. Create feature branch
2. Write tests
3. Submit PR

## Questions?

Open an issue for help.
"""
    file_path = tmp_path / "CONTRIBUTING.md"
    file_path.write_text(content)
    return file_path


@pytest.fixture
def sample_contributing_incomplete(tmp_path):
    """Create an incomplete CONTRIBUTING.md missing required sections."""
    content = """# Contributing to Project

## Test-Driven Development Workflow

We follow TDD practices.

## Code Style

Follow PEP 8.
"""
    file_path = tmp_path / "CONTRIBUTING.md"
    file_path.write_text(content)
    return file_path


def test_extract_sections():
    """Test section extraction from markdown."""
    content = """# Title

## Section One

Content for section one.

## Section Two

Content for section two.
Line 2.

## Section Three

Content three.
"""
    sections = extract_sections(content)

    assert "Section One" in sections
    assert "Section Two" in sections
    assert "Section Three" in sections

    # Check section content
    line_num, section_content = sections["Section One"]
    assert "Content for section one" in section_content
    assert line_num == 3

    line_num, section_content = sections["Section Two"]
    assert "Content for section two" in section_content
    assert "Line 2" in section_content


def test_check_required_sections_complete():
    """Test that all required sections are detected when present."""
    sections = {
        "Test-Driven Development Workflow": (1, "content"),
        "Running Tests": (5, "content"),
        "Code Style": (10, "content"),
        "Commit Messages": (15, "content"),
        "Pull Request Process": (20, "content"),
    }

    issues = check_required_sections(sections)

    # Should have no errors for required sections
    errors = [issue for issue in issues if issue.level == "error"]
    assert len(errors) == 0


def test_check_required_sections_missing():
    """Test detection of missing required sections."""
    sections = {
        "Test-Driven Development Workflow": (1, "content"),
        "Code Style": (10, "content"),
    }

    issues = check_required_sections(sections)

    errors = [issue for issue in issues if issue.level == "error"]
    assert len(errors) == 3  # Missing: Running Tests, Commit Messages, PR Process

    error_codes = [issue.code for issue in errors]
    assert all(code == "missing_required_section" for code in error_codes)


def test_check_recommended_sections_missing():
    """Test warnings for missing recommended sections."""
    sections = {
        "Test-Driven Development Workflow": (1, "content"),
        "Running Tests": (5, "content"),
        "Code Style": (10, "content"),
        "Commit Messages": (15, "content"),
        "Pull Request Process": (20, "content"),
    }

    issues = check_required_sections(sections)

    warnings = [issue for issue in issues if issue.level == "warning"]
    assert len(warnings) > 0  # Should warn about missing recommended sections


def test_check_section_completeness_empty_section():
    """Test detection of empty sections."""
    sections = {
        "Test Section": (1, ""),
        "Another Section": (5, "\n\n"),
    }

    issues = check_section_completeness(sections)

    assert len(issues) == 2
    assert all(issue.code == "empty_section" for issue in issues)


def test_check_section_completeness_substantial_content():
    """Test that sections with substantial content pass."""
    sections = {
        "Test Section": (1, "Line 1\nLine 2\nLine 3"),
    }

    issues = check_section_completeness(sections)

    assert len(issues) == 0


def test_extract_code_blocks():
    """Test extraction of code blocks."""
    content = """Some text

```python
def hello():
    print("Hello")
```

More text

```bash
pytest
```
"""
    code_blocks = extract_code_blocks(content)

    assert len(code_blocks) == 2

    _line_num, language, code = code_blocks[0]
    assert language == "python"
    assert "def hello()" in code

    _line_num, language, code = code_blocks[1]
    assert language == "bash"
    assert "pytest" in code


def test_validate_python_code_valid():
    """Test validation of valid Python code."""
    code = """
def test_example():
    assert True
"""
    error = validate_python_code(code)
    assert error is None


def test_validate_python_code_invalid():
    """Test detection of invalid Python syntax."""
    code = """
def test_example(
    assert True
"""
    error = validate_python_code(code)
    assert error is not None
    assert "Invalid Python syntax" in error


def test_check_code_examples_valid():
    """Test validation of valid code examples."""
    content = """
## Example

```python
def test():
    return True
```
"""
    issues = check_code_examples(content)
    assert len(issues) == 0


def test_check_code_examples_invalid():
    """Test detection of invalid code examples."""
    content = """
## Example

```python
def test(
    return True
```
"""
    issues = check_code_examples(content)
    assert len(issues) == 1
    assert issues[0].code == "invalid_python_syntax"


def test_extract_links():
    """Test extraction of markdown links."""
    content = """
See [documentation](docs/README.md) for more info.
Visit [website](https://example.com) for details.
Check [section](#heading) below.
"""
    links = extract_links(content)

    assert len(links) == 3
    assert any(url == "docs/README.md" for _, _, url in links)
    assert any(url == "https://example.com" for _, _, url in links)
    assert any(url == "#heading" for _, _, url in links)


def test_check_internal_links_valid(tmp_path):
    """Test validation of valid internal links."""
    # Create a real file
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    readme = docs_dir / "README.md"
    readme.write_text("# Docs")

    links = [(1, "docs", "docs/README.md")]
    issues = check_internal_links(links, tmp_path)

    assert len(issues) == 0


def test_check_internal_links_broken(tmp_path):
    """Test detection of broken internal links."""
    links = [(1, "missing", "docs/MISSING.md")]
    issues = check_internal_links(links, tmp_path)

    assert len(issues) == 1
    assert issues[0].code == "broken_internal_link"


def test_check_internal_links_skips_external(tmp_path):
    """Test that external URLs are skipped."""
    links = [
        (1, "external", "https://example.com"),
        (2, "anchor", "#section"),
    ]
    issues = check_internal_links(links, tmp_path)

    assert len(issues) == 0


def test_extract_commands():
    """Test extraction of commands from bash blocks."""
    content = """
```bash
# Run tests
pytest
pytest tests/test_example.py
```

```python
# Not a command
import sys
```
"""
    commands = extract_commands(content)

    assert len(commands) == 2
    assert any("pytest" in cmd for _, cmd in commands)
    assert not any("import sys" in cmd for _, cmd in commands)


def test_check_command_executability_valid(tmp_path):
    """Test validation of existing scripts."""
    # Create a real script
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    script = scripts_dir / "test.py"
    script.write_text("#!/usr/bin/env python3\nprint('hello')")

    commands = [(1, "python scripts/test.py")]
    issues = check_command_executability(commands, tmp_path)

    assert len(issues) == 0


def test_check_command_executability_missing(tmp_path):
    """Test detection of missing scripts."""
    commands = [(1, "python scripts/missing.py")]
    issues = check_command_executability(commands, tmp_path)

    assert len(issues) == 1
    assert issues[0].code == "missing_script"


def test_check_command_executability_skips_builtins(tmp_path):
    """Test that shell builtins are skipped."""
    commands = [
        (1, "cd /tmp"),
        (2, "export PATH=/usr/bin"),
        (3, "source venv/bin/activate"),
    ]
    issues = check_command_executability(commands, tmp_path)

    assert len(issues) == 0


def test_check_project_consistency_pytest(tmp_path):
    """Test detection of pytest dependency."""
    content = "Run tests with pytest"

    # Create pyproject.toml
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("[tool.pytest.ini_options]")

    issues = check_project_consistency(content, tmp_path)

    # Should not warn if pyproject.toml exists
    assert len(issues) == 0


def test_check_project_consistency_missing_files(tmp_path):
    """Test detection of mentioned files that don't exist."""
    content = "See tests/test_example.py for examples"

    issues = check_project_consistency(content, tmp_path)

    assert len(issues) >= 1
    assert any(issue.code == "mentioned_file_missing" for issue in issues)


def test_validate_contributing_md_complete(sample_contributing_complete):
    """Test validation of complete CONTRIBUTING.md."""
    report = validate_contributing_md(
        sample_contributing_complete, sample_contributing_complete.parent
    )

    # Should pass with no errors (may have warnings)
    assert report.ok
    assert len(report.errors) == 0


def test_validate_contributing_md_incomplete(sample_contributing_incomplete):
    """Test validation of incomplete CONTRIBUTING.md."""
    report = validate_contributing_md(
        sample_contributing_incomplete, sample_contributing_incomplete.parent
    )

    # Should fail due to missing sections
    assert not report.ok
    assert len(report.errors) > 0

    # Check for missing section errors
    missing_sections = [
        issue
        for issue in report.errors
        if issue.code == "missing_required_section"
    ]
    assert len(missing_sections) > 0


def test_validate_contributing_md_file_not_found(tmp_path):
    """Test validation when CONTRIBUTING.md doesn't exist."""
    file_path = tmp_path / "MISSING.md"

    report = validate_contributing_md(file_path, tmp_path)

    assert not report.ok
    assert len(report.errors) == 1
    assert report.errors[0].code == "file_not_found"


def test_validate_contributing_md_to_dict(sample_contributing_complete):
    """Test conversion of report to dictionary."""
    report = validate_contributing_md(
        sample_contributing_complete, sample_contributing_complete.parent
    )

    report_dict = report.to_dict()

    assert "ok" in report_dict
    assert "file_path" in report_dict
    assert "error_count" in report_dict
    assert "warning_count" in report_dict
    assert "errors" in report_dict
    assert "warnings" in report_dict


def test_validation_with_broken_code_and_links(tmp_path):
    """Test validation catches both code errors and broken links."""
    content = """# Contributing

## Test-Driven Development Workflow

Example code:

```python
def broken(
    pass
```

## Running Tests

See [missing docs](docs/missing.md).

```bash
python scripts/nonexistent.py
```

## Code Style

Follow PEP 8.

## Commit Messages

Use conventional commits.

## Pull Request Process

Submit PRs for review.
"""
    file_path = tmp_path / "CONTRIBUTING.md"
    file_path.write_text(content)

    report = validate_contributing_md(file_path, tmp_path)

    assert not report.ok

    # Should have errors for:
    # 1. Invalid Python syntax
    # 2. Broken internal link
    # 3. Missing script
    assert len(report.errors) >= 3

    error_codes = {issue.code for issue in report.errors}
    assert "invalid_python_syntax" in error_codes
    assert "broken_internal_link" in error_codes
    assert "missing_script" in error_codes


def test_cli_json_output(sample_contributing_complete, capsys):
    """Test CLI JSON output."""
    from scripts.validate_contributing import main

    exit_code = main(
        [
            "--file",
            str(sample_contributing_complete),
            "--project-root",
            str(sample_contributing_complete.parent),
            "--json",
        ]
    )

    assert exit_code == 0

    captured = capsys.readouterr()
    output = json.loads(captured.out)

    assert "ok" in output
    assert "error_count" in output
    assert "warning_count" in output


def test_cli_text_output(sample_contributing_incomplete, capsys):
    """Test CLI text output."""
    from scripts.validate_contributing import main

    exit_code = main(
        [
            "--file",
            str(sample_contributing_incomplete),
            "--project-root",
            str(sample_contributing_incomplete.parent),
        ]
    )

    assert exit_code == 1  # Should fail due to missing sections

    captured = capsys.readouterr()
    assert "ERROR:" in captured.err
    assert "missing_required_section" in captured.err
