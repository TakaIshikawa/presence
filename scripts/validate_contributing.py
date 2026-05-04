#!/usr/bin/env python3
"""Validate CONTRIBUTING.md completeness and quality.

Checks for required sections, content completeness, code example validity,
link validity, command executability, and project consistency.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


@dataclass
class ValidationIssue:
    """Represents a validation issue."""

    level: str  # "error" or "warning"
    code: str
    message: str
    section: str | None = None
    line: int | None = None


@dataclass
class ContributingValidationReport:
    """Complete validation report for CONTRIBUTING.md."""

    ok: bool
    file_path: str
    errors: list[ValidationIssue] = field(default_factory=list)
    warnings: list[ValidationIssue] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "ok": self.ok,
            "file_path": self.file_path,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "errors": [
                {
                    "level": issue.level,
                    "code": issue.code,
                    "message": issue.message,
                    "section": issue.section,
                    "line": issue.line,
                }
                for issue in self.errors
            ],
            "warnings": [
                {
                    "level": issue.level,
                    "code": issue.code,
                    "message": issue.message,
                    "section": issue.section,
                    "line": issue.line,
                }
                for issue in self.warnings
            ],
        }


# Required sections for a complete CONTRIBUTING.md
REQUIRED_SECTIONS = [
    "Test-Driven Development Workflow",
    "Running Tests",
    "Code Style",
    "Commit Messages",
    "Pull Request Process",
]

# Optional but recommended sections
RECOMMENDED_SECTIONS = [
    "Getting Started",
    "Development Setup",
    "Questions",
]


def extract_sections(content: str) -> dict[str, tuple[int, str]]:
    """Extract markdown sections from content.

    Returns:
        Dictionary mapping section name to (line_number, section_content)
    """
    sections = {}
    current_section = None
    current_content = []
    current_line = 0

    for line_num, line in enumerate(content.split("\n"), 1):
        # Match markdown headers (## Section Name)
        header_match = re.match(r"^##\s+(.+)$", line)
        if header_match:
            # Save previous section
            if current_section:
                sections[current_section] = (current_line, "\n".join(current_content))

            # Start new section
            current_section = header_match.group(1).strip()
            current_line = line_num
            current_content = []
        elif current_section:
            current_content.append(line)

    # Save last section
    if current_section:
        sections[current_section] = (current_line, "\n".join(current_content))

    return sections


def check_required_sections(
    sections: dict[str, tuple[int, str]]
) -> list[ValidationIssue]:
    """Check that all required sections are present."""
    issues = []

    for required_section in REQUIRED_SECTIONS:
        if required_section not in sections:
            issues.append(
                ValidationIssue(
                    level="error",
                    code="missing_required_section",
                    message=f"Required section '{required_section}' is missing",
                    section=None,
                )
            )

    for recommended_section in RECOMMENDED_SECTIONS:
        if recommended_section not in sections:
            issues.append(
                ValidationIssue(
                    level="warning",
                    code="missing_recommended_section",
                    message=f"Recommended section '{recommended_section}' is missing",
                    section=None,
                )
            )

    return issues


def check_section_completeness(
    sections: dict[str, tuple[int, str]]
) -> list[ValidationIssue]:
    """Check that sections have substantial content (not just headers)."""
    issues = []

    for section_name, (line_num, content) in sections.items():
        # Remove code blocks and count remaining substantive lines
        content_without_code = re.sub(r"```[\s\S]*?```", "", content)
        substantive_lines = [
            line.strip()
            for line in content_without_code.split("\n")
            if line.strip() and not line.strip().startswith("#")
        ]

        if len(substantive_lines) < 2:
            issues.append(
                ValidationIssue(
                    level="warning",
                    code="empty_section",
                    message=f"Section '{section_name}' has insufficient content",
                    section=section_name,
                    line=line_num,
                )
            )

    return issues


def extract_code_blocks(content: str) -> list[tuple[int, str, str]]:
    """Extract code blocks from markdown content.

    Returns:
        List of (line_number, language, code) tuples
    """
    code_blocks = []
    pattern = re.compile(r"```(\w+)?\n([\s\S]*?)```", re.MULTILINE)

    for match in pattern.finditer(content):
        language = match.group(1) or ""
        code = match.group(2)
        # Find line number by counting newlines before match
        line_num = content[: match.start()].count("\n") + 1
        code_blocks.append((line_num, language, code))

    return code_blocks


def validate_python_code(code: str) -> str | None:
    """Validate Python syntax.

    Returns:
        Error message if invalid, None if valid
    """
    try:
        ast.parse(code)
        return None
    except SyntaxError as e:
        return f"Invalid Python syntax: {e}"


def check_code_examples(content: str) -> list[ValidationIssue]:
    """Check that code examples are syntactically valid."""
    issues = []
    code_blocks = extract_code_blocks(content)

    for line_num, language, code in code_blocks:
        if language.lower() == "python":
            error = validate_python_code(code)
            if error:
                issues.append(
                    ValidationIssue(
                        level="error",
                        code="invalid_python_syntax",
                        message=error,
                        section=None,
                        line=line_num,
                    )
                )

    return issues


def extract_links(content: str) -> list[tuple[int, str, str]]:
    """Extract markdown links from content.

    Returns:
        List of (line_number, link_text, url) tuples
    """
    links = []
    # Match [text](url) style links
    pattern = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")

    for line_num, line in enumerate(content.split("\n"), 1):
        for match in pattern.finditer(line):
            link_text = match.group(1)
            url = match.group(2)
            links.append((line_num, link_text, url))

    return links


def check_internal_links(
    links: list[tuple[int, str, str]], project_root: Path
) -> list[ValidationIssue]:
    """Check that internal links (file paths) exist."""
    issues = []

    for line_num, _link_text, url in links:
        # Skip external URLs
        if url.startswith(("http://", "https://", "#")):
            continue

        # Check if file exists
        target_path = project_root / url
        if not target_path.exists():
            issues.append(
                ValidationIssue(
                    level="error",
                    code="broken_internal_link",
                    message=f"Internal link '{url}' does not exist",
                    section=None,
                    line=line_num,
                )
            )

    return issues


def extract_commands(content: str) -> list[tuple[int, str]]:
    """Extract shell commands from bash/shell code blocks.

    Returns:
        List of (line_number, command) tuples
    """
    commands = []
    code_blocks = extract_code_blocks(content)

    for line_num, language, code in code_blocks:
        if language.lower() in ["bash", "shell", "sh"]:
            for cmd_line in code.split("\n"):
                cmd = cmd_line.strip()
                # Skip comments and empty lines
                if cmd and not cmd.startswith("#"):
                    commands.append((line_num, cmd))

    return commands


def check_command_executability(
    commands: list[tuple[int, str]], project_root: Path
) -> list[ValidationIssue]:
    """Check that mentioned commands/tools exist."""
    issues = []

    for line_num, command in commands:
        # Extract the base command (first word)
        parts = command.split()
        if not parts:
            continue

        base_command = parts[0]

        # Check if command exists in PATH or project
        # Skip common shell constructs
        if base_command in ["cd", "export", "source", ".", "if", "for", "while"]:
            continue

        # Check for project scripts
        if base_command == "python" or base_command.startswith("./"):
            # Extract script path
            script_path_match = re.search(r"(?:python\s+)?(\S+\.py)", command)
            if script_path_match:
                script_path = script_path_match.group(1)
                if not (project_root / script_path).exists():
                    issues.append(
                        ValidationIssue(
                            level="error",
                            code="missing_script",
                            message=f"Script '{script_path}' mentioned in command does not exist",
                            section=None,
                            line=line_num,
                        )
                    )

    return issues


def check_project_consistency(
    content: str, project_root: Path
) -> list[ValidationIssue]:
    """Check consistency with actual project setup."""
    issues = []

    # Check if mentioned tools exist
    if "pytest" in content:
        # Check if pytest is in requirements or pyproject.toml
        pyproject = project_root / "pyproject.toml"
        requirements = project_root / "requirements.txt"

        if not (pyproject.exists() or requirements.exists()):
            issues.append(
                ValidationIssue(
                    level="warning",
                    code="tool_not_configured",
                    message="pytest mentioned but no dependency file found",
                    section=None,
                )
            )

    # Check file paths mentioned in content
    path_pattern = re.compile(r"(?:tests/|src/|scripts/)[\w/\._-]+\.py")
    for match in path_pattern.finditer(content):
        file_path = match.group(0)
        if not (project_root / file_path).exists():
            issues.append(
                ValidationIssue(
                    level="warning",
                    code="mentioned_file_missing",
                    message=f"Mentioned file '{file_path}' does not exist",
                    section=None,
                )
            )

    return issues


def validate_contributing_md(
    file_path: Path, project_root: Path | None = None
) -> ContributingValidationReport:
    """Validate CONTRIBUTING.md file.

    Args:
        file_path: Path to CONTRIBUTING.md
        project_root: Project root directory (defaults to file_path.parent)

    Returns:
        ContributingValidationReport with all validation results
    """
    if project_root is None:
        project_root = file_path.parent

    if not file_path.exists():
        return ContributingValidationReport(
            ok=False,
            file_path=str(file_path),
            errors=[
                ValidationIssue(
                    level="error",
                    code="file_not_found",
                    message=f"CONTRIBUTING.md not found at {file_path}",
                )
            ],
        )

    content = file_path.read_text()
    report = ContributingValidationReport(ok=True, file_path=str(file_path))

    # Extract sections
    sections = extract_sections(content)

    # Run all checks
    section_issues = check_required_sections(sections)
    for issue in section_issues:
        if issue.level == "error":
            report.errors.append(issue)
        else:
            report.warnings.append(issue)

    report.warnings.extend(check_section_completeness(sections))
    report.errors.extend(check_code_examples(content))

    # Check links
    links = extract_links(content)
    report.errors.extend(check_internal_links(links, project_root))

    # Check commands
    commands = extract_commands(content)
    report.errors.extend(check_command_executability(commands, project_root))

    # Check project consistency
    report.warnings.extend(check_project_consistency(content, project_root))

    # Set overall status
    report.ok = len(report.errors) == 0

    return report


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--file",
        default="CONTRIBUTING.md",
        help="Path to CONTRIBUTING.md file (default: CONTRIBUTING.md)",
    )
    parser.add_argument(
        "--project-root",
        help="Project root directory (default: same as CONTRIBUTING.md directory)",
    )
    parser.add_argument(
        "--json", action="store_true", help="Output results as JSON"
    )
    args = parser.parse_args(argv)

    file_path = Path(args.file)
    project_root = Path(args.project_root) if args.project_root else file_path.parent

    report = validate_contributing_md(file_path, project_root)

    if args.json:
        print(json.dumps(report.to_dict(), indent=2))
    else:
        error_count = len(report.errors)
        warning_count = len(report.warnings)
        print(
            f"CONTRIBUTING.md validation: {error_count} error(s), {warning_count} warning(s)"
        )

        for error in report.errors:
            location = f" (line {error.line})" if error.line else ""
            section = f" in section '{error.section}'" if error.section else ""
            print(
                f"ERROR: {error.code}{section}{location}: {error.message}",
                file=sys.stderr,
            )

        for warning in report.warnings:
            location = f" (line {warning.line})" if warning.line else ""
            section = f" in section '{warning.section}'" if warning.section else ""
            print(f"WARNING: {warning.code}{section}{location}: {warning.message}")

    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
