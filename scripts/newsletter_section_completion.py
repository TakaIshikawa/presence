#!/usr/bin/env python3
"""Report missing or thin sections in the latest draft newsletter."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_section_completion import (  # noqa: E402
    DEFAULT_MIN_SECTION_WORDS,
    DEFAULT_REQUIRED_SECTIONS,
    analyze_newsletter_section_completion,
    build_newsletter_section_completion_report,
    format_newsletter_section_completion_json,
    format_newsletter_section_completion_text,
)
from runner import script_context  # noqa: E402


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _required_sections(value: str) -> tuple[str, ...]:
    sections = tuple(part.strip() for part in value.split(",") if part.strip())
    if not sections:
        raise argparse.ArgumentTypeError("at least one required section is needed")
    return sections


def _section_minimums(value: str) -> dict[str, int]:
    minimums: dict[str, int] = {}
    if not value.strip():
        return minimums
    for part in value.split(","):
        if "=" not in part:
            raise argparse.ArgumentTypeError("section minimums must use name=value pairs")
        name, raw_length = part.split("=", 1)
        name = name.strip()
        if not name:
            raise argparse.ArgumentTypeError("section minimum names cannot be empty")
        minimums[name] = _non_negative_int(raw_length.strip())
    return minimums


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--newsletter-id",
        help="Analyze one newsletter_sends id or issue_id instead of the latest draft.",
    )
    parser.add_argument(
        "--required-sections",
        type=_required_sections,
        default=DEFAULT_REQUIRED_SECTIONS,
        help=(
            "Comma-separated required section names "
            f"(default: {','.join(DEFAULT_REQUIRED_SECTIONS)})."
        ),
    )
    parser.add_argument(
        "--min-section-words",
        type=_non_negative_int,
        default=DEFAULT_MIN_SECTION_WORDS,
        help=f"Default minimum words per required section (default: {DEFAULT_MIN_SECTION_WORDS}).",
    )
    parser.add_argument(
        "--section-minimums",
        type=_section_minimums,
        default={},
        help="Comma-separated per-section word minimums, e.g. intro=20,curated links=10.",
    )
    parser.add_argument(
        "--input",
        help="Analyze a newsletter payload from this file instead of the database; use '-' for stdin.",
    )
    parser.add_argument(
        "--subject",
        default="",
        help="Optional subject label for --input reports.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        try:
            args = parse_args(argv)
        except SystemExit as exc:
            return int(exc.code or 0)

        if args.input:
            body = sys.stdin.read() if args.input == "-" else Path(args.input).read_text()
            report = analyze_newsletter_section_completion(
                body,
                newsletter_id=args.input,
                subject=args.subject,
                required_sections=args.required_sections,
                min_section_words=args.min_section_words,
                section_minimums=args.section_minimums,
            )
        else:
            with script_context() as (_config, db):
                report = build_newsletter_section_completion_report(
                    db,
                    newsletter_id=args.newsletter_id,
                    required_sections=args.required_sections,
                    min_section_words=args.min_section_words,
                    section_minimums=args.section_minimums,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_newsletter_section_completion_json(report))
    else:
        print(format_newsletter_section_completion_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
