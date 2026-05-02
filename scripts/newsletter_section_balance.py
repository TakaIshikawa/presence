#!/usr/bin/env python3
"""Report generated newsletter draft section balance before delivery."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_section_balance import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MAX_SECTION_WORD_SHARE,
    DEFAULT_REQUIRED_SECTIONS,
    build_newsletter_section_balance_report,
    build_newsletter_section_balance_report_from_text,
    format_newsletter_section_balance_json,
    format_newsletter_section_balance_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _share(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid float: {value}") from exc
    if parsed <= 0 or parsed > 1:
        raise argparse.ArgumentTypeError("value must be greater than 0 and at most 1")
    return parsed


def _required_sections(value: str) -> tuple[str, ...]:
    sections = tuple(part.strip() for part in value.split(",") if part.strip())
    if not sections:
        raise argparse.ArgumentTypeError("at least one required section is needed")
    return sections


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days by newsletter timestamp (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum newsletter rows to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--max-section-word-share",
        type=_share,
        default=DEFAULT_MAX_SECTION_WORD_SHARE,
        help=(
            "Flag sections above this share of issue words "
            f"(default: {DEFAULT_MAX_SECTION_WORD_SHARE})."
        ),
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
        "--input",
        help="Analyze a newsletter body from this file instead of the database; use '-' for stdin.",
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
            report = build_newsletter_section_balance_report_from_text(
                body,
                newsletter_id=args.input,
                subject=args.subject,
                required_sections=args.required_sections,
                max_section_word_share=args.max_section_word_share,
            )
        else:
            with script_context() as (_config, db):
                report = build_newsletter_section_balance_report(
                    db,
                    days=args.days,
                    limit=args.limit,
                    required_sections=args.required_sections,
                    max_section_word_share=args.max_section_word_share,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_newsletter_section_balance_json(report))
    else:
        print(format_newsletter_section_balance_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
