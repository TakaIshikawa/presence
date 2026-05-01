#!/usr/bin/env python3
"""Lint an assembled newsletter draft before Buttondown delivery."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_deliverability_linter import (  # noqa: E402
    DEFAULT_MAX_LINKS,
    DEFAULT_MAX_PREHEADER_CHARS,
    format_newsletter_deliverability_json,
    format_newsletter_deliverability_text,
    lint_newsletter_deliverability,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "draft",
        nargs="?",
        default="-",
        help="HTML or Markdown newsletter draft file to lint, or '-' for stdin (default: stdin).",
    )
    parser.add_argument(
        "--subject",
        default="",
        help="Subject line planned for Buttondown delivery.",
    )
    parser.add_argument(
        "--preheader",
        default="",
        help="Preview/preheader text planned for inbox clients.",
    )
    parser.add_argument(
        "--plaintext",
        "--plain-text",
        default="",
        help="Plaintext body planned for non-HTML email clients.",
    )
    parser.add_argument(
        "--plaintext-file",
        "--plain-text-file",
        help="Read plaintext body from this file instead of --plaintext.",
    )
    parser.add_argument(
        "--max-links",
        type=int,
        default=DEFAULT_MAX_LINKS,
        help=f"Maximum draft link count before warning (default: {DEFAULT_MAX_LINKS}).",
    )
    parser.add_argument(
        "--max-preheader-chars",
        type=int,
        default=DEFAULT_MAX_PREHEADER_CHARS,
        help=(
            "Maximum preheader/preview text length before warning "
            f"(default: {DEFAULT_MAX_PREHEADER_CHARS})."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print deterministic JSON instead of the default text report.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        _validate_limits(args.max_links, args.max_preheader_chars)
        draft = _read_text_arg(args.draft, stdin=True)
        plaintext = (
            _read_text_arg(args.plaintext_file, stdin=False)
            if args.plaintext_file
            else args.plaintext
        )
        report = lint_newsletter_deliverability(
            subject=args.subject,
            preheader=args.preheader,
            html=draft,
            plaintext=plaintext,
            max_links=args.max_links,
            max_preheader_chars=args.max_preheader_chars,
        )
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_newsletter_deliverability_json(report))
    else:
        print(format_newsletter_deliverability_text(report))

    if report.blocking_issue_count:
        return 1
    return 0


def _read_text_arg(path: str, *, stdin: bool) -> str:
    if stdin and path == "-":
        return sys.stdin.read()
    return Path(path).read_text()


def _validate_limits(max_links: int, max_preheader_chars: int) -> None:
    if max_links < 0:
        raise ValueError("max_links must be non-negative")
    if max_preheader_chars < 0:
        raise ValueError("max_preheader_chars must be non-negative")


if __name__ == "__main__":
    raise SystemExit(main())
