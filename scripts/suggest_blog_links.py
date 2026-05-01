#!/usr/bin/env python3
"""Suggest internal links for a blog draft or generated_content row."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.blog_internal_links import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_SCORE,
    build_blog_internal_link_suggestions,
    format_blog_internal_links_json,
    format_blog_internal_links_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--draft",
        help="Markdown draft file to analyze.",
    )
    source.add_argument(
        "--content-id",
        type=int,
        help="generated_content ID to analyze.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum suggestions to return (default: {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=DEFAULT_MIN_SCORE,
        help=f"Minimum score to include (default: {DEFAULT_MIN_SCORE})",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        with script_context() as (_config, db):
            report = build_blog_internal_link_suggestions(
                db,
                draft_path=args.draft,
                content_id=args.content_id,
                limit=args.limit,
                min_score=args.min_score,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_blog_internal_links_json(report))
    else:
        print(format_blog_internal_links_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
