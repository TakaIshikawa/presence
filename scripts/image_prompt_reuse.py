#!/usr/bin/env python3
"""Audit reused image generation prompts."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.image_prompt_reuse import (  # noqa: E402
    DEFAULT_SIMILARITY_THRESHOLD,
    build_image_prompt_reuse_report,
    format_image_prompt_reuse_json,
    format_image_prompt_reuse_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to look back by generated_content.created_at (default: 30)",
    )
    parser.add_argument(
        "--similarity-threshold",
        type=float,
        default=DEFAULT_SIMILARITY_THRESHOLD,
        help="Token-overlap threshold for near duplicates (default: 0.8)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum findings to include (default: 20)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "text"],
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
            report = build_image_prompt_reuse_report(
                db,
                days=args.days,
                similarity_threshold=args.similarity_threshold,
                limit=args.limit,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_image_prompt_reuse_json(report))
    else:
        print(format_image_prompt_reuse_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
