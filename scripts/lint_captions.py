#!/usr/bin/env python3
"""Lint generated captions and selected platform variants before publication."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.caption_policy_linter import (  # noqa: E402
    CaptionPolicyRecordNotFound,
    format_json_report,
    format_text_report,
    lint_caption_policy,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--content-id", type=int, help="generated_content id to lint.")
    target.add_argument("--queue-id", type=int, help="publish_queue id to lint.")
    parser.add_argument(
        "--platform",
        choices=("x", "bluesky", "all"),
        default="all",
        help="Platform policy to apply (default: all).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Promote configured policy warnings to blocking errors.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING if args.format == "json" else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        with script_context() as (_config, db):
            report = lint_caption_policy(
                db,
                content_id=args.content_id,
                queue_id=args.queue_id,
                platform=args.platform,
                strict=args.strict,
            )
    except (CaptionPolicyRecordNotFound, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_json_report(report))
    else:
        print(format_text_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
