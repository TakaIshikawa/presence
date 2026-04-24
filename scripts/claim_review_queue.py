#!/usr/bin/env python3
"""Export generated content with unsupported claims for manual review."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.claim_review_queue import (  # noqa: E402
    build_claim_review_payload,
    format_json,
    format_markdown,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of generated-content days to inspect (default: 30)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "md"],
        default="md",
        help="Output format (default: md)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the export to this path instead of stdout",
    )
    parser.add_argument(
        "--include-published",
        action="store_true",
        help="Include rows whose generated content has already been published",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.days < 1:
        raise SystemExit("--days must be at least 1")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        payload = build_claim_review_payload(
            db,
            days=args.days,
            include_published=args.include_published,
        )

    output = format_json(payload) if args.format == "json" else format_markdown(payload)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
    else:
        print(output)


if __name__ == "__main__":
    main()
