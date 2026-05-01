#!/usr/bin/env python3
"""Export high-quality reply drafts as knowledge seed candidates."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.reply_seed_export import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_QUALITY,
    build_reply_knowledge_seed_export,
    format_reply_knowledge_seed_export_json,
    format_reply_knowledge_seed_export_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Recent reply activity window to consider (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-quality",
        type=float,
        default=DEFAULT_MIN_QUALITY,
        help=(
            "Minimum reply quality score required for a seed "
            f"(default: {DEFAULT_MIN_QUALITY:.1f})."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output deterministic JSON instead of text.",
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
            export = build_reply_knowledge_seed_export(
                db,
                days=args.days,
                min_quality=args.min_quality,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_reply_knowledge_seed_export_json(export))
    else:
        print(format_reply_knowledge_seed_export_text(export))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
