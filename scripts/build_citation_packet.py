#!/usr/bin/env python3
"""Build a generated-content citation packet for operator review."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.citation_packet import (  # noqa: E402
    build_citation_packet,
    format_json_packet,
    format_markdown_packet,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--content-id",
        type=int,
        required=True,
        help="generated_content.id to inspect",
    )
    parser.add_argument(
        "--format",
        choices=("markdown", "json"),
        default="markdown",
        help="Output format (default: markdown).",
    )
    parser.add_argument(
        "--include-supported",
        action="store_true",
        help="Include fully supported claims alongside unsupported and weak claims.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            packet = build_citation_packet(
                db,
                args.content_id,
                include_supported=args.include_supported,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_json_packet(packet))
    else:
        print(format_markdown_packet(packet))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
