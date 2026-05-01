#!/usr/bin/env python3
"""Build source attribution blocks for generated content."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.attribution_packet import (  # noqa: E402
    build_attribution_packet,
    format_attribution_packet_json,
    format_attribution_packet_text,
)
from runner import script_context  # noqa: E402


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
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--include-open",
        action="store_true",
        help="Include open-license sources that do not require attribution.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            packet = build_attribution_packet(
                db,
                args.content_id,
                include_open=args.include_open,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_attribution_packet_json(packet))
    else:
        print(format_attribution_packet_text(packet))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
