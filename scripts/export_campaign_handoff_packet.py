#!/usr/bin/env python3
"""Export a read-only campaign handoff packet."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.campaign_handoff_packet import (  # noqa: E402
    STATUS_OK,
    build_campaign_handoff_packet,
    format_campaign_handoff_packet_json,
    format_campaign_handoff_packet_text,
)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--campaign-id", type=_positive_int, help="Campaign ID to export.")
    selector.add_argument("--campaign", help="Campaign slug or name to export.")
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the packet to this path instead of stdout.",
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
            packet = build_campaign_handoff_packet(
                db,
                campaign_id=args.campaign_id,
                campaign=args.campaign,
            )
    except (OSError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    output = (
        format_campaign_handoff_packet_json(packet)
        if args.format == "json"
        else format_campaign_handoff_packet_text(packet)
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
    else:
        print(output)
    return 0 if packet.status == STATUS_OK else 1


if __name__ == "__main__":
    raise SystemExit(main())
