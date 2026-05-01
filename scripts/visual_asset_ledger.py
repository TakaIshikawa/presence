#!/usr/bin/env python3
"""Print a ledger of visual asset usage and publication state."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.visual_asset_ledger import (  # noqa: E402
    DEFAULT_DAYS,
    LEDGER_STATUSES,
    build_visual_asset_ledger,
    format_visual_asset_ledger_json,
    format_visual_asset_ledger_table,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--status",
        choices=LEDGER_STATUSES,
        help="Filter by effective asset status",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Only include rows with local missing-file warnings",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        rows = build_visual_asset_ledger(
            db,
            days=args.days,
            status=args.status,
            missing_only=args.missing_only,
        )

    output = (
        format_visual_asset_ledger_json(rows)
        if args.json
        else format_visual_asset_ledger_table(rows, days=args.days)
    )
    print(output)


if __name__ == "__main__":
    main()
