#!/usr/bin/env python3
"""Export a manifest of generated visual assets."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.visual_manifest import (
    VisualManifestFilters,
    list_visual_manifest_entries,
    manifest_to_json,
    manifest_to_table,
)
from runner import script_context


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--since-days",
        type=int,
        help="Only include visual assets generated in the last N days",
    )
    parser.add_argument(
        "--content-id",
        type=int,
        help="Only include one generated_content id",
    )
    parser.add_argument(
        "--missing-alt-only",
        action="store_true",
        help="Only include visual assets without usable alt text",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of a table",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write output to a file instead of stdout",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    filters = VisualManifestFilters(
        since_days=args.since_days,
        content_id=args.content_id,
        missing_alt_only=args.missing_alt_only,
    )

    with script_context() as (_config, db):
        entries = list_visual_manifest_entries(db, filters)

    output = manifest_to_json(entries) if args.json else manifest_to_table(entries)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n")
    else:
        print(output)


if __name__ == "__main__":
    main()
