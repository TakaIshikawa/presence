#!/usr/bin/env python3
"""Import manually downloaded Bluesky engagement metrics from CSV."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.bluesky_engagement_import import import_bluesky_engagement_csv
from runner import script_context


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Bluesky engagement metrics from a CSV export."
    )
    parser.add_argument("--csv", required=True, help="Path to the CSV file to import")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and match rows without writing engagement metrics",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON with per-row status and aggregate counts",
    )
    return parser.parse_args()


def _print_text_summary(result: dict) -> None:
    counts = result["counts"]
    mode = "dry run" if result["dry_run"] else "import"
    print(
        f"Bluesky engagement {mode}: "
        f"{counts['matched']} matched, "
        f"{counts['skipped']} skipped, "
        f"{counts['invalid']} invalid"
    )
    if not result["dry_run"]:
        print(f"{counts['inserted']} inserted, {counts['updated']} updated")


def main() -> None:
    args = parse_args()
    with script_context() as (_config, db):
        result = import_bluesky_engagement_csv(db, args.csv, dry_run=args.dry_run)

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text_summary(result)


if __name__ == "__main__":
    main()
