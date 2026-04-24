#!/usr/bin/env python3
"""Import LinkedIn engagement metrics from a CSV export."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.linkedin_engagement import import_linkedin_engagement_csv  # noqa: E402
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, help="Path to the LinkedIn metrics CSV")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report matched inserts without writing engagement snapshots",
    )
    parser.add_argument(
        "--fetched-at",
        help="ISO timestamp to store on imported snapshots (defaults to now)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            result = import_linkedin_engagement_csv(
                db,
                args.csv,
                dry_run=args.dry_run,
                fetched_at=args.fetched_at,
            )
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    action = "Would insert" if result.dry_run else "Inserted"
    print(
        f"{action} {result.insert_count} LinkedIn engagement snapshot"
        f"{'' if result.insert_count == 1 else 's'}."
    )
    if result.unmatched:
        print(f"Unmatched rows: {result.unmatched_count}")
        for row in result.unmatched:
            ref = row.linkedin_url or row.post_id or "(missing URL/post ID)"
            print(f"  row {row.source_row}: {ref}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
