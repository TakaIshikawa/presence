#!/usr/bin/env python3
"""Import Mastodon engagement metrics from a CSV export."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.mastodon_engagement_import import (  # noqa: E402
    import_mastodon_engagement_csv,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, help="Path to the Mastodon metrics CSV")
    parser.add_argument(
        "--fetched-at",
        help="ISO timestamp to store on imported snapshots (defaults to now)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report matched rows without writing engagement snapshots",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON with per-row status and aggregate counts",
    )
    return parser.parse_args(argv)


def _print_text_summary(result: dict) -> None:
    counts = result["counts"]
    action = "Would insert" if result["dry_run"] else "Inserted"
    print(
        f"{action} {counts['inserted']} Mastodon engagement snapshot"
        f"{'' if counts['inserted'] == 1 else 's'}."
    )
    print(
        f"{counts['matched']} matched, "
        f"{counts['unmatched']} unmatched, "
        f"{counts['duplicates']} duplicates, "
        f"{counts['invalid']} invalid"
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            result = import_mastodon_engagement_csv(
                db,
                args.csv,
                fetched_at=args.fetched_at,
                dry_run=args.dry_run,
            )
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text_summary(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
