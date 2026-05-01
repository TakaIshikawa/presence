#!/usr/bin/env python3
"""Import LinkedIn comment exports into reply_queue."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.linkedin_comments import import_linkedin_comments  # noqa: E402
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Path to the LinkedIn comment export")
    parser.add_argument("--format", choices=("csv", "json"), required=True, help="Input export format")
    parser.add_argument("--dry-run", action="store_true", help="Report imports without writing reply_queue")
    parser.add_argument("--limit", type=int, help="Maximum rows to inspect")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            result = import_linkedin_comments(
                db,
                args.input,
                format=args.format,
                dry_run=args.dry_run,
                limit=args.limit,
            )
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    action = "Would queue" if result.dry_run else "Queued"
    print(
        f"{action} {result.insert_count} LinkedIn comment"
        f"{'' if result.insert_count == 1 else 's'}."
    )
    if result.skipped:
        print(f"Skipped rows: {result.skipped_count}")
        for item in result.skipped:
            ref = item.get("comment_id") or f"row {item.get('source_row')}"
            print(f"  {ref}: {item['reason']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
