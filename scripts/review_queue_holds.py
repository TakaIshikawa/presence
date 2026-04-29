#!/usr/bin/env python3
"""Review, release, or cancel held publish queue items."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.queue_holds import (
    format_held_items,
    held_item_record,
    parse_iso_timestamp,
    report_held_items,
)
from runner import script_context


def _parse_iso_arg(value: str) -> str:
    try:
        return parse_iso_timestamp(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _add_common_filters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--json",
        action="store_true",
        default=argparse.SUPPRESS,
        help="Emit JSON output",
    )
    parser.add_argument(
        "--before",
        type=_parse_iso_arg,
        help="Only include held items scheduled before this ISO timestamp",
    )
    parser.add_argument(
        "--reason-match",
        help="Only include held items whose hold reason contains this text",
    )
    parser.add_argument(
        "--platform",
        choices=["x", "bluesky", "all"],
        help="Only include held items for this platform",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum held items to inspect (default: 50)",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON output",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    report_parser = subparsers.add_parser("report", help="Show held queue items")
    _add_common_filters(report_parser)

    release_parser = subparsers.add_parser(
        "release",
        help="Release matching held items back to queued",
    )
    _add_common_filters(release_parser)
    release_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview matching held items without writing changes",
    )

    cancel_parser = subparsers.add_parser(
        "cancel",
        help="Cancel matching held items",
    )
    _add_common_filters(cancel_parser)
    cancel_parser.add_argument(
        "--message",
        default="Cancelled from queue hold review",
        help="Status message recorded on cancelled items",
    )
    cancel_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview matching held items without writing changes",
    )

    return parser.parse_args(argv)


def _payload(
    *,
    command: str,
    dry_run: bool,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "command": command,
        "dry_run": dry_run,
        "count": len(records),
        "items": records,
    }


def _print_result(args: argparse.Namespace, records: list[dict[str, Any]]) -> None:
    dry_run = bool(getattr(args, "dry_run", False))
    if args.json:
        print(json.dumps(_payload(command=args.command, dry_run=dry_run, records=records), indent=2))
        return

    if args.command == "report":
        print(format_held_items(records))
        return

    action = "Would release" if dry_run and args.command == "release" else "Released"
    if args.command == "cancel":
        action = "Would cancel" if dry_run else "Cancelled"
    print(f"{action} {len(records)} held publish queue item(s).")
    print(format_held_items(records))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        with script_context() as (_config, db):
            if args.command == "report":
                records = report_held_items(
                    db,
                    before=args.before,
                    reason_match=args.reason_match,
                    platform=args.platform,
                    limit=args.limit,
                )
            elif args.command == "release":
                rows = db.release_held_publish_queue_items(
                    before=args.before,
                    reason_match=args.reason_match,
                    platform=args.platform,
                    limit=args.limit,
                    dry_run=args.dry_run,
                )
                records = [held_item_record(row) for row in rows]
            elif args.command == "cancel":
                rows = db.cancel_held_publish_queue_items(
                    status_message=args.message,
                    before=args.before,
                    reason_match=args.reason_match,
                    platform=args.platform,
                    limit=args.limit,
                    dry_run=args.dry_run,
                )
                records = [held_item_record(row) for row in rows]
            else:
                raise ValueError(f"unsupported command: {args.command}")
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    _print_result(args, records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
