#!/usr/bin/env python3
"""Manage scheduled publish queue items without direct SQL edits."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.queue_scheduler import QueueScheduler
from runner import script_context
from output.preview import refresh_deterministic_variants


def _parse_iso_timestamp(value: str) -> str:
    """Validate and normalize an ISO timestamp accepted by datetime."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid ISO timestamp: {value}"
        ) from exc
    return parsed.isoformat()


def _shorten(value: Any, width: int) -> str:
    if value is None:
        return "-"
    text = str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def format_queue_rows(rows: list[dict]) -> str:
    """Format publish queue rows as a compact operator table."""
    if not rows:
        return "No publish queue items found."

    columns = [
        ("id", "ID", 5),
        ("content_id", "CID", 5),
        ("status", "STATUS", 10),
        ("platform", "PLATFORM", 8),
        ("scheduled_at", "SCHEDULED", 25),
        ("hold_reason", "HOLD_REASON", 24),
        ("error", "ERROR", 24),
        ("content", "CONTENT", 44),
    ]
    lines = [
        "  ".join(label.ljust(width) for _, label, width in columns),
        "  ".join("-" * width for _, _, width in columns),
    ]
    for row in rows:
        lines.append(
            "  ".join(
                _shorten(row.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)


def _print_changed(action: str, row: dict) -> None:
    print(f"{action} publish queue item {row['id']}:")
    print(format_queue_rows([row]))


def _print_prepared_variants(queue_id: int, variants: list[dict]) -> None:
    prepared = ", ".join(
        f"{variant['platform']}/{variant['variant_type']}#{variant['id']}"
        for variant in variants
    )
    print(f"Prepared variants for publish queue item {queue_id}: {prepared}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List publish queue items")
    list_parser.add_argument(
        "--status",
        choices=["queued", "published", "failed", "cancelled", "held"],
        help="Filter by queue status",
    )
    list_parser.add_argument(
        "--platform",
        choices=["x", "bluesky", "all"],
        help="Filter by target platform",
    )
    list_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum rows to show (default: 50)",
    )

    schedule_parser = subparsers.add_parser(
        "schedule",
        help="Schedule generated content for publishing",
    )
    schedule_parser.add_argument("content_id", type=int, help="Generated content ID")
    schedule_parser.add_argument(
        "--platform",
        choices=["x", "bluesky", "all"],
        default="all",
        help="Target platform (default: all)",
    )
    schedule_time = schedule_parser.add_mutually_exclusive_group(required=True)
    schedule_time.add_argument(
        "--scheduled-at",
        type=_parse_iso_timestamp,
        help="ISO timestamp to schedule, for example 2026-04-23T12:00:00+00:00",
    )
    schedule_time.add_argument(
        "--next-recommended",
        action="store_true",
        help="Schedule at the next recommended platform slot",
    )
    schedule_parser.add_argument(
        "--recommendation-days",
        type=int,
        default=90,
        help="Days of engagement history for recommendations (default: 90)",
    )
    schedule_parser.add_argument(
        "--recommendation-limit",
        type=int,
        default=3,
        help="Top recommended windows to consider (default: 3)",
    )

    reschedule_parser = subparsers.add_parser(
        "reschedule",
        help="Move an unpublished queue item to a new scheduled time",
    )
    reschedule_parser.add_argument("queue_id", type=int, help="Publish queue item ID")
    reschedule_parser.add_argument(
        "scheduled_at",
        type=_parse_iso_timestamp,
        help="New ISO timestamp, for example 2026-04-23T12:00:00+00:00",
    )

    cancel_parser = subparsers.add_parser(
        "cancel",
        help="Cancel an unpublished queue item",
    )
    cancel_parser.add_argument("queue_id", type=int, help="Publish queue item ID")

    hold_parser = subparsers.add_parser(
        "hold",
        help="Place one or more unpublished queue items on manual hold",
    )
    hold_parser.add_argument("queue_ids", nargs="+", type=int, help="Publish queue item IDs")
    hold_parser.add_argument(
        "--reason",
        help="Optional reason for the manual hold",
    )

    release_parser = subparsers.add_parser(
        "release",
        help="Release one or more held queue items back to queued",
    )
    release_parser.add_argument(
        "queue_ids",
        nargs="+",
        type=int,
        help="Publish queue item IDs",
    )

    restore_parser = subparsers.add_parser(
        "restore",
        help="Restore a cancelled or failed queue item to queued",
    )
    restore_parser.add_argument("queue_id", type=int, help="Publish queue item ID")
    restore_parser.add_argument(
        "--scheduled-at",
        type=_parse_iso_timestamp,
        help="Optional replacement ISO scheduled time",
    )

    prepare_parser = subparsers.add_parser(
        "prepare-variants",
        help="Generate deterministic platform variants for queued items without publishing",
    )
    prepare_parser.add_argument(
        "queue_ids",
        nargs="+",
        type=int,
        help="Publish queue item IDs",
    )
    prepare_parser.add_argument(
        "--suggest-hashtags",
        action="store_true",
        help="Include deterministic hashtag suggestions in refreshed variants",
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
            if args.command == "list":
                rows = db.get_publish_queue_items(
                    status=args.status,
                    platform=args.platform,
                    limit=args.limit,
                )
                print(format_queue_rows(rows))
            elif args.command == "schedule":
                if args.next_recommended:
                    scheduler = QueueScheduler(
                        db,
                        _config,
                        recommendation_days=args.recommendation_days,
                        recommendation_limit=args.recommendation_limit,
                    )
                    result = scheduler.schedule_content(
                        args.content_id,
                        args.platform,
                    )
                    row = db.get_publish_queue_item(result.queue_id)
                else:
                    queue_id = db.queue_for_publishing(
                        args.content_id,
                        args.scheduled_at,
                        platform=args.platform,
                    )
                    row = db.get_publish_queue_item(queue_id)
                _print_changed("Scheduled", row)
            elif args.command == "reschedule":
                row = db.reschedule_publish_queue_item(
                    args.queue_id,
                    args.scheduled_at,
                )
                _print_changed("Rescheduled", row)
            elif args.command == "cancel":
                row = db.cancel_publish_queue_item(args.queue_id)
                _print_changed("Cancelled", row)
            elif args.command == "hold":
                for queue_id in args.queue_ids:
                    row = db.hold_publish_queue_item(queue_id, reason=args.reason)
                    _print_changed("Held", row)
            elif args.command == "release":
                for queue_id in args.queue_ids:
                    row = db.release_publish_queue_item(queue_id)
                    _print_changed("Released", row)
            elif args.command == "restore":
                row = db.restore_publish_queue_item(
                    args.queue_id,
                    scheduled_at=args.scheduled_at,
                )
                _print_changed("Restored", row)
            elif args.command == "prepare-variants":
                for queue_id in args.queue_ids:
                    row = db.get_publish_queue_item(queue_id)
                    if row is None:
                        raise ValueError(f"publish queue item not found: {queue_id}")
                    variants = refresh_deterministic_variants(
                        db,
                        {
                            "id": row["content_id"],
                            "content": row["content"],
                            "content_type": row["content_type"],
                        },
                        include_hashtag_suggestions=args.suggest_hashtags,
                    )
                    _print_prepared_variants(queue_id, variants)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
