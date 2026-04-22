#!/usr/bin/env python3
"""Print a concise ledger of generated content publication state."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context


def _shorten(value: Any, width: int) -> str:
    if value is None:
        return "-"
    text = str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _display_tweet_id(row: dict) -> str | None:
    if row.get("platform") == "x" and row.get("platform_post_id"):
        return row["platform_post_id"]
    return row.get("tweet_id")


def _display_bluesky_uri(row: dict) -> str | None:
    if row.get("platform") == "bluesky" and row.get("platform_post_id"):
        return row["platform_post_id"]
    return row.get("bluesky_uri")


def ledger_rows_for_json(rows: list[dict]) -> list[dict]:
    """Normalize ledger rows for machine-readable output."""
    normalized = []
    for row in rows:
        normalized.append(
            {
                "content_id": row["content_id"],
                "content_type": row["content_type"],
                "content": row["content"],
                "generated_at": row["generated_at"],
                "platform": row["platform"],
                "status": row["status"],
                "publish_queue": {
                    "id": row["queue_id"],
                    "platform": row["queue_platform"],
                    "status": row["queue_status"],
                    "scheduled_at": row["scheduled_at"],
                    "published_at": row["queue_published_at"],
                    "error": row["queue_error"],
                    "hold_reason": row["queue_hold_reason"],
                },
                "content_publication": {
                    "id": row["publication_id"],
                    "status": row["publication_status"],
                    "platform_post_id": row["platform_post_id"],
                    "platform_url": row["platform_url"],
                    "attempt_count": row["attempt_count"],
                    "published_at": row["platform_published_at"],
                    "next_retry_at": row["next_retry_at"],
                    "last_error_at": row["last_error_at"],
                    "updated_at": row["publication_updated_at"],
                    "error": row["publication_error"],
                },
                "tweet_id": _display_tweet_id(row),
                "bluesky_uri": _display_bluesky_uri(row),
                "published_at": row["published_at"],
                "error": row["error"],
                "hold_reason": row["hold_reason"],
            }
        )
    return normalized


def format_json_ledger(rows: list[dict]) -> str:
    """Format ledger rows as JSON."""
    return json.dumps(ledger_rows_for_json(rows), indent=2)


def format_table_ledger(rows: list[dict]) -> str:
    """Format ledger rows as a compact text table."""
    if not rows:
        return "No publication ledger rows found."

    columns = [
        ("content", "CID", 5),
        ("type", "TYPE", 10),
        ("platform", "PLATFORM", 8),
        ("queue", "QID", 5),
        ("pub", "PID", 5),
        ("status", "STATUS", 10),
        ("tweet", "TWEET_ID", 14),
        ("bsky", "BLUESKY_URI", 24),
        ("scheduled", "SCHEDULED", 19),
        ("published", "PUBLISHED", 19),
        ("hold_reason", "HOLD_REASON", 24),
        ("error", "ERROR", 24),
        ("preview", "CONTENT", 32),
    ]

    table_rows = []
    for row in rows:
        table_rows.append(
            {
                "content": row["content_id"],
                "type": row["content_type"],
                "platform": row["platform"],
                "queue": row["queue_id"],
                "pub": row["publication_id"],
                "status": row["status"],
                "tweet": _display_tweet_id(row),
                "bsky": _display_bluesky_uri(row),
                "scheduled": row["scheduled_at"],
                "published": row["published_at"],
                "hold_reason": row["hold_reason"],
                "error": row["error"],
                "preview": row["content"],
            }
        )

    lines = []
    header = "  ".join(label.ljust(width) for _, label, width in columns)
    divider = "  ".join("-" * width for _, _, width in columns)
    lines.append(header)
    lines.append(divider)
    for table_row in table_rows:
        lines.append(
            "  ".join(
                _shorten(table_row[key], width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to look back (default: 30)",
    )
    parser.add_argument(
        "--status",
        help="Filter by effective status, such as queued, published, failed, or generated",
    )
    parser.add_argument(
        "--platform",
        default="all",
        choices=["all", "x", "bluesky"],
        help="Platform to include (default: all)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        rows = db.get_publication_ledger(
            days=args.days,
            status=args.status,
            platform=args.platform,
        )

    if args.json:
        print(format_json_ledger(rows))
    else:
        print(format_table_ledger(rows))


if __name__ == "__main__":
    main()
