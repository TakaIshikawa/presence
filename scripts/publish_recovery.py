#!/usr/bin/env python3
"""Print recovery recommendations for failed publish attempts."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publish_recovery import get_publish_recovery_recommendations
from runner import script_context


def _shorten(value: Any, width: int) -> str:
    if value is None:
        return "-"
    text = str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def format_json_recommendations(groups: list[dict]) -> str:
    """Format recovery recommendations as stable JSON."""
    return json.dumps(groups, indent=2, sort_keys=True)


def format_table_recommendations(groups: list[dict]) -> str:
    """Format recovery recommendations as a compact operator table."""
    if not groups:
        return "No publish recovery recommendations found."

    columns = [
        ("action", "ACTION", 16),
        ("platform", "PLATFORM", 8),
        ("error_category", "CATEGORY", 10),
        ("attempt_count", "ATTEMPTS", 8),
        ("next_retry_at", "NEXT_RETRY", 25),
        ("count", "COUNT", 5),
        ("sample", "SAMPLE", 42),
    ]
    lines = [
        "  ".join(label.ljust(width) for _, label, width in columns),
        "  ".join("-" * width for _, _, width in columns),
    ]
    for group in groups:
        first = group["items"][0] if group.get("items") else {}
        row = {
            **group,
            "sample": first.get("content"),
        }
        lines.append(
            "  ".join(
                _shorten(row.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--platform",
        choices=["x", "bluesky"],
        help="Filter by platform",
    )
    parser.add_argument(
        "--status",
        choices=["queued", "failed", "held", "cancelled"],
        help="Filter by effective recovery status",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum recommendation groups to show (default: 50)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
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
            groups = get_publish_recovery_recommendations(
                db.conn,
                platform=args.platform,
                status=args.status,
                limit=args.limit,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_json_recommendations(groups))
    else:
        print(format_table_recommendations(groups))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
