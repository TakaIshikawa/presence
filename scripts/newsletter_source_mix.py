#!/usr/bin/env python3
"""Report newsletter source diversity across recent sends."""

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_source_mix import (  # noqa: E402
    NewsletterSourceMix,
    NewsletterSourceMixRow,
)
from runner import script_context  # noqa: E402


def format_json_report(rows: list[NewsletterSourceMixRow]) -> str:
    """Format source mix rows as stable JSON."""
    return json.dumps([asdict(row) for row in rows], indent=2, sort_keys=True)


def format_text_report(rows: list[NewsletterSourceMixRow], days: int) -> str:
    """Format source mix rows as a human-readable table."""
    lines = [
        "",
        "=" * 100,
        f"Newsletter Source Mix (last {days} days)",
        "=" * 100,
        "",
    ]
    if not rows:
        lines.append("No newsletter sends found.")
        return "\n".join(lines)

    header = (
        "Send  Issue        Sources  Types                 Knowledge  Topics"
        "                    Warnings"
    )
    lines.append(header)
    lines.append("-" * len(header))
    for row in rows:
        type_summary = (
            f"x:{row.x_post_count} th:{row.thread_count} "
            f"blog:{row.blog_post_count} other:{row.other_content_count}"
        )
        topic_summary = _format_mapping(row.topic_distribution)
        warnings = ", ".join(row.warnings) if row.warnings else "-"
        lines.append(
            f"{row.newsletter_send_id:<5} "
            f"{_clip(row.issue_id or '-', 12):<12} "
            f"{row.found_source_count}/{row.source_count:<7} "
            f"{_clip(type_summary, 21):<21} "
            f"{row.knowledge_backed_item_count:<9} "
            f"{_clip(topic_summary, 25):<25} "
            f"{warnings}"
        )

    return "\n".join(lines).rstrip()


def _format_mapping(values: dict[str, int]) -> str:
    if not values:
        return "-"
    return ", ".join(f"{key}:{value}" for key, value in values.items())


def _clip(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    return value[: width - 3] + "..."


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit recent newsletter sends for source diversity."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to look back (default: 30)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum sends to include after date filtering",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output stable JSON instead of a human-readable table",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv or [])
    with script_context() as (config, db):
        rows = NewsletterSourceMix(db).summarize(days=args.days, limit=args.limit)

    if args.json:
        print(format_json_report(rows))
    else:
        print(format_text_report(rows, days=args.days))


if __name__ == "__main__":
    main(sys.argv[1:])
