#!/usr/bin/env python3
"""Report topical concentration in newsletter candidate content."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_topic_balance import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MAX_SHARE,
    build_newsletter_topic_balance_report,
    format_newsletter_topic_balance_json,
    format_newsletter_topic_balance_markdown,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _share(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid share: {value}") from exc
    if parsed <= 0 or parsed > 1:
        raise argparse.ArgumentTypeError("share must be greater than 0 and at most 1")
    return parsed


def _item_ids(value: str) -> tuple[int, ...]:
    if not value.strip():
        return ()
    ids = []
    for part in value.split(","):
        text = part.strip()
        if not text:
            continue
        try:
            item_id = int(text)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"invalid content id: {text}") from exc
        if item_id <= 0:
            raise argparse.ArgumentTypeError("content ids must be positive")
        ids.append(item_id)
    return tuple(ids)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window for generated content candidates (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--max-topic-share",
        type=_share,
        default=DEFAULT_MAX_SHARE,
        help=f"Maximum allowed share for one topic, 0-1 (default: {DEFAULT_MAX_SHARE}).",
    )
    parser.add_argument(
        "--item-ids",
        type=_item_ids,
        default=(),
        help="Comma-separated generated_content IDs to analyze instead of the lookback window.",
    )
    parser.add_argument(
        "--format",
        choices=("markdown", "json"),
        default="markdown",
        help="Output format (default: markdown).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        try:
            args = parse_args(argv)
        except SystemExit as exc:
            return int(exc.code or 0)

        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_newsletter_topic_balance_report(
                    conn,
                    days=args.days,
                    max_topic_share=args.max_topic_share,
                    item_ids=args.item_ids,
                )
        else:
            with script_context() as (_config, db):
                report = build_newsletter_topic_balance_report(
                    db,
                    days=args.days,
                    max_topic_share=args.max_topic_share,
                    item_ids=args.item_ids,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_newsletter_topic_balance_json(report))
    else:
        print(format_newsletter_topic_balance_markdown(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
