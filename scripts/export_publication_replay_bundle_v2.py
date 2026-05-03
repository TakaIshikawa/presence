#!/usr/bin/env python3
"""Export a publication replay bundle for one content or queue row."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_replay_bundle import (  # noqa: E402
    build_publication_replay_bundle,
    format_publication_replay_bundle_json,
    format_publication_replay_bundle_text,
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--content-id", type=_positive_int, help="generated_content id to export.")
    target.add_argument("--queue-id", type=_positive_int, help="publish_queue id to export.")
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--format",
        choices=["json", "text"],
        default="json",
        help="Output format (default: json).",
    )
    parser.add_argument(
        "--redact",
        action="store_true",
        help="Redact URLs and author handles from free-text fields.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                bundle = build_publication_replay_bundle(
                    conn,
                    content_id=args.content_id,
                    queue_id=args.queue_id,
                    redact=args.redact,
                )
        else:
            with script_context() as (_config, db):
                bundle = build_publication_replay_bundle(
                    db,
                    content_id=args.content_id,
                    queue_id=args.queue_id,
                    redact=args.redact,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publication_replay_bundle_json(bundle))
    else:
        print(format_publication_replay_bundle_text(bundle))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
