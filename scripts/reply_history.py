#!/usr/bin/env python3
"""Show local reply history context for an inbound author."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_history import (  # noqa: E402
    DEFAULT_LIMIT,
    build_reply_author_history,
    format_reply_author_history_text,
)
from runner import script_context  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Summarize prior reply_queue interactions for a handle or author ID."
    )
    identity = parser.add_mutually_exclusive_group(required=True)
    identity.add_argument("--handle", help="Inbound author handle, with or without @.")
    identity.add_argument("--author-id", help="Inbound author ID or DID.")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum recent interactions to include. Default: {DEFAULT_LIMIT}.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format. Default: text.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.limit <= 0:
        raise ValueError("--limit must be positive")

    logging.basicConfig(level=logging.WARNING)

    with script_context() as (_config, db):
        history = build_reply_author_history(
            db,
            handle=args.handle,
            author_id=args.author_id,
            limit=args.limit,
        )

    if args.format == "json":
        print(json.dumps(history, indent=2, sort_keys=True))
    else:
        print(format_reply_author_history_text(history))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
