#!/usr/bin/env python3
"""Link queued replies to relevant knowledge rows."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_knowledge_linker import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_SCORE,
    format_reply_knowledge_report_json,
    format_reply_knowledge_report_text,
    link_reply_knowledge,
)
from knowledge.embeddings import get_embedding_provider  # noqa: E402
from knowledge.store import KnowledgeStore  # noqa: E402
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reply-id", type=int, help="Link one reply_queue row by id")
    parser.add_argument(
        "--status",
        default="pending",
        help="Reply status to batch-link when --reply-id is omitted (default: pending)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum replies and search results to inspect (default: {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=DEFAULT_MIN_SCORE,
        help=f"Minimum knowledge relevance score to link (default: {DEFAULT_MIN_SCORE})",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Insert new reply_knowledge_links instead of reporting a dry run",
    )
    parser.add_argument(
        "--format",
        choices=["json", "text"],
        default="text",
        help="Output format (default: text)",
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
        with script_context() as (config, db):
            if not getattr(config, "embeddings", None):
                raise ValueError("embeddings are not configured")
            embedder = get_embedding_provider(
                config.embeddings.provider,
                config.embeddings.api_key,
                config.embeddings.model,
            )
            report = link_reply_knowledge(
                db,
                reply_id=args.reply_id,
                status=args.status,
                limit=args.limit,
                min_score=args.min_score,
                dry_run=not args.apply,
                search_provider=KnowledgeStore(db.conn, embedder),
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_knowledge_report_json(report))
    else:
        print(format_reply_knowledge_report_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
