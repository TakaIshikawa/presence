#!/usr/bin/env python3
"""Import a CSV or JSON campaign topic backlog into planned_topics."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.campaign_topic_import import (  # noqa: E402
    format_campaign_topic_import_json,
    format_campaign_topic_import_text,
    import_campaign_topics,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--campaign-id",
        type=int,
        required=True,
        help="Campaign id that imported planned_topics should belong to.",
    )
    parser.add_argument(
        "--file",
        required=True,
        help="CSV or JSON file containing topic, angle, target_date, priority, source, and notes fields.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write valid rows to planned_topics. Defaults to dry-run.",
    )
    parser.add_argument(
        "--skip-duplicates",
        action="store_true",
        help="Skip duplicate campaign/topic/angle rows instead of blocking apply mode.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
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
            report = import_campaign_topics(
                db,
                campaign_id=args.campaign_id,
                file_path=args.file,
                apply=args.apply,
                skip_duplicates=args.skip_duplicates,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_campaign_topic_import_json(report))
    else:
        print(format_campaign_topic_import_text(report))

    if report.invalid or report.blocked:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
