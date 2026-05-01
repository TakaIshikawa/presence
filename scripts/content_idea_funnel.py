#!/usr/bin/env python3
"""Report content idea conversion through publication and resonance."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_idea_funnel import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_GROUP_BY,
    DEFAULT_RESONANCE_SCORE_THRESHOLD,
    VALID_GROUP_BY,
    build_content_idea_funnel_report,
    format_content_idea_funnel_json,
    format_content_idea_funnel_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback-days",
        "--days",
        dest="days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--group-by",
        choices=sorted(VALID_GROUP_BY),
        default=DEFAULT_GROUP_BY,
        help=f"Group rows by source, topic, or both (default: {DEFAULT_GROUP_BY}).",
    )
    parser.add_argument(
        "--source",
        help="Only include ideas from this source.",
    )
    parser.add_argument(
        "--topic",
        help="Only include ideas with this topic.",
    )
    parser.add_argument(
        "--resonance-score-threshold",
        type=float,
        default=DEFAULT_RESONANCE_SCORE_THRESHOLD,
        help=(
            "Engagement score required to count published content as resonated "
            f"(default: {DEFAULT_RESONANCE_SCORE_THRESHOLD})."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print deterministic JSON instead of the default text table.",
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
            report = build_content_idea_funnel_report(
                db,
                days=args.days,
                group_by=args.group_by,
                source=args.source,
                topic=args.topic,
                resonance_score_threshold=args.resonance_score_threshold,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_content_idea_funnel_json(report))
    else:
        print(format_content_idea_funnel_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
