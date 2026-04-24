#!/usr/bin/env python3
"""Seed reviewable rewrite ideas from low-resonance published content."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.low_resonance_rewriter import (
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_SCORE_GAP,
    LowResonanceRewriter,
    RewriteSeedResult,
)


def _shorten(text: str | None, width: int = 78) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def seed_rewrite_ideas(
    db,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_score_gap: float = DEFAULT_MIN_SCORE_GAP,
    dry_run: bool = False,
    priority: str = "normal",
) -> list[RewriteSeedResult]:
    return LowResonanceRewriter(db).seed_ideas(
        days=days,
        limit=limit,
        min_score_gap=min_score_gap,
        dry_run=dry_run,
        priority=priority,
    )


def format_results_table(results: list[RewriteSeedResult]) -> str:
    lines = [
        f"{'Status':8s}  {'ID':>4s}  {'Source':>6s}  {'Topic':18s}  Reason / rewrite idea",
        f"{'-' * 8:8s}  {'-' * 4:>4s}  {'-' * 6:>6s}  {'-' * 18:18s}  {'-' * 40}",
    ]
    if not results:
        lines.append("none      ----  ------  ------------------  no eligible rewrite ideas")
        return "\n".join(lines)

    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        detail = f"{result.reason}: {_shorten(result.note)}"
        lines.append(
            f"{result.status:8s}  "
            f"{idea_id:>4s}  "
            f"{result.source_content_id:>6d}  "
            f"{_shorten(result.topic, 18):18s}  "
            f"{detail}"
        )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Lookback window for published low-resonance content (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum rewrite ideas to process (default: {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--min-score-gap",
        type=float,
        default=DEFAULT_MIN_SCORE_GAP,
        help="Minimum expected-minus-actual engagement gap required (default: 0)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show rewrite candidates without creating content ideas",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of a table",
    )
    parser.add_argument(
        "--priority",
        choices=("high", "normal", "low"),
        default="normal",
        help="Priority for created ideas (default: normal)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        results = seed_rewrite_ideas(
            db,
            days=args.days,
            limit=args.limit,
            min_score_gap=args.min_score_gap,
            dry_run=args.dry_run,
            priority=args.priority,
        )
    if args.json:
        print(json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True))
    else:
        print(format_results_table(results))


if __name__ == "__main__":
    main()
