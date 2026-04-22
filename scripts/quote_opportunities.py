#!/usr/bin/env python3
"""Find curated source items worth quote-posting."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.quote_opportunities import (  # noqa: E402
    QuoteOpportunity,
    QuoteOpportunityRecommender,
    opportunities_to_dict,
)
from runner import script_context  # noqa: E402


def _shorten(text: str | None, width: int = 72) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def format_json_output(opportunities: list[QuoteOpportunity], enqueued_ids: list[int] | None = None) -> str:
    payload = {
        "opportunities": opportunities_to_dict(opportunities),
        "enqueued_ids": enqueued_ids or [],
    }
    return json.dumps(payload, indent=2)


def format_table_output(opportunities: list[QuoteOpportunity], enqueued_ids: list[int] | None = None) -> str:
    lines = [
        f"{'Score':>5s}  {'Fresh':>5s}  {'Novel':>5s}  {'Author':18s}  {'Topics':24s}  Source",
        f"{'-' * 5:>5s}  {'-' * 5:>5s}  {'-' * 5:>5s}  {'-' * 18:18s}  {'-' * 24:24s}  {'-' * 48}",
    ]
    for item in opportunities:
        lines.append(
            f"{item.score:5.2f}  "
            f"{item.freshness:5.2f}  "
            f"{item.novelty:5.2f}  "
            f"{_shorten(item.author, 18):18s}  "
            f"{_shorten(', '.join(item.topics), 24):24s}  "
            f"{_shorten(item.source_url or item.source_id or str(item.knowledge_id), 72)}"
        )
        reason = "; ".join(item.reasons)
        if reason:
            lines.append(f"       reason: {_shorten(reason, 110)}")
    if enqueued_ids:
        lines.append("")
        lines.append(f"Enqueued proactive quote actions: {', '.join(str(item) for item in enqueued_ids)}")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7, help="Recent source lookback in days (default: 7)")
    parser.add_argument("--limit", type=int, default=10, help="Maximum opportunities to show")
    parser.add_argument("--campaign-id", type=int, help="Only align to this active campaign")
    parser.add_argument("--min-score", type=float, default=0.35, help="Minimum total score")
    parser.add_argument("--enqueue", action="store_true", help="Create pending quote_tweet review actions")
    parser.add_argument(
        "--enqueue-limit",
        type=int,
        help="Maximum number of displayed opportunities to enqueue",
    )
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        recommender = QuoteOpportunityRecommender(db)
        opportunities = recommender.recommend(
            days=args.days,
            limit=args.limit,
            campaign_id=args.campaign_id,
            min_score=args.min_score,
        )
        enqueued_ids = recommender.enqueue(
            opportunities,
            limit=args.enqueue_limit,
        ) if args.enqueue else []

    if args.json:
        print(format_json_output(opportunities, enqueued_ids))
    else:
        print(format_table_output(opportunities, enqueued_ids))


if __name__ == "__main__":
    main()
