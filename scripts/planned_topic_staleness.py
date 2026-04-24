#!/usr/bin/env python3
"""Scan planned topics that are stale before generation."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.planned_topic_staleness import (
    mark_stale_topics_skipped,
    scan_planned_topic_staleness,
    staleness_to_dict,
)


def format_json_report(stale_topics, updates: list[dict] | None = None) -> str:
    payload = {"stale_topics": staleness_to_dict(stale_topics)}
    if updates is not None:
        payload["updates"] = updates
    return json.dumps(payload, indent=2, sort_keys=True)


def format_text_report(stale_topics, updates: list[dict] | None = None) -> str:
    lines = [
        "",
        "=" * 70,
        "Planned Topic Staleness",
        "=" * 70,
        "",
        f"Stale planned topics: {len(stale_topics)}",
    ]
    if stale_topics:
        for item in stale_topics:
            days = "n/a" if item.days_overdue is None else str(item.days_overdue)
            campaign = f", campaign #{item.campaign_id}" if item.campaign_id is not None else ""
            lines.append(
                f"- #{item.topic_id}{campaign}: {item.classification}, "
                f"{days} day(s) overdue, recommend {item.recommendation}. {item.reason}"
            )
    else:
        lines.append("- none")

    if updates is not None:
        lines.append("")
        lines.append("Updates")
        if updates:
            for update in updates:
                status = "skipped" if update["updated"] else "unchanged"
                lines.append(f"- #{update['topic_id']}: {status}. {update['reason']}")
        else:
            lines.append("- none")

    lines.extend(["", "=" * 70])
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days-overdue",
        type=int,
        default=0,
        help="Minimum whole days past target date before classifying as overdue (default: 0)",
    )
    parser.add_argument(
        "--campaign-id",
        type=int,
        help="Only scan planned topics for this campaign ID",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    parser.add_argument(
        "--mark-skipped",
        action="store_true",
        help="Mark returned stale planned topics as skipped",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.days_overdue < 0:
        raise SystemExit("--days-overdue must be non-negative")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        stale_topics = scan_planned_topic_staleness(
            db,
            days_overdue=args.days_overdue,
            campaign_id=args.campaign_id,
        )
        updates = mark_stale_topics_skipped(db, stale_topics) if args.mark_skipped else None

    if args.json:
        print(format_json_report(stale_topics, updates))
    else:
        print(format_text_report(stale_topics, updates))


if __name__ == "__main__":
    main()
