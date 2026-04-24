#!/usr/bin/env python3
"""Report upcoming open posting windows by platform."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.posting_schedule import embargo_windows_from_config
from evaluation.posting_window_coverage import (
    CoverageSlot,
    PostingWindowCoveragePlanner,
    coverage_slots_to_dicts,
)
from runner import script_context


def format_text_report(slots: list[CoverageSlot], *, days_ahead: int, platform: str) -> str:
    """Format coverage recommendations as a stable operator table."""
    lines = [
        "",
        "=" * 78,
        f"Posting Window Coverage (next {days_ahead} days, platform: {platform})",
        "=" * 78,
        "",
    ]
    if not slots:
        lines.append("No open posting window slots found for this selection.")
        return "\n".join(lines)

    columns = [
        ("platform", "PLATFORM", 8),
        ("scheduled_at", "SCHEDULED_UTC", 25),
        ("window", "WINDOW", 18),
        ("source", "SOURCE", 8),
        ("score", "SCORE", 7),
        ("confidence_label", "CONF", 8),
        ("sample_size", "SAMPLES", 7),
    ]
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for slot in slots:
        row = {
            "platform": slot.platform,
            "scheduled_at": slot.scheduled_at.isoformat(),
            "window": f"{slot.day_name} {slot.hour_utc:02d}:00",
            "source": slot.source,
            "score": f"{slot.score:.2f}",
            "confidence_label": slot.confidence_label,
            "sample_size": str(slot.sample_size),
        }
        lines.append("  ".join(str(row[key]).ljust(width) for key, _, width in columns))

    return "\n".join(lines)


def format_json_report(slots: list[CoverageSlot]) -> str:
    """Format coverage recommendations as JSON."""
    return json.dumps(coverage_slots_to_dicts(slots), indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--platform",
        default="all",
        choices=["all", "x", "bluesky"],
        help="Platform to plan for (default: all)",
    )
    parser.add_argument(
        "--days-ahead",
        type=int,
        default=7,
        help="Number of days ahead to inspect (default: 7)",
    )
    parser.add_argument(
        "--include-published",
        action="store_true",
        help="Treat recently published posts in the horizon as occupied slots",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        planner = PostingWindowCoveragePlanner(db)
        slots = planner.recommend_slots(
            days_ahead=args.days_ahead,
            platform=args.platform,
            include_published=args.include_published,
            embargo_windows=embargo_windows_from_config(config),
        )

    if args.json:
        print(format_json_report(slots))
    else:
        print(format_text_report(slots, days_ahead=args.days_ahead, platform=args.platform))


if __name__ == "__main__":
    main()
