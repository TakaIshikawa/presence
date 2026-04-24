#!/usr/bin/env python3
"""Recommend reply drafts that need reviewer attention."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_escalation import (  # noqa: E402
    recommendations_to_jsonable,
    recommend_reply_escalations,
)
from runner import script_context  # noqa: E402


DEFAULT_MIN_AGE_HOURS = 6.0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Recommend pending reply drafts that need review, revision, or dismissal."
    )
    parser.add_argument(
        "--min-age-hours",
        type=float,
        default=DEFAULT_MIN_AGE_HOURS,
        help="Recommend otherwise healthy drafts for review after this many hours.",
    )
    parser.add_argument(
        "--include-low-priority",
        action="store_true",
        help="Include low-priority pending drafts in the recommendation queue.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output stable machine-readable JSON.",
    )
    return parser


def format_text_report(payload: dict) -> str:
    lines = [
        "",
        "=" * 88,
        "Reply Escalation Recommendations",
        "=" * 88,
        "",
        f"Drafts: {payload['total']}",
        "",
    ]
    if not payload["drafts"]:
        lines.append("No pending reply drafts matched.")
        return "\n".join(lines)

    lines.append(f"{'ID':>5}  {'Age':>8}  {'Action':<10}  {'Target':<18}  Reasons")
    lines.append("-" * 88)
    for item in payload["drafts"]:
        target = item["target"] or "unknown"
        reasons = "; ".join(item["reasons"])
        lines.append(
            f"{item['draft_id']:>5}  {item['age_hours']:>7.1f}h  "
            f"{item['recommendation']:<10}  {target[:18]:<18}  {reasons}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.min_age_hours < 0:
        raise ValueError("--min-age-hours must be non-negative")

    logging.basicConfig(
        level=logging.WARNING if args.json else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        rows = db.get_pending_reply_sla()
        recommendations = recommend_reply_escalations(
            rows,
            min_age_hours=args.min_age_hours,
            include_low_priority=args.include_low_priority,
        )
        payload = recommendations_to_jsonable(
            recommendations,
            min_age_hours=args.min_age_hours,
            include_low_priority=args.include_low_priority,
        )

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(format_text_report(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
