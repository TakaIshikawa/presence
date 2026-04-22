#!/usr/bin/env python3
"""Report pending reply SLA status and optionally dismiss stale low-priority drafts."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context

logger = logging.getLogger(__name__)

DEFAULT_STALE_MAX_AGE_HOURS = 48


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Report pending reply drafts by SLA age, priority, platform, and author."
    )
    parser.add_argument(
        "--max-age-hours",
        type=int,
        help=(
            "Only include pending replies up to this age; with --mark-stale, "
            "dismiss low-priority replies older than this threshold."
        ),
    )
    parser.add_argument(
        "--platform",
        choices=["x", "bluesky"],
        help="Filter to a single platform.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON.",
    )
    parser.add_argument(
        "--mark-stale",
        action="store_true",
        help="Dismiss low-priority pending replies older than --max-age-hours.",
    )
    return parser


def _relationship_tier(relationship_context: str | None) -> str | None:
    if not relationship_context:
        return None
    try:
        context = json.loads(relationship_context)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(context, dict):
        return None

    tier_name = context.get("tier_name")
    tier = context.get("dunbar_tier")
    if tier_name and tier is not None:
        return f"{tier_name} (tier {tier})"
    if tier_name:
        return str(tier_name)
    if tier is not None:
        return f"tier {tier}"
    return None


def _reply_item(row: dict[str, Any]) -> dict[str, Any]:
    age_hours = round(float(row.get("age_hours") or 0.0), 2)
    return {
        "id": row["id"],
        "age_hours": age_hours,
        "priority": row.get("priority") or "normal",
        "platform": row.get("platform") or "x",
        "author": row.get("inbound_author_handle"),
        "relationship_tier": _relationship_tier(row.get("relationship_context")),
        "quality_score": row.get("quality_score"),
        "intent": row.get("intent"),
        "detected_at": row.get("detected_at"),
        "inbound_tweet_id": row.get("inbound_tweet_id"),
    }


def build_report(
    rows: list[dict[str, Any]],
    *,
    max_age_hours: int | None,
    platform: str | None,
    stale_dismissed: int = 0,
) -> dict[str, Any]:
    replies = [_reply_item(row) for row in rows]
    return {
        "filters": {
            "max_age_hours": max_age_hours,
            "platform": platform,
        },
        "stale_dismissed": stale_dismissed,
        "total_pending": len(replies),
        "by_priority": dict(Counter(item["priority"] for item in replies)),
        "by_platform": dict(Counter(item["platform"] for item in replies)),
        "by_author": dict(Counter(item["author"] or "unknown" for item in replies)),
        "by_relationship_tier": dict(
            Counter(item["relationship_tier"] or "unknown" for item in replies)
        ),
        "replies": replies,
    }


def format_text_report(report: dict[str, Any]) -> str:
    lines = [
        "",
        "=" * 88,
        "Reply SLA Report",
        "=" * 88,
        "",
    ]
    filters = report["filters"]
    if filters["platform"] or filters["max_age_hours"]:
        parts = []
        if filters["platform"]:
            parts.append(f"platform={filters['platform']}")
        if filters["max_age_hours"]:
            parts.append(f"max_age_hours={filters['max_age_hours']}")
        lines.append("Filters: " + ", ".join(parts))
    lines.append(f"Pending replies: {report['total_pending']}")
    if report["stale_dismissed"]:
        lines.append(f"Stale low-priority replies dismissed: {report['stale_dismissed']}")
    lines.append("")

    lines.append("Breakdown")
    lines.append(f"  Priority: {report['by_priority']}")
    lines.append(f"  Platform: {report['by_platform']}")
    lines.append(f"  Author: {report['by_author']}")
    lines.append(f"  Relationship tier: {report['by_relationship_tier']}")
    lines.append("")

    if not report["replies"]:
        lines.append("No pending replies matched.")
        return "\n".join(lines)

    lines.append(
        f"{'Age':>8}  {'Priority':<8}  {'Platform':<8}  {'Author':<18}  "
        f"{'Tier':<24}  {'Quality':>7}"
    )
    lines.append("-" * 88)
    for item in report["replies"]:
        author = item["author"] or "unknown"
        tier = item["relationship_tier"] or "unknown"
        quality = "n/a" if item["quality_score"] is None else f"{item['quality_score']:.1f}"
        lines.append(
            f"{item['age_hours']:>7.1f}h  {item['priority']:<8}  "
            f"{item['platform']:<8}  {author[:18]:<18}  {tier[:24]:<24}  {quality:>7}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.max_age_hours is not None and args.max_age_hours <= 0:
        raise ValueError("--max-age-hours must be positive")

    logging.basicConfig(
        level=logging.WARNING if args.json else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stale_dismissed = 0
    with script_context() as (_config, db):
        if args.mark_stale:
            threshold = args.max_age_hours or DEFAULT_STALE_MAX_AGE_HOURS
            stale_dismissed = db.dismiss_stale_low_priority_replies(
                threshold,
                platform=args.platform,
            )
            logger.info("Dismissed %d stale low-priority replies.", stale_dismissed)

        rows = db.get_pending_reply_sla(
            max_age_hours=args.max_age_hours,
            platform=args.platform,
        )
        report = build_report(
            rows,
            max_age_hours=args.max_age_hours,
            platform=args.platform,
            stale_dismissed=stale_dismissed,
        )

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(format_text_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
