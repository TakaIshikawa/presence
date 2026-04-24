#!/usr/bin/env python3
"""Recommend review actions for stored inbound mention reply drafts."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_action_recommender import (  # noqa: E402
    ACTION_ORDER,
    ReplyActionRecommendation,
    ReplyActionRecommender,
    group_recommendations,
    recommendations_to_dict,
)
from runner import script_context  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", choices=["x", "bluesky"], help="Filter to a single platform.")
    parser.add_argument("--status", default="pending", help="Filter by reply queue status (default: pending).")
    parser.add_argument("--days", type=int, help="Only include mentions detected in the last N days.")
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON.")
    parser.add_argument("--limit", type=int, help="Maximum rows to evaluate.")
    return parser


def fetch_reply_rows(
    db,
    *,
    platform: str | None = None,
    status: str | None = "pending",
    days: int | None = None,
    limit: int | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    if days is not None and days <= 0:
        raise ValueError("--days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("--limit must be positive")

    filters: list[str] = []
    params: list[Any] = []
    if platform:
        filters.append("platform = ?")
        params.append(platform)
    if status:
        filters.append("status = ?")
        params.append(status)
    if days is not None:
        cutoff = (now or datetime.now(timezone.utc)).astimezone(timezone.utc) - timedelta(days=days)
        filters.append("detected_at IS NOT NULL AND datetime(detected_at) >= datetime(?)")
        params.append(cutoff.isoformat())

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    sql = f"""SELECT * FROM reply_queue
              {where}
              ORDER BY datetime(detected_at) ASC, id ASC"""
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    cursor = db.conn.execute(sql, params)
    return [dict(row) for row in cursor.fetchall()]


def build_payload(
    recommendations: list[ReplyActionRecommendation],
    *,
    filters: dict[str, Any],
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    items = recommendations_to_dict(recommendations)
    return {
        "generated_at": (generated_at or datetime.now(timezone.utc)).isoformat(),
        "filters": filters,
        "total": len(items),
        "by_action": dict(Counter(item["action"] for item in items)),
        "recommendations": items,
    }


def _shorten(text: str | None, width: int = 72) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def format_text_output(recommendations: list[ReplyActionRecommendation]) -> str:
    if not recommendations:
        return "No reply actions matched."

    lines: list[str] = []
    grouped = group_recommendations(recommendations)
    for action in ACTION_ORDER:
        items = grouped[action]
        if not items:
            continue
        if lines:
            lines.append("")
        lines.append(f"{action} ({len(items)})")
        lines.append("-" * len(lines[-1]))
        for item in items:
            author = f"@{item.author}" if item.author else "@unknown"
            quality = "n/a" if item.quality_score is None else f"{item.quality_score:.1f}"
            lines.append(
                f"#{item.reply_id or '-'} {item.platform:<7} {author:<18} "
                f"{item.intent:<13} q={quality:<4} {item.reason}"
            )
            lines.append(f"  mention: {_shorten(item.inbound_text, 92)}")
            if item.draft_text:
                lines.append(f"  draft:   {_shorten(item.draft_text, 92)}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    filters = {
        "platform": args.platform,
        "status": args.status,
        "days": args.days,
        "limit": args.limit,
    }
    with script_context() as (_config, db):
        rows = fetch_reply_rows(
            db,
            platform=args.platform,
            status=args.status,
            days=args.days,
            limit=args.limit,
        )
        recommendations = ReplyActionRecommender().recommend_many(rows)

    if args.json:
        print(json.dumps(build_payload(recommendations, filters=filters), indent=2, sort_keys=True))
    else:
        print(format_text_output(recommendations))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
