#!/usr/bin/env python3
"""Export pending reply drafts as portable review packets."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.reply_review_packet import (  # noqa: E402
    build_reply_review_packets,
    format_reply_packet_summary,
    write_reply_review_packets,
)
from runner import script_context  # noqa: E402


SUPPORTED_PLATFORMS = {"x", "bluesky"}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7, help="Pending draft lookback window")
    parser.add_argument("--platform", choices=sorted(SUPPORTED_PLATFORMS), help="Limit by platform")
    parser.add_argument("--draft-id", type=int, help="Export a single reply_queue draft id")
    parser.add_argument("--json", action="store_true", help="Print full packet JSON instead of a summary")
    parser.add_argument("--output-dir", type=Path, help="Write one JSON file per draft")
    return parser.parse_args(argv)


def list_pending_reply_drafts(
    db: Any,
    *,
    days: int = 7,
    platform: str | None = None,
    draft_id: int | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """List pending reply drafts for packet export."""
    if days <= 0:
        raise ValueError("days must be positive")
    if platform is not None and platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"platform must be one of: {', '.join(sorted(SUPPORTED_PLATFORMS))}")

    filters = ["status = 'pending'"]
    params: list[Any] = []
    if draft_id is not None:
        filters.append("id = ?")
        params.append(draft_id)
    else:
        cutoff = (_normalize_now(now) - timedelta(days=days)).isoformat()
        filters.append("detected_at IS NOT NULL")
        filters.append("datetime(detected_at) >= datetime(?)")
        params.append(cutoff)
    if platform:
        filters.append("platform = ?")
        params.append(platform)

    rows = db.conn.execute(
        f"""SELECT *
            FROM reply_queue
            WHERE {' AND '.join(filters)}
            ORDER BY platform ASC, id ASC""",
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            replies = list_pending_reply_drafts(
                db,
                days=args.days,
                platform=args.platform,
                draft_id=args.draft_id,
            )
            packets = build_reply_review_packets(replies)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.output_dir:
        paths = write_reply_review_packets(packets, args.output_dir)
        print(f"Wrote {len(paths)} reply review packet{'s' if len(paths) != 1 else ''}.")
        for path in paths:
            print(path)
        return 0

    if args.json:
        print(json.dumps(packets, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
    else:
        print(format_reply_packet_summary(packets), end="")
    return 0


def _normalize_now(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now


if __name__ == "__main__":
    raise SystemExit(main())
