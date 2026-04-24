#!/usr/bin/env python3
"""Report reply outcome conversion by platform, intent, and priority."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.reply_outcomes import (  # noqa: E402
    build_reply_outcome_report,
    format_reply_outcome_json,
    format_reply_outcome_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Look back this many days by detected_at (default: 30)",
    )
    parser.add_argument(
        "--platform",
        choices=["x", "bluesky"],
        help="Restrict to one platform.",
    )
    parser.add_argument(
        "--intent",
        help="Restrict to one reply intent.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON.",
    )
    return parser.parse_args(argv)


def fetch_reply_rows(db, *, days: int, platform: str | None, intent: str | None) -> list[dict[str, Any]]:
    clauses = [
        "detected_at IS NOT NULL",
        "datetime(detected_at) >= datetime('now', ?)",
    ]
    params: list[Any] = [f"-{days} days"]
    if platform:
        clauses.append("platform = ?")
        params.append(platform)
    if intent:
        clauses.append("intent = ?")
        params.append(intent)

    cursor = db.conn.execute(
        f"""SELECT *
            FROM reply_queue
            WHERE {' AND '.join(clauses)}
            ORDER BY platform ASC, intent ASC, priority ASC, status ASC, datetime(detected_at) ASC, id ASC""",
        params,
    )
    return [dict(row) for row in cursor.fetchall()]


def fetch_reply_review_events(db, reply_ids: list[int]) -> list[dict[str, Any]]:
    if not reply_ids:
        return []
    placeholders = ", ".join("?" for _ in reply_ids)
    cursor = db.conn.execute(
        f"""SELECT *
            FROM reply_review_events
            WHERE reply_queue_id IN ({placeholders})
            ORDER BY reply_queue_id ASC, datetime(created_at) ASC, id ASC""",
        reply_ids,
    )
    return [dict(row) for row in cursor.fetchall()]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.days <= 0:
        raise ValueError("--days must be positive")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        rows = fetch_reply_rows(
            db,
            days=args.days,
            platform=args.platform,
            intent=args.intent,
        )
        events = fetch_reply_review_events(db, [int(row["id"]) for row in rows])
        report = build_reply_outcome_report(
            rows,
            events,
            days=args.days,
            platform=args.platform,
            intent=args.intent,
        )

    if args.json:
        print(format_reply_outcome_json(report))
    else:
        print(format_reply_outcome_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
