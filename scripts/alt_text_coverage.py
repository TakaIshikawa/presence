#!/usr/bin/env python3
"""Audit generated visual post alt-text coverage."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.alt_text_coverage import audit_alt_text_coverage

logger = logging.getLogger(__name__)


def fetch_visual_content_rows(db, *, days: int) -> list[dict]:
    """Fetch generated visual content rows within the created_at lookback."""
    rows = db.conn.execute(
        """SELECT id AS content_id, image_path, content_type, created_at,
                  content, image_prompt, image_alt_text
           FROM generated_content
           WHERE (image_path IS NOT NULL OR content_type = 'x_visual')
             AND created_at >= datetime('now', ?)
           ORDER BY created_at DESC, id DESC""",
        (f"-{days} days",),
    ).fetchall()
    return [dict(row) for row in rows]


def format_json_report(report, *, days: int, min_length: int) -> str:
    payload = report.as_dict()
    payload["days"] = days
    payload["min_length"] = min_length
    return json.dumps(payload, indent=2)


def format_text_report(report, *, days: int, min_length: int) -> str:
    totals = report.as_dict()["totals"]
    lines = [
        "",
        "=" * 78,
        f"Alt Text Coverage Report (last {days} days)",
        "=" * 78,
        "",
        f"Minimum length: {min_length}",
        f"Total visual posts: {totals['total']}",
        f"OK: {totals['ok']}",
        f"Missing: {totals['missing']}",
        f"Too short: {totals['too_short']}",
        f"Duplicate content: {totals['duplicate_content']}",
        f"Low quality: {totals['low_quality']}",
    ]
    if report.items:
        lines.extend(
            [
                "",
                f"{'ID':>5s} {'Status':18s} {'Type':12s} {'Created':19s} Issues",
                f"{'-' * 5:>5s} {'-' * 18:18s} {'-' * 12:12s} {'-' * 19:19s} {'-' * 24}",
            ]
        )
        for item in report.items:
            issue_text = ", ".join(item.issue_codes) or "ok"
            lines.append(
                f"{str(item.content_id or ''):>5s} "
                f"{item.status[:18]:18s} "
                f"{str(item.content_type or '')[:12]:12s} "
                f"{str(item.created_at or '')[:19]:19s} "
                f"{issue_text}"
            )
    lines.extend(["", "=" * 78, ""])
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to look back using generated_content.created_at.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a JSON report instead of human-readable text.",
    )
    parser.add_argument(
        "--min-length",
        type=int,
        default=20,
        help="Minimum alt text character length before reporting too_short.",
    )
    parser.add_argument(
        "--include-ok",
        action="store_true",
        help="Include rows with ok alt text in per-item output.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        rows = fetch_visual_content_rows(db, days=args.days)
        report = audit_alt_text_coverage(
            rows,
            min_length=args.min_length,
            include_ok=args.include_ok,
        )
        if args.json:
            print(format_json_report(report, days=args.days, min_length=args.min_length))
        else:
            print(format_text_report(report, days=args.days, min_length=args.min_length))


if __name__ == "__main__":
    main()
