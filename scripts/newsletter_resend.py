#!/usr/bin/env python3
"""Report newsletter resend and subject retest candidates."""

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_resend import (  # noqa: E402
    NewsletterResendFinder,
    NewsletterResendReport,
)
from runner import script_context  # noqa: E402


def _format_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def format_text_report(report: NewsletterResendReport) -> str:
    """Format resend candidates as concise human-readable text."""
    lines = [
        f"Newsletter resend candidates (last {report.period_days} days)",
        (
            f"resend {report.resend_count}, "
            f"subject_retest {report.subject_retest_count}, "
            f"no_action {report.no_action_count}"
        ),
    ]
    if not report.rows:
        lines.append("No recent newsletter-like content found.")
        return "\n".join(lines)

    for row in report.rows:
        content_label = row.content_id if row.content_id is not None else "n/a"
        subject = row.subject or "Untitled"
        timestamp = row.sent_at or row.published_at or "n/a"
        lines.append(
            f"- {row.recommendation}: content {content_label} | {subject} | "
            f"{timestamp} | opens {_format_rate(row.open_rate)} | "
            f"clicks {_format_rate(row.click_rate)}"
        )
        if row.newsletter_send_id is not None:
            lines.append(
                f"  send {row.newsletter_send_id}"
                f"{' issue ' + row.issue_id if row.issue_id else ''}"
            )
        lines.append(f"  reasons: {'; '.join(row.reasons)}")
    return "\n".join(lines)


def format_json_report(report: NewsletterResendReport) -> str:
    """Format resend candidates as stable JSON."""
    payload = asdict(report)
    payload["resend_count"] = report.resend_count
    payload["subject_retest_count"] = report.subject_retest_count
    payload["no_action_count"] = report.no_action_count
    return json.dumps(payload, indent=2, sort_keys=True)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find newsletter resend and subject retest candidates."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days to look back (default: 90)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit stable JSON instead of text.",
    )
    parser.add_argument(
        "--min-open-rate",
        type=float,
        default=0.40,
        help="Minimum open rate for resend candidates (default: 0.40)",
    )
    parser.add_argument(
        "--max-click-rate",
        type=float,
        default=0.04,
        help="Maximum click rate considered weak follow-through (default: 0.04)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum rows to emit (default: 20)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv or [])
    with script_context() as (config, db):
        report = NewsletterResendFinder(db).find(
            days=args.days,
            min_open_rate=args.min_open_rate,
            max_click_rate=args.max_click_rate,
            limit=args.limit,
        )

    if args.json:
        print(format_json_report(report))
    else:
        print(format_text_report(report))


if __name__ == "__main__":
    main(sys.argv[1:])
