#!/usr/bin/env python3
"""Report newsletter link-click attribution from Buttondown metrics."""

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_link_performance import (  # noqa: E402
    NewsletterLinkPerformance,
    NewsletterLinkPerformanceReport,
)
from runner import script_context  # noqa: E402


def format_json_report(report: NewsletterLinkPerformanceReport) -> str:
    """Format link performance as stable JSON."""
    return json.dumps(asdict(report), indent=2, sort_keys=True)


def format_text_report(report: NewsletterLinkPerformanceReport) -> str:
    """Format link performance as human-readable text."""
    lines = [
        "",
        "=" * 72,
        f"Newsletter Link Performance (last {report.period_days} days)",
        "=" * 72,
        "",
    ]
    if report.issue_id:
        lines.append(f"Issue: {report.issue_id}")
    if report.content_id is not None:
        lines.append(f"Content: {report.content_id}")
    if report.issue_id or report.content_id is not None:
        lines.append("")

    lines.append(
        f"Clicks: {report.total_clicks} total, "
        f"{report.mapped_clicks} mapped, "
        f"{report.unmapped_clicks} unmapped, "
        f"{report.ambiguous_clicks} ambiguous"
    )
    lines.append(
        f"Links: {report.unmapped_link_count} unmapped, "
        f"{report.ambiguous_link_count} ambiguous"
    )
    if report.malformed_send_count:
        lines.append(f"Malformed send metadata skipped: {report.malformed_send_count}")
    lines.append("")

    if not report.ranked_urls:
        lines.append("No Buttondown link-click snapshots found.")
        return "\n".join(lines).rstrip()

    for index, item in enumerate(report.ranked_urls, start=1):
        label = item.attribution_status
        if item.content_id is not None:
            label = f"{item.section or item.content_type} content {item.content_id}"
        lines.append(f"{index}. {item.url}")
        lines.append(
            f"   {item.clicks} clicks across {item.issue_count} issue(s); {label}"
        )
        for issue in item.issues[:3]:
            lines.append(
                f"   - {issue.issue_id or 'n/a'}: {issue.clicks} clicks "
                f"({issue.sent_at or 'unknown date'})"
            )
        lines.append("")

    return "\n".join(lines).rstrip()


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Attribute Buttondown newsletter link clicks to generated content."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days to look back by newsletter sent_at (default: 90)",
    )
    parser.add_argument("--issue-id", help="Only include one Buttondown issue id")
    parser.add_argument(
        "--content-id",
        type=int,
        help="Only include clicks attributed to one generated_content id",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output stable JSON instead of text",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum ranked URLs/content rows to show (default: 20)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv or [])
    with script_context() as (_config, db):
        report = NewsletterLinkPerformance(db).summarize(
            days=args.days,
            issue_id=args.issue_id,
            content_id=args.content_id,
            limit=args.limit,
        )

    if args.json:
        print(format_json_report(report))
    else:
        print(format_text_report(report))


if __name__ == "__main__":
    main(sys.argv[1:])
