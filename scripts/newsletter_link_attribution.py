#!/usr/bin/env python3
"""Report newsletter link-click attribution to generated content."""

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_link_attribution import (  # noqa: E402
    NewsletterLinkAttribution,
    NewsletterLinkAttributionReport,
)
from runner import script_context  # noqa: E402


def format_json_report(report: NewsletterLinkAttributionReport) -> str:
    """Format link attribution as JSON."""
    return json.dumps(asdict(report), indent=2)


def format_text_report(report: NewsletterLinkAttributionReport) -> str:
    """Format link attribution as human-readable text."""
    title = f"Newsletter Link Attribution (last {report.period_days} days)"
    if report.issue_id:
        title += f" - {report.issue_id}"
    lines = [
        "",
        "=" * 70,
        title,
        "=" * 70,
        "",
    ]
    if not report.attributed_content:
        lines.append("No newsletter link clicks matched generated content yet.")
    else:
        for index, item in enumerate(report.attributed_content, start=1):
            unique = (
                f", {item.unique_clicks} unique"
                if item.unique_clicks is not None
                else ""
            )
            lines.append(
                f"{index}. Content {item.content_id} ({item.content_type}) - "
                f"{item.clicks} clicks{unique}"
            )
            lines.append(f"   Issue {item.issue_id}: {item.url or 'n/a'}")
            if item.subject:
                lines.append(f"   Subject: {item.subject}")
            lines.append("")

    if report.unmatched_links:
        lines.append("Unmatched links:")
        for item in report.unmatched_links:
            lines.append(
                f"- Issue {item.issue_id}: {item.url} "
                f"({item.clicks} clicks)"
            )

    return "\n".join(lines).rstrip()


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Attribute Buttondown newsletter link clicks to generated content."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days to look back (default: 90)",
    )
    parser.add_argument(
        "--issue-id",
        help="Only report one Buttondown issue ID.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON instead of text.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv or [])
    with script_context() as (config, db):
        report = NewsletterLinkAttribution(db).summarize(
            days=args.days,
            issue_id=args.issue_id,
        )

    if args.json:
        print(format_json_report(report))
    else:
        print(format_text_report(report))


if __name__ == "__main__":
    main(sys.argv[1:])
