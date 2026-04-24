#!/usr/bin/env python3
"""Report newsletter link click opportunities from Buttondown metrics."""

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_link_opportunities import (  # noqa: E402
    NewsletterLinkOpportunitySummary,
    NewsletterLinkOpportunityAnalyzer,
)
from runner import script_context  # noqa: E402


def _format_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def format_text_report(summary: NewsletterLinkOpportunitySummary) -> str:
    """Format newsletter link opportunities as human-readable text."""
    lines = [
        "",
        "=" * 70,
        f"Newsletter Link Opportunities (last {summary.period_days} days)",
        "=" * 70,
        f"Minimum clicks: {summary.min_clicks}",
        "",
    ]
    if not summary.opportunities:
        lines.append("No newsletter links met the reporting threshold.")
        return "\n".join(lines)

    for index, item in enumerate(summary.opportunities, start=1):
        title = item.title or item.url
        lines.append(
            f"{index}. {title} "
            f"(score {item.score:.2f}, {item.clicks} clicks, CTR {_format_rate(item.ctr)})"
        )
        lines.append(f"   Send {item.newsletter_send_id} / {item.issue_id or 'n/a'}")
        lines.append(f"   URL: {item.url}")
        lines.append(
            "   Components: "
            + ", ".join(
                f"{name} {value:.2f}" for name, value in item.score_components.items()
            )
        )
        lines.append(f"   Angle: {item.suggested_follow_up_angle}")
        lines.append("")

    return "\n".join(lines).rstrip()


def format_json_report(summary: NewsletterLinkOpportunitySummary) -> str:
    """Format newsletter link opportunities as JSON."""
    return json.dumps(asdict(summary), indent=2)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rank clicked newsletter links that deserve follow-up posts."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days to look back (default: 90)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of opportunities to show (default: 20)",
    )
    parser.add_argument(
        "--min-clicks",
        type=int,
        default=1,
        help="Exclude links below this click count (default: 1)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON instead of text",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv or [])
    with script_context() as (config, db):
        summary = NewsletterLinkOpportunityAnalyzer(db).summarize(
            days=args.days,
            limit=args.limit,
            min_clicks=args.min_clicks,
        )

    if args.json:
        print(format_json_report(summary))
    else:
        print(format_text_report(summary))


if __name__ == "__main__":
    main(sys.argv[1:])
