#!/usr/bin/env python3
"""Report newsletter CTA performance from Buttondown metrics."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_cta_performance import (  # noqa: E402
    NewsletterCtaPerformance,
    NewsletterCtaPerformanceReport,
)
from runner import script_context  # noqa: E402


def format_json_report(report: NewsletterCtaPerformanceReport) -> str:
    """Format CTA performance as stable JSON."""
    return json.dumps(asdict(report), indent=2, sort_keys=True)


def format_text_report(report: NewsletterCtaPerformanceReport) -> str:
    """Format CTA performance as concise text."""
    lines = [
        "",
        "=" * 70,
        f"Newsletter CTA Performance (last {report.period_days} days)",
        "=" * 70,
        "",
        (
            f"Sends: {report.included_sends}/{report.total_sends} included; "
            f"{report.unknown_sends} unknown CTA; "
            f"minimum {report.min_sends} send(s)"
        ),
        "",
    ]
    if not report.ctas:
        lines.append("No CTA buckets met the minimum send count.")
        return "\n".join(lines).rstrip()

    for index, cta in enumerate(report.ctas, start=1):
        lines.append(
            f"{index}. {cta.cta_id}: {cta.sends} sends, "
            f"opens {_format_rate(cta.open_rate)}, "
            f"clicks {_format_rate(cta.click_rate)}, "
            f"unsubscribes {_format_rate(cta.unsubscribe_rate)}"
        )
        lines.append(
            f"   Totals: {cta.opens} opens, {cta.clicks} clicks, "
            f"{cta.link_clicks} link clicks, {cta.unsubscribes} unsubscribes"
        )
        if cta.best_examples:
            best = cta.best_examples[0]
            lines.append(
                f"   Best: {best.issue_id or best.newsletter_send_id} "
                f"({best.subject or 'untitled'}), "
                f"clicks {_format_rate(best.click_rate)}, "
                f"opens {_format_rate(best.open_rate)}"
            )
        if cta.worst_examples:
            worst = cta.worst_examples[0]
            lines.append(
                f"   Worst: {worst.issue_id or worst.newsletter_send_id} "
                f"({worst.subject or 'untitled'}), "
                f"clicks {_format_rate(worst.click_rate)}, "
                f"opens {_format_rate(worst.open_rate)}"
            )
        lines.append("")

    return "\n".join(lines).rstrip()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days to look back by newsletter sent_at (default: 90)",
    )
    parser.add_argument(
        "--min-sends",
        type=int,
        default=1,
        help="Minimum sends required for a CTA bucket (default: 1)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output stable JSON instead of text",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum CTA buckets and examples to include (default: 10)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    with script_context() as (_config, db):
        report = NewsletterCtaPerformance(db).summarize(
            days=args.days,
            min_sends=args.min_sends,
            limit=args.limit,
        )

    if args.json:
        print(format_json_report(report))
    else:
        print(format_text_report(report))
    return 0


def _format_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


if __name__ == "__main__":
    raise SystemExit(main())
