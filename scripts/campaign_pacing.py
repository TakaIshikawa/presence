#!/usr/bin/env python3
"""Report campaign pacing and recommended next actions."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.campaign_pacing import CampaignPacingAnalyzer, CampaignPacingReport
from runner import script_context


def format_json_report(report: CampaignPacingReport | None) -> str:
    """Format a pacing report as machine-readable JSON."""
    if report is None:
        return json.dumps({"error": "No campaign data found"}, indent=2)
    return json.dumps(asdict(report), indent=2)


def format_text_report(report: CampaignPacingReport | None) -> str:
    """Format a pacing report for humans."""
    if report is None:
        return "No campaign data found."

    campaign = report.campaign
    expected = (
        "n/a"
        if report.expected_progress is None
        else f"{report.expected_progress * 100:.1f}%"
    )
    actual = f"{report.actual_progress * 100:.1f}%"
    lines = [
        "",
        "=" * 70,
        f"Campaign Pacing: {campaign['name']}",
        "=" * 70,
        "",
        f"Campaign ID:       {campaign['id']}",
        f"Status:            {report.status}",
        f"Window:            {campaign.get('start_date') or 'n/a'} to {campaign.get('end_date') or 'n/a'}",
        f"Expected progress: {expected}",
        f"Actual progress:   {actual}",
        f"Planned topics:    {report.planned_topics}",
        f"Published items:   {report.published_items}",
        f"Scheduled items:   {len(report.scheduled_items)}",
        f"Ready unscheduled: {report.generated_unscheduled}",
        "",
        "Recommendations:",
    ]
    for recommendation in report.recommendations:
        lines.append(
            f"  - {recommendation['action']}: {recommendation['reason']}"
        )

    lines.append("")
    lines.append("Remaining Topics:")
    if report.remaining_topics:
        for topic in report.remaining_topics:
            generated = "generated" if topic["generated"] else "not generated"
            angle = f" ({topic['angle']})" if topic.get("angle") else ""
            lines.append(
                f"  - #{topic['planned_topic_id']} {topic['topic']}{angle}: "
                f"{generated}, target {topic.get('target_date') or 'n/a'}"
            )
    else:
        lines.append("  - none")

    lines.append("")
    lines.append("Scheduled Items:")
    if report.scheduled_items:
        for item in report.scheduled_items:
            lines.append(
                f"  - queue #{item['queue_id']} content #{item['content_id']} "
                f"{item['platform']} at {item['scheduled_at']} "
                f"for {item['topic']}"
            )
    else:
        lines.append("  - none")

    lines.append("=" * 70)
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report active or selected campaign pacing"
    )
    parser.add_argument(
        "--campaign-id",
        type=int,
        help="Content campaign ID to report. Defaults to the active campaign.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Shortcut for --format json",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    output_format = "json" if args.json else args.format
    with script_context() as (_config, db):
        report = CampaignPacingAnalyzer(db).report(
            campaign_id=args.campaign_id,
            active=args.campaign_id is None,
        )
        if output_format == "json":
            print(format_json_report(report))
        else:
            print(format_text_report(report))


if __name__ == "__main__":
    main()
