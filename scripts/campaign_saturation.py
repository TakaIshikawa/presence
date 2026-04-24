#!/usr/bin/env python3
"""Generate campaign topic saturation reports."""

import argparse
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.campaign_saturation import (  # noqa: E402
    CampaignSaturationAnalyzer,
    CampaignSaturationReport,
)
from runner import script_context  # noqa: E402


def _report_to_dict(report: CampaignSaturationReport) -> dict:
    data = asdict(report)
    data["period_start"] = report.period_start.isoformat()
    data["period_end"] = report.period_end.isoformat()
    return data


def format_json_report(report: CampaignSaturationReport | None) -> str:
    """Format a saturation report as stable monitoring JSON."""
    if report is None:
        return json.dumps({"error": "No campaign data found"}, indent=2, sort_keys=True)
    return json.dumps(_report_to_dict(report), indent=2, sort_keys=True)


def format_text_report(report: CampaignSaturationReport | None) -> str:
    """Format a saturation report for terminal review."""
    if report is None:
        return "No campaign data found."

    campaign = report.campaign
    rows = [
        "",
        "=" * 72,
        f"Campaign Saturation: {campaign['name']}",
        "=" * 72,
        "",
        f"Campaign ID:   {campaign['id']}",
        f"Status:        {campaign['status']}",
        f"Lookback:      last {report.period_days} days",
        f"Min published: {report.min_published}",
        "",
        "Topic Saturation:",
    ]
    if not report.topics:
        rows.append("  No planned topics found.")
        return "\n".join(rows)

    table = []
    for topic in report.topics:
        table.append([
            topic.topic,
            str(topic.planned_count),
            str(topic.generated_count),
            str(topic.published_count),
            topic.last_published_at or "-",
            topic.saturation_level,
            topic.recommendation,
        ])

    headers = ["Topic", "Plan", "Gen", "Pub", "Last Published", "Level", "Action"]
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in table))
        for index in range(len(headers))
    ]
    rows.append("  " + "  ".join(header.ljust(widths[i]) for i, header in enumerate(headers)))
    rows.append("  " + "  ".join("-" * width for width in widths))
    for row in table:
        rows.append("  " + "  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))
    return "\n".join(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report campaign topic saturation and over-covered topics"
    )
    parser.add_argument("--campaign-id", type=int, help="Content campaign ID to report")
    parser.add_argument(
        "--active",
        action="store_true",
        help="Report the currently active campaign",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Generated/publication lookback in days (default: 30)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    parser.add_argument(
        "--min-published",
        type=int,
        default=3,
        help="Published count before a repeated topic is saturated (default: 3)",
    )
    args = parser.parse_args()

    if args.campaign_id is not None and args.active:
        parser.error("--campaign-id and --active are mutually exclusive")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        analyzer = CampaignSaturationAnalyzer(db)
        report = analyzer.report(
            campaign_id=args.campaign_id,
            active=args.active or args.campaign_id is None,
            days=args.days,
            min_published=args.min_published,
        )
        print(format_json_report(report) if args.json else format_text_report(report))


if __name__ == "__main__":
    main()
