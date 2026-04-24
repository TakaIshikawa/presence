#!/usr/bin/env python3
"""Generate campaign pacing forecasts."""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.campaign_forecast import CampaignForecastReport, CampaignForecaster
from runner import script_context


def format_json_report(report: CampaignForecastReport) -> str:
    """Format a campaign forecast as JSON."""
    return json.dumps(report.to_dict(), indent=2)


def format_text_report(report: CampaignForecastReport) -> str:
    """Format a campaign forecast for terminal or Markdown output."""
    if not report.campaigns:
        return "No active campaign data found."

    lines = [
        "",
        "# Campaign Forecast",
        "",
        f"Generated: {report.generated_at}",
        f"Lookback: last {report.days} days",
        "",
    ]
    for forecast in report.campaigns:
        campaign = forecast.campaign
        rec = forecast.recommendation
        lines.extend(
            [
                f"## {campaign['name']} (ID {campaign['id']})",
                "",
                f"- Status: {campaign.get('status') or 'n/a'}",
                f"- Window: {campaign.get('start_date') or 'n/a'} to {campaign.get('end_date') or 'n/a'}",
                f"- Planned: {forecast.planned_count}",
                f"- Generated: {forecast.generated_count}",
                f"- Queued: {forecast.queued_count}",
                f"- Overdue: {forecast.overdue_count}",
                f"- Remaining: {forecast.remaining_count}",
                f"- Days remaining: {_display_value(forecast.days_remaining)}",
                f"- Estimated generation rate: {forecast.estimated_generation_rate:.3f}/day",
                f"- Required generation rate: {forecast.required_generation_rate:.3f}/day",
                f"- Miss risk: {forecast.miss_risk}",
                "",
                "Recommendation:",
                f"- Planned topic ID: {_display_value(rec.planned_topic_id)}",
                f"- Topic: {_display_value(rec.topic)}",
                f"- Angle: {_display_value(rec.angle)}",
                f"- Target date: {_display_value(rec.target_date)}",
                f"- Content type: {rec.content_type}",
                f"- Reason: {rec.reason}",
                "",
            ]
        )
        if forecast.overdue_topics:
            lines.append("Overdue Topics:")
            for topic in forecast.overdue_topics:
                lines.append(
                    f"- #{topic['planned_topic_id']} {topic['topic']} "
                    f"({topic.get('target_date') or 'n/a'})"
                )
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _display_value(value) -> str:
    return "n/a" if value is None else str(value)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Forecast active campaign pacing against planned topics"
    )
    parser.add_argument(
        "--campaign-id",
        type=int,
        help="Limit report to one campaign ID",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Generation cadence lookback in days (default: 14)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    parser.add_argument(
        "--output",
        help="Write report to a file instead of stdout",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        forecaster = CampaignForecaster(db)
        try:
            report = forecaster.forecast(
                campaign_id=args.campaign_id,
                days=args.days,
            )
        except ValueError as exc:
            parser.exit(2, f"error: {exc}\n")

    body = format_json_report(report) if args.json else format_text_report(report)
    if args.output:
        Path(args.output).write_text(body + ("" if body.endswith("\n") else "\n"), encoding="utf-8")
    else:
        print(body, end="" if body.endswith("\n") else "\n")


if __name__ == "__main__":
    main()
