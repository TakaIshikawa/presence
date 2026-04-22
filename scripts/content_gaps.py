#!/usr/bin/env python3
"""Report content gaps across plans, generated topics, and source activity."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.content_gaps import ContentGapDetector, ContentGapReport, report_to_dict


def _shorten(text: str | None, width: int = 86) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)] + "..."


def format_json_report(report: ContentGapReport) -> str:
    return json.dumps(report_to_dict(report), indent=2)


def format_text_report(report: ContentGapReport) -> str:
    lines = [
        "",
        "=" * 70,
        "Content Gap Report",
        "=" * 70,
        "",
        f"Window:      {report.period_start} to {report.period_end}",
        f"Lookback:    last {report.days} days",
    ]
    if report.campaign_id is not None:
        lines.append(f"Campaign ID: {report.campaign_id}")
    lines.append("")

    lines.append(f"Planned Topic Gaps ({len(report.planned_gaps)})")
    if report.planned_gaps:
        for gap in report.planned_gaps:
            campaign = f" [{gap.campaign_name}]" if gap.campaign_name else ""
            nearest = gap.nearest_generated_at or "none"
            delta = f", {gap.days_from_target}d from target" if gap.days_from_target is not None else ""
            angle = f" - {_shorten(gap.angle, 60)}" if gap.angle else ""
            lines.append(
                f"- #{gap.planned_topic_id} {gap.topic}{campaign}{angle} "
                f"(target {gap.target_date or 'unscheduled'}, nearest {nearest}{delta})"
            )
    else:
        lines.append("- none")
    lines.append("")

    lines.append(f"Overused Recent Topics ({len(report.overused_topics)})")
    if report.overused_topics:
        for topic in report.overused_topics:
            lines.append(
                f"- {topic.topic}: {topic.count} generated topics "
                f"({topic.share:.0%}), latest {topic.latest_generated_at or 'n/a'}"
            )
    else:
        lines.append("- none")
    lines.append("")

    lines.append(f"Source-Rich Uncovered Topics ({len(report.source_rich_gaps)})")
    if report.source_rich_gaps:
        for gap in report.source_rich_gaps:
            lines.append(
                f"- {gap.topic}: {gap.source_count} sources "
                f"({gap.commit_count} commits, {gap.message_count} messages), "
                f"latest {gap.latest_source_at or 'n/a'}"
            )
            for example in gap.examples[:2]:
                lines.append(f"  example: {_shorten(example)}")
    else:
        lines.append("- none")

    lines.extend(["", "=" * 70])
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Lookback window in days for generated content and source activity (default: 14)",
    )
    parser.add_argument(
        "--campaign-id",
        type=int,
        help="Only check planned topic gaps for this campaign ID",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        report = ContentGapDetector(db).detect(days=args.days, campaign_id=args.campaign_id)

    if args.json:
        print(format_json_report(report))
    else:
        print(format_text_report(report))


if __name__ == "__main__":
    main()
