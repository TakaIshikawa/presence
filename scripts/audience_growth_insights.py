#!/usr/bin/env python3
"""Generate weekly audience growth insights."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.audience_growth_insights import (
    AudienceGrowthInsights,
    AudienceGrowthInsightsReport,
)
from runner import script_context
from storage.db import Database


def format_json_report(report: AudienceGrowthInsightsReport) -> str:
    """Format an audience growth insights report as JSON."""
    data = asdict(report)
    data["period_start"] = report.period_start.isoformat()
    data["period_end"] = report.period_end.isoformat()
    for windows in data["platforms"].values():
        for window in windows:
            window["week_start"] = window["week_start"].isoformat()
            window["week_end"] = window["week_end"].isoformat()
            for post in window["top_posts"]:
                post["published_at"] = post["published_at"].isoformat()
    for quiet_period in data["quiet_periods"]:
        quiet_period["week_start"] = quiet_period["week_start"].isoformat()
        quiet_period["week_end"] = quiet_period["week_end"].isoformat()
    return json.dumps(data, indent=2)


def format_text_report(report: AudienceGrowthInsightsReport) -> str:
    """Format an audience growth insights report for terminal reading."""
    lines = [
        "",
        "=" * 70,
        f"Audience Growth Insights (last {report.weeks} weeks)",
        "=" * 70,
        "",
        f"Period: {report.period_start.strftime('%Y-%m-%d')} to {report.period_end.strftime('%Y-%m-%d')}",
        "",
    ]
    for platform, windows in report.platforms.items():
        lines.append(platform.upper())
        lines.append("-" * len(platform))
        for window in windows:
            delta = _format_delta(window.follower_delta)
            growth_rate = _format_pct(window.growth_rate_pct)
            lines.append(
                f"{window.week_start.strftime('%Y-%m-%d')} to "
                f"{window.week_end.strftime('%Y-%m-%d')}: "
                f"followers {window.start_followers or 'n/a'} -> "
                f"{window.end_followers or 'n/a'} ({delta}, {growth_rate}); "
                f"posts {window.published_count}; "
                f"engagement {window.total_engagement_score:.2f}"
            )
            for post in window.top_posts:
                ratio = (
                    f"{post.engagement_to_growth_ratio:.2f}"
                    if post.engagement_to_growth_ratio is not None
                    else "n/a"
                )
                lines.append(
                    f"  - #{post.content_id} score {post.engagement_score:.2f}, "
                    f"engagement/growth {ratio}: {post.content_preview}"
                )
        lines.append("")

    if report.quiet_periods:
        lines.append("QUIET PERIODS")
        lines.append("-------------")
        for period in report.quiet_periods:
            lines.append(
                f"{period.platform.upper()} {period.week_start.strftime('%Y-%m-%d')} "
                f"to {period.week_end.strftime('%Y-%m-%d')}: "
                f"{_format_delta(period.follower_delta)}, "
                f"{period.published_count} posts. {period.reason}"
            )
        lines.append("")

    lines.append("=" * 70)
    return "\n".join(lines)


def write_output(rendered: str, output_path: str | None) -> None:
    """Write rendered report to a file or stdout."""
    if output_path:
        Path(output_path).write_text(rendered + "\n")
    else:
        print(rendered)


def build_report(db: Database, weeks: int, platform: str) -> AudienceGrowthInsightsReport:
    """Build a report with production defaults."""
    return AudienceGrowthInsights(db).generate(weeks=weeks, platform=platform)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Explain weekly audience growth using profile metrics and published content"
    )
    parser.add_argument("--db", help="Path to SQLite database; defaults to configured runtime DB")
    parser.add_argument("--weeks", type=int, default=4, help="Number of weekly windows to report")
    parser.add_argument(
        "--platform",
        default="all",
        choices=["all", "x", "bluesky"],
        help="Platform to report (default: all)",
    )
    parser.add_argument("--output", help="Write output to this path instead of stdout")
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.db:
        db = Database(args.db)
        db.connect()
        try:
            report = build_report(db, args.weeks, args.platform)
        finally:
            db.close()
    else:
        with script_context() as (config, db):
            report = build_report(db, args.weeks, args.platform)

    rendered = format_json_report(report) if args.json else format_text_report(report)
    write_output(rendered, args.output)


def _format_delta(value: int | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+d}"


def _format_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.2f}%"


if __name__ == "__main__":
    main()
