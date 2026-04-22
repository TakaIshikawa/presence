#!/usr/bin/env python3
"""Report newsletter subject performance from Buttondown metrics."""

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_subject_performance import (  # noqa: E402
    NewsletterSubjectPerformance,
    NewsletterSubjectPerformanceSummary,
)
from runner import script_context  # noqa: E402


def _format_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def format_text_report(summary: NewsletterSubjectPerformanceSummary) -> str:
    """Format subject performance as human-readable text."""
    lines = [
        "",
        "=" * 70,
        f"Newsletter Subject Performance (last {summary.period_days} days)",
        "=" * 70,
        "",
    ]
    if not summary.ranked_subjects:
        lines.append("No selected subjects with fetched Buttondown metrics yet.")
        return "\n".join(lines)

    lines.append(
        "Average rates: "
        f"open {_format_rate(summary.average_open_rate)}, "
        f"click {_format_rate(summary.average_click_rate)}"
    )
    lines.append("")

    for index, subject in enumerate(summary.ranked_subjects, start=1):
        lines.append(
            f"{index}. {subject.subject} "
            f"(score {subject.performance_score:.2f}, "
            f"opens {_format_rate(subject.open_rate)}, "
            f"clicks {_format_rate(subject.click_rate)})"
        )
        lines.append(
            f"   Issue {subject.issue_id or 'n/a'}: "
            f"{subject.opens}/{subject.subscriber_count} opens, "
            f"{subject.clicks}/{subject.subscriber_count} clicks"
        )
        if subject.alternatives:
            lines.append("   Alternatives:")
            for alternative in subject.alternatives:
                lines.append(
                    f"   - {alternative.subject} "
                    f"(candidate {alternative.candidate_score:.2f}, "
                    f"rank {alternative.rank or 'n/a'})"
                )
        else:
            lines.append("   Alternatives: none recorded")
        lines.append("")

    return "\n".join(lines).rstrip()


def format_json_report(summary: NewsletterSubjectPerformanceSummary) -> str:
    """Format subject performance as JSON."""
    return json.dumps(asdict(summary), indent=2)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rank selected newsletter subjects using fetched Buttondown metrics."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days to look back (default: 90)",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv or [])
    with script_context() as (config, db):
        summary = NewsletterSubjectPerformance(db).summarize(days=args.days)

    if args.format == "json":
        print(format_json_report(summary))
    else:
        print(format_text_report(summary))


if __name__ == "__main__":
    main(sys.argv[1:])
