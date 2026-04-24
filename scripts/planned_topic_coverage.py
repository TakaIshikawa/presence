#!/usr/bin/env python3
"""Report supporting knowledge coverage for planned topics."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.planned_topic_coverage import (  # noqa: E402
    PlannedTopicCoverageReport,
    build_planned_topic_coverage_report,
)
from runner import script_context  # noqa: E402


def _shorten(text: str | None, width: int = 84) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def format_json_report(report: PlannedTopicCoverageReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_text_report(report: PlannedTopicCoverageReport) -> str:
    lines = [
        "",
        "=" * 70,
        "Planned Topic Knowledge Coverage",
        "=" * 70,
        "",
        f"Planned topics: {report.planned_topic_count}",
        f"Minimum sources: {report.min_sources}",
    ]
    if report.campaign_id is not None:
        lines.append(f"Campaign ID: {report.campaign_id}")
    lines.append("")

    sections = [
        ("Covered Topics", report.covered_topics),
        ("Weakly Covered Topics", report.weakly_covered_topics),
        ("Missing Topics", report.missing_topics),
    ]
    for title, topics in sections:
        lines.append(f"{title} ({len(topics)})")
        if not topics:
            lines.append("- none")
        for topic in topics:
            campaign = f" [{topic.campaign_name}]" if topic.campaign_name else ""
            angle = f" - {_shorten(topic.angle, 56)}" if topic.angle else ""
            lines.append(
                f"- #{topic.planned_topic_id} {topic.topic}{campaign}{angle} "
                f"({topic.source_count}/{topic.min_sources} sources)"
            )
            if topic.matched_knowledge_ids:
                lines.append(
                    "  knowledge: "
                    + ", ".join(str(item) for item in topic.matched_knowledge_ids)
                )
                lines.append(
                    "  authors: "
                    + (", ".join(topic.source_authors) if topic.source_authors else "-")
                    + " | types: "
                    + (", ".join(topic.source_types) if topic.source_types else "-")
                )
            if topic.suggested_search_terms:
                lines.append(
                    "  search: " + ", ".join(topic.suggested_search_terms)
                )
        lines.append("")

    lines.append(f"Top Matching Sources ({len(report.top_matching_sources)})")
    if not report.top_matching_sources:
        lines.append("- none")
    for source in report.top_matching_sources:
        author = source.author or "-"
        terms = ", ".join(source.matched_terms[:5]) or "-"
        lines.append(
            f"- knowledge #{source.knowledge_id}: {source.source_type} by {author} "
            f"score={source.score:g} terms={terms}"
        )

    lines.extend(["", "=" * 70])
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--campaign-id",
        type=int,
        help="Only evaluate planned topics attached to this campaign ID",
    )
    parser.add_argument(
        "--min-sources",
        type=int,
        default=2,
        help="Minimum matching knowledge sources required for coverage (default: 2)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the report to this path instead of stdout",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.min_sources < 1:
        raise SystemExit("--min-sources must be at least 1")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        report = build_planned_topic_coverage_report(
            db,
            campaign_id=args.campaign_id,
            min_sources=args.min_sources,
        )

    output = format_json_report(report) if args.json else format_text_report(report)
    if args.output:
        args.output.write_text(output + "\n")
    else:
        print(output)


if __name__ == "__main__":
    main()

