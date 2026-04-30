#!/usr/bin/env python3
"""Report curated knowledge sources with repeated text overlap."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_overlap import (
    DEFAULT_DAYS,
    DEFAULT_MIN_OVERLAP,
    SourceOverlapReport,
    build_source_overlap_report,
)
from runner import script_context


logger = logging.getLogger(__name__)


def format_json_report(report: SourceOverlapReport) -> str:
    return json.dumps(report.to_dict(), indent=2, default=str)


def format_text_report(report: SourceOverlapReport) -> str:
    lines = [
        "Knowledge Source Overlap Report",
        f"Window: last {report.days} days",
        f"Minimum overlaps: {report.min_overlap}",
        f"Similarity threshold: {report.similarity_threshold:g}",
        f"Rows scanned: {report.row_count}",
        f"Sources scanned: {report.source_count}",
        f"Overlapping pairs: {report.pair_count}",
    ]
    if report.include_restricted:
        lines.append("Filter: approved curated rows, including restricted licenses")
    else:
        lines.append("Filter: approved curated rows, excluding restricted licenses")

    if not report.pairs:
        lines.append("")
        lines.append("No overlapping curated source pairs found.")
        return "\n".join(lines)

    for index, pair in enumerate(report.pairs, start=1):
        lines.append("")
        lines.append(
            f"{index}. {pair.left_source.label} <> {pair.right_source.label} "
            f"overlaps={pair.overlap_count} avg_similarity={pair.average_similarity:.3f}"
        )
        lines.append(
            "   representative_item_ids="
            f"{', '.join(str(item_id) for item_id in pair.representative_item_ids)}"
        )
        lines.append(f"   suggested_action={pair.suggested_action}")
        sample_matches = pair.matches[:3]
        match_text = ", ".join(
            f"{match.left_id}:{match.right_id}@{match.similarity:.3f}"
            for match in sample_matches
        )
        lines.append(f"   matches={match_text}")

    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of knowledge days to inspect (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--min-overlap",
        type=int,
        default=DEFAULT_MIN_OVERLAP,
        help=(
            "Minimum number of near-identical item pairs required for a "
            f"source pair (default: {DEFAULT_MIN_OVERLAP})"
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of source pairs to print",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--include-restricted",
        action="store_true",
        help="Include approved knowledge rows with restricted licenses",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.days < 1:
        raise SystemExit("--days must be at least 1")
    if args.min_overlap < 1:
        raise SystemExit("--min-overlap must be at least 1")
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit must be at least 1")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        report = build_source_overlap_report(
            db.conn,
            days=args.days,
            min_overlap=args.min_overlap,
            limit=args.limit,
            include_restricted=args.include_restricted,
        )

    output = format_json_report(report) if args.format == "json" else format_text_report(report)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
