#!/usr/bin/env python3
"""Export semantic knowledge source freshness report."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.freshness_report import (
    DEFAULT_STALE_AFTER_DAYS,
    KnowledgeFreshnessReport,
    build_knowledge_freshness_report,
)
from runner import script_context

logger = logging.getLogger(__name__)


def format_json_report(report: KnowledgeFreshnessReport) -> str:
    return json.dumps(report.to_dict(), indent=2, default=str)


def _format_age(days: float | None) -> str:
    if days is None:
        return "unknown"
    return f"{days:.1f}d"


def _format_license_mix(license_mix: dict[str, int]) -> str:
    if not license_mix:
        return "-"
    return ", ".join(f"{license_value}:{count}" for license_value, count in license_mix.items())


def format_text_report(report: KnowledgeFreshnessReport) -> str:
    lines = [
        "KNOWLEDGE FRESHNESS REPORT",
        "=" * 80,
        f"Generated: {report.generated_at}",
        f"Stale after: {report.stale_after_days:g} days",
        f"Sources: {report.source_count}",
        f"Stale sources: {report.stale_source_count}",
    ]
    if report.source_type:
        lines.append(f"Filter: source_type={report.source_type}")

    if not report.sources:
        lines.append("")
        lines.append("No knowledge sources found.")
        return "\n".join(lines)

    lines.append("")
    lines.append(
        f"{'Status':<8} {'Type':<20} {'Source':<28} {'Items':<7} "
        f"{'Newest':<25} {'Oldest':<25} {'Age':<9} License Mix"
    )
    lines.append("-" * 130)
    for source in report.sources:
        status = "STALE" if source.stale else "fresh"
        lines.append(
            f"{status:<8} {source.source_type:<20} "
            f"{source.source_identifier[:28]:<28} "
            f"{source.item_count:<7} "
            f"{(source.newest_item_timestamp or '-')[:25]:<25} "
            f"{(source.oldest_item_timestamp or '-')[:25]:<25} "
            f"{_format_age(source.days_since_newest_item):<9} "
            f"{_format_license_mix(source.license_mix)}"
        )

    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stale-after-days",
        type=float,
        default=DEFAULT_STALE_AFTER_DAYS,
        help=f"Mark sources stale when newest item is older than this many days (default: {DEFAULT_STALE_AFTER_DAYS:g})",
    )
    parser.add_argument(
        "--source-type",
        help="Only report a single knowledge source_type, such as curated_x",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of human-readable text",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.stale_after_days < 0:
        raise SystemExit("--stale-after-days must be non-negative")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        report = build_knowledge_freshness_report(
            db.conn,
            stale_after_days=args.stale_after_days,
            source_type=args.source_type,
        )

    if args.json:
        print(format_json_report(report))
    else:
        print(format_text_report(report))


if __name__ == "__main__":
    main()
