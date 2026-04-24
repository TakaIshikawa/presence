#!/usr/bin/env python3
"""Report semantic knowledge freshness as either source summaries or item-level findings."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.freshness import build_freshness_report, report_to_dict
from knowledge.freshness_report import (
    DEFAULT_STALE_AFTER_DAYS,
    KnowledgeFreshnessReport,
    build_knowledge_freshness_report,
)
from runner import script_context

logger = logging.getLogger(__name__)


def build_report_payload(
    db,
    *,
    stale_days: int,
    unused_days: int,
    source_type: str | None,
    limit: int | None,
) -> dict:
    findings = build_freshness_report(
        db.conn,
        stale_days=stale_days,
        unused_days=unused_days,
        source_type=source_type,
        limit=limit,
    )
    return report_to_dict(
        findings,
        stale_days=stale_days,
        unused_days=unused_days,
        source_type=source_type,
        limit=limit,
    )


def format_json_report(payload: dict) -> str:
    return json.dumps(payload, indent=2, default=str)


def format_summary_json(report: KnowledgeFreshnessReport) -> str:
    return json.dumps(report.to_dict(), indent=2, default=str)


def format_text_report(payload: dict) -> str:
    lines = [
        "Knowledge Freshness Report",
        (
            f"Thresholds: stale>={payload['stale_days']}d, "
            f"unused>={payload['unused_days']}d"
        ),
    ]
    if payload["source_type"]:
        lines.append(f"Filter: source_type={payload['source_type']}")
    if payload["limit"]:
        lines.append(f"Limit: {payload['limit']}")

    findings = payload["findings"]
    if not findings:
        lines.append("\nNo stale, unused, or inactive-sourced approved knowledge found.")
        return "\n".join(lines)

    for finding in findings:
        lines.append("")
        recommendations = ",".join(finding["recommendations"])
        lines.append(
            f"Knowledge #{finding['knowledge_id']} [{finding['source_type']}] "
            f"recommend={recommendations}"
        )
        lines.append(
            f"  source_id={finding['source_id'] or '-'} "
            f"author={finding['author'] or '-'} url={finding['source_url'] or '-'}"
        )
        lines.append(
            f"  age_days={_display(finding['age_days'])} "
            f"usage_count={finding['usage_count']} "
            f"last_used_at={finding['last_used_at'] or '-'}"
        )
        flags = [
            label
            for label, enabled in (
                ("stale", finding["stale"]),
                ("unused", finding["unused"]),
                ("inactive_source", finding["inactive_source"]),
            )
            if enabled
        ]
        lines.append(f"  flags={','.join(flags)}")
        inactive = finding["inactive_source_metadata"]
        if inactive:
            lines.append(
                f"  inactive_source={inactive['source_type']}:{inactive['identifier']} "
                f"status={inactive.get('status') or '-'} active={inactive.get('active')}"
            )
        if finding["insight"]:
            lines.append(f"  insight={_shorten(finding['insight'])}")
        else:
            lines.append(f"  content={_shorten(finding['content_preview'])}")

    return "\n".join(lines)


def format_summary_text(report: KnowledgeFreshnessReport) -> str:
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
        "--mode",
        choices=("items", "summary"),
        default="items",
        help="Report mode: item-level findings or source summary (default: items)",
    )
    parser.add_argument(
        "--stale-days",
        type=int,
        default=180,
        help="Age in days after which published/ingested knowledge is stale in item mode",
    )
    parser.add_argument(
        "--unused-days",
        type=int,
        default=90,
        help="Age in days after which never-linked knowledge is unused in item mode",
    )
    parser.add_argument(
        "--stale-after-days",
        type=float,
        default=DEFAULT_STALE_AFTER_DAYS,
        help="Age in days after which a knowledge source is stale in summary mode",
    )
    parser.add_argument(
        "--source-type",
        help="Restrict report to a knowledge source_type such as curated_x",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of findings to print in item mode",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.stale_days < 1:
        raise SystemExit("--stale-days must be at least 1")
    if args.unused_days < 1:
        raise SystemExit("--unused-days must be at least 1")
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit must be at least 1")
    if args.stale_after_days < 0:
        raise SystemExit("--stale-after-days must be non-negative")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        if args.mode == "summary":
            report = build_knowledge_freshness_report(
                db.conn,
                stale_after_days=args.stale_after_days,
                source_type=args.source_type,
            )
            output = (
                format_summary_json(report)
                if args.format == "json"
                else format_summary_text(report)
            )
        else:
            payload = build_report_payload(
                db,
                stale_days=args.stale_days,
                unused_days=args.unused_days,
                source_type=args.source_type,
                limit=args.limit,
            )
            output = (
                format_json_report(payload)
                if args.format == "json"
                else format_text_report(payload)
            )

    print(output)
    return 0


def _display(value: object) -> str:
    return "-" if value is None else str(value)


def _shorten(value: str | None, limit: int = 120) -> str:
    text = (value or "").replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _format_age(days: float | None) -> str:
    if days is None:
        return "unknown"
    return f"{days:.1f}d"


def _format_license_mix(license_mix: dict[str, int]) -> str:
    if not license_mix:
        return "-"
    return ", ".join(f"{license_value}:{count}" for license_value, count in license_mix.items())


if __name__ == "__main__":
    raise SystemExit(main())
