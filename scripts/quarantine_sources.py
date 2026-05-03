#!/usr/bin/env python3
"""Review and mutate unhealthy curated sources."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_quarantine import (  # noqa: E402
    pause_quarantined_sources,
    quarantine_curated_sources,
    reject_quarantined_sources,
    resume_quarantined_sources,
)
from runner import script_context  # noqa: E402

SOURCE_TYPES = ["x_account", "blog", "newsletter"]


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


def _source_label(source: dict) -> str:
    identifier = source.get("identifier") or ""
    return f"@{identifier}" if source.get("source_type") == "x_account" else identifier


def _truncate(value: str | None, width: int) -> str:
    text = (value or "").replace("\n", " ")
    return text if len(text) <= width else text[: width - 3] + "..."


def _print_report(report: dict) -> None:
    mode = "APPLY" if report.get("applied") else "DRY RUN"
    print(f"CURATED SOURCE QUARANTINE ({mode})")
    print(
        f"Thresholds: failures >= {report['failure_threshold']}; "
        f"stale >= {report['stale_days']} days"
    )
    if report.get("source_type"):
        print(f"Source type: {report['source_type']}")
    print(
        "Counts: "
        f"healthy={report['counts']['healthy']} "
        f"watch={report['counts']['watch']} "
        f"quarantine={report['counts']['quarantine']}"
    )
    print(
        f"Planned pauses: {report['planned_pauses']}; "
        f"updated: {report['updated']}"
    )
    print()

    if not report["sources"]:
        print("No curated sources found.")
        return

    print(
        f"{'Class':<12} {'ID':<5} {'Source':<28} {'Type':<12} "
        f"{'Failures':<8} {'Status':<10} {'Last failure':<25} Reason"
    )
    print("-" * 126)
    for source in report["sources"]:
        print(
            f"{source['classification']:<12} {source['id']!s:<5} "
            f"{_source_label(source):<28} {source['source_type']:<12} "
            f"{source['consecutive_failures']!s:<8} {source['status']:<10} "
            f"{(source.get('last_failure_at') or '-'):<25} {source['reason']}"
        )
        if source.get("last_error"):
            print(f"{'':<49} error={_truncate(source.get('last_error'), 74)}")


def _print_mutation(report: dict, noun: str) -> None:
    planned = report.get("planned", report.get("planned_pauses", 0))
    updated = report.get("updated", 0)
    dry_run = report.get("dry_run", False)
    action = f"would {noun}" if dry_run else noun
    print(f"Curated sources {action}: {planned}; updated: {updated}")
    if not report.get("sources"):
        return
    print(f"{'ID':<5} {'Source':<28} {'Type':<12} {'Status':<10} {'Failures':<8} Last Error")
    print("-" * 96)
    for source in report["sources"]:
        print(
            f"{source.get('id')!s:<5} {_source_label(source):<28} "
            f"{source.get('source_type') or '':<12} {source.get('status') or '':<10} "
            f"{str(source.get('consecutive_failures') or 0):<8} "
            f"{_truncate(source.get('last_error'), 45)}"
        )


def _risk_score(source: dict) -> int:
    if source.get("classification") == "quarantine":
        return 100
    if source.get("classification") == "watch":
        return 50
    return 0


def _json_report(report: dict) -> dict:
    return {
        **report,
        "sources": [
            {
                **source,
                "risk": source.get("classification", "unknown"),
                "score": _risk_score(source),
            }
            for source in report.get("sources", [])
        ],
    }


def _add_report_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--failure-threshold",
        type=_non_negative_int,
        default=3,
        help="Consecutive failures required for quarantine (default: 3).",
    )
    parser.add_argument(
        "--stale-days",
        type=_non_negative_int,
        default=30,
        help="Days since last successful fetch required for quarantine (default: 30).",
    )
    parser.add_argument(
        "--source-type",
        choices=SOURCE_TYPES,
        help="Limit quarantine checks to one source type.",
    )


def _add_output_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report planned changes without modifying the database.",
    )


def _add_target_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("targets", nargs="*", help="curated_sources IDs or identifiers.")
    parser.add_argument("--id", dest="source_ids", type=int, action="append", default=[])
    parser.add_argument("--identifier", dest="identifiers", action="append", default=[])
    parser.add_argument("--source-type", choices=SOURCE_TYPES)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Review and mutate unhealthy curated sources."
    )
    subparsers = parser.add_subparsers(dest="command")

    report = subparsers.add_parser("report", help="List curated source health.")
    _add_report_options(report)
    _add_output_options(report)

    pause = subparsers.add_parser("pause", help="Pause sources over failure thresholds.")
    _add_report_options(pause)
    _add_output_options(pause)

    resume = subparsers.add_parser("resume", help="Reactivate paused sources.")
    _add_target_options(resume)
    _add_output_options(resume)

    reject = subparsers.add_parser("reject", help="Reject matching sources.")
    _add_target_options(reject)
    _add_output_options(reject)

    # Legacy report invocation, kept for existing automation.
    _add_report_options(parser)
    parser.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--dry-run", action="store_true", help=argparse.SUPPRESS)
    legacy_mode = parser.add_mutually_exclusive_group()
    legacy_mode.add_argument("--apply", action="store_true", help=argparse.SUPPRESS)
    return parser


def _targets_from_args(args: argparse.Namespace) -> list[str]:
    return [*(str(item) for item in args.source_ids), *args.identifiers, *args.targets]


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    command = args.command or "report"

    with script_context() as (_config, db):
        if command == "report":
            report = quarantine_curated_sources(
                db,
                failure_threshold=args.failure_threshold,
                stale_days=args.stale_days,
                source_type=args.source_type,
                apply=getattr(args, "apply", False),
            )
        elif command == "pause":
            report = pause_quarantined_sources(
                db,
                failure_threshold=args.failure_threshold,
                stale_days=args.stale_days,
                source_type=args.source_type,
                dry_run=args.dry_run,
            )
        elif command == "resume":
            targets = _targets_from_args(args)
            if not targets:
                parser.error("resume requires at least one ID or identifier")
            report = resume_quarantined_sources(
                db,
                targets,
                source_type=args.source_type,
                dry_run=args.dry_run,
            )
        else:
            targets = _targets_from_args(args)
            if not targets:
                parser.error("reject requires at least one ID or identifier")
            report = reject_quarantined_sources(
                db,
                targets,
                source_type=args.source_type,
                dry_run=args.dry_run,
            )

    if args.json:
        print(json.dumps(_json_report(report), indent=2, sort_keys=True))
    elif command in {"report", "pause"}:
        _print_report(report)
    elif command == "resume":
        _print_mutation(report, "resume")
    else:
        _print_mutation(report, "reject")


if __name__ == "__main__":
    main()
