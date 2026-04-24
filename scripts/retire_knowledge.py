#!/usr/bin/env python3
"""Retire stale low-value approved knowledge rows without deleting lineage."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.retirement import KnowledgeRetirementPolicy, build_retirement_report
from runner import script_context

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--older-than-days",
        type=int,
        default=180,
        help="Retirement age threshold for knowledge rows (default: 180).",
    )
    parser.add_argument(
        "--source-type",
        help="Only consider approved knowledge rows with this source_type.",
    )
    parser.add_argument(
        "--license",
        help="Only consider approved knowledge rows with this license.",
    )
    parser.add_argument(
        "--min-unused-days",
        type=int,
        default=30,
        help="Retain rows linked to content or replies within this many days (default: 30).",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Report retirement candidates without updating the database.",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="Mark selected knowledge rows unapproved.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON.",
    )
    return parser


def _shorten(value: str | None, limit: int = 72) -> str:
    text = (value or "").replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def format_text_report(payload: dict) -> str:
    totals = payload["totals"]
    mode = payload["mode"]
    lines = [
        f"Knowledge Retirement Report ({mode})",
        (
            f"Considered: {totals['considered']}  "
            f"Retained: {totals['retained']}  "
            f"Retired: {totals['retired']}"
        ),
    ]

    if totals["by_reason"]:
        reasons = ", ".join(
            f"{reason}={count}" for reason, count in totals["by_reason"].items()
        )
        lines.append(f"Retirement reasons: {reasons}")

    if totals["by_source_type"]:
        source_types = ", ".join(
            f"{source_type}={count}"
            for source_type, count in totals["by_source_type"].items()
        )
        lines.append(f"Retired by source_type: {source_types}")

    if totals["by_license"]:
        licenses = ", ".join(
            f"{license_value}={count}"
            for license_value, count in totals["by_license"].items()
        )
        lines.append(f"Retired by license: {licenses}")

    if not payload["items"]:
        lines.append("\nNo approved knowledge rows matched the filters.")
        return "\n".join(lines)

    lines.append("")
    for item in payload["items"]:
        reasons = ",".join(item["reasons"]) or "-"
        retain_reasons = ",".join(item["retain_reasons"]) or "-"
        lines.append(
            f"{item['action'].upper():<6} #{item['id']} "
            f"[{item['source_type']} {item['license']}] "
            f"created={item['created_at']} last_used={item['last_used_at'] or '-'} "
            f"reasons={reasons} retain={retain_reasons} "
            f"{_shorten(item.get('source_url') or item.get('source_id'))}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    apply_changes = bool(args.apply)

    try:
        policy = KnowledgeRetirementPolicy(
            older_than_days=args.older_than_days,
            source_type=args.source_type,
            license=args.license,
            min_unused_days=args.min_unused_days,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    logging.basicConfig(
        level=logging.WARNING if args.json else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    with script_context() as (_config, db):
        payload = build_retirement_report(db, policy, apply=apply_changes)

    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(format_text_report(payload))
        if payload["mode"] == "dry_run" and payload["totals"]["retired"]:
            print("\nDry run only. Re-run with --apply to retire these rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
