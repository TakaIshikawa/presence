#!/usr/bin/env python3
"""Audit persisted content for unredacted secrets and private paths."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.redaction_audit import (
    SUPPORTED_TABLES,
    audit_redaction_leaks,
    build_audit_payload,
)
from runner import script_context

logger = logging.getLogger(__name__)


def format_json_report(payload: dict) -> str:
    return json.dumps(payload, indent=2, default=str)


def format_text_report(payload: dict) -> str:
    lines = [
        f"Redaction Audit (last {payload['days']} days)",
        f"Tables: {', '.join(payload['tables'])}",
        f"Total matches: {payload['total_matches']}",
    ]
    if payload["total_matches"] == 0:
        lines.append("\nNo redaction pattern matches found.")
        return "\n".join(lines)

    for table in payload["tables"]:
        rows = payload["matches"].get(table, [])
        if not rows:
            continue
        lines.append("")
        lines.append(f"{table}: {len(rows)} match(es)")
        for row in rows:
            lines.append(
                "  - row {row_id} {field} [{pattern_label}]: {redacted_preview}".format(
                    **row
                )
            )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to inspect (default: 30)",
    )
    parser.add_argument(
        "--table",
        action="append",
        choices=SUPPORTED_TABLES,
        help="Limit audit to a supported table. Repeat to scan multiple tables.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--fail-on-match",
        action="store_true",
        help="Exit non-zero when any match is found.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.days < 1:
        raise SystemExit("--days must be at least 1")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    tables = tuple(args.table) if args.table else SUPPORTED_TABLES
    with script_context() as (config, db):
        matches = audit_redaction_leaks(
            db.conn,
            days=args.days,
            tables=tables,
            patterns=config.privacy.redaction_patterns,
        )

    payload = build_audit_payload(matches, days=args.days, tables=tables)
    if args.format == "json":
        print(format_json_report(payload))
    else:
        print(format_text_report(payload))

    if args.fail_on_match and matches:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
