#!/usr/bin/env python3
"""Audit inconsistent or non-canonical publication URLs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publication_url_audit import (  # noqa: E402
    DEFAULT_DAYS,
    SUPPORTED_PLATFORMS,
    build_publication_url_audit,
    format_publication_url_audit_json,
    format_publication_url_audit_table,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--platform",
        default="all",
        choices=SUPPORTED_PLATFORMS,
        help="Platform to audit (default: all)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_DAYS})",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Exit non-zero when audit warnings exist",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        report = build_publication_url_audit(
            db,
            platform=args.platform,
            days=args.days,
        )

    output = (
        format_publication_url_audit_json(report)
        if args.json
        else format_publication_url_audit_table(report)
    )
    print(output)
    if args.fail_on_warning and report["warning_count"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
