#!/usr/bin/env python3
"""Audit stored knowledge source URL redirect drift."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.link_metadata_enricher import SOURCE_TYPES  # noqa: E402
from knowledge.source_redirect_audit import (  # noqa: E402
    DEFAULT_LIMIT,
    audit_knowledge_source_redirects,
    format_source_redirect_audit_json,
    format_source_redirect_audit_text,
)
from runner import script_context  # noqa: E402


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-type",
        choices=SOURCE_TYPES,
        default="all",
        help="Limit audit to one source family (default: all).",
    )
    parser.add_argument(
        "--domain-change-only",
        action="store_true",
        help="Only include stored redirects that change domains.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit deterministic JSON instead of compact text.",
    )
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum findings to return (default: {DEFAULT_LIMIT}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        with script_context() as (_config, db):
            report = audit_knowledge_source_redirects(
                db,
                source_type=args.source_type,
                domain_change_only=args.domain_change_only,
                limit=args.limit,
            )
    except (sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_source_redirect_audit_json(report))
    else:
        print(format_source_redirect_audit_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
