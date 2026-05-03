#!/usr/bin/env python3
"""Audit model_usage rows for incomplete accounting fields."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.model_usage_accounting_audit import (  # noqa: E402
    DEFAULT_DAYS,
    build_model_usage_accounting_audit_report,
    format_model_usage_accounting_audit_json,
    format_model_usage_accounting_audit_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for model_usage rows (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--operation",
        help="Restrict to one model_usage.operation_name.",
    )
    parser.add_argument(
        "--model",
        help="Restrict to one model_usage.model_name.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--fail-on-issues",
        action="store_true",
        help="Exit with status 1 when audit findings are found.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        with script_context() as (_config, db):
            report = build_model_usage_accounting_audit_report(
                db,
                days=args.days,
                operation=args.operation,
                model=args.model,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_model_usage_accounting_audit_json(report))
    else:
        print(format_model_usage_accounting_audit_text(report))
    if args.fail_on_issues and report.has_issues:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
