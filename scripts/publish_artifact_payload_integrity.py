#!/usr/bin/env python3
"""Validate serialized publish artifact payloads before platform clients."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publish_artifact_payload_integrity import (  # noqa: E402
    DEFAULT_LIMIT,
    build_publish_artifact_payload_integrity_report,
    format_publish_artifact_payload_integrity_json,
    format_publish_artifact_payload_integrity_text,
    parse_length_limit,
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


def _length_limit(value: str) -> tuple[str, int]:
    try:
        return parse_length_limit(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", default="all")
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument(
        "--length-limit",
        action="append",
        default=[],
        type=_length_limit,
        metavar="PLATFORM:LIMIT",
        help="Override a platform text length limit; may be repeated.",
    )
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true", help="Print the human-readable table output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        length_limits = dict(args.length_limit) if args.length_limit else None
        with script_context() as (_config, db):
            report = build_publish_artifact_payload_integrity_report(
                db,
                platform=args.platform,
                limit=args.limit,
                length_limits=length_limits,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except SystemExit as exc:
        return int(exc.code or 0)
    print(
        format_publish_artifact_payload_integrity_text(report)
        if args.table or args.format == "text"
        else format_publish_artifact_payload_integrity_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
