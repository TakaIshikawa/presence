#!/usr/bin/env python3
"""Validate exported publication replay bundles without publishing."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publication_replay_validator import (  # noqa: E402
    export_to_json,
    format_text_report,
    validate_publication_replay_bundle,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "bundle_path",
        help="Path to a publication replay bundle JSON file.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat warnings as blocking validation failures.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def _load_bundle(path: str) -> dict:
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except OSError as exc:
        raise ValueError(f"could not read bundle file: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"bundle file is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("bundle JSON must be an object")
    return payload


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        bundle = _load_bundle(args.bundle_path)
        with script_context() as (_config, db):
            report = validate_publication_replay_bundle(
                bundle,
                db=db,
                strict=args.strict,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(export_to_json(report))
    else:
        print(format_text_report(report))
    return 1 if report.blocked_count or report.bundle_issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
