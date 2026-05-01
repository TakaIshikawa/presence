#!/usr/bin/env python3
"""Plan stale visual asset archival without deleting files."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.visual_asset_expiry import (  # noqa: E402
    DEFAULT_MINIMUM_AGE_DAYS,
    DEFAULT_ROOT_PATH,
    build_visual_asset_expiry_plan,
    format_visual_asset_expiry_json,
    format_visual_asset_expiry_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root-path",
        "--root",
        default=DEFAULT_ROOT_PATH,
        help=f"Local visual output root to scan for orphan files (default: {DEFAULT_ROOT_PATH}).",
    )
    parser.add_argument(
        "--minimum-age-days",
        "--min-age-days",
        type=int,
        default=DEFAULT_MINIMUM_AGE_DAYS,
        help=f"Minimum file age before archive is proposed (default: {DEFAULT_MINIMUM_AGE_DAYS}).",
    )
    parser.add_argument(
        "--include-unpublished",
        action="store_true",
        help="Allow unpublished generated assets to become archive candidates.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print deterministic JSON instead of the default text plan.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_visual_asset_expiry_plan(
                db,
                root_path=args.root_path,
                minimum_age_days=args.minimum_age_days,
                include_unpublished=args.include_unpublished,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_visual_asset_expiry_json(report))
    else:
        print(format_visual_asset_expiry_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
