#!/usr/bin/env python3
"""Poll Mastodon mention notifications into the reply queue."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.mastodon_mentions import (  # noqa: E402
    DEFAULT_LIMIT,
    format_mastodon_mentions_json,
    format_mastodon_mentions_text,
    poll_mastodon_mentions,
)
from runner import script_context, update_monitoring  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print a summary without writing reply_queue rows or advancing the cursor.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum notifications to request (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    return parser.parse_args(argv)


def _mastodon_config_value(config: object, name: str) -> str:
    mastodon = getattr(config, "mastodon", None)
    return str(getattr(mastodon, name, "") or "") if mastodon else ""


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (config, db):
            mastodon = getattr(config, "mastodon", None)
            if mastodon and not getattr(mastodon, "enabled", True):
                print("Mastodon mention polling is disabled.", file=sys.stderr)
                return 0

            report = poll_mastodon_mentions(
                db=db,
                base_url=_mastodon_config_value(config, "base_url"),
                access_token=_mastodon_config_value(config, "access_token"),
                limit=args.limit,
                dry_run=args.dry_run,
                timeout=float(getattr(getattr(config, "timeouts", None), "http_seconds", 30)),
            )
            if not args.dry_run:
                update_monitoring("poll-mastodon-mentions")
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_mastodon_mentions_json(report))
    else:
        print(format_mastodon_mentions_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
