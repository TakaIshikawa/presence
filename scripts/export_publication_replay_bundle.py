#!/usr/bin/env python3
"""Export redacted publication attempt replay bundles."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publication_replay_bundle import (  # noqa: E402
    build_publication_replay_bundle,
    publication_replay_bundle_to_json,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--content-id",
        type=int,
        help="Only include attempts for one generated_content id.",
    )
    parser.add_argument(
        "--platform",
        choices=["all", "x", "bluesky"],
        default="all",
        help="Only include attempts for one platform (default: all).",
    )
    parser.add_argument(
        "--since",
        help="Only include attempts at or after this ISO timestamp.",
    )
    parser.add_argument(
        "--output",
        default="-",
        help="Output file path, or '-' for stdout (default: stdout).",
    )
    parser.add_argument(
        "--include-successful",
        action="store_true",
        help="Include successful attempts as well as failures.",
    )
    return parser.parse_args(argv)


def render_replay_bundle(args: argparse.Namespace) -> str:
    with script_context() as (_config, db):
        bundle = build_publication_replay_bundle(
            db,
            content_id=args.content_id,
            platform=args.platform,
            since=args.since,
            include_successful=args.include_successful,
        )
    return publication_replay_bundle_to_json(bundle) + "\n"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        payload = render_replay_bundle(args)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.output == "-":
        print(payload, end="")
    else:
        Path(args.output).write_text(payload, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
