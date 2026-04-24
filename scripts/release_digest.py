#!/usr/bin/env python3
"""Build a newsletter/blog-ready digest from GitHub release activity."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.release_digest import (
    build_release_digest,
    format_release_digest_text,
    seed_digest_content_ideas,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Lookback window in days for release activity (default: 14)",
    )
    parser.add_argument("--repo", help="Only include releases for this repo name")
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--seed-ideas",
        action="store_true",
        help="Create idempotent content ideas for releases in the digest",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        digest = build_release_digest(db, days=args.days, repo=args.repo)
        seed_results = seed_digest_content_ideas(db, digest) if args.seed_ideas else []

    if args.format == "json":
        payload = digest.to_dict()
        if args.seed_ideas:
            payload["seed_ideas"] = [
                {
                    "status": result.status,
                    "repo_name": result.repo_name,
                    "tag_name": result.tag_name,
                    "idea_id": result.idea_id,
                    "reason": result.reason,
                }
                for result in seed_results
            ]
        import json

        print(json.dumps(payload, indent=2, sort_keys=True))
        return

    print(format_release_digest_text(digest))
    if args.seed_ideas:
        created = sum(1 for result in seed_results if result.status == "created")
        skipped = sum(1 for result in seed_results if result.status == "skipped")
        print("")
        print(f"Seed ideas: created={created} skipped={skipped}")


if __name__ == "__main__":
    main()
