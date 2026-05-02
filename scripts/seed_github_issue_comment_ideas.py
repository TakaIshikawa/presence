#!/usr/bin/env python3
"""Seed content ideas from recent GitHub issue comments."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.github_issue_comment_idea_seeder import (  # noqa: E402
    DEFAULT_DAYS,
    format_github_issue_comment_seed_json,
    format_github_issue_comment_seed_text,
    seed_github_issue_comment_ideas,
)


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
        help=f"Lookback window in days for issue comments (default: {DEFAULT_DAYS})",
    )
    parser.add_argument("--repo", help="Only seed comments from this repo_name")
    parser.add_argument(
        "--author",
        action="append",
        default=None,
        help="Only include this comment author; repeat for multiple authors",
    )
    parser.add_argument(
        "--exclude-author",
        action="append",
        default=None,
        help="Exclude this comment author; repeat for multiple authors",
    )
    parser.add_argument("--limit", type=_positive_int, default=25, help="Maximum comments to inspect")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview ideas without writing content_ideas",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    now = datetime.now(timezone.utc)
    with script_context() as (_config, db):
        results = seed_github_issue_comment_ideas(
            db,
            days=args.days,
            repo=args.repo,
            author=args.author,
            exclude_author=args.exclude_author,
            limit=args.limit,
            dry_run=args.dry_run,
            now=now,
        )
    print(
        format_github_issue_comment_seed_json(results)
        if args.json
        else format_github_issue_comment_seed_text(results)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
