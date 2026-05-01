#!/usr/bin/env python3
"""Select and apply an evaluated newsletter subject candidate."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_subject_selection import (  # noqa: E402
    apply_newsletter_subject_selection,
    format_newsletter_subject_selection_json,
    format_newsletter_subject_selection_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--send-id",
        type=int,
        required=True,
        help="Newsletter send id whose subject should be selected",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--candidate-id",
        type=int,
        help="Apply this explicit subject candidate id",
    )
    group.add_argument(
        "--best",
        action="store_true",
        help="Apply the highest-scored non-rejected candidate deterministically",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report the proposed subject without updating newsletter_sends",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
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
            report = apply_newsletter_subject_selection(
                db,
                send_id=args.send_id,
                candidate_id=args.candidate_id,
                best=args.best,
                dry_run=args.dry_run,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_newsletter_subject_selection_json(report))
    else:
        print(format_newsletter_subject_selection_text(report))
    return 0 if report["status"] in {"applied", "dry_run"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
