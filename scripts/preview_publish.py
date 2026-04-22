#!/usr/bin/env python3
"""Preview what a generated content item would publish to X and Bluesky."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.preview import (  # noqa: E402
    PreviewRecordNotFound,
    build_publication_preview,
    format_preview,
    preview_to_json,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--content-id", type=int, help="generated_content id to preview")
    target.add_argument("--queue-id", type=int, help="publish_queue id to preview")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of the text preview",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.WARNING)

    with script_context() as (_config, db):
        try:
            preview = build_publication_preview(
                db,
                content_id=args.content_id,
                queue_id=args.queue_id,
            )
        except PreviewRecordNotFound as exc:
            print(str(exc), file=sys.stderr)
            return 1

    print(preview_to_json(preview) if args.json else format_preview(preview))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
