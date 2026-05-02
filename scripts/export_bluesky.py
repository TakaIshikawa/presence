#!/usr/bin/env python3
"""Export a Bluesky draft from generated content or the publish queue."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.bluesky_export import (  # noqa: E402
    BlueskyExportError,
    BlueskyExportOptions,
    bluesky_export_to_json,
    build_bluesky_export_from_db,
    format_bluesky_markdown,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--content-id", type=int, help="generated_content id to export")
    target.add_argument("--queue-id", type=int, help="publish_queue id to export")
    parser.add_argument(
        "--format",
        choices=("markdown", "json"),
        default="markdown",
        help="Artifact format to emit",
    )
    parser.add_argument(
        "--max-length",
        type=int,
        help="Override the Bluesky grapheme limit used for post splitting",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    options_kwargs: dict[str, object] = {}
    if args.max_length is not None:
        options_kwargs["max_length"] = args.max_length

    try:
        with script_context() as (_config, db):
            export = build_bluesky_export_from_db(
                db,
                content_id=args.content_id,
                queue_id=args.queue_id,
                options=BlueskyExportOptions(**options_kwargs),
            )
    except BlueskyExportError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(bluesky_export_to_json(export))
    else:
        print(format_bluesky_markdown(export), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
