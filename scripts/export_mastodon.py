#!/usr/bin/env python3
"""Export Mastodon draft artifacts from generated content."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.mastodon_export import (  # noqa: E402
    MastodonExportError,
    MastodonExportOptions,
    build_mastodon_export_from_db,
    format_mastodon_markdown,
    mastodon_export_to_json,
    write_mastodon_artifact,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--content-id", type=int, required=True, help="generated_content id to export")
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Maximum characters per Mastodon status",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/mastodon"),
        help="Directory where the artifact should be written",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Write JSON instead of Markdown",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the artifact without writing a file",
    )
    parser.add_argument(
        "--cw",
        help="Optional Mastodon content warning text to include with each status",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    artifact_format = "json" if args.json else "markdown"

    try:
        with script_context() as (_config, db):
            export = build_mastodon_export_from_db(
                db,
                content_id=args.content_id,
                options=MastodonExportOptions(limit=args.limit, cw=args.cw),
            )
    except (MastodonExportError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.dry_run:
        if args.json:
            print(mastodon_export_to_json(export))
        else:
            print(format_mastodon_markdown(export), end="")
        print("Dry run; no Mastodon artifact written", file=sys.stderr)
        return 0

    try:
        path = write_mastodon_artifact(
            export,
            args.output_dir,
            artifact_format=artifact_format,
        )
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"Mastodon artifact: {path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
