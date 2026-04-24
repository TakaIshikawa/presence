#!/usr/bin/env python3
"""Export social metadata for generated blog posts."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.blog_metadata import (
    BlogMetadataExporter,
    metadata_to_json,
    metadata_to_markdown,
)
from runner import script_context


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--content-id",
        type=int,
        help="Export metadata for one generated_content blog_post ID.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Look back this many days for recent published blog posts (default: 30).",
    )
    parser.add_argument(
        "--format",
        choices=["json", "markdown"],
        default="json",
        help="Output format (default: json).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write output to this path instead of stdout.",
    )
    return parser.parse_args(argv)


def render_metadata(args: argparse.Namespace) -> str:
    with script_context() as (_config, db):
        exporter = BlogMetadataExporter(db)
        if args.content_id is not None:
            metadata = exporter.export_content_id(args.content_id)
        else:
            metadata = exporter.export_recent(days=args.days)

    if args.format == "markdown":
        return metadata_to_markdown(metadata)
    return metadata_to_json(metadata)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    output = render_metadata(args)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output)
    else:
        print(output, end="")


if __name__ == "__main__":
    main()
