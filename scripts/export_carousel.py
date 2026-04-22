#!/usr/bin/env python3
"""Export an X thread as a carousel slide planning artifact."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.carousel_export import (  # noqa: E402
    CarouselExportError,
    build_carousel_export_from_db,
    build_carousel_export_from_preview,
    write_carousel_artifact,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--content-id", type=int, help="generated_content id to export")
    target.add_argument(
        "--preview-payload",
        type=Path,
        help="Path to a JSON payload produced by scripts/preview_publish.py --json",
    )
    parser.add_argument("--out", type=Path, required=True, help="Artifact output path")
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="json",
        help="Artifact format to write",
    )
    parser.add_argument(
        "--max-slides",
        type=int,
        default=8,
        help="Maximum number of carousel slides to include",
    )
    parser.add_argument(
        "--max-bullets",
        type=int,
        default=3,
        help="Maximum body bullets per slide",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.WARNING)

    try:
        if args.preview_payload:
            preview = json.loads(args.preview_payload.read_text(encoding="utf-8"))
            export = build_carousel_export_from_preview(
                preview,
                max_slides=args.max_slides,
                max_bullets=args.max_bullets,
            )
        else:
            with script_context() as (_config, db):
                export = build_carousel_export_from_db(
                    db,
                    content_id=args.content_id,
                    max_slides=args.max_slides,
                    max_bullets=args.max_bullets,
                )
        write_carousel_artifact(export, args.out, artifact_format=args.format)
    except (CarouselExportError, OSError, json.JSONDecodeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(f"Carousel artifact: {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
