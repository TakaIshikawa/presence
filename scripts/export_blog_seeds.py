#!/usr/bin/env python3
"""Export reviewable blog draft briefs from resonated X posts and threads."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.blog_seed_export import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_MIN_ENGAGEMENT,
    BlogSeedExportError,
    BlogSeedExporter,
    default_export_filename,
    export_to_dict,
    write_export,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-path",
        "--out",
        dest="output_path",
        type=Path,
        required=True,
        help="File path or directory where the artifact should be written.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="json",
        help="Artifact format to write.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="Published-content lookback window.",
    )
    parser.add_argument(
        "--min-engagement",
        type=float,
        default=DEFAULT_MIN_ENGAGEMENT,
        help="Minimum latest engagement score for non-resonated content.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Maximum number of source posts or threads to export.",
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> dict:
    """Build and write a blog seed export artifact."""
    with script_context() as (_config, db):
        exporter = BlogSeedExporter(db)
        export = exporter.build_export(
            lookback_days=args.lookback_days,
            min_engagement=args.min_engagement,
            limit=args.limit,
        )
        output_path = args.output_path
        if output_path.suffix == "":
            output_path = output_path / default_export_filename(
                export,
                artifact_format=args.format,
            )
        write_export(export, output_path, artifact_format=args.format)
        return {
            "artifact_path": str(output_path),
            "seed_count": len(export.seeds),
            "artifact": export_to_dict(export),
        }


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    try:
        result = run(args)
    except (BlogSeedExportError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2, sort_keys=True))
    print(f"Blog seed export: {result['artifact_path']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
