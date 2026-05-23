#!/usr/bin/env python3
"""Catalog publication attempt response metadata shapes."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from _focused_report_cli import non_negative_int, positive_int, run  # noqa: E402
from evaluation.publication_attempt_response_shape_catalog import DEFAULT_LIMIT, DEFAULT_MAX_METADATA_BYTES, DEFAULT_RARE_SHAPE_THRESHOLD, build_publication_attempt_response_shape_catalog_report_from_db, format_publication_attempt_response_shape_catalog_json, format_publication_attempt_response_shape_catalog_text  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json", "text"), default="json")
    p.add_argument("--max-metadata-bytes", type=non_negative_int, default=DEFAULT_MAX_METADATA_BYTES)
    p.add_argument("--rare-shape-threshold", type=positive_int, default=DEFAULT_RARE_SHAPE_THRESHOLD)
    p.add_argument("--limit", type=positive_int, default=DEFAULT_LIMIT)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    return run(args, build_publication_attempt_response_shape_catalog_report_from_db, format_publication_attempt_response_shape_catalog_json, format_publication_attempt_response_shape_catalog_text, {"max_metadata_bytes": args.max_metadata_bytes, "rare_shape_threshold": args.rare_shape_threshold, "limit": args.limit})


if __name__ == "__main__":
    raise SystemExit(main())
