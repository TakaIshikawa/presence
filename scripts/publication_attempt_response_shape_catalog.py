#!/usr/bin/env python3
"""Catalog publication attempt response metadata shapes."""

from __future__ import annotations

from _batch_gap_report_cli import positive_int, run
from evaluation.publication_attempt_response_shape_catalog import DEFAULT_LIMIT, DEFAULT_MAX_METADATA_BYTES, DEFAULT_RARE_SHAPE_THRESHOLD, build_publication_attempt_response_shape_catalog_report_from_db, format_publication_attempt_response_shape_catalog_json, format_publication_attempt_response_shape_catalog_text


def main(argv=None) -> int:
    return run(argv, description=__doc__ or "", builder=build_publication_attempt_response_shape_catalog_report_from_db, json_formatter=format_publication_attempt_response_shape_catalog_json, text_formatter=format_publication_attempt_response_shape_catalog_text, options=[("max_metadata_bytes", positive_int, DEFAULT_MAX_METADATA_BYTES), ("rare_shape_threshold", positive_int, DEFAULT_RARE_SHAPE_THRESHOLD), ("limit", positive_int, DEFAULT_LIMIT)])


if __name__ == "__main__":
    raise SystemExit(main())
