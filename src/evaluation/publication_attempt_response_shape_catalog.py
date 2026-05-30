"""Catalog publication attempt response metadata shapes."""

from __future__ import annotations

from ._batch_gap_reports import publication_attempt_shape_from_db as build_publication_attempt_response_shape_catalog_report_from_db
from ._batch_gap_reports import publication_attempt_shape_report as build_publication_attempt_response_shape_catalog_report
from ._gap_report_utils import DEFAULT_LIMIT, format_json, format_text

DEFAULT_MAX_METADATA_BYTES = 4096
DEFAULT_RARE_SHAPE_THRESHOLD = 1


def format_publication_attempt_response_shape_catalog_json(report):
    return format_json(report)


def format_publication_attempt_response_shape_catalog_text(report):
    return format_text("Publication Attempt Response Shape Catalog", report)
