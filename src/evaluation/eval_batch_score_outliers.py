"""Report eval batch score outliers."""

from __future__ import annotations

from ._batch_gap_reports import eval_outliers_from_db as build_eval_batch_score_outliers_report_from_db
from ._batch_gap_reports import eval_outliers_report as build_eval_batch_score_outliers_report
from ._gap_report_utils import DEFAULT_LIMIT, format_json, format_text

DEFAULT_Z_THRESHOLD = 2.0
DEFAULT_MIN_BATCH_SIZE = 2


def format_eval_batch_score_outliers_json(report):
    return format_json(report)


def format_eval_batch_score_outliers_text(report):
    return format_text("Eval Batch Score Outliers", report)
