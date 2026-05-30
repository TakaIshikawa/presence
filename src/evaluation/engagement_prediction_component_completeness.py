"""Report engagement prediction component completeness."""

from __future__ import annotations

from ._batch_gap_reports import engagement_prediction_from_db as build_engagement_prediction_component_completeness_report_from_db
from ._batch_gap_reports import engagement_prediction_report as build_engagement_prediction_component_completeness_report
from ._gap_report_utils import DEFAULT_LIMIT, format_json, format_text

DEFAULT_MAX_BACKFILL_AGE_HOURS = 72


def format_engagement_prediction_component_completeness_json(report):
    return format_json(report)


def format_engagement_prediction_component_completeness_text(report):
    return format_text("Engagement Prediction Component Completeness", report)
