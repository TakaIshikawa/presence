"""Report content feedback revision adoption."""

from __future__ import annotations

from ._batch_gap_reports import feedback_revision_from_db as build_content_feedback_revision_adoption_report_from_db
from ._batch_gap_reports import feedback_revision_report as build_content_feedback_revision_adoption_report
from ._gap_report_utils import DEFAULT_LIMIT, format_json, format_text

DEFAULT_MIN_AGE_HOURS = 24


def format_content_feedback_revision_adoption_json(report):
    return format_json(report)


def format_content_feedback_revision_adoption_text(report):
    return format_text("Content Feedback Revision Adoption", report)
