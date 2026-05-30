"""Report LinkedIn engagement identity gaps."""

from __future__ import annotations

from ._batch_gap_reports import linkedin_identity_from_db as build_linkedin_engagement_identity_gaps_report_from_db
from ._batch_gap_reports import linkedin_identity_report as build_linkedin_engagement_identity_gaps_report
from ._gap_report_utils import DEFAULT_LIMIT, format_json, format_text


def format_linkedin_engagement_identity_gaps_json(report):
    return format_json(report)


def format_linkedin_engagement_identity_gaps_text(report):
    return format_text("LinkedIn Engagement Identity Gaps", report)
