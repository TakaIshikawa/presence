"""Report newsletter subject candidate rank gaps."""

from __future__ import annotations

from ._batch_gap_reports import newsletter_rank_from_db as build_newsletter_subject_candidate_rank_gaps_report_from_db
from ._batch_gap_reports import newsletter_rank_report as build_newsletter_subject_candidate_rank_gaps_report
from ._gap_report_utils import DEFAULT_LIMIT, format_json, format_text

DEFAULT_SCORE_GAP_THRESHOLD = 0.05


def format_newsletter_subject_candidate_rank_gaps_json(report):
    return format_json(report)


def format_newsletter_subject_candidate_rank_gaps_text(report):
    return format_text("Newsletter Subject Candidate Rank Gaps", report)
