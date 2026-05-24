#!/usr/bin/env python3
"""Report newsletter subject candidate rank gaps."""

from __future__ import annotations

from _batch_gap_report_cli import non_negative_float, positive_int, run
from evaluation.newsletter_subject_candidate_rank_gaps import DEFAULT_LIMIT, DEFAULT_SCORE_GAP_THRESHOLD, build_newsletter_subject_candidate_rank_gaps_report_from_db, format_newsletter_subject_candidate_rank_gaps_json, format_newsletter_subject_candidate_rank_gaps_text


def main(argv=None) -> int:
    return run(argv, description=__doc__ or "", builder=build_newsletter_subject_candidate_rank_gaps_report_from_db, json_formatter=format_newsletter_subject_candidate_rank_gaps_json, text_formatter=format_newsletter_subject_candidate_rank_gaps_text, options=[("score_gap_threshold", non_negative_float, DEFAULT_SCORE_GAP_THRESHOLD), ("limit", positive_int, DEFAULT_LIMIT)])


if __name__ == "__main__":
    raise SystemExit(main())
