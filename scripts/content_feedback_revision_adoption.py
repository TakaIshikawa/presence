#!/usr/bin/env python3
"""Report content feedback revision adoption."""

from __future__ import annotations

from _batch_gap_report_cli import non_negative_float, positive_int, run
from evaluation.content_feedback_revision_adoption import DEFAULT_LIMIT, DEFAULT_MIN_AGE_HOURS, build_content_feedback_revision_adoption_report_from_db, format_content_feedback_revision_adoption_json, format_content_feedback_revision_adoption_text


def main(argv=None) -> int:
    return run(argv, description=__doc__ or "", builder=build_content_feedback_revision_adoption_report_from_db, json_formatter=format_content_feedback_revision_adoption_json, text_formatter=format_content_feedback_revision_adoption_text, options=[("min_age_hours", non_negative_float, DEFAULT_MIN_AGE_HOURS), ("limit", positive_int, DEFAULT_LIMIT)])


if __name__ == "__main__":
    raise SystemExit(main())
