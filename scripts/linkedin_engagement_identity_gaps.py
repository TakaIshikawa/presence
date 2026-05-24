#!/usr/bin/env python3
"""Report LinkedIn engagement identity gaps."""

from __future__ import annotations

from _batch_gap_report_cli import positive_int, run
from evaluation.linkedin_engagement_identity_gaps import DEFAULT_LIMIT, build_linkedin_engagement_identity_gaps_report_from_db, format_linkedin_engagement_identity_gaps_json, format_linkedin_engagement_identity_gaps_text


def main(argv=None) -> int:
    return run(argv, description=__doc__ or "", builder=build_linkedin_engagement_identity_gaps_report_from_db, json_formatter=format_linkedin_engagement_identity_gaps_json, text_formatter=format_linkedin_engagement_identity_gaps_text, options=[("limit", positive_int, DEFAULT_LIMIT)])


if __name__ == "__main__":
    raise SystemExit(main())
