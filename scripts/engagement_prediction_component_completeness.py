#!/usr/bin/env python3
"""Report engagement prediction component completeness."""

from __future__ import annotations

from _batch_gap_report_cli import non_negative_float, positive_int, run
from evaluation.engagement_prediction_component_completeness import DEFAULT_LIMIT, DEFAULT_MAX_BACKFILL_AGE_HOURS, build_engagement_prediction_component_completeness_report_from_db, format_engagement_prediction_component_completeness_json, format_engagement_prediction_component_completeness_text


def main(argv=None) -> int:
    return run(argv, description=__doc__ or "", builder=build_engagement_prediction_component_completeness_report_from_db, json_formatter=format_engagement_prediction_component_completeness_json, text_formatter=format_engagement_prediction_component_completeness_text, options=[("max_backfill_age_hours", non_negative_float, DEFAULT_MAX_BACKFILL_AGE_HOURS), ("limit", positive_int, DEFAULT_LIMIT)])


if __name__ == "__main__":
    raise SystemExit(main())
