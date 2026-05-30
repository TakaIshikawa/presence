#!/usr/bin/env python3
"""Report profile metric following spikes."""

from __future__ import annotations

from _batch_gap_report_cli import non_negative_float, positive_int, run
from evaluation.profile_metric_following_spikes import DEFAULT_FOLLOWING_DELTA_THRESHOLD, DEFAULT_LIMIT, DEFAULT_RATIO_THRESHOLD, build_profile_metric_following_spikes_report_from_db, format_profile_metric_following_spikes_json, format_profile_metric_following_spikes_text


def main(argv=None) -> int:
    return run(argv, description=__doc__ or "", builder=build_profile_metric_following_spikes_report_from_db, json_formatter=format_profile_metric_following_spikes_json, text_formatter=format_profile_metric_following_spikes_text, options=[("following_delta_threshold", positive_int, DEFAULT_FOLLOWING_DELTA_THRESHOLD), ("ratio_threshold", non_negative_float, DEFAULT_RATIO_THRESHOLD), ("limit", positive_int, DEFAULT_LIMIT)])


if __name__ == "__main__":
    raise SystemExit(main())
