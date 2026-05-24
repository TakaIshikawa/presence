"""Report profile metric following spikes."""

from __future__ import annotations

from ._batch_gap_reports import profile_following_from_db as build_profile_metric_following_spikes_report_from_db
from ._batch_gap_reports import profile_following_report as build_profile_metric_following_spikes_report
from ._gap_report_utils import DEFAULT_LIMIT, format_json, format_text

DEFAULT_FOLLOWING_DELTA_THRESHOLD = 100
DEFAULT_RATIO_THRESHOLD = 0.5


def format_profile_metric_following_spikes_json(report):
    return format_json(report)


def format_profile_metric_following_spikes_text(report):
    return format_text("Profile Metric Following Spikes", report)
