#!/usr/bin/env python3
"""Report Mastodon engagement snapshot drift."""

from __future__ import annotations

from _batch_gap_report_cli import non_negative_float, positive_int, run
from evaluation.mastodon_engagement_snapshot_drift import DEFAULT_LIMIT, DEFAULT_MAX_GAP_HOURS, build_mastodon_engagement_snapshot_drift_report_from_db, format_mastodon_engagement_snapshot_drift_json, format_mastodon_engagement_snapshot_drift_text


def main(argv=None) -> int:
    return run(argv, description=__doc__ or "", builder=build_mastodon_engagement_snapshot_drift_report_from_db, json_formatter=format_mastodon_engagement_snapshot_drift_json, text_formatter=format_mastodon_engagement_snapshot_drift_text, options=[("max_gap_hours", non_negative_float, DEFAULT_MAX_GAP_HOURS), ("limit", positive_int, DEFAULT_LIMIT)])


if __name__ == "__main__":
    raise SystemExit(main())
