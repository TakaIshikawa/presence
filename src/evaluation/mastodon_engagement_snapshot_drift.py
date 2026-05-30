"""Report Mastodon engagement snapshot drift."""

from __future__ import annotations

from ._batch_gap_reports import mastodon_from_db as build_mastodon_engagement_snapshot_drift_report_from_db
from ._batch_gap_reports import mastodon_report as build_mastodon_engagement_snapshot_drift_report
from ._gap_report_utils import DEFAULT_LIMIT, format_json, format_text

DEFAULT_MAX_GAP_HOURS = 48


def format_mastodon_engagement_snapshot_drift_json(report):
    return format_json(report)


def format_mastodon_engagement_snapshot_drift_text(report):
    return format_text("Mastodon Engagement Snapshot Drift", report)
