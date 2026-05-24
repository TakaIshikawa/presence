"""Report proactive action follow-up readiness."""

from __future__ import annotations

from ._batch_gap_reports import proactive_followup_from_db as build_proactive_action_followup_readiness_report_from_db
from ._batch_gap_reports import proactive_followup_report as build_proactive_action_followup_readiness_report
from ._gap_report_utils import DEFAULT_LIMIT, format_json, format_text

DEFAULT_WINDOW_HOURS = 24


def format_proactive_action_followup_readiness_json(report):
    return format_json(report)


def format_proactive_action_followup_readiness_text(report):
    return format_text("Proactive Action Followup Readiness", report)
