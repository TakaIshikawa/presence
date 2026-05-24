#!/usr/bin/env python3
"""Report proactive action follow-up readiness."""

from __future__ import annotations

from _batch_gap_report_cli import non_negative_float, positive_int, run
from evaluation.proactive_action_followup_readiness import DEFAULT_LIMIT, DEFAULT_WINDOW_HOURS, build_proactive_action_followup_readiness_report_from_db, format_proactive_action_followup_readiness_json, format_proactive_action_followup_readiness_text


def main(argv=None) -> int:
    return run(argv, description=__doc__ or "", builder=build_proactive_action_followup_readiness_report_from_db, json_formatter=format_proactive_action_followup_readiness_json, text_formatter=format_proactive_action_followup_readiness_text, options=[("window_hours", non_negative_float, DEFAULT_WINDOW_HOURS), ("limit", positive_int, DEFAULT_LIMIT)])


if __name__ == "__main__":
    raise SystemExit(main())
