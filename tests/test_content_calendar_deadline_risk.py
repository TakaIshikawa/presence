from __future__ import annotations

from datetime import datetime, timezone

from evaluation.content_calendar_deadline_risk import build_content_calendar_deadline_risk_report

NOW = datetime(2026, 6, 4, 12, tzinfo=timezone.utc)


def test_overdue_and_soon_items_are_flagged():
    report = build_content_calendar_deadline_risk_report([
        {"idea_id": "over", "planned_at": "2026-06-04T10:00:00+00:00", "status": "idea"},
        {"idea_id": "soon", "planned_at": "2026-06-05T00:00:00+00:00", "status": "outline"},
    ], now=NOW)
    assert [row["risk_level"] for row in report["findings"]] == ["overdue", "high"]
    assert report["findings"][0]["hours_until_deadline"] < 0


def test_ready_approved_and_unscheduled_items_are_ignored():
    report = build_content_calendar_deadline_risk_report([
        {"idea_id": "ready", "planned_at": "2026-06-04T13:00:00+00:00", "status": "draft-ready"},
        {"idea_id": "approved", "planned_at": "2026-06-04T13:00:00+00:00", "status": "approved"},
        {"idea_id": "unscheduled", "status": "idea"},
    ], now=NOW)
    assert report["findings"] == []
