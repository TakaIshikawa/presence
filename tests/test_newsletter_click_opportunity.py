"""Tests for newsletter click-through opportunity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.newsletter_click_opportunity import (
    AUDIENCE_FIT_REVIEW,
    LINK_PLACEMENT_REVIEW,
    MISSING_METRICS,
    SUBJECT_BODY_CTA_REVIEW,
    analyze_newsletter_click_opportunities,
    build_newsletter_click_opportunity_report,
    format_newsletter_click_opportunity_json,
    format_newsletter_click_opportunity_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_click_opportunity.py"
)
spec = importlib.util.spec_from_file_location("newsletter_click_opportunity_script", SCRIPT_PATH)
newsletter_click_opportunity_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_click_opportunity_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _send(
    db,
    *,
    issue_id: str,
    subscribers: int = 100,
    sent_at: datetime | None = None,
    status: str = "sent",
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=f"Issue {issue_id}",
        content_ids=[],
        subscriber_count=subscribers,
        status=status,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        ((sent_at or NOW).isoformat(), send_id),
    )
    db.conn.commit()
    return int(send_id)


def _engagement(
    db,
    send_id: int,
    *,
    issue_id: str,
    opens: int,
    clicks: int,
    fetched_at: datetime | None = None,
) -> int:
    return int(
        db.insert_newsletter_engagement(
            send_id,
            issue_id,
            opens=opens,
            clicks=clicks,
            unsubscribes=0,
            fetched_at=(fetched_at or NOW).isoformat(),
        )
    )


def test_low_click_issue_is_ranked_with_rates_and_recommendation(db):
    healthy = _send(db, issue_id="healthy")
    low_click = _send(db, issue_id="low-click")
    _engagement(db, healthy, issue_id="healthy", opens=45, clicks=8)
    _engagement(db, low_click, issue_id="low-click", opens=60, clicks=1)

    report = build_newsletter_click_opportunity_report(db, days=14, now=NOW)

    assert report.to_dict()["artifact_type"] == "newsletter_click_opportunity"
    assert report.totals == {
        "flagged_issue_count": 1,
        "measured_issue_count": 2,
        "send_count": 2,
    }
    issue = report.issues[0]
    assert issue.newsletter_send_id == low_click
    assert issue.open_rate == 0.6
    assert issue.click_rate == 0.01
    assert issue.click_to_open_rate == pytest.approx(0.0167)
    assert issue.opportunity_score > 0
    assert issue.issue_codes == (SUBJECT_BODY_CTA_REVIEW, LINK_PLACEMENT_REVIEW)
    assert "link placement review" in issue.recommendation

    text = format_newsletter_click_opportunity_text(report)
    assert "# Newsletter Click-Through Opportunity" in text
    assert "send=" in text
    assert "low-click" in text
    assert "healthy" not in text


def test_healthy_issue_has_rates_but_no_low_click_flag():
    issues = analyze_newsletter_click_opportunities(
        [
            {
                "newsletter_send_id": 7,
                "issue_id": "good",
                "subject": "Good issue",
                "subscriber_count": 200,
                "opens": 100,
                "clicks": 20,
                "sent_at": NOW.isoformat(),
            }
        ]
    )

    issue = issues[0]
    assert issue.issue_codes == ()
    assert issue.flagged is False
    assert issue.open_rate == 0.5
    assert issue.click_rate == 0.1
    assert issue.click_to_open_rate == 0.2
    assert issue.opportunity_score == 0.0


def test_missing_and_zero_metrics_do_not_divide_by_zero(db):
    missing = _send(db, issue_id="missing", subscribers=100)
    zero_subscribers = _send(db, issue_id="zero-subscribers", subscribers=0)
    zero_opens = _send(db, issue_id="zero-opens", subscribers=100)
    low_audience_fit = _send(db, issue_id="audience-fit", subscribers=100)
    _engagement(db, zero_subscribers, issue_id="zero-subscribers", opens=20, clicks=0)
    _engagement(db, zero_opens, issue_id="zero-opens", opens=0, clicks=0)
    _engagement(db, low_audience_fit, issue_id="audience-fit", opens=10, clicks=1)

    report = build_newsletter_click_opportunity_report(db, days=14, now=NOW)

    by_id = {issue.newsletter_send_id: issue for issue in report.issues}
    assert by_id[missing].issue_codes == (MISSING_METRICS,)
    assert by_id[missing].open_rate is None
    assert by_id[zero_subscribers].click_rate is None
    assert by_id[zero_subscribers].click_to_open_rate == 0.0
    assert by_id[zero_opens].click_to_open_rate is None
    assert by_id[low_audience_fit].issue_codes == (AUDIENCE_FIT_REVIEW,)
    assert report.totals["flagged_issue_count"] == 1


def test_filters_json_cli_and_missing_schema_are_stable(db, monkeypatch, capsys):
    old_send = _send(db, issue_id="old", sent_at=NOW - timedelta(days=90))
    target = _send(db, issue_id="target", sent_at=NOW - timedelta(days=1))
    draft = _send(db, issue_id="draft", sent_at=NOW - timedelta(days=1), status="draft")
    _engagement(db, old_send, issue_id="old", opens=80, clicks=0)
    _engagement(db, target, issue_id="target", opens=70, clicks=0)
    _engagement(db, draft, issue_id="draft", opens=70, clicks=0)
    db.conn.execute("UPDATE newsletter_sends SET status = 'draft' WHERE id = ?", (draft,))
    db.conn.commit()

    report = build_newsletter_click_opportunity_report(db, days=7, limit=1, now=NOW)
    payload = json.loads(format_newsletter_click_opportunity_json(report))

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["filters"]["limit"] == 1
    assert payload["totals"] == {
        "flagged_issue_count": 1,
        "measured_issue_count": 1,
        "send_count": 1,
    }
    assert payload["issues"][0]["issue_id"] == "target"

    monkeypatch.setattr(
        newsletter_click_opportunity_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        newsletter_click_opportunity_script,
        "build_newsletter_click_opportunity_report",
        lambda db, **kwargs: build_newsletter_click_opportunity_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )
    assert newsletter_click_opportunity_script.main(["--days", "7", "--format", "json"]) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["issues"][0]["issue_id"] == "target"
    assert newsletter_click_opportunity_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    empty_report = build_newsletter_click_opportunity_report(conn, now=NOW)
    assert empty_report.issues == ()
    assert empty_report.missing_tables == ("newsletter_sends", "newsletter_engagement")
    assert "Missing tables: newsletter_sends, newsletter_engagement" in (
        format_newsletter_click_opportunity_text(empty_report)
    )
    conn.close()
