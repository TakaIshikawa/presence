"""Tests for newsletter CTA performance reporting."""

from __future__ import annotations

import importlib.util
import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.newsletter_cta_performance import NewsletterCtaPerformance


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_cta_performance.py"
)
spec = importlib.util.spec_from_file_location(
    "newsletter_cta_performance_script", SCRIPT_PATH
)
newsletter_cta_performance_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(newsletter_cta_performance_script)

BASE_TIME = datetime.now(timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _record_send(
    db,
    issue_id: str,
    *,
    cta_metadata: dict | None,
    subscribers: int,
    opens: int,
    clicks: int,
    unsubscribes: int = 0,
    link_clicks: int = 0,
    sent_days_ago: int = 0,
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=f"Subject {issue_id}",
        content_ids=[],
        subscriber_count=subscribers,
        metadata=cta_metadata,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        ((BASE_TIME - timedelta(days=sent_days_ago)).isoformat(), send_id),
    )
    db.conn.commit()
    db.insert_newsletter_engagement(
        newsletter_send_id=send_id,
        issue_id=issue_id,
        opens=1,
        clicks=1,
        unsubscribes=0,
        fetched_at=(BASE_TIME - timedelta(hours=2)).isoformat(),
    )
    db.insert_newsletter_engagement(
        newsletter_send_id=send_id,
        issue_id=issue_id,
        opens=opens,
        clicks=clicks,
        unsubscribes=unsubscribes,
        fetched_at=(BASE_TIME - timedelta(hours=1)).isoformat(),
    )
    if link_clicks:
        db.insert_newsletter_link_clicks(
            newsletter_send_id=send_id,
            issue_id=issue_id,
            link_clicks=[{"url": f"https://example.com/{issue_id}", "clicks": 1}],
            fetched_at=(BASE_TIME - timedelta(hours=2)).isoformat(),
        )
        db.insert_newsletter_link_clicks(
            newsletter_send_id=send_id,
            issue_id=issue_id,
            link_clicks=[
                {"url": f"https://example.com/{issue_id}", "clicks": link_clicks}
            ],
            fetched_at=(BASE_TIME - timedelta(hours=1)).isoformat(),
        )
    return send_id


def test_report_aggregates_cta_metrics_from_latest_snapshots(db):
    _record_send(
        db,
        "issue-demo-1",
        cta_metadata={"cta_id": "demo"},
        subscribers=100,
        opens=50,
        clicks=8,
        link_clicks=4,
    )
    _record_send(
        db,
        "issue-demo-2",
        cta_metadata={"newsletter_cta_id": "demo"},
        subscribers=100,
        opens=30,
        clicks=3,
        unsubscribes=1,
        link_clicks=2,
    )
    _record_send(
        db,
        "issue-signup",
        cta_metadata={"cta": {"id": "signup"}},
        subscribers=50,
        opens=20,
        clicks=5,
        link_clicks=5,
    )

    report = NewsletterCtaPerformance(db).summarize(days=30, min_sends=1, limit=10)

    assert report.total_sends == 3
    demo = next(cta for cta in report.ctas if cta.cta_id == "demo")
    assert demo.sends == 2
    assert demo.opens == 80
    assert demo.clicks == 11
    assert demo.link_clicks == 6
    assert demo.unsubscribes == 1
    assert demo.open_rate == 0.4
    assert demo.click_rate == 0.055
    assert demo.unsubscribe_rate == 0.005
    assert demo.best_examples[0].issue_id == "issue-demo-1"
    assert demo.worst_examples[0].issue_id == "issue-demo-2"


def test_report_keeps_sends_without_cta_metadata_in_unknown_bucket(db):
    _record_send(
        db,
        "issue-unknown",
        cta_metadata={},
        subscribers=100,
        opens=25,
        clicks=2,
    )
    send_id = _record_send(
        db,
        "issue-malformed",
        cta_metadata={"cta_id": "will-be-overwritten"},
        subscribers=100,
        opens=20,
        clicks=1,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET metadata = ? WHERE id = ?",
        ("not-json", send_id),
    )
    db.conn.commit()

    report = NewsletterCtaPerformance(db).summarize(days=30, min_sends=1)

    unknown = next(cta for cta in report.ctas if cta.cta_id == "unknown")
    assert report.unknown_sends == 2
    assert unknown.sends == 2
    assert unknown.clicks == 3


def test_report_filters_by_lookback_window_and_min_send_count(db):
    _record_send(
        db,
        "issue-demo-1",
        cta_metadata={"cta_id": "demo"},
        subscribers=100,
        opens=40,
        clicks=4,
    )
    _record_send(
        db,
        "issue-demo-2",
        cta_metadata={"cta_id": "demo"},
        subscribers=100,
        opens=30,
        clicks=3,
    )
    _record_send(
        db,
        "issue-once",
        cta_metadata={"cta_id": "once"},
        subscribers=100,
        opens=80,
        clicks=10,
    )
    _record_send(
        db,
        "issue-old",
        cta_metadata={"cta_id": "demo"},
        subscribers=100,
        opens=90,
        clicks=20,
        sent_days_ago=45,
    )

    report = NewsletterCtaPerformance(db).summarize(days=30, min_sends=2)

    assert [cta.cta_id for cta in report.ctas] == ["demo"]
    assert report.total_sends == 3
    assert report.ctas[0].sends == 2
    assert report.ctas[0].clicks == 7


def test_script_outputs_stable_json_and_concise_text(db, capsys):
    _record_send(
        db,
        "issue-json",
        cta_metadata={"cta_id": "demo"},
        subscribers=100,
        opens=50,
        clicks=6,
    )

    with patch.object(
        newsletter_cta_performance_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = newsletter_cta_performance_script.main(
            ["--days", "30", "--min-sends", "1", "--json", "--limit", "5"]
        )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["period_days"] == 30
    assert payload["ctas"][0]["cta_id"] == "demo"
    assert payload["ctas"][0]["best_examples"][0]["issue_id"] == "issue-json"

    with patch.object(
        newsletter_cta_performance_script,
        "script_context",
        return_value=_script_context(db),
    ):
        newsletter_cta_performance_script.main(["--days", "30"])

    text = capsys.readouterr().out
    assert "Newsletter CTA Performance (last 30 days)" in text
    assert "demo: 1 sends" in text
