"""Tests for newsletter click anomaly reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from output.newsletter_click_anomalies import (
    build_newsletter_click_anomaly_report,
    format_newsletter_click_anomaly_json,
    format_newsletter_click_anomaly_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_click_anomalies.py"
spec = importlib.util.spec_from_file_location("newsletter_click_anomalies_script", SCRIPT_PATH)
newsletter_click_anomalies_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_click_anomalies_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _send(db, issue_id: str, subject: str, *, days_ago: int = 0) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject,
        content_ids=[],
        subscriber_count=100,
    )
    sent_at = NOW - timedelta(days=days_ago)
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        (sent_at.isoformat(), send_id),
    )
    db.conn.commit()
    return send_id


def _clicks(db, send_id: int, issue_id: str, rows: list[dict], *, fetched_day: int = 0) -> None:
    fetched_at = NOW + timedelta(days=fetched_day)
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id=issue_id,
        link_clicks=rows,
        fetched_at=fetched_at.isoformat(),
    )


def test_normal_distribution_has_no_anomalies(db):
    send_id = _send(db, "issue-normal", "Balanced")
    _clicks(
        db,
        send_id,
        "issue-normal",
        [
            {"url": "https://example.com/a", "clicks": 4},
            {"url": "https://example.com/b", "clicks": 3},
            {"url": "https://example.com/c", "clicks": 3},
        ],
    )

    report = build_newsletter_click_anomaly_report(db, days=30, dominance_threshold=0.8)

    assert report.total_sends_inspected == 1
    assert report.total_clicks == 10
    assert report.anomalous_sends == 0
    assert report.anomalies == ()


def test_dominant_link_is_flagged_with_subject_and_share(db):
    send_id = _send(db, "issue-dom", "Dominant")
    _clicks(
        db,
        send_id,
        "issue-dom",
        [
            {"url": "https://example.com/main", "clicks": 9},
            {"url": "https://example.com/side", "clicks": 1},
        ],
    )

    report = build_newsletter_click_anomaly_report(db, days=30, dominance_threshold=0.8)
    text = format_newsletter_click_anomaly_text(report)

    assert report.anomalous_sends == 1
    assert [(item.link_url, item.click_count, item.reason) for item in report.anomalies] == [
        ("https://example.com/main", 9, "dominant_link")
    ]
    assert report.anomalies[0].share == 0.9
    assert f"send {send_id} (Dominant)" in text
    assert "https://example.com/main" in text
    assert "9 clicks" in text
    assert "dominant_link" in text


def test_zero_click_tracked_links_are_flagged(db):
    send_id = _send(db, "issue-zero", "Zeroes")
    _clicks(
        db,
        send_id,
        "issue-zero",
        [
            {"url": "https://example.com/read", "clicks": 5},
            {"url": "https://example.com/more", "clicks": 5},
            {"url": "https://example.com/ignored", "clicks": 0},
        ],
    )

    report = build_newsletter_click_anomaly_report(db, days=30, dominance_threshold=0.95)

    assert report.total_clicks == 10
    assert [(item.link_url, item.reason) for item in report.anomalies] == [
        ("https://example.com/ignored", "zero_click_link")
    ]


def test_sends_with_no_click_rows_are_flagged(db):
    send_id = _send(db, "issue-empty", "No Rows")

    report = build_newsletter_click_anomaly_report(db, days=30)

    assert report.total_sends_inspected == 1
    assert report.anomalous_sends == 1
    assert report.anomalies[0].send_id == send_id
    assert report.anomalies[0].link_url is None
    assert report.anomalies[0].reason == "no_click_rows"


def test_send_filter_limits_inspection_and_uses_latest_click_snapshot(db):
    included = _send(db, "issue-keep", "Keep")
    excluded = _send(db, "issue-skip", "Skip")
    _clicks(
        db,
        included,
        "issue-keep",
        [{"url": "https://example.com/keep", "clicks": 1}],
        fetched_day=0,
    )
    _clicks(
        db,
        included,
        "issue-keep",
        [{"url": "https://example.com/keep", "clicks": 8}],
        fetched_day=1,
    )
    _clicks(
        db,
        excluded,
        "issue-skip",
        [{"url": "https://example.com/skip", "clicks": 10}],
    )

    report = build_newsletter_click_anomaly_report(
        db,
        days=30,
        dominance_threshold=0.8,
        send_id=included,
    )

    assert report.send_id == included
    assert [send.send_id for send in report.sends] == [included]
    assert report.total_clicks == 8
    assert report.anomalies[0].send_id == included


def test_json_output_and_cli_include_summary_counts(db, monkeypatch, capsys):
    send_id = _send(db, "issue-json", "JSON")
    _clicks(
        db,
        send_id,
        "issue-json",
        [
            {"url": "https://example.com/top", "clicks": 10},
            {"url": "https://example.com/nope", "clicks": 0},
        ],
    )
    report = build_newsletter_click_anomaly_report(db, days=30, dominance_threshold=0.7)
    payload = json.loads(format_newsletter_click_anomaly_json(report))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_click_anomalies"
    assert payload["total_sends_inspected"] == 1
    assert payload["anomalous_sends"] == 1
    assert payload["total_clicks"] == 10
    assert payload["dominance_threshold"] == 0.7

    monkeypatch.setattr(
        newsletter_click_anomalies_script,
        "script_context",
        lambda: _script_context(db),
    )
    exit_code = newsletter_click_anomalies_script.main(
        [
            "--days",
            "30",
            "--dominance-threshold",
            "0.7",
            "--send-id",
            str(send_id),
            "--format",
            "json",
        ]
    )
    cli_payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert cli_payload["send_id"] == send_id
    assert {item["reason"] for item in cli_payload["anomalies"]} == {
        "dominant_link",
        "zero_click_link",
    }
