"""Tests for newsletter link freshness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.newsletter_link_freshness import (
    build_newsletter_link_freshness_report,
    format_newsletter_link_freshness_json,
    format_newsletter_link_freshness_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_link_freshness.py"
spec = importlib.util.spec_from_file_location("newsletter_link_freshness_script", SCRIPT_PATH)
newsletter_link_freshness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_link_freshness_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _send(db, issue_id: str, *, sent_at: datetime = NOW) -> int:
    send_id = db.insert_newsletter_send(issue_id, f"Subject {issue_id}", [], subscriber_count=100)
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        (sent_at.isoformat(), send_id),
    )
    db.conn.commit()
    return int(send_id)


def _link(db, send_id: int, issue_id: str, url: str, metadata: dict | None, *, fetched_at: datetime = NOW) -> None:
    db.insert_newsletter_link_clicks(
        send_id,
        issue_id,
        [
            {
                "url": url,
                "clicks": 1,
                "raw_metrics": metadata or {},
            }
        ],
        fetched_at=fetched_at.isoformat(),
    )


def test_ranks_stale_and_missing_metadata_before_healthy_links(db):
    send_id = _send(db, "freshness")
    _link(
        db,
        send_id,
        "freshness",
        "https://healthy.example/path",
        {
            "title": "Healthy link",
            "domain": "healthy.example",
            "last_checked_at": (NOW - timedelta(days=1)).isoformat(),
        },
    )
    _link(
        db,
        send_id,
        "freshness",
        "https://stale.example/path",
        {
            "title": "Stale link",
            "domain": "stale.example",
            "last_checked_at": (NOW - timedelta(days=45)).isoformat(),
        },
    )
    _link(db, send_id, "freshness", "https://missing.example/path", {})

    report = build_newsletter_link_freshness_report(db, days=30, limit=10, now=NOW)

    assert [row.domain for row in report.links] == [
        "stale.example",
        "missing.example",
        "healthy.example",
    ]
    assert report.links[0].issue_labels == ("stale",)
    assert report.links[0].age_days == 45
    assert report.links[1].issue_labels == (
        "missing_last_checked",
        "missing_title",
        "missing_domain",
    )
    assert report.links[1].recommended_action == "refresh link metadata before send"
    assert report.totals["flagged_link_count"] == 2


def test_recent_window_limit_and_formatters_are_stable(db):
    recent = _send(db, "recent", sent_at=NOW - timedelta(days=2))
    old = _send(db, "old", sent_at=NOW - timedelta(days=90))
    _link(db, recent, "recent", "https://recent.example/a", {})
    _link(db, old, "old", "https://old.example/a", {})

    report = build_newsletter_link_freshness_report(db, days=7, limit=1, now=NOW)
    payload = json.loads(format_newsletter_link_freshness_json(report))
    text = format_newsletter_link_freshness_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_link_freshness"
    assert payload["totals"]["row_count"] == 1
    assert len(payload["links"]) == 1
    assert "recent.example" in text
    assert "old.example" not in text


def test_missing_schema_returns_empty_report_with_warnings():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_link_freshness_report(conn, now=NOW)

    assert report.links == ()
    assert report.totals["row_count"] == 0
    assert report.schema_warnings == (
        "missing table: newsletter_sends",
        "missing table: newsletter_link_clicks",
    )
    assert "Schema warnings:" in format_newsletter_link_freshness_text(report)


def test_cli_outputs_text_and_json(db, monkeypatch, capsys):
    send_id = _send(db, "cli")
    _link(db, send_id, "cli", "https://cli.example/a", {})
    monkeypatch.setattr(
        newsletter_link_freshness_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        newsletter_link_freshness_script,
        "build_newsletter_link_freshness_report",
        lambda db, **kwargs: build_newsletter_link_freshness_report(db, now=NOW, **kwargs),
    )

    assert newsletter_link_freshness_script.main(["--days", "7", "--limit", "5", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["links"][0]["domain"] == "cli.example"

    assert newsletter_link_freshness_script.main(["--days", "7"]) == 0
    assert "Newsletter Link Freshness" in capsys.readouterr().out
