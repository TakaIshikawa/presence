"""Tests for newsletter preview fatigue reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import sqlite3

import pytest

from evaluation.newsletter_preview_fatigue import (
    build_newsletter_preview_fatigue_report,
    format_newsletter_preview_fatigue_json,
    format_newsletter_preview_fatigue_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_preview_fatigue.py"
)
spec = importlib.util.spec_from_file_location(
    "newsletter_preview_fatigue_script",
    SCRIPT_PATH,
)
newsletter_preview_fatigue_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_preview_fatigue_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _send(
    db,
    *,
    issue_id: str,
    subject: str | None = None,
    metadata: dict | None = None,
    sent_at: datetime = NOW,
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject or f"Newsletter {issue_id}",
        content_ids=[],
        subscriber_count=10,
        metadata=metadata,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        (sent_at.isoformat(), send_id),
    )
    db.conn.commit()
    return send_id


def test_empty_report_when_no_preview_metadata_is_present(db):
    _send(db, issue_id="plain", metadata={"cta_id": "primary"})

    report = build_newsletter_preview_fatigue_report(db, days=7, now=NOW)

    assert report.has_repeats is False
    assert report.groups == ()
    assert report.totals["send_count"] == 1
    assert report.totals["preview_count"] == 0
    assert "No repeated newsletter preview openings" in format_newsletter_preview_fatigue_text(report)


def test_metadata_key_fallback_prefers_first_available_preview_key(db):
    first_id = _send(
        db,
        issue_id="fallback-1",
        subject="Fallback One",
        metadata={"preheader": "This week: the reliability notes"},
    )
    second_id = _send(
        db,
        issue_id="fallback-2",
        subject="Fallback Two",
        metadata={
            "description": "ignored because preheader wins",
            "preheader": "This week: the launch notes",
        },
    )
    _send(
        db,
        issue_id="summary",
        metadata={"summary": "Elsewhere: a different opening"},
    )

    report = build_newsletter_preview_fatigue_report(db, days=7, threshold=2, now=NOW)

    assert len(report.groups) == 1
    group = report.groups[0]
    assert group.normalized_opening == "this week"
    assert group.punctuation_pattern == ":"
    assert group.sample_previews == (
        "This week: the launch notes",
        "This week: the reliability notes",
    )
    assert [example.newsletter_send_id for example in group.examples] == [second_id, first_id]
    assert group.examples[0].issue_id == "fallback-2"
    assert group.examples[0].subject == "Fallback Two"
    assert group.examples[0].sent_at == NOW.isoformat()


def test_malformed_metadata_is_counted_and_ignored(db):
    _send(db, issue_id="valid", metadata={"preview_text": "Fresh angle: one note"})
    malformed_id = _send(db, issue_id="bad", metadata={"preview_text": "bad"})
    db.conn.execute(
        "UPDATE newsletter_sends SET metadata = ? WHERE id = ?",
        ("{not-json", malformed_id),
    )
    db.conn.commit()

    report = build_newsletter_preview_fatigue_report(db, days=7, threshold=2, now=NOW)

    assert report.groups == ()
    assert report.totals["malformed_metadata_count"] == 1
    assert report.totals["preview_count"] == 1
    assert "Malformed metadata rows: 1" in format_newsletter_preview_fatigue_text(report)


def test_repeat_detection_uses_normalized_opening_and_punctuation_pattern(db):
    _send(db, issue_id="one", metadata={"preview_text": "The Deep dive: queue metrics"})
    _send(db, issue_id="two", metadata={"preview_text": "deep dive: retry budgets"})
    _send(db, issue_id="dash", metadata={"preview_text": "Deep dive - rollout notes"})
    _send(
        db,
        issue_id="old",
        metadata={"preview_text": "deep dive: too old"},
        sent_at=NOW - timedelta(days=20),
    )

    report = build_newsletter_preview_fatigue_report(db, days=7, threshold=2, now=NOW)

    assert len(report.groups) == 1
    group = report.groups[0]
    assert group.normalized_opening == "deep dive"
    assert group.punctuation_pattern == ":"
    assert group.repeat_count == 2
    assert report.totals["repeated_send_count"] == 2


def test_threshold_controls_repeat_groups(db):
    _send(db, issue_id="one", metadata={"preview_text": "Systems note: first"})
    _send(db, issue_id="two", metadata={"preview_text": "Systems note: second"})
    _send(db, issue_id="three", metadata={"preview_text": "Another path: third"})

    report = build_newsletter_preview_fatigue_report(db, days=7, threshold=3, now=NOW)

    assert report.groups == ()
    assert report.totals["preview_count"] == 3


def test_json_and_text_formatting_include_group_details(db):
    _send(db, issue_id="json-1", subject="First", metadata={"preview_text": "Signals: alpha"})
    _send(db, issue_id="json-2", subject="Second", metadata={"preview_text": "Signals: beta"})

    report = build_newsletter_preview_fatigue_report(db, days=7, threshold=2, now=NOW)
    payload = json.loads(format_newsletter_preview_fatigue_json(report))
    text = format_newsletter_preview_fatigue_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_preview_fatigue"
    assert payload["groups"][0]["examples"][0]["issue_id"] == "json-2"
    assert payload["groups"][0]["sample_previews"] == ["Signals: beta", "Signals: alpha"]
    assert "opening='signals'" in text
    assert "issue=json-2" in text
    assert "preview='Signals: beta'" in text


def test_cli_uses_database_context_and_json_output(db, monkeypatch, capsys):
    _send(db, issue_id="cli-1", metadata={"preview_text": "Preview loop: first"})
    _send(db, issue_id="cli-2", metadata={"preview_text": "Preview loop: second"})
    monkeypatch.setattr(
        newsletter_preview_fatigue_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        newsletter_preview_fatigue_script,
        "build_newsletter_preview_fatigue_report",
        lambda db, **kwargs: build_newsletter_preview_fatigue_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = newsletter_preview_fatigue_script.main(
        ["--days", "7", "--threshold", "2", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["repeat_group_count"] == 1
    assert payload["groups"][0]["normalized_opening"] == "preview loop"


def test_cli_returns_nonzero_on_database_error(monkeypatch, capsys):
    monkeypatch.setattr(
        newsletter_preview_fatigue_script,
        "script_context",
        lambda: _script_context(SimpleNamespace()),
    )
    monkeypatch.setattr(
        newsletter_preview_fatigue_script,
        "build_newsletter_preview_fatigue_report",
        lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.Error("db failed")),
    )

    exit_code = newsletter_preview_fatigue_script.main([])

    assert exit_code == 1
    assert "error: db failed" in capsys.readouterr().err


def test_cli_invalid_arguments_exit_nonzero():
    with pytest.raises(SystemExit) as exc_info:
        newsletter_preview_fatigue_script.main(["--threshold", "0"])

    assert exc_info.value.code != 0
