"""Tests for newsletter spam-trigger drift reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from output.newsletter_spam_trigger_drift import (
    EXCESSIVE_PUNCTUATION,
    build_newsletter_spam_trigger_drift_report,
    format_newsletter_spam_trigger_drift_json,
    format_newsletter_spam_trigger_drift_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "newsletter_spam_trigger_drift.py"
)
spec = importlib.util.spec_from_file_location("newsletter_spam_trigger_drift_script", SCRIPT_PATH)
newsletter_spam_trigger_drift_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_spam_trigger_drift_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _send(
    db,
    *,
    issue_id: str,
    subject: str,
    metadata: dict | None = None,
    sent_at: datetime = NOW,
    status: str = "sent",
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject,
        content_ids=[],
        subscriber_count=10,
        status=status,
        metadata=metadata,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        (sent_at.isoformat(), send_id),
    )
    db.conn.commit()
    return send_id


def test_matches_across_subject_preview_and_body_metadata(db):
    _send(
        db,
        issue_id="one",
        subject="Limited time reliability fixes",
        metadata={"preview": "A careful release note", "body": "Read the details."},
        sent_at=NOW - timedelta(days=2),
    )
    _send(
        db,
        issue_id="two",
        subject="Delivery note",
        metadata={
            "preheader": "Limited time routing cleanup",
            "sections": [{"body": "This is not urgent."}],
        },
        sent_at=NOW - timedelta(days=1),
    )
    _send(
        db,
        issue_id="three",
        subject="Calm note",
        metadata={"content": "A limited time source audit."},
    )

    report = build_newsletter_spam_trigger_drift_report(
        db,
        days=7,
        threshold=2,
        now=NOW,
    )
    payload = json.loads(format_newsletter_spam_trigger_drift_json(report))
    text = format_newsletter_spam_trigger_drift_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_spam_trigger_drift"
    assert payload["totals"]["send_count"] == 3
    assert payload["totals"]["triggered_send_count"] == 3
    finding = next(
        item for item in payload["findings"] if item["normalized_phrase"] == "limited time"
    )
    assert finding["category"] == "urgency"
    assert finding["send_count"] == 3
    assert finding["field_counts"] == {
        "metadata.content": 1,
        "metadata.preheader": 1,
        "subject": 1,
    }
    assert finding["examples"][0]["issue_id"] == "three"
    assert finding["examples"][0]["field"] == "metadata.content"
    assert "Newsletter Spam Trigger Drift" in text
    assert "Spam trigger drift findings:" in text
    assert "issue=three" in text


def test_normalization_and_excessive_punctuation_are_grouped(db):
    _send(
        db,
        issue_id="one",
        subject="Risk-Free updates!!!",
        metadata={"preview_text": "No fluff."},
        sent_at=NOW - timedelta(hours=3),
    )
    _send(
        db,
        issue_id="two",
        subject="Risk free rollout???",
        metadata={"preview_text": "No fluff."},
        sent_at=NOW - timedelta(hours=2),
    )

    report = build_newsletter_spam_trigger_drift_report(
        db,
        days=7,
        threshold=2,
        now=NOW,
    )
    findings = {
        (finding.category, finding.normalized_phrase): finding
        for finding in report.findings
    }

    assert findings[("financial_claims", "risk free")].send_count == 2
    assert findings[("excessive_punctuation", EXCESSIVE_PUNCTUATION)].send_count == 2


def test_no_trigger_drift_text_is_clear(db):
    _send(
        db,
        issue_id="one",
        subject="Reliability notes",
        metadata={"preview": "A calm summary", "body": "Details without risky claims."},
    )

    report = build_newsletter_spam_trigger_drift_report(
        db,
        days=7,
        threshold=2,
        now=NOW,
    )

    assert report.has_findings is False
    assert report.findings == ()
    assert "No newsletter spam trigger drift found." in (
        format_newsletter_spam_trigger_drift_text(report)
    )


def test_malformed_metadata_is_counted_without_blocking_subject_matches(db):
    first_id = _send(
        db,
        issue_id="one",
        subject="Act now on delivery",
        metadata={"preview": "Valid"},
        sent_at=NOW - timedelta(hours=2),
    )
    second_id = _send(
        db,
        issue_id="two",
        subject="ACT NOW on routing",
        metadata={"preview": "Will be malformed"},
        sent_at=NOW - timedelta(hours=1),
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET metadata = ? WHERE id IN (?, ?)",
        ("{not-json", first_id, second_id),
    )
    db.conn.commit()

    report = build_newsletter_spam_trigger_drift_report(
        db,
        days=7,
        threshold=2,
        now=NOW,
    )

    assert report.totals["malformed_metadata_count"] == 2
    assert sorted(report.warnings) == [
        f"malformed_metadata:{first_id}",
        f"malformed_metadata:{second_id}",
    ]
    assert report.findings[0].normalized_phrase == "act now"
    assert report.findings[0].send_count == 2


def test_schema_gaps_for_missing_table_and_columns():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_spam_trigger_drift_report(conn, now=NOW)

    assert report.missing_tables == ("newsletter_sends",)
    assert report.totals["send_count"] == 0
    assert "Missing tables: newsletter_sends" in format_newsletter_spam_trigger_drift_text(report)

    conn.execute(
        """CREATE TABLE newsletter_sends (
               id INTEGER PRIMARY KEY,
               subject TEXT,
               sent_at TEXT
           )"""
    )
    legacy_report = build_newsletter_spam_trigger_drift_report(conn, now=NOW)

    assert legacy_report.missing_columns == {
        "newsletter_sends": ("issue_id", "metadata")
    }
    assert "Missing columns: newsletter_sends(issue_id, metadata)" in (
        format_newsletter_spam_trigger_drift_text(legacy_report)
    )


def test_cli_json_output_and_invalid_args(db, monkeypatch, capsys):
    _send(
        db,
        issue_id="one",
        subject="Last chance delivery note",
        metadata={"preview": "Useful detail"},
        sent_at=NOW - timedelta(minutes=2),
    )
    _send(
        db,
        issue_id="two",
        subject="Delivery note",
        metadata={"preheader": "Last chance to review"},
        sent_at=NOW - timedelta(minutes=1),
    )
    monkeypatch.setattr(
        newsletter_spam_trigger_drift_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        newsletter_spam_trigger_drift_script,
        "build_newsletter_spam_trigger_drift_report",
        lambda db, **kwargs: build_newsletter_spam_trigger_drift_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert newsletter_spam_trigger_drift_script.main(
        ["--days", "7", "--threshold", "2", "--format", "json"]
    ) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["filters"]["days"] == 7
    assert payload["filters"]["threshold"] == 2
    assert payload["findings"][0]["normalized_phrase"] == "last chance"

    assert newsletter_spam_trigger_drift_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert newsletter_spam_trigger_drift_script.main(["--threshold", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
