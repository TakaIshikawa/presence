"""Tests for newsletter CTA placement reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from output.newsletter_cta_placement import (
    analyze_newsletter_cta_placements,
    build_newsletter_cta_placement_report,
    format_newsletter_cta_placement_json,
    format_newsletter_cta_placement_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_cta_placement.py"
spec = importlib.util.spec_from_file_location("newsletter_cta_placement_script", SCRIPT_PATH)
newsletter_cta_placement_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_cta_placement_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _ensure_body_column(db) -> None:
    columns = {row["name"] for row in db.conn.execute("PRAGMA table_info(newsletter_sends)")}
    if "body" not in columns:
        db.conn.execute("ALTER TABLE newsletter_sends ADD COLUMN body TEXT")
        db.conn.commit()


def _send(
    db,
    issue_id: str,
    subject: str,
    *,
    body: str | None,
    metadata: dict | None = None,
    status: str = "draft",
    days_ago: int = 0,
) -> int:
    _ensure_body_column(db)
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject,
        content_ids=[101],
        subscriber_count=100,
        status=status,
        metadata=metadata or {},
    )
    sent_at = NOW - timedelta(days=days_ago)
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ?, body = ? WHERE id = ?",
        (sent_at.isoformat(), body, send_id),
    )
    db.conn.commit()
    return send_id


def test_clean_newsletter_has_no_cta_placement_warnings(db):
    _send(
        db,
        "issue-clean",
        "Useful launch notes",
        body=(
            "Here is the main update for the week.\n\n"
            "The second note adds context and source detail.\n\n"
            "Reply with the workflow you want covered next."
        ),
    )

    report = build_newsletter_cta_placement_report(db, days=30, limit=5)
    text = format_newsletter_cta_placement_text(report)

    assert report.total_rows_inspected == 1
    assert report.flagged_rows == 0
    assert report.issues[0].warnings == ()
    assert report.issues[0].first_cta_paragraph == 3
    assert "warnings=clean" in text


def test_missing_cta_is_flagged(db):
    _send(
        db,
        "issue-missing",
        "No action requested",
        body="A summary paragraph.\n\nA second paragraph with no reader action.",
    )

    issue = build_newsletter_cta_placement_report(db, days=30, limit=5).issues[0]

    assert issue.warnings == ("missing_cta",)
    assert issue.cta_count == 0
    assert issue.first_cta_paragraph is None


def test_cta_only_in_first_paragraph_is_flagged(db):
    _send(
        db,
        "issue-early",
        "Action too soon",
        body=(
            "Reply if this is useful.\n\n"
            "The body then explains the evidence.\n\n"
            "The close has no additional action."
        ),
    )

    issue = build_newsletter_cta_placement_report(db, days=30, limit=5).issues[0]

    assert issue.warnings == ("cta_only_first_paragraph",)
    assert issue.first_cta_paragraph == 1


def test_repeated_identical_cta_is_flagged(db):
    cta = "Reply with the workflow you want covered next."
    _send(
        db,
        "issue-repeat",
        "Repeated CTA",
        body=f"Intro paragraph.\n\n{cta}\n\nSupporting detail.\n\n{cta}",
    )

    issue = build_newsletter_cta_placement_report(db, days=30, limit=5).issues[0]

    assert issue.warnings == ("repeated_identical_cta",)
    assert issue.repeated_ctas == (cta,)
    assert issue.cta_count == 2


def test_cta_after_paragraph_threshold_is_flagged_and_configurable(db):
    _send(
        db,
        "issue-late",
        "Late action",
        body=(
            "One.\n\n"
            "Two.\n\n"
            "Three.\n\n"
            "Four.\n\n"
            "Reply with the workflow you want covered next."
        ),
    )

    report = build_newsletter_cta_placement_report(
        db,
        days=30,
        limit=5,
        paragraph_threshold=3,
    )
    relaxed = build_newsletter_cta_placement_report(
        db,
        days=30,
        limit=5,
        paragraph_threshold=5,
    )

    assert report.issues[0].warnings == ("cta_after_paragraph_threshold",)
    assert report.issues[0].first_cta_paragraph == 5
    assert relaxed.issues[0].warnings == ()


def test_marker_patterns_are_configurable():
    rows = [
        {
            "id": 1,
            "issue_id": "issue-custom",
            "subject": "Custom marker",
            "status": "draft",
            "body": "Intro.\n\nTap the waitlist link when you are ready.",
        }
    ]

    default_issue = analyze_newsletter_cta_placements(rows)[0]
    custom_issue = analyze_newsletter_cta_placements(
        rows,
        cta_marker_patterns=(r"\bwaitlist\b",),
    )[0]

    assert default_issue.warnings == ("missing_cta",)
    assert custom_issue.warnings == ()
    assert custom_issue.occurrences[0].marker == r"\bwaitlist\b"


def test_link_metadata_can_supply_cta_detection(db):
    _send(
        db,
        "issue-metadata",
        "CTA in metadata",
        body="Intro.\n\nDetails.",
        metadata={"links": [{"label": "Book a demo", "url": "https://example.com/demo"}]},
    )

    issue = build_newsletter_cta_placement_report(db, days=30, limit=5).issues[0]

    assert issue.warnings == ()
    assert issue.cta_count == 1
    assert issue.occurrences[0].text == "Book a demo https://example.com/demo"


def test_cli_json_output_is_sorted_and_limited(db, monkeypatch, capsys):
    _send(
        db,
        "issue-json",
        "CTA JSON",
        body="Intro.\n\nDetails.\n\nReply with what you need next.",
    )
    _send(
        db,
        "issue-old",
        "Old",
        body="Intro.\n\nReply with what you need next.",
        days_ago=45,
    )
    report = build_newsletter_cta_placement_report(db, days=30, limit=1)
    payload = json.loads(format_newsletter_cta_placement_json(report))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_cta_placement"
    assert payload["total_rows_inspected"] == 1
    assert payload["issues"][0]["issue_id"] == "issue-json"

    monkeypatch.setattr(
        newsletter_cta_placement_script,
        "script_context",
        lambda: _script_context(db),
    )
    exit_code = newsletter_cta_placement_script.main(
        ["--days", "30", "--limit", "1", "--json"]
    )
    cli_payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert list(cli_payload) == sorted(cli_payload)
    assert cli_payload["issues"][0]["issue_id"] == "issue-json"
