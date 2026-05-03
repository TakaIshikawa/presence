"""Tests for newsletter section completion reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from output.newsletter_section_completion import (
    analyze_newsletter_section_completion,
    build_newsletter_section_completion_report,
    format_newsletter_section_completion_json,
    format_newsletter_section_completion_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_section_completion.py"
spec = importlib.util.spec_from_file_location("newsletter_section_completion_script", SCRIPT_PATH)
newsletter_section_completion_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_section_completion_script)


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
    body: str,
    *,
    status: str = "draft",
    days_ago: int = 0,
    metadata: dict | None = None,
) -> int:
    _ensure_body_column(db)
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=f"Subject {issue_id}",
        content_ids=[101],
        subscriber_count=100,
        status=status,
        metadata=metadata,
    )
    sent_at = NOW - timedelta(days=days_ago)
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ?, body = ? WHERE id = ?",
        (sent_at.isoformat(), body, send_id),
    )
    db.conn.commit()
    return send_id


def test_complete_draft_marks_all_required_sections_complete(db):
    _send(
        db,
        "issue-complete",
        """## Intro
Opening note explains the week and frames the operator decision.

## Work Highlights
Shipped the draft assembly checks and improved the review flow today.

## Curated Links
Read the release note and the source guide before sending this issue.

## Closing Note
Reply with the section that needs more evidence next week.
""",
    )

    report = build_newsletter_section_completion_report(
        db,
        required_sections=("intro", "work highlights", "curated links", "closing note"),
        min_section_words=5,
        now=NOW,
    )
    payload = json.loads(format_newsletter_section_completion_json(report))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_section_completion"
    assert payload["newsletter_id"] == "issue-complete"
    assert payload["totals"] == {
        "complete": 4,
        "missing": 0,
        "sections_checked": 4,
        "thin": 0,
    }
    assert {row["status"] for row in payload["rows"]} == {"complete"}
    assert all(row["observed_length"] >= row["minimum_length"] for row in payload["rows"])
    assert "Newsletter Section Completion" in format_newsletter_section_completion_text(report)


def test_missing_sections_are_reported_with_zero_observed_length():
    report = analyze_newsletter_section_completion(
        """## Intro
Opening note has enough words for the configured threshold.

## Work Highlights
This section is present and has enough words too.
""",
        required_sections=("intro", "work highlights", "curated links", "closing note"),
        min_section_words=4,
        now=NOW,
    )

    by_section = {row.section_name: row for row in report.rows}

    assert report.totals["missing"] == 2
    assert by_section["curated links"].status == "missing"
    assert by_section["curated links"].observed_length == 0
    assert by_section["curated links"].reason == "required section not found"
    assert by_section["closing note"].status == "missing"


def test_thin_sections_use_configurable_minimum_lengths():
    payload = {
        "sections": [
            {"title": "Intro", "body": "Opening is long enough now."},
            {"title": "Shipped", "body": "Tiny."},
            {"title": "Links", "body": "Read this useful link today."},
            {"title": "CTA", "body": "Reply soon."},
        ]
    }

    report = analyze_newsletter_section_completion(
        payload,
        newsletter_id="structured",
        required_sections=("intro", "work highlights", "curated links", "closing note"),
        min_section_words=3,
        section_minimums={"work highlights": 5, "closing note": 2},
        now=NOW,
    )
    by_section = {row.section_name: row for row in report.rows}

    assert by_section["intro"].status == "complete"
    assert by_section["work highlights"].status == "thin"
    assert by_section["work highlights"].matched_heading == "Shipped"
    assert by_section["work highlights"].minimum_length == 5
    assert by_section["closing note"].status == "complete"
    assert report.totals == {"complete": 3, "missing": 0, "sections_checked": 4, "thin": 1}


def test_latest_draft_and_specific_newsletter_id_selection(db):
    _send(db, "older-draft", "## Intro\nOlder body has enough words.\n", days_ago=2)
    specific_id = _send(
        db,
        "specific-draft",
        "## Intro\nSpecific draft should be selected by issue id.\n",
        days_ago=1,
    )
    _send(
        db,
        "latest-draft",
        "## Intro\nLatest draft should be selected by default.\n",
        days_ago=0,
    )
    _send(
        db,
        "sent-issue",
        "## Intro\nSent issue should not be latest draft by default.\n",
        status="sent",
        days_ago=0,
    )

    latest = build_newsletter_section_completion_report(
        db,
        required_sections=("intro",),
        min_section_words=1,
        now=NOW,
    )
    by_issue = build_newsletter_section_completion_report(
        db,
        newsletter_id="specific-draft",
        required_sections=("intro",),
        min_section_words=1,
        now=NOW,
    )
    by_id = build_newsletter_section_completion_report(
        db,
        newsletter_id=str(specific_id),
        required_sections=("intro",),
        min_section_words=1,
        now=NOW,
    )

    assert latest.newsletter_id == "latest-draft"
    assert by_issue.newsletter_id == "specific-draft"
    assert by_id.newsletter_id == "specific-draft"


def test_no_draft_available_returns_empty_report_with_warning(db):
    _send(db, "sent-only", "## Intro\nSent body.", status="sent")

    report = build_newsletter_section_completion_report(db, now=NOW)

    assert report.rows == ()
    assert report.totals == {"complete": 0, "missing": 0, "sections_checked": 0, "thin": 0}
    assert report.warnings == ("no draft newsletter found",)


def test_metadata_assembled_payload_is_supported(db):
    _send(
        db,
        "metadata-payload",
        "",
        metadata={
            "assembled_payload": {
                "sections": [
                    {"title": "Opening", "body": "Welcome to this issue."},
                    {"title": "Resources", "body": "Two links and one short note."},
                ]
            }
        },
    )

    report = build_newsletter_section_completion_report(
        db,
        required_sections=("intro", "curated links"),
        min_section_words=3,
        now=NOW,
    )

    assert [row.status for row in report.rows] == ["complete", "complete"]
    assert [row.matched_heading for row in report.rows] == ["Opening", "Resources"]


def test_cli_json_flags_and_invalid_args(db, monkeypatch, capsys):
    _send(
        db,
        "issue-cli",
        "## Intro\nOpening with enough words.\n\n## Links\nA useful reference here.\n",
    )
    monkeypatch.setattr(
        newsletter_section_completion_script,
        "script_context",
        lambda: _script_context(db),
    )

    assert newsletter_section_completion_script.main(["--min-section-words", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err

    exit_code = newsletter_section_completion_script.main(
        [
            "--newsletter-id",
            "issue-cli",
            "--required-sections",
            "intro,curated links",
            "--section-minimums",
            "curated links=2",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["newsletter_id"] == "issue-cli"
    assert payload["rows"][1]["minimum_length"] == 2
