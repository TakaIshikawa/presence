"""Tests for newsletter draft section-balance reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.newsletter_section_balance import (
    analyze_newsletter_section_balance,
    build_newsletter_section_balance_report,
    build_newsletter_section_balance_report_from_text,
    format_newsletter_section_balance_json,
    format_newsletter_section_balance_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_section_balance.py"
spec = importlib.util.spec_from_file_location("newsletter_section_balance_script", SCRIPT_PATH)
newsletter_section_balance_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_section_balance_script)


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
    body: str,
    *,
    days_ago: int = 0,
) -> int:
    _ensure_body_column(db)
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject,
        content_ids=[101],
        subscriber_count=100,
        status="draft",
    )
    sent_at = NOW - timedelta(days=days_ago)
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ?, body = ? WHERE id = ?",
        (sent_at.isoformat(), body, send_id),
    )
    db.conn.commit()
    return send_id


def test_balanced_newsletter_counts_sections_without_warnings():
    body = """# Intro
This week starts with one practical note for readers.

## Shipped
We shipped the queue audit. It adds clearer checks before delivery.

## Learned
The lesson was to keep the review surface small and repeatable.

## Links
Read the short write-up at https://example.com/writeup.

## CTA
Reply with the section that needs more detail next week.
"""

    issue = analyze_newsletter_section_balance(
        body,
        newsletter_id="issue-balanced",
        required_sections=("intro", "shipped", "learned", "links", "cta"),
        max_section_word_share=0.5,
    )

    assert issue.warnings == ()
    assert issue.total_sections == 5
    assert issue.total_headings == 5
    assert issue.total_paragraphs == 5
    assert issue.total_links == 1
    assert [section.heading for section in issue.sections] == [
        "Intro",
        "Shipped",
        "Learned",
        "Links",
        "CTA",
    ]
    assert issue.sections[3].link_count == 1
    assert sum(section.word_count for section in issue.sections) == issue.total_words


def test_dominated_section_is_flagged_with_share_and_paragraph_counts():
    body = """## Intro
Short opening.

## Shipped
""" + "\n\n".join(
        [
            " ".join(
                [
                    "delivery",
                    "preflight",
                    "section",
                    "balance",
                    "newsletter",
                    "draft",
                    "review",
                    "quality",
                ]
            )
            for _ in range(5)
        ]
    ) + """

## Learned
Small takeaway.

## Links
https://example.com

## CTA
Reply anytime.
"""

    issue = analyze_newsletter_section_balance(
        body,
        required_sections=("intro", "shipped", "learned", "links", "cta"),
        max_section_word_share=0.55,
    )

    assert issue.warnings == ("dominant_section:Shipped",)
    shipped = next(section for section in issue.sections if section.heading == "Shipped")
    assert shipped.paragraph_count == 5
    assert shipped.word_share > 0.55


def test_missing_required_sections_are_flagged():
    body = """## Intro
Opening note.

## Shipped
A compact release note.
"""

    issue = analyze_newsletter_section_balance(
        body,
        required_sections=("intro", "shipped", "learned", "links", "cta"),
        max_section_word_share=0.9,
    )

    assert issue.warnings == (
        "missing_required_section:learned",
        "missing_required_section:links",
        "missing_required_section:cta",
    )


def test_structured_sections_are_supported():
    issue = analyze_newsletter_section_balance(
        {
            "sections": [
                {"title": "Intro", "paragraphs": ["Opening note."]},
                {
                    "title": "Links",
                    "body": "Read https://example.com and https://example.com/next.",
                },
            ]
        },
        required_sections=("intro", "links"),
        max_section_word_share=0.8,
    )

    assert issue.total_sections == 2
    assert issue.total_links == 2
    assert issue.warnings == ()
    assert issue.sections[0].heading_level is None


def test_report_json_text_and_database_loading(db):
    _send(
        db,
        "issue-db",
        "Balanced issue",
        """## Intro
Opening note.

## Shipped
Small shipped note.

## Learned
Small lesson.

## Links
Read https://example.com.

## CTA
Reply with questions.
""",
    )

    report = build_newsletter_section_balance_report(
        db,
        days=30,
        limit=5,
        required_sections=("intro", "shipped", "learned", "links", "cta"),
        max_section_word_share=0.6,
    )
    payload = json.loads(format_newsletter_section_balance_json(report))
    text = format_newsletter_section_balance_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_section_balance"
    assert payload["totals"]["drafts_scanned"] == 1
    assert payload["issues"][0]["newsletter_id"] == "issue-db"
    assert "Newsletter Section Balance" in text
    assert "warnings=clean" in text


def test_cli_supports_database_json_and_file_input(db, tmp_path, monkeypatch, capsys):
    _send(
        db,
        "issue-cli",
        "CLI issue",
        "## Intro\nOpening.\n\n## Shipped\nBuilt it.\n",
    )
    monkeypatch.setattr(
        newsletter_section_balance_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = newsletter_section_balance_script.main(
        [
            "--limit",
            "1",
            "--required-sections",
            "intro,shipped",
            "--max-section-word-share",
            "0.9",
            "--format",
            "json",
        ]
    )
    cli_payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert cli_payload["issues"][0]["newsletter_id"] == "issue-cli"

    input_path = tmp_path / "newsletter.md"
    input_path.write_text("## Intro\nOpening.\n\n## Links\nhttps://example.com\n")

    exit_code = newsletter_section_balance_script.main(
        [
            "--input",
            str(input_path),
            "--required-sections",
            "intro,links",
            "--format",
            "json",
        ]
    )
    file_payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert file_payload["filters"]["input"] == str(input_path)
    assert file_payload["issues"][0]["total_links"] == 1


def test_text_input_report_builder():
    report = build_newsletter_section_balance_report_from_text(
        "## Intro\nOpening only.\n",
        required_sections=("intro", "links"),
        max_section_word_share=1.0,
        now=NOW,
    )

    assert report.totals["drafts_scanned"] == 1
    assert report.issues[0].warnings == ("missing_required_section:links",)
