"""Tests for scripts/newsletter_subject_report.py."""

import importlib.util
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_subject_report.py"
)
spec = importlib.util.spec_from_file_location("newsletter_subject_report", SCRIPT_PATH)
newsletter_subject_report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(newsletter_subject_report)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _record_issue(db):
    send_id = db.insert_newsletter_send(
        issue_id="issue-report",
        subject="Practical AI notes",
        content_ids=[1],
        subscriber_count=100,
    )
    db.insert_newsletter_subject_candidates(
        [
            {"subject": "Practical AI notes", "score": 8.5},
            {"subject": "Weekly Digest", "score": 6.0},
        ],
        selected_subject="Practical AI notes",
        newsletter_send_id=send_id,
        issue_id="issue-report",
    )
    db.insert_newsletter_engagement(
        newsletter_send_id=send_id,
        issue_id="issue-report",
        opens=50,
        clicks=6,
        unsubscribes=0,
    )


def test_format_text_report_shows_ranked_subjects_and_alternatives(db):
    _record_issue(db)
    summary = newsletter_subject_report.NewsletterSubjectPerformance(db).summarize(
        days=30
    )

    text = newsletter_subject_report.format_text_report(summary)

    assert "Newsletter Subject Performance (last 30 days)" in text
    assert "1. Practical AI notes" in text
    assert "opens 50.0%" in text
    assert "Weekly Digest" in text


def test_format_json_report_serializes_summary(db):
    _record_issue(db)
    summary = newsletter_subject_report.NewsletterSubjectPerformance(db).summarize(
        days=30
    )

    data = json.loads(newsletter_subject_report.format_json_report(summary))

    assert data["subject_count"] == 1
    assert data["ranked_subjects"][0]["subject"] == "Practical AI notes"
    assert data["ranked_subjects"][0]["alternatives"][0]["subject"] == "Weekly Digest"


def test_main_supports_days_and_json_format(db, capsys):
    _record_issue(db)

    with patch.object(
        newsletter_subject_report,
        "script_context",
        return_value=_script_context(db),
    ):
        newsletter_subject_report.main(["--days", "30", "--format", "json"])

    data = json.loads(capsys.readouterr().out)
    assert data["period_days"] == 30
    assert data["best_subject"]["subject"] == "Practical AI notes"
