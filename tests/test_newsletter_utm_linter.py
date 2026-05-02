"""Tests for pre-send newsletter UTM linting."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from output.newsletter_utm_linter import (
    build_newsletter_utm_lint_report_for_issue,
    format_newsletter_utm_lint_json,
    format_newsletter_utm_lint_text,
    lint_newsletter_utm_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "lint_newsletter_utm.py"
spec = importlib.util.spec_from_file_location("lint_newsletter_utm_script", SCRIPT_PATH)
lint_newsletter_utm_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(lint_newsletter_utm_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_lint_detects_missing_and_inconsistent_utm_parameters():
    report = lint_newsletter_utm_text(
        """
<p><a href="https://example.com/a?utm_source=newsletter&utm_medium=email&utm_campaign=weekly-1">A</a></p>
<p><a href="https://example.com/b?utm_source=social&utm_medium=email&utm_campaign=weekly-1">B</a></p>
<p>[C](https://example.com/c?utm_source=newsletter)</p>
""",
        issue_id="weekly-1",
    )
    payload = json.loads(format_newsletter_utm_lint_json(report))

    assert report.ok is False
    assert report.link_count == 3
    assert payload["issue_count"] == 3
    issues = {(issue["url"], issue["code"]) for link in payload["links"] for issue in link["issues"]}
    assert (
        "https://example.com/b?utm_source=social&utm_medium=email&utm_campaign=weekly-1",
        "inconsistent_utm_source",
    ) in issues
    assert (
        "https://example.com/c?utm_source=newsletter",
        "missing_utm_medium",
    ) in issues
    assert (
        "https://example.com/c?utm_source=newsletter",
        "missing_utm_campaign",
    ) in issues


def test_ignores_non_trackable_and_subscriber_management_links():
    report = lint_newsletter_utm_text(
        """
<a href="mailto:hello@example.com">Mail</a>
<a href="#top">Top</a>
<a href="{{ unsubscribe_url }}">Unsubscribe</a>
<a href="https://newsletter.example.com/unsubscribe?id=1">Unsubscribe</a>
<a href="https://newsletter.example.com/manage-preferences?id=1">Preferences</a>
<a href="https://localhost/post">Local</a>
<a href="https://example.com/post?utm_source=newsletter&utm_medium=email&utm_campaign=issue-7">Post</a>
""",
        issue_id="issue-7",
    )

    assert report.ok is True
    assert report.checked_count == 1
    assert report.ignored_count == 6
    assert {link.ignore_reason for link in report.links if link.ignored} == {
        "local",
        "mailto",
        "subscriber_management",
        "unsupported_scheme",
        "internal_anchor",
    }


def test_metadata_can_set_expected_campaign_values():
    report = lint_newsletter_utm_text(
        "Read https://example.com/post?utm_source=newsletter&utm_medium=email&utm_campaign=wrong",
        metadata={"utm": {"campaign": "weekly-ops"}},
        issue_id="issue-1",
    )

    assert report.expected_utm["utm_campaign"] == "weekly-ops"
    assert report.issue_count == 1
    assert report.links[0].issues[0].code == "inconsistent_utm_campaign"
    assert report.links[0].issues[0].expected == "weekly-ops"


def test_issue_lookup_lints_newsletter_send_metadata(db):
    db.insert_newsletter_send(
        issue_id="issue-99",
        subject="Weekly",
        content_ids=[],
        subscriber_count=10,
        metadata={
            "utm": {"source": "newsletter", "medium": "email", "campaign": "issue-99"},
            "html": (
                '<a href="https://example.com/a?utm_source=newsletter&utm_medium=email">'
                "A</a>"
            ),
            "cta_url": "https://example.com/b?utm_source=newsletter&utm_medium=email&utm_campaign=issue-99",
        },
    )

    report = build_newsletter_utm_lint_report_for_issue(db, "issue-99")
    urls = {link.url: link for link in report.links}

    assert report.source == "issue:issue-99"
    assert urls[
        "https://example.com/a?utm_source=newsletter&utm_medium=email"
    ].issues[0].code == "missing_utm_campaign"
    assert urls[
        "https://example.com/b?utm_source=newsletter&utm_medium=email&utm_campaign=issue-99"
    ].issues == ()


def test_text_and_json_reports_are_deterministic():
    report = lint_newsletter_utm_text(
        "Read https://example.com/post?utm_source=newsletter&utm_medium=email",
        issue_id="issue-1",
    )
    payload = json.loads(format_newsletter_utm_lint_json(report))
    text = format_newsletter_utm_lint_text(report)

    assert payload["artifact_type"] == "newsletter_utm_lint"
    assert list(payload) == sorted(payload)
    assert "Newsletter UTM Lint" in text
    assert "missing_utm_campaign" in text
    assert format_newsletter_utm_lint_json(report) == format_newsletter_utm_lint_json(report)


def test_cli_reads_file_and_exits_nonzero_for_utm_issues(tmp_path, capsys):
    draft = tmp_path / "draft.md"
    draft.write_text("[Post](https://example.com/post?utm_source=newsletter)")

    exit_code = lint_newsletter_utm_script.main(["--file", str(draft), "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["blocking_issue_count"] == 2
    assert [issue["code"] for link in payload["links"] for issue in link["issues"]] == [
        "missing_utm_medium",
        "missing_utm_campaign",
    ]


def test_cli_supports_issue_lookup(db, monkeypatch, capsys):
    db.insert_newsletter_send(
        issue_id="issue-1",
        subject="Weekly",
        content_ids=[],
        metadata={
            "body": "Read https://example.com/post?utm_source=newsletter&utm_medium=email&utm_campaign=issue-1"
        },
    )
    monkeypatch.setattr(
        lint_newsletter_utm_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = lint_newsletter_utm_script.main(["--issue-id", "issue-1", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["ok"] is True
    assert payload["source"] == "issue:issue-1"
