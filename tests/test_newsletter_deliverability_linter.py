"""Tests for newsletter deliverability linting."""

from __future__ import annotations

import importlib.util
import io
import json
import sys
from pathlib import Path
from unittest.mock import patch

from output.newsletter_deliverability_linter import (
    format_newsletter_deliverability_json,
    format_newsletter_deliverability_text,
    lint_newsletter_deliverability,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "lint_newsletter_deliverability.py"
)
spec = importlib.util.spec_from_file_location(
    "lint_newsletter_deliverability_script",
    SCRIPT_PATH,
)
lint_newsletter_deliverability_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(lint_newsletter_deliverability_script)


def _clean_html() -> str:
    return """
<article>
  <p>This week: a concise product note with one practical example.</p>
  <p><a href="https://example.com/post">Read the full post</a></p>
  <p><a href="{{ unsubscribe_url }}">Unsubscribe</a></p>
</article>
"""


def test_clean_draft_has_no_issues_and_json_is_deterministic():
    report = lint_newsletter_deliverability(
        subject="A practical note on release rhythm",
        preheader="A short weekly note on shipping habits.",
        html=_clean_html(),
        plaintext="This week: a concise product note.\nUnsubscribe: {{ unsubscribe_url }}",
    )
    payload = json.loads(format_newsletter_deliverability_json(report))
    text = format_newsletter_deliverability_text(report)

    assert report.ok is True
    assert report.issues == ()
    assert payload["ok"] is True
    assert payload["issues"] == []
    assert list(payload) == sorted(payload)
    assert "No newsletter deliverability issues found." in text
    assert format_newsletter_deliverability_json(
        report
    ) == format_newsletter_deliverability_json(report)


def test_blocking_issues_cover_plaintext_and_unsubscribe_placeholder():
    report = lint_newsletter_deliverability(
        subject="Weekly operations notes",
        preheader="This week's field notes.",
        html='<p>Useful notes. <a href="{unsubscribe_url}">Unsubscribe</a></p>',
        plaintext="",
    )
    by_code = {issue.code: issue for issue in report.issues}

    assert report.ok is False
    assert report.blocking_issue_count == 2
    assert by_code["missing_plaintext_body"].severity == "error"
    assert by_code["missing_plaintext_body"].target == "plaintext"
    assert by_code["broken_unsubscribe_placeholder"].severity == "error"
    assert by_code["broken_unsubscribe_placeholder"].target == "html"
    assert all(issue.remediation_hint for issue in report.issues)


def test_warnings_cover_subject_preheader_links_and_repeated_ctas():
    links = "\n".join(
        f'<a href="https://example.com/{index}">Learn more</a>'
        for index in range(1, 7)
    )
    report = lint_newsletter_deliverability(
        subject="URGENT FREE OFFER",
        preheader="Preview " + ("text " * 40),
        html=f"<div>{links}</div>",
        plaintext="Plaintext body is present.",
        max_links=5,
        max_preheader_chars=80,
    )
    codes = [issue.code for issue in report.issues]

    assert report.ok is True
    assert report.blocking_issue_count == 0
    assert "all_caps_subject" in codes
    assert "spammy_subject_pattern" in codes
    assert "oversized_preview_text" in codes
    assert "excessive_links" in codes
    assert "repeated_cta" in codes
    assert {issue.severity for issue in report.issues} == {"warning"}


def test_subject_html_plaintext_and_preheader_checks_are_independent():
    subject_only = lint_newsletter_deliverability(
        subject="WINNER",
        preheader="",
        html="<p>Footer managed by Buttondown.</p>",
        plaintext="Plaintext body.",
    )
    plaintext_only = lint_newsletter_deliverability(
        subject="Weekly note",
        preheader="",
        html="<p>Footer managed by Buttondown.</p>",
        plaintext="",
    )
    html_only = lint_newsletter_deliverability(
        subject="Weekly note",
        preheader="",
        html='<a href="{unsubscribe_url}">Unsubscribe</a>',
        plaintext="Plaintext body.",
    )
    preheader_only = lint_newsletter_deliverability(
        subject="Weekly note",
        preheader="x" * 141,
        html="<p>Footer managed by Buttondown.</p>",
        plaintext="Plaintext body.",
    )

    assert [issue.target for issue in subject_only.issues] == ["subject"]
    assert [issue.target for issue in plaintext_only.issues] == ["plaintext"]
    assert [issue.target for issue in html_only.issues] == ["html"]
    assert [issue.target for issue in preheader_only.issues] == ["preheader"]


def test_cli_reads_file_outputs_json_and_exits_for_blocking_issues(tmp_path, capsys):
    draft = tmp_path / "draft.html"
    draft.write_text('<p>Body <a href="{unsubscribe_url}">Unsubscribe</a></p>')

    result = lint_newsletter_deliverability_script.main(
        [
            str(draft),
            "--subject",
            "Weekly note",
            "--preheader",
            "Short preview",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert result == 1
    assert payload["blocking_issue_count"] == 2
    assert [issue["code"] for issue in payload["issues"]] == [
        "broken_unsubscribe_placeholder",
        "missing_plaintext_body",
    ]


def test_cli_reads_stdin_and_plaintext_file_for_clean_draft(tmp_path, capsys):
    plaintext = tmp_path / "plain.txt"
    plaintext.write_text("Plain body\nUnsubscribe: {{ unsubscribe_url }}")

    with patch.object(sys, "stdin", io.StringIO(_clean_html())):
        result = lint_newsletter_deliverability_script.main(
            [
                "-",
                "--subject",
                "Weekly note",
                "--preheader",
                "Short preview",
                "--plaintext-file",
                str(plaintext),
                "--json",
            ]
        )
    payload = json.loads(capsys.readouterr().out)

    assert result == 0
    assert payload["ok"] is True
    assert payload["issues"] == []


def test_cli_reports_validation_errors(capsys):
    result = lint_newsletter_deliverability_script.main(["--max-links", "-1"])
    captured = capsys.readouterr()

    assert result == 1
    assert "max_links must be non-negative" in captured.err
