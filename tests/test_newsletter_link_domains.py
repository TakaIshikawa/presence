"""Tests for newsletter link domain mix reporting."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from output.newsletter_link_domains import (
    build_newsletter_link_domain_report,
    format_newsletter_link_domain_json,
    format_newsletter_link_domain_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_link_domains.py"
spec = importlib.util.spec_from_file_location("newsletter_link_domains_script", SCRIPT_PATH)
newsletter_link_domains_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_link_domains_script)


def test_counts_markdown_raw_duplicate_domains_and_strips_tracking_params_for_domains():
    report = build_newsletter_link_domain_report(
        """
[Primary](https://Example.com/post?utm_source=newsletter&utm_medium=email)
Raw: https://example.com/other?utm_campaign=issue-1&utm_source=newsletter.
Again: https://partner.example/path?utm_source=newsletter
[Outside](https://outside.example/read?utm_medium=email)
""",
        preferred_domains=["example.com"],
    )
    payload = json.loads(format_newsletter_link_domain_json(report))

    assert payload["total_links"] == 4
    assert payload["unique_domains"] == 3
    assert payload["domain_counts"] == {
        "example.com": 2,
        "outside.example": 1,
        "partner.example": 1,
    }
    assert payload["internal_links"] == 2
    assert payload["external_links"] == 2
    assert payload["dominant_domains"] == [
        {"domain": "example.com", "count": 2, "share": 0.5}
    ]


def test_duplicate_links_count_occurrences_without_corrupting_unique_domain_counts():
    report = build_newsletter_link_domain_report(
        """
[A](https://example.com/a?utm_source=newsletter)
[A again](https://example.com/a?utm_source=newsletter)
https://example.com/a?utm_source=newsletter
https://other.example/path
""",
        preferred_domains=["example.com"],
    )

    links_by_url = {link.url: link for link in report.links}

    assert report.total_links == 4
    assert report.unique_domains == 2
    assert report.domain_counts == {"example.com": 3, "other.example": 1}
    assert links_by_url["https://example.com/a?utm_source=newsletter"].occurrence_count == 3


def test_invalid_urls_are_reported_from_markdown_destinations():
    report = build_newsletter_link_domain_report(
        """
[Missing host](https://?utm_source=newsletter)
[Unsupported](ftp://example.com/file)
[Relative](/local/path)
""",
    )
    payload = json.loads(format_newsletter_link_domain_json(report))

    assert payload["total_links"] == 0
    assert payload["invalid_url_count"] == 3
    assert {(item["url"], item["reason"]) for item in payload["invalid_urls"]} == {
        ("https://?utm_source=newsletter", "missing_domain"),
        ("ftp://example.com/file", "unsupported_scheme"),
        ("/local/path", "unsupported_scheme"),
    }


def test_text_report_lists_domain_mix_unpreferred_and_invalid_urls():
    report = build_newsletter_link_domain_report(
        "[A](https://example.com/a) [B](https://outside.example/b) [Bad](https://)",
        preferred_domains=["example.com"],
        source="draft.md",
    )
    text = format_newsletter_link_domain_text(report)

    assert "Newsletter Link Domains" in text
    assert "Source: draft.md" in text
    assert "Mix: 1 internal, 1 external" in text
    assert "outside.example: https://outside.example/b" in text
    assert "missing_domain: https://" in text


def test_cli_reads_file_and_emits_json_by_default(tmp_path, capsys):
    draft = tmp_path / "draft.md"
    draft.write_text(
        "[Post](https://www.Example.com/post?utm_source=newsletter) "
        "https://outside.example/read?utm_medium=email"
    )

    exit_code = newsletter_link_domains_script.main(
        [str(draft), "--preferred-domain", "https://example.com"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["source"] == str(draft)
    assert payload["domain_counts"] == {"example.com": 1, "outside.example": 1}
    assert payload["preferred_domains"] == ["example.com"]
    assert payload["internal_links"] == 1
    assert payload["external_links"] == 1


def test_cli_supports_text_format_from_stdin(monkeypatch, capsys):
    monkeypatch.setattr(
        newsletter_link_domains_script.sys,
        "stdin",
        type("Stdin", (), {"read": lambda self: "https://example.com/post"})(),
    )

    exit_code = newsletter_link_domains_script.main(
        ["-", "--preferred-domain", "example.com", "--format", "text"]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Source: stdin" in output
    assert "example.com: 1" in output


def test_cli_supports_text_argument(capsys):
    exit_code = newsletter_link_domains_script.main(
        ["--text", "[Post](https://example.com/post)", "--preferred-domain", "example.com"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["source"] == "text"
    assert payload["domain_counts"] == {"example.com": 1}
