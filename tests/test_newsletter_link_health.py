"""Tests for pre-send newsletter link health checks."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import patch

from output.newsletter_link_health import (
    FetchResult,
    check_newsletter_links,
    dedupe_links,
    extract_newsletter_links,
    format_newsletter_link_health_json,
    format_newsletter_link_health_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "check_newsletter_links.py"
spec = importlib.util.spec_from_file_location("check_newsletter_links_script", SCRIPT_PATH)
check_newsletter_links_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(check_newsletter_links_script)


def test_extracts_links_from_subject_plaintext_and_html():
    links = extract_newsletter_links(
        subject="This week https://example.com/subject.",
        body=(
            "Read [the post](https://example.com/post?utm_source=n&utm_medium=e&utm_campaign=w).\n"
            "Bare https://example.com/bare, and ignored image "
            "![alt](https://cdn.example.com/image.png)"
        ),
        html=(
            '<p><a href="https://example.com/html">HTML</a></p>'
            '<p><a href="#footer">Footer</a></p>'
            '<p>Visible https://example.com/visible.</p>'
        ),
    )

    assert [link.url for link in links] == [
        "https://example.com/subject",
        "https://example.com/post?utm_source=n&utm_medium=e&utm_campaign=w",
        "https://example.com/bare",
        "https://example.com/html",
        "#footer",
        "https://example.com/visible",
    ]
    assert [link.source for link in links] == ["subject", "body", "body", "html", "html", "html"]


def test_deduplicates_urls_preserving_first_seen_order():
    links = extract_newsletter_links(
        subject="https://example.com/a",
        body="Again https://example.com/a and then https://example.com/b",
        html='<a href="https://example.com/b">B</a><a href="mailto:team@example.com">email</a>',
    )

    grouped = dedupe_links(links)

    assert [url for url, _occurrences in grouped] == [
        "https://example.com/a",
        "https://example.com/b",
        "mailto:team@example.com",
    ]
    assert [len(occurrences) for _url, occurrences in grouped] == [2, 2, 1]


def test_classifies_healthy_redirected_broken_skipped_and_missing_utm():
    calls: list[tuple[str, float]] = []

    def fake_fetcher(url: str, timeout: float) -> FetchResult:
        calls.append((url, timeout))
        if url.endswith("/ok?utm_source=n&utm_medium=e&utm_campaign=w"):
            return FetchResult(status_code=200)
        if url.endswith("/redirect?utm_source=n&utm_medium=e&utm_campaign=w"):
            return FetchResult(status_code=200, final_url="https://example.com/final")
        if url.endswith("/missing"):
            return FetchResult(status_code=404, error="HTTP 404")
        if url.endswith("/no-utm"):
            return FetchResult(status_code=200)
        raise AssertionError(f"unexpected URL {url}")

    report = check_newsletter_links(
        body="\n".join(
            [
                "https://example.com/ok?utm_source=n&utm_medium=e&utm_campaign=w",
                "https://example.com/redirect?utm_source=n&utm_medium=e&utm_campaign=w",
                "https://example.com/missing",
                "https://example.com/no-utm",
                "[email](mailto:team@example.com)",
                "[section](#section)",
                "[archive](ftp://files.example.com/archive)",
            ]
        ),
        require_utm=True,
        timeout=3,
        fetcher=fake_fetcher,
    )

    by_url = {result.url: result for result in report.results}

    assert by_url["https://example.com/ok?utm_source=n&utm_medium=e&utm_campaign=w"].status == "healthy"
    assert by_url["https://example.com/redirect?utm_source=n&utm_medium=e&utm_campaign=w"].status == "redirected"
    assert by_url["https://example.com/missing"].status == "broken"
    assert by_url["https://example.com/missing"].missing_utm_parameters == (
        "utm_source",
        "utm_medium",
        "utm_campaign",
    )
    assert by_url["https://example.com/no-utm"].status == "missing_utm"
    assert by_url["mailto:team@example.com"].skip_reason == "mailto"
    assert by_url["#section"].skip_reason == "internal_anchor"
    assert by_url["ftp://files.example.com/archive"].skip_reason == "unsupported_scheme"
    assert report.broken_required_count == 1
    assert report.ok is False
    assert calls == [
        ("https://example.com/ok?utm_source=n&utm_medium=e&utm_campaign=w", 3),
        ("https://example.com/redirect?utm_source=n&utm_medium=e&utm_campaign=w", 3),
        ("https://example.com/missing", 3),
        ("https://example.com/no-utm", 3),
    ]


def test_fetcher_exception_marks_required_link_broken():
    def fake_fetcher(_url: str, _timeout: float) -> FetchResult:
        raise TimeoutError("timed out")

    report = check_newsletter_links(body="https://example.com/down", fetcher=fake_fetcher)

    assert report.ok is False
    assert report.results[0].status == "broken"
    assert report.results[0].error == "timed out"


def test_json_and_text_output_are_stable():
    report = check_newsletter_links(
        body="https://example.com/no-utm",
        require_utm=True,
        fetcher=lambda _url, _timeout: FetchResult(status_code=200),
    )

    payload = json.loads(format_newsletter_link_health_json(report))
    text = format_newsletter_link_health_text(report)

    assert list(payload) == sorted(payload)
    assert payload["status_counts"]["missing_utm"] == 1
    assert payload["broken_required_count"] == 0
    assert "Newsletter Link Health" in text
    assert "missing=utm_source,utm_medium,utm_campaign" in text


def test_cli_reads_files_outputs_json_and_exits_for_broken_required_links(tmp_path, capsys):
    body = tmp_path / "body.txt"
    html = tmp_path / "body.html"
    body.write_text("https://example.com/down")
    html.write_text('<a href="mailto:team@example.com">Email</a>')

    fake_report = check_newsletter_links(
        body="https://example.com/down",
        html='<a href="mailto:team@example.com">Email</a>',
        fetcher=lambda url, _timeout: FetchResult(status_code=500, error=f"HTTP 500 for {url}"),
    )

    with patch.object(
        check_newsletter_links_script,
        "check_newsletter_links",
        return_value=fake_report,
    ) as mock_check:
        result = check_newsletter_links_script.main(
            [
                "--body-file",
                str(body),
                "--html-file",
                str(html),
                "--subject",
                "Weekly note",
                "--format",
                "json",
                "--timeout",
                "2",
                "--require-utm",
            ]
        )

    payload = json.loads(capsys.readouterr().out)

    assert result == 1
    assert payload["broken_required_count"] == 1
    mock_check.assert_called_once_with(
        subject="Weekly note",
        body="https://example.com/down",
        html='<a href="mailto:team@example.com">Email</a>',
        timeout=2,
        require_utm=True,
    )


def test_cli_returns_zero_for_missing_utm_without_broken_required_links(tmp_path, capsys):
    body = tmp_path / "body.txt"
    body.write_text("https://example.com/no-utm")
    fake_report = check_newsletter_links(
        body="https://example.com/no-utm",
        require_utm=True,
        fetcher=lambda _url, _timeout: FetchResult(status_code=200),
    )

    with patch.object(check_newsletter_links_script, "check_newsletter_links", return_value=fake_report):
        result = check_newsletter_links_script.main(["--body-file", str(body), "--format", "json"])

    payload = json.loads(capsys.readouterr().out)

    assert result == 0
    assert payload["missing_utm_count"] == 1


def test_cli_reports_validation_errors(capsys):
    result = check_newsletter_links_script.main(["--timeout", "0"])
    captured = capsys.readouterr()

    assert result == 1
    assert "timeout must be positive" in captured.err
