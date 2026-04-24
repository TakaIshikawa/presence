"""Tests for newsletter Markdown link health checks."""

from unittest.mock import MagicMock

import pytest

from output.link_health import LinkHealthChecker, extract_markdown_links


class _Response:
    def __init__(self, status_code):
        self.status_code = status_code


def test_extracts_markdown_links_without_images_or_image_alt_text():
    markdown = (
        "Read [the post](https://example.com/post).\n"
        "![https://not-a-link.example](https://cdn.example/image.png)\n"
        "Contact [me](mailto:taka@example.com) or [jump](#section)."
    )

    links = extract_markdown_links(markdown)

    assert [link.normalized_url for link in links] == [
        "https://example.com/post",
        "mailto:taka@example.com",
        "#section",
    ]
    assert all("cdn.example" not in link.normalized_url for link in links)
    assert all("not-a-link.example" not in link.normalized_url for link in links)


def test_duplicate_links_are_checked_once_and_report_all_occurrences():
    session = MagicMock()
    session.head.return_value = _Response(200)
    checker = LinkHealthChecker(timeout=4, session=session)

    report = checker.check_markdown(
        "[first](https://example.com/post)\n"
        "Again [second](https://example.com/post)"
    )

    assert report.ok is True
    assert len(report.checked) == 1
    assert report.checked[0].url == "https://example.com/post"
    assert len(report.checked[0].occurrences) == 2
    assert [occurrence.line for occurrence in report.checked[0].occurrences] == [1, 2]
    session.head.assert_called_once_with(
        "https://example.com/post",
        allow_redirects=True,
        timeout=4,
    )


def test_skips_mailto_and_fragment_only_links():
    session = MagicMock()
    session.head.return_value = _Response(200)
    checker = LinkHealthChecker(timeout=2, session=session)

    report = checker.check_markdown(
        "[email](mailto:taka@example.com)\n"
        "[section](#details)\n"
        "[site](https://example.com)"
    )

    assert len(report.checked) == 1
    assert report.checked[0].url == "https://example.com"
    assert {result.skip_reason for result in report.skipped} == {"mailto", "fragment"}
    session.head.assert_called_once()


def test_http_error_status_fails_required_link():
    session = MagicMock()
    session.head.return_value = _Response(404)
    checker = LinkHealthChecker(timeout=2, session=session)

    report = checker.check_markdown("[missing](https://example.com/missing)")

    assert report.ok is False
    assert report.failure_count == 1
    assert report.failures[0].status_code == 404
    assert report.failures[0].error == "HTTP 404"


def test_head_405_falls_back_to_get():
    session = MagicMock()
    session.head.return_value = _Response(405)
    session.get.return_value = _Response(200)
    checker = LinkHealthChecker(timeout=2, session=session)

    report = checker.check_markdown("[ok](https://example.com/head-disabled)")

    assert report.ok is True
    assert report.checked[0].method == "GET"
    session.get.assert_called_once_with(
        "https://example.com/head-disabled",
        allow_redirects=True,
        timeout=2,
    )


def test_request_failure_fails_required_link():
    import requests

    session = MagicMock()
    session.head.side_effect = requests.RequestException("network down")
    checker = LinkHealthChecker(timeout=2, session=session)

    report = checker.check_markdown("[down](https://example.com/down)")

    assert report.ok is False
    assert report.failures[0].status_code is None
    assert report.failures[0].error == "network down"
