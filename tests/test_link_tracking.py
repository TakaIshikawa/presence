"""Tests for outbound link tracking decoration."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from output.link_tracking import decorate_links, decorate_url, extract_links


SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "decorate_links.py"


def _query(url: str) -> dict[str, list[str]]:
    return parse_qs(urlparse(url).query, keep_blank_values=True)


def test_decorates_http_and_https_links_with_requested_utm_params():
    content = "Read https://example.com/a and [docs](http://docs.example.com/path)."

    result = decorate_links(
        content,
        utm_source="newsletter",
        utm_medium="email",
        utm_campaign="launch",
    )

    assert (
        "https://example.com/a?utm_source=newsletter&utm_medium=email&utm_campaign=launch"
        in result.content
    )
    assert (
        "http://docs.example.com/path?utm_source=newsletter&utm_medium=email&utm_campaign=launch"
        in result.content
    )
    assert result.decorated_count == 2


def test_preserves_existing_query_strings_and_fragments():
    decorated, changed, reason = decorate_url(
        "https://example.com/page?ref=abc&empty=#section",
        utm_source="blog",
        utm_campaign="spring notes",
    )

    assert changed is True
    assert reason == "decorated"
    parsed = urlparse(decorated)
    assert parsed.fragment == "section"
    assert _query(decorated) == {
        "ref": ["abc"],
        "empty": [""],
        "utm_source": ["blog"],
        "utm_campaign": ["spring notes"],
    }


def test_existing_utm_params_are_not_overwritten_by_default():
    decorated, changed, reason = decorate_url(
        "https://example.com/page?utm_source=original&x=1",
        utm_source="new",
        utm_medium="email",
    )

    assert changed is True
    assert reason == "decorated"
    assert _query(decorated)["utm_source"] == ["original"]
    assert _query(decorated)["utm_medium"] == ["email"]


def test_existing_utm_params_can_be_replaced_explicitly():
    decorated, changed, reason = decorate_url(
        "https://example.com/page?utm_source=original&utm_medium=social",
        utm_source="newsletter",
        utm_medium="email",
        replace=True,
    )

    assert changed is True
    assert reason == "decorated"
    assert _query(decorated)["utm_source"] == ["newsletter"]
    assert _query(decorated)["utm_medium"] == ["email"]


def test_skips_local_relative_and_mailto_links():
    content = (
        "[local](/about) [mail](mailto:hi@example.com) "
        "http://localhost:8000/path https://service.local/path "
        "https://example.com/path"
    )

    result = decorate_links(content, utm_source="newsletter")

    assert "[local](/about)" in result.content
    assert "[mail](mailto:hi@example.com)" in result.content
    assert "http://localhost:8000/path" in result.content
    assert "https://service.local/path" in result.content
    assert "https://example.com/path?utm_source=newsletter" in result.content
    assert result.decorated_count == 1


def test_extract_links_reports_markdown_html_and_bare_links_once():
    content = (
        '[markdown](https://example.com/m) '
        '<a href="https://example.com/h">HTML</a> '
        "https://example.com/b."
    )

    links = extract_links(content)

    assert [(link.url, link.context) for link in links] == [
        ("https://example.com/m", "markdown"),
        ("https://example.com/h", "html"),
        ("https://example.com/b", "text"),
    ]


def test_cli_decorates_stdin_deterministically():
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--utm-source",
            "newsletter",
            "--utm-medium",
            "email",
            "--utm-campaign",
            "week-17",
        ],
        input="Read https://example.com/a?x=1#frag\n",
        text=True,
        capture_output=True,
        check=True,
    )

    assert completed.stdout == (
        "Read https://example.com/a?x=1&utm_source=newsletter"
        "&utm_medium=email&utm_campaign=week-17#frag\n"
    )


def test_cli_emits_json_diagnostics_for_database_content(file_db):
    content_id = file_db.insert_generated_content(
        content_type="newsletter",
        source_commits=[],
        source_messages=[],
        content=(
            "[Read](https://example.com/read?utm_source=kept) "
            "and https://localhost/dev"
        ),
        eval_score=8.0,
        eval_feedback="ok",
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--content-id",
            str(content_id),
            "--db-path",
            str(file_db.db_path),
            "--utm-source",
            "new",
            "--utm-medium",
            "email",
            "--json",
        ],
        text=True,
        capture_output=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["decorated_count"] == 1
    assert payload["skipped_count"] == 1
    assert payload["content"] == (
        "[Read](https://example.com/read?utm_source=kept&utm_medium=email) "
        "and https://localhost/dev"
    )
    assert payload["links"][0]["reason"] == "decorated"
    assert payload["links"][1]["reason"] == "local_link"
