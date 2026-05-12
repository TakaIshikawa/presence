"""Tests for publication URL hygiene reporting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from publication_url_hygiene import main  # noqa: E402
from evaluation.publication_url_hygiene import (  # noqa: E402
    build_publication_url_hygiene_report,
    format_publication_url_hygiene_text,
)


NOW = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)


def _content(db, text: str) -> int:
    return db.insert_generated_content("x_post", [], [], text, 7.0, "ok")


def _publish(db, content_id: int, platform: str, post_id: str | None, url: str | None) -> None:
    db.upsert_publication_success(content_id, platform, post_id, url, NOW.isoformat())


def test_flags_missing_malformed_duplicate_and_identifier_mismatch(db):
    missing = _content(db, "missing")
    malformed = _content(db, "malformed")
    dup1 = _content(db, "dup1")
    dup2 = _content(db, "dup2")
    mismatch = _content(db, "mismatch")
    _publish(db, missing, "x", "1", None)
    _publish(db, malformed, "x", "2", "not-a-url")
    _publish(db, dup1, "x", "3", "https://x.com/u/status/3")
    _publish(db, dup2, "x", "4", "https://x.com/u/status/3")
    _publish(db, mismatch, "x", "5", "https://x.com/u/status/999")

    report = build_publication_url_hygiene_report(db, now=NOW)
    issues = {(item["content_id"], item["issue_type"]) for item in report["issues"]}

    assert (missing, "missing_published_url") in issues
    assert (malformed, "malformed_url") in issues
    assert (mismatch, "identifier_url_mismatch") in issues
    duplicate = [item for item in report["issues"] if item["issue_type"] == "duplicate_url"]
    assert [item["content_id"] for item in duplicate] == [dup1, dup2]
    assert all(item["duplicate_content_ids"] == [dup1, dup2] for item in duplicate)
    assert "Issues:" in format_publication_url_hygiene_text(report)


def test_bluesky_uri_mismatch_and_filters(db):
    content_id = _content(db, "bsky")
    _publish(
        db,
        content_id,
        "bluesky",
        "at://did:plc:abc/app.bsky.feed.post/rkey1",
        "https://bsky.app/profile/did:plc:abc/post/other",
    )

    report = build_publication_url_hygiene_report(
        db,
        platform="bluesky",
        issue_type="identifier_url_mismatch",
        now=NOW,
    )

    assert len(report["issues"]) == 1
    assert report["issues"][0]["content_id"] == content_id
    assert report["issues"][0]["platform"] == "bluesky"


def test_cli_supports_json_output(db, capsys):
    content_id = _content(db, "missing")
    _publish(db, content_id, "x", "1", None)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("publication_url_hygiene.script_context", fake_script_context):
        result = main(["--days", "30", "--platform", "x", "--issue-type", "missing_published_url", "--format", "json"])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["issues"][0]["content_id"] == content_id
