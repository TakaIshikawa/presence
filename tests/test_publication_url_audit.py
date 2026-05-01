"""Tests for publication URL canonicalization audit."""

from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from output.publication_url_audit import (
    build_publication_url_audit,
    canonicalize_publication_url,
    format_publication_url_audit_json,
    format_publication_url_audit_table,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_url_audit.py"
spec = importlib.util.spec_from_file_location("publication_url_audit", SCRIPT_PATH)
publication_url_audit = importlib.util.module_from_spec(spec)
sys.modules["publication_url_audit"] = publication_url_audit
spec.loader.exec_module(publication_url_audit)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _content(db, text: str = "Published content") -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )


def _publish(
    db,
    *,
    content_id: int | None = None,
    platform: str = "x",
    url: str | None = "https://x.com/taka/status/1",
    published_at: datetime | None = None,
) -> int:
    content_id = content_id or _content(db)
    db.upsert_publication_success(
        content_id,
        platform,
        platform_post_id=f"{platform}-{content_id}",
        platform_url=url,
        published_at=(published_at or NOW - timedelta(hours=1)).isoformat(),
    )
    return content_id


def _issue_types(report: dict) -> list[str]:
    return [issue["issue_type"] for issue in report["issues"]]


def test_reports_successful_publication_rows_without_urls(db):
    content_id = _publish(db, url=None)

    report = build_publication_url_audit(db, days=7, now=NOW)

    assert report["warning_count"] == 1
    assert report["issues"][0] == {
        "issue_type": "missing_url",
        "publication_id": 1,
        "content_id": content_id,
        "platform": "x",
        "platform_url": None,
        "canonical_url": None,
        "detail": "published row has no platform_url",
        "related_content_ids": (),
        "related_publication_ids": (),
    }


def test_reports_duplicate_canonical_urls_across_content_ids(db):
    first = _publish(db, url="https://x.com/taka/status/42")
    second = _publish(db, url="https://x.com/taka/status/42/")

    report = build_publication_url_audit(db, days=7, now=NOW)

    assert _issue_types(report) == ["duplicate_url", "duplicate_url"]
    assert {issue["content_id"] for issue in report["issues"]} == {first, second}
    assert report["issues"][0]["canonical_url"] == "https://x.com/taka/status/42"
    assert report["issues"][0]["related_content_ids"] == (first, second)


def test_tracking_query_parameters_are_ignored_for_duplicates(db):
    first = _publish(
        db,
        url="https://x.com/taka/status/42?utm_source=newsletter&ref=keep",
    )
    second = _publish(
        db,
        url="https://x.com/taka/status/42?ref=keep&gclid=tracking",
    )

    report = build_publication_url_audit(db, days=7, now=NOW)

    assert _issue_types(report) == [
        "tracking_variant_duplicate",
        "tracking_variant_duplicate",
    ]
    assert {issue["content_id"] for issue in report["issues"]} == {first, second}
    assert all(
        issue["canonical_url"] == "https://x.com/taka/status/42?ref=keep"
        for issue in report["issues"]
    )
    assert canonicalize_publication_url(
        "HTTPS://WWW.X.COM/taka/status/42/?utm_campaign=test#ignored"
    ) == "https://x.com/taka/status/42"


def test_reports_platform_host_mismatches_and_honors_filters(db):
    _publish(db, platform="x", url="https://bsky.app/profile/taka/post/abc")
    _publish(
        db,
        platform="bluesky",
        url="https://bsky.app/profile/taka.bsky.social/post/ok",
    )
    _publish(
        db,
        platform="x",
        url="https://example.com/old",
        published_at=NOW - timedelta(days=40),
    )

    report = build_publication_url_audit(db, platform="x", days=7, now=NOW)

    assert _issue_types(report) == ["host_mismatch"]
    assert report["scanned_count"] == 1
    assert report["issues"][0]["platform_url"] == "https://bsky.app/profile/taka/post/abc"


def test_formatters_and_cli_fail_on_warning(db, capsys):
    _publish(db, url=None)
    report = build_publication_url_audit(db, days=7, now=NOW)

    payload = json.loads(format_publication_url_audit_json(report))
    table = format_publication_url_audit_table(report)
    assert payload["warning_count"] == 1
    assert payload["issues"][0]["issue_type"] == "missing_url"
    assert "Publication URL Audit" in table
    assert "missing_url" in table

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(publication_url_audit, "script_context", fake_script_context):
        with pytest.raises(SystemExit) as exc_info:
            publication_url_audit.main(["--days", "7", "--json", "--fail-on-warning"])

    assert exc_info.value.code == 1
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["warning_count"] == 1
