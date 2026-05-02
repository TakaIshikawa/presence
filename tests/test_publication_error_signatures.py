"""Tests for publication attempt error signature digest."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.publication_error_signatures import (
    MAX_SIGNATURE_LENGTH,
    build_publication_error_signature_report,
    format_publication_error_signature_json,
    format_publication_error_signature_text,
    normalize_publication_error_signature,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_error_signatures.py"
spec = importlib.util.spec_from_file_location("publication_error_signatures_script", SCRIPT_PATH)
publication_error_signatures_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_error_signatures_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str) -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )


def _attempt(
    db,
    *,
    content_id: int,
    platform: str = "x",
    error: str | None = None,
    category: str | None = None,
    success: bool = False,
    minutes_ago: int = 10,
) -> int:
    return db.record_publication_attempt(
        None,
        content_id,
        platform,
        success,
        attempted_at=(NOW - timedelta(minutes=minutes_ago)).isoformat(),
        error=error,
        error_category=category,
    )


def test_normalization_collapses_ids_urls_timestamps_and_long_tails():
    first = normalize_publication_error_signature(
        "Upload failed for media id 123456 at 2026-05-01T12:30:45+00:00: "
        "https://cdn.example.test/media/abc.png?token=aaa "
        + ("details " * 80)
    )
    second = normalize_publication_error_signature(
        "UPLOAD failed for media id 987654 at 2026-05-02T09:15:00Z: "
        "https://cdn.example.test/media/def.png?token=bbb "
        + ("details " * 80)
    )

    assert first == second
    assert "<url>" in first
    assert "<timestamp>" in first
    assert "media id <id>" in first
    assert len(first) <= MAX_SIGNATURE_LENGTH


def test_groups_only_unsuccessful_attempts_by_signature(db):
    first = _content(db, "first")
    second = _content(db, "second")
    success = _content(db, "success")
    other = _content(db, "other")
    first_attempt = _attempt(
        db,
        content_id=first,
        error="429 too many requests for request id req-123456 at 2026-05-01T11:00:00Z",
        minutes_ago=50,
    )
    second_attempt = _attempt(
        db,
        content_id=second,
        error="429 too many requests for request id req-999999 at 2026-05-01T11:05:00Z",
        minutes_ago=40,
    )
    _attempt(
        db,
        content_id=success,
        error="429 too many requests for request id req-555555 at 2026-05-01T11:10:00Z",
        success=True,
        minutes_ago=30,
    )
    _attempt(
        db,
        content_id=other,
        error="invalid credentials for user 123456",
        category="auth",
        minutes_ago=20,
    )

    report = build_publication_error_signature_report(
        db,
        days=1,
        min_count=2,
        now=NOW,
    )

    assert len(report.signatures) == 1
    signature = report.signatures[0]
    assert signature.count == 2
    assert signature.error_category == "rate_limit"
    assert signature.recommended_action == "retry_later"
    assert signature.affected_content_ids == (first, second)
    assert signature.attempt_ids == (first_attempt, second_attempt)
    assert signature.first_attempted_at == (NOW - timedelta(minutes=50)).isoformat()
    assert signature.last_attempted_at == (NOW - timedelta(minutes=40)).isoformat()
    assert report.totals["failed_attempts"] == 3
    assert report.totals["signature_groups_scanned"] == 2
    assert report.totals["finding_count"] == 1


def test_filters_by_platform_days_and_min_count(db):
    x_one = _content(db, "x one")
    x_two = _content(db, "x two")
    old = _content(db, "old")
    bluesky = _content(db, "blue")
    _attempt(db, content_id=x_one, platform="x", error="duplicate status id 111", minutes_ago=10)
    _attempt(db, content_id=x_two, platform="x", error="duplicate status id 222", minutes_ago=20)
    _attempt(
        db,
        content_id=old,
        platform="x",
        error="duplicate status id 333",
        minutes_ago=60 * 48,
    )
    _attempt(
        db,
        content_id=bluesky,
        platform="bluesky",
        error="duplicate status id 444",
        minutes_ago=15,
    )

    report = build_publication_error_signature_report(
        db,
        days=1,
        platform="x",
        min_count=2,
        now=NOW,
    )
    strict = build_publication_error_signature_report(
        db,
        days=1,
        platform="x",
        min_count=3,
        now=NOW,
    )

    assert len(report.signatures) == 1
    assert report.signatures[0].count == 2
    assert report.signatures[0].recommended_action == "cancel_duplicate"
    assert report.totals["by_platform"] == {"x": 2}
    assert strict.signatures == ()
    assert strict.totals["failed_attempts"] == 2
    assert strict.totals["signature_groups_scanned"] == 1


def test_json_text_and_cli_fail_on_issues_are_deterministic(db, monkeypatch, capsys):
    first = _content(db, "first")
    second = _content(db, "second")
    _attempt(db, content_id=first, error="media upload failed for media id 123")
    _attempt(db, content_id=second, error="media upload failed for media id 999")
    fixed_report = build_publication_error_signature_report(
        db,
        days=7,
        min_count=2,
        platform="all",
        now=NOW,
    )
    payload = json.loads(format_publication_error_signature_json(fixed_report))
    text = format_publication_error_signature_text(fixed_report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["artifact_type"] == "publication_error_signatures"
    assert payload["has_issues"] is True
    assert payload["signatures"][0]["recommended_action"] == "fix_media"
    assert "Publication Error Signatures" in text
    assert "media upload failed for media id <id>" in text

    monkeypatch.setattr(
        publication_error_signatures_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        publication_error_signatures_script,
        "build_publication_error_signature_report",
        lambda db, **kwargs: fixed_report,
    )

    exit_code = publication_error_signatures_script.main(
        [
            "--days",
            "7",
            "--min-count",
            "2",
            "--platform",
            "all",
            "--format",
            "json",
            "--fail-on-issues",
        ]
    )
    cli_payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert cli_payload["filters"]["min_count"] == 2


def test_invalid_arguments_and_missing_schema(db):
    with pytest.raises(ValueError, match="days must be positive"):
        build_publication_error_signature_report(db, days=0, now=NOW)
    with pytest.raises(ValueError, match="min_count must be positive"):
        build_publication_error_signature_report(db, min_count=0, now=NOW)
    with pytest.raises(ValueError, match="invalid platform"):
        build_publication_error_signature_report(db, platform="mastodon", now=NOW)

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_publication_error_signature_report(conn, now=NOW)
    assert report.missing_tables == ("publication_attempts",)
    assert report.signatures == ()
