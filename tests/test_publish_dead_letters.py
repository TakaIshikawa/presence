"""Tests for content_publications dead-letter exports."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from output.publish_dead_letters import (
    build_publish_dead_letter_report,
    format_json_report,
    format_text_report,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "export_publish_dead_letters.py"
spec = importlib.util.spec_from_file_location("export_publish_dead_letters_script", SCRIPT_PATH)
export_publish_dead_letters_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_publish_dead_letters_script)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _iso(hours_ago: int) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _content(db, text: str) -> int:
    cursor = db.conn.execute(
        """INSERT INTO generated_content
           (content_type, content, eval_score, retry_count, created_at)
           VALUES ('x_post', ?, 8, 0, ?)""",
        (text, _iso(48)),
    )
    return int(cursor.lastrowid)


def _publication(
    db,
    content_id: int,
    *,
    platform: str = "x",
    status: str = "failed",
    error: str = "Gateway timeout",
    error_category: str = "network",
    attempt_count: int = 3,
    next_retry_hours_ago: int | None = 1,
    last_error_hours_ago: int = 1,
    platform_post_id: str | None = None,
    platform_url: str | None = None,
) -> int:
    next_retry_at = None if next_retry_hours_ago is None else _iso(next_retry_hours_ago)
    cursor = db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, platform_post_id, platform_url, error,
            error_category, attempt_count, next_retry_at, last_error_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            platform,
            status,
            platform_post_id,
            platform_url,
            error,
            error_category,
            attempt_count,
            next_retry_at,
            _iso(last_error_hours_ago),
            _iso(last_error_hours_ago),
        ),
    )
    return int(cursor.lastrowid)


def test_failed_rows_at_or_above_attempt_threshold_are_exported(db):
    included = _content(db, "Network keeps failing")
    excluded = _content(db, "Still retrying normally")
    publication_id = _publication(
        db,
        included,
        platform="x",
        error_category="network",
        attempt_count=3,
        platform_post_id="tw-1",
        platform_url="https://x.test/tw-1",
    )
    _publication(
        db,
        excluded,
        platform="x",
        error_category="network",
        attempt_count=2,
        next_retry_hours_ago=1,
    )
    db.conn.commit()

    report = build_publish_dead_letter_report(
        db,
        min_attempts=3,
        stale_hours=24,
        now=NOW,
    )

    assert report["items"] == [
        {
            "publication_id": publication_id,
            "content_id": included,
            "platform": "x",
            "error_category": "network",
            "stuck_reason": "min_attempts",
            "operator_action": "manual_replay_or_cancel",
            "attempt_count": 3,
            "next_retry_at": _iso(1),
            "last_error_at": _iso(1),
            "platform_post_id": "tw-1",
            "platform_url": "https://x.test/tw-1",
            "content_excerpt": "Network keeps failing",
            "error": "Gateway timeout",
        }
    ]
    assert report["groups"] == [{"platform": "x", "error_category": "network", "count": 1}]


def test_stale_retry_rows_are_included_with_configurable_threshold(db):
    stale = _content(db, "Retry date is stale")
    fresh = _content(db, "Retry date is recent")
    _publication(
        db,
        stale,
        platform="bluesky",
        error_category="rate_limit",
        attempt_count=1,
        next_retry_hours_ago=30,
    )
    _publication(
        db,
        fresh,
        platform="bluesky",
        error_category="rate_limit",
        attempt_count=1,
        next_retry_hours_ago=12,
    )
    db.conn.commit()

    report = build_publish_dead_letter_report(
        db,
        min_attempts=5,
        stale_hours=24,
        now=NOW,
    )

    assert [item["content_id"] for item in report["items"]] == [stale]
    assert report["items"][0]["stuck_reason"] == "stale_retry"
    assert report["items"][0]["operator_action"] == "reschedule_or_replay"


def test_report_is_deterministic_and_groups_by_platform_and_error_category(db):
    x_auth = _content(db, "Auth issue")
    bsky_auth = _content(db, "Bluesky auth issue")
    x_media = _content(db, "Media issue")
    _publication(db, x_media, platform="x", error_category="media", attempt_count=4)
    _publication(db, bsky_auth, platform="bluesky", error_category="auth", attempt_count=4)
    _publication(db, x_auth, platform="x", error_category="auth", attempt_count=4)
    db.conn.commit()

    report = build_publish_dead_letter_report(db, min_attempts=3, stale_hours=24, now=NOW)

    assert [(item["platform"], item["error_category"], item["content_id"]) for item in report["items"]] == [
        ("bluesky", "auth", bsky_auth),
        ("x", "auth", x_auth),
        ("x", "media", x_media),
    ]
    assert report["totals"]["by_platform"] == {"bluesky": 1, "x": 2}
    assert report["totals"]["by_error_category"] == {"auth": 2, "media": 1}
    assert report["groups"] == [
        {"platform": "bluesky", "error_category": "auth", "count": 1},
        {"platform": "x", "error_category": "auth", "count": 1},
        {"platform": "x", "error_category": "media", "count": 1},
    ]


def test_json_and_text_formatters_are_stable(db):
    content_id = _content(db, 'First line\n"quoted", with comma')
    _publication(db, content_id, platform="x", error_category="auth", attempt_count=3)
    db.conn.commit()

    report = build_publish_dead_letter_report(
        db,
        min_attempts=3,
        stale_hours=24,
        limit=10,
        now=NOW,
    )

    assert json.loads(format_json_report(report))["artifact_type"] == "publish_dead_letters"
    assert format_text_report(report) == "\n".join(
        [
            "Publish Dead-Letter Export",
            "Generated: 2026-05-01T12:00:00+00:00",
            "Filters: min_attempts=3 stale_hours=24 platform=all limit=10",
            "Total: 1",
            "",
            "Groups:",
            "  Platform  Category     Count",
            "  --------  -----------  -----",
            "  x         auth             1",
            "",
            "Items:",
            "  ID  Platform  Category     Attempts  Next retry                 Action",
            "  --  --------  -----------  --------  -------------------------  -----------------------",
            f"  {content_id:<2}  x         auth                3  2026-05-01T11:00:00+00:00  fix_credentials",
        ]
    )
    assert report["items"][0]["content_excerpt"] == 'First line "quoted", with comma'


def test_platform_filter_and_limit_are_applied_after_deterministic_sort(db):
    first = _content(db, "First x failure")
    second = _content(db, "Second x failure")
    _publication(db, second, platform="x", error_category="media", attempt_count=3)
    _publication(db, first, platform="x", error_category="auth", attempt_count=3)
    _publication(db, _content(db, "Bluesky failure"), platform="bluesky", attempt_count=3)
    db.conn.commit()

    report = build_publish_dead_letter_report(
        db,
        min_attempts=3,
        stale_hours=24,
        platform="x",
        limit=1,
        now=NOW,
    )

    assert [item["content_id"] for item in report["items"]] == [first]
    assert report["totals"]["items"] == 1


def test_missing_publication_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content TEXT)")

    report = build_publish_dead_letter_report(conn, now=NOW)

    assert report["items"] == []
    assert report["totals"]["items"] == 0
    assert report["missing_required"] == ["content_publications"]


def test_cli_supports_requested_flags(db, monkeypatch, capsys):
    content_id = _content(db, "Credential failure")
    _publication(db, content_id, platform="x", error_category="auth", attempt_count=3)
    db.conn.commit()
    monkeypatch.setattr(
        export_publish_dead_letters_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = export_publish_dead_letters_script.main(
        [
            "--min-attempts",
            "3",
            "--stale-hours",
            "24",
            "--platform",
            "x",
            "--limit",
            "5",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["min_attempts"] == 3
    assert payload["filters"]["stale_hours"] == 24.0
    assert payload["filters"]["platform"] == "x"
    assert payload["filters"]["limit"] == 5
    assert payload["items"][0]["content_id"] == content_id
