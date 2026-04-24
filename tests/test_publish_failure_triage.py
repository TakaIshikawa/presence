"""Tests for publish failure triage reporting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publish_failure_triage import build_publish_failure_triage
from publish_failure_triage import (
    format_triage_json,
    format_triage_table,
    main,
    parse_args,
)


BASE_TIME = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def _insert_content(db, text: str) -> int:
    content_id = db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published, created_at)
           VALUES (?, 'x_post', 8.0, 0, ?)""",
        (text, (BASE_TIME - timedelta(hours=2)).isoformat()),
    ).lastrowid
    db.conn.commit()
    return content_id


def _queue_item(
    db,
    *,
    content_id: int,
    platform: str,
    status: str,
    error: str | None = None,
    error_category: str | None = None,
    hold_reason: str | None = None,
) -> int:
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, error, error_category,
            hold_reason, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            (BASE_TIME - timedelta(hours=1)).isoformat(),
            platform,
            status,
            error,
            error_category,
            hold_reason,
            (BASE_TIME - timedelta(minutes=30)).isoformat(),
        ),
    ).lastrowid
    db.conn.commit()
    return queue_id


def seed_triage_rows(db) -> dict[str, int]:
    rate_limited = _insert_content(db, "Rate limited X post")
    auth_failed = _insert_content(db, "Bluesky auth failure")
    held_all = _insert_content(db, "Held cross-post")
    published = _insert_content(db, "Already published")

    rate_queue = _queue_item(
        db,
        content_id=rate_limited,
        platform="x",
        status="failed",
        error="429 too many requests",
    )
    db.upsert_publication_failure(
        rate_limited,
        "x",
        "429 too many requests",
    )

    auth_queue = _queue_item(
        db,
        content_id=auth_failed,
        platform="bluesky",
        status="failed",
        error="temporary queue wrapper",
        error_category="unknown",
    )
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, error, error_category, attempt_count,
            last_error_at, updated_at)
           VALUES (?, 'bluesky', 'failed', ?, 'auth', 2, ?, ?)""",
        (
            auth_failed,
            "invalid app password",
            (BASE_TIME - timedelta(minutes=15)).isoformat(),
            (BASE_TIME - timedelta(minutes=15)).isoformat(),
        ),
    )

    held_queue = _queue_item(
        db,
        content_id=held_all,
        platform="all",
        status="held",
        hold_reason="campaign paused",
    )

    _queue_item(
        db,
        content_id=published,
        platform="x",
        status="published",
    )
    db.conn.commit()
    return {
        "rate_limited": rate_limited,
        "rate_queue": rate_queue,
        "auth_failed": auth_failed,
        "auth_queue": auth_queue,
        "held_all": held_all,
        "held_queue": held_queue,
    }


@contextmanager
def _script_context(db):
    yield None, db


def test_triage_groups_failed_and_held_items(db):
    ids = seed_triage_rows(db)

    report = build_publish_failure_triage(db, days=7, now=BASE_TIME)

    groups = {
        (group["platform"], group["category"], group["recommended_action"]): group
        for group in report["groups"]
    }
    assert report["total_items"] == 4
    assert groups[("x", "rate_limit", "retry_later")]["queue_ids"] == [
        ids["rate_queue"]
    ]
    assert groups[("x", "rate_limit", "retry_later")]["retryable"] is True
    assert groups[("bluesky", "auth", "fix_credentials")]["queue_ids"] == [
        ids["auth_queue"]
    ]
    assert groups[("bluesky", "unknown", "review_hold")]["queue_ids"] == [
        ids["held_queue"]
    ]
    assert groups[("x", "unknown", "review_hold")]["queue_ids"] == [
        ids["held_queue"]
    ]


def test_triage_filters_platform_and_status(db):
    ids = seed_triage_rows(db)

    report = build_publish_failure_triage(
        db,
        days=7,
        platform="bluesky",
        status="failed",
        now=BASE_TIME,
    )

    assert [item["queue_id"] for item in report["items"]] == [ids["auth_queue"]]
    assert report["items"][0]["platform"] == "bluesky"
    assert report["items"][0]["recommended_action"] == "fix_credentials"


def test_triage_include_content_is_opt_in(db):
    seed_triage_rows(db)

    hidden = build_publish_failure_triage(db, days=7, now=BASE_TIME)
    included = build_publish_failure_triage(
        db,
        days=7,
        include_content=True,
        now=BASE_TIME,
    )

    assert "content" not in hidden["items"][0]
    assert any(item.get("content") == "Held cross-post" for item in included["items"])


def test_triage_is_read_only(db):
    seed_triage_rows(db)
    before = [
        dict(row)
        for row in db.conn.execute("SELECT * FROM publish_queue ORDER BY id").fetchall()
    ]

    build_publish_failure_triage(db, days=7, include_content=True, now=BASE_TIME)

    after = [
        dict(row)
        for row in db.conn.execute("SELECT * FROM publish_queue ORDER BY id").fetchall()
    ]
    assert after == before


def test_formatters_emit_stable_json_and_readable_table(db):
    seed_triage_rows(db)

    report = build_publish_failure_triage(db, days=7, now=BASE_TIME)
    data = json.loads(format_triage_json(report))
    table = format_triage_table(report)

    assert data["groups"] == report["groups"]
    assert "PLATFORM" in table
    assert "CATEGORY" in table
    assert "retry_later" in table
    assert "fix_credentials" in table


def test_parse_args_validates_days():
    args = parse_args(["--days", "3", "--platform", "x", "--status", "failed"])
    assert args.days == 3
    assert args.platform == "x"
    assert args.status == "failed"

    try:
        parse_args(["--days", "0"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("parse_args should reject non-positive days")


def test_main_outputs_json(db, capsys):
    seed_triage_rows(db)

    with patch("publish_failure_triage.script_context", return_value=_script_context(db)):
        result = main(["--platform", "x", "--status", "held", "--json"])

    data = json.loads(capsys.readouterr().out)
    assert result == 0
    assert data["platform"] == "x"
    assert data["status"] == "held"
    assert data["total_items"] == 1
    assert data["groups"][0]["recommended_action"] == "review_hold"


def test_main_outputs_table_with_content(db, capsys):
    seed_triage_rows(db)

    with patch("publish_failure_triage.script_context", return_value=_script_context(db)):
        result = main(["--platform", "bluesky", "--include-content"])

    output = capsys.readouterr().out
    assert result == 0
    assert "SAMPLE_ERROR" in output
    assert "CONTENT" in output
    assert "Held cross-post" in output
