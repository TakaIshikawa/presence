"""Tests for cross-platform publication parity audit."""

from __future__ import annotations

import importlib.util
import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from output.publication_parity import (
    find_publication_parity_gaps,
    format_json_report,
    format_text_report,
)


BASE_TIME = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).parent.parent / "scripts" / "publication_parity.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location("publication_parity_script", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _insert_content(db, text: str, created_at: datetime | None = None) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        ((created_at or BASE_TIME).isoformat(), content_id),
    )
    db.conn.commit()
    return content_id


def _issue_by_content(issues):
    return {issue.content_id: issue for issue in issues}


def test_detects_published_x_missing_bluesky_and_reverse(db):
    x_only = _insert_content(db, "Published on X only")
    bsky_only = _insert_content(db, "Published on Bluesky only")

    db.upsert_publication_success(
        x_only,
        "x",
        platform_post_id="tw-1",
        platform_url="https://x.test/tw-1",
        published_at=BASE_TIME.isoformat(),
    )
    db.upsert_publication_success(
        bsky_only,
        "bluesky",
        platform_post_id="at://did:plc:test/app.bsky.feed.post/1",
        platform_url="https://bsky.app/profile/test/post/1",
        published_at=BASE_TIME.isoformat(),
    )

    issues = _issue_by_content(find_publication_parity_gaps(db.conn, days=7, now=BASE_TIME))

    assert issues[x_only].present_platforms == ("x",)
    assert issues[x_only].missing_platforms == ("bluesky",)
    assert issues[bsky_only].present_platforms == ("bluesky",)
    assert issues[bsky_only].missing_platforms == ("x",)


def test_include_queued_controls_queued_state_detection(db):
    queued_x = _insert_content(db, "Queued for X only")
    db.queue_for_publishing(
        queued_x,
        (BASE_TIME + timedelta(hours=1)).isoformat(),
        platform="x",
    )

    without_queued = find_publication_parity_gaps(
        db.conn,
        days=7,
        include_queued=False,
        now=BASE_TIME,
    )
    with_queued = _issue_by_content(
        find_publication_parity_gaps(
            db.conn,
            days=7,
            include_queued=True,
            now=BASE_TIME,
        )
    )

    assert queued_x not in _issue_by_content(without_queued)
    assert with_queued[queued_x].present_platforms == ("x",)
    assert with_queued[queued_x].missing_platforms == ("bluesky",)


def test_queued_counterpart_satisfies_parity_when_included(db):
    content_id = _insert_content(db, "X is published while Bluesky is queued")
    db.upsert_publication_success(
        content_id,
        "x",
        platform_post_id="tw-2",
        platform_url="https://x.test/tw-2",
        published_at=BASE_TIME.isoformat(),
    )
    db.queue_for_publishing(
        content_id,
        (BASE_TIME + timedelta(hours=1)).isoformat(),
        platform="bluesky",
    )

    excluded = _issue_by_content(
        find_publication_parity_gaps(
            db.conn,
            days=7,
            include_queued=False,
            now=BASE_TIME,
        )
    )
    included = _issue_by_content(
        find_publication_parity_gaps(
            db.conn,
            days=7,
            include_queued=True,
            now=BASE_TIME,
        )
    )

    assert excluded[content_id].missing_platforms == ("bluesky",)
    assert content_id not in included


def test_variants_reveal_intent_without_publication_rows(db):
    content_id = _insert_content(db, "Variant-only cross-post")
    db.upsert_content_variant(content_id, "x", "post", "Variant for X")
    db.upsert_content_variant(content_id, "bluesky", "post", "Variant for Bluesky")

    issues = _issue_by_content(find_publication_parity_gaps(db.conn, days=7, now=BASE_TIME))

    assert issues[content_id].missing_platforms == ("x", "bluesky")
    assert issues[content_id].variant_platforms == ("x", "bluesky")
    assert "variant_without_state" in issues[content_id].reasons


def test_variant_counterpart_missing_state_is_reported(db):
    content_id = _insert_content(db, "Published X with Bluesky variant")
    db.upsert_publication_success(
        content_id,
        "x",
        platform_post_id="tw-3",
        platform_url="https://x.test/tw-3",
        published_at=BASE_TIME.isoformat(),
    )
    db.upsert_content_variant(content_id, "bluesky", "post", "Variant for Bluesky")

    issues = _issue_by_content(find_publication_parity_gaps(db.conn, days=7, now=BASE_TIME))

    assert issues[content_id].missing_platforms == ("bluesky",)
    assert issues[content_id].variant_platforms == ("bluesky",)
    assert "variant_without_state" in issues[content_id].reasons


def test_old_content_is_filtered_by_created_at(db):
    old_id = _insert_content(db, "Old X-only publication", BASE_TIME - timedelta(days=40))
    db.upsert_publication_success(
        old_id,
        "x",
        platform_post_id="tw-old",
        platform_url="https://x.test/tw-old",
        published_at=BASE_TIME.isoformat(),
    )

    issues = find_publication_parity_gaps(db.conn, days=7, now=BASE_TIME)

    assert old_id not in _issue_by_content(issues)


def test_json_and_text_output(db):
    content_id = _insert_content(db, "Output formatting sample")
    db.upsert_publication_success(
        content_id,
        "x",
        platform_post_id="tw-json",
        platform_url="https://x.test/tw-json",
        published_at=BASE_TIME.isoformat(),
    )
    issues = find_publication_parity_gaps(db.conn, days=7, now=BASE_TIME)

    data = json.loads(format_json_report(issues))
    text = format_text_report(issues)

    assert data[0]["content_id"] == content_id
    assert data[0]["missing_platforms"] == ["bluesky"]
    assert "CID" in text
    assert "MISSING" in text
    assert "bluesky" in text
    assert "Output formatting sample" in text


def test_audit_does_not_mutate_queue_or_publication_rows(db):
    content_id = _insert_content(db, "Read-only audit sample")
    db.queue_for_publishing(
        content_id,
        (BASE_TIME + timedelta(hours=1)).isoformat(),
        platform="x",
    )
    before = {
        "queue": db.conn.execute("SELECT COUNT(*) FROM publish_queue").fetchone()[0],
        "publications": db.conn.execute("SELECT COUNT(*) FROM content_publications").fetchone()[0],
    }

    find_publication_parity_gaps(
        db.conn,
        days=7,
        include_queued=True,
        now=BASE_TIME,
    )

    after = {
        "queue": db.conn.execute("SELECT COUNT(*) FROM publish_queue").fetchone()[0],
        "publications": db.conn.execute("SELECT COUNT(*) FROM content_publications").fetchone()[0],
    }
    assert after == before


def test_cli_exit_codes_and_formats(db, capsys):
    script = _load_script_module()
    content_id = _insert_content(db, "CLI parity gap")
    db.upsert_publication_success(
        content_id,
        "x",
        platform_post_id="tw-cli",
        platform_url="https://x.test/tw-cli",
        published_at=BASE_TIME.isoformat(),
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch.object(script, "script_context", fake_script_context):
        ok_code = script.main(["--days", "7", "--format", "text"])
        fail_code = script.main(["--days", "7", "--format", "json", "--fail-on-missing"])

    output = capsys.readouterr().out
    assert ok_code == 0
    assert fail_code == 1
    assert "CLI parity gap" in output
    assert '"missing_platforms": [' in output
