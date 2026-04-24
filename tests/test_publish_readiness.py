"""Tests for queued publish readiness preflight."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from output.publish_readiness import check_publish_readiness
from publish_readiness import main as readiness_main


BASE_TIME = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def _config(*, bluesky_enabled: bool = True, restricted_behavior: str = "strict"):
    bluesky = (
        SimpleNamespace(
            enabled=True,
            handle="test.bsky.social",
            app_password="test-password",
        )
        if bluesky_enabled
        else None
    )
    return SimpleNamespace(
        bluesky=bluesky,
        curated_sources=SimpleNamespace(
            restricted_prompt_behavior=restricted_behavior,
        ),
    )


def _content(db, text: str = "Ready post", **kwargs) -> int:
    return db.insert_generated_content(
        content_type=kwargs.pop("content_type", "x_post"),
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="Good",
        **kwargs,
    )


def _queue(db, content_id: int, *, platform: str = "x", hours: int = -1) -> int:
    return db.queue_for_publishing(
        content_id,
        (BASE_TIME + timedelta(hours=hours)).isoformat(),
        platform=platform,
    )


def test_readiness_lists_due_and_future_queued_content(db):
    due_content_id = _content(db, "Due post")
    future_content_id = _content(db, "Future post")
    due_queue_id = _queue(db, due_content_id, hours=-1)
    future_queue_id = _queue(db, future_content_id, hours=2)

    results = check_publish_readiness(
        db,
        config=_config(),
        now_iso=BASE_TIME.isoformat(),
    )

    assert [result.queue_id for result in results] == [due_queue_id, future_queue_id]
    assert [result.status for result in results] == ["ready", "ready"]
    assert [result.due for result in results] == [True, False]


def test_json_cli_emits_stable_machine_readable_objects(db, capsys):
    content_id = _content(db, "JSON post")
    queue_id = _queue(db, content_id, platform="x")

    with patch("publish_readiness.script_context") as mock_context, patch(
        "publish_readiness.datetime"
    ) as mock_datetime:
        mock_context.return_value.__enter__.return_value = (_config(), db)
        mock_context.return_value.__exit__.return_value = False
        mock_datetime.now.return_value = BASE_TIME

        exit_code = readiness_main(["--queue-id", str(queue_id), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload == [
        {
            "content_id": content_id,
            "due": True,
            "platform": "x",
            "queue_id": queue_id,
            "queue_status": "queued",
            "reasons": [],
            "scheduled_at": (BASE_TIME - timedelta(hours=1)).isoformat(),
            "status": "ready",
        }
    ]


def test_visual_posts_missing_image_path_or_alt_text_are_blocked(db, tmp_path):
    missing_path_content_id = _content(
        db,
        "Visual post without file path",
        content_type="x_visual",
        image_alt_text="Annotated dashboard showing launch metrics and trend labels.",
    )
    missing_alt_content_id = _content(
        db,
        "Visual post without alt",
        content_type="x_visual",
        image_path=str(tmp_path / "visual.png"),
        image_prompt="Annotated launch dashboard",
    )
    _queue(db, missing_path_content_id)
    _queue(db, missing_alt_content_id)

    results = check_publish_readiness(db, config=_config())

    assert [result.status for result in results] == ["blocked", "blocked"]
    codes = [{reason.code for reason in result.reasons} for result in results]
    assert "missing_image_path" in codes[0]
    assert "missing_alt_text" in codes[1]
    assert "missing_image_file" in codes[1]


def test_bluesky_only_queue_without_credentials_is_blocked(db):
    content_id = _content(db, "Bluesky post")
    _queue(db, content_id, platform="bluesky")

    result = check_publish_readiness(db, config=_config(bluesky_enabled=False))[0]

    assert result.status == "blocked"
    assert result.reasons[0].code == "missing_bluesky_credentials"


def test_x_over_limit_and_empty_content_are_blocked(db):
    over_limit_content_id = _content(db, "x" * 281)
    empty_content_id = _content(db, " ")
    _queue(db, over_limit_content_id)
    _queue(db, empty_content_id)

    results = check_publish_readiness(db, config=_config())

    assert [result.status for result in results] == ["blocked", "blocked"]
    assert results[0].reasons[0].code == "x_post_over_limit"
    assert results[1].reasons[0].code == "empty_content"


def test_license_guard_blocks_or_warns_using_existing_helper(db):
    content_id = _content(db, "Restricted source post")
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "restricted-article",
            "https://source.example/restricted",
            "Source Author",
            "Restricted source context",
            "restricted",
            1,
        ),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])
    _queue(db, content_id)

    blocked = check_publish_readiness(db, config=_config(restricted_behavior="strict"))[0]
    warned = check_publish_readiness(
        db,
        config=_config(restricted_behavior="permissive"),
    )[0]

    assert blocked.status == "blocked"
    assert blocked.reasons[0].code == "license_guard_blocked"
    assert warned.status == "warning"
    assert warned.reasons[0].code == "license_guard_warning"


def test_attribution_guard_blocks_missing_visible_citation(db):
    content_id = _content(db, "Source-backed post without citation")
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "attribution-article",
            "https://source.example/attribution",
            "Source Author",
            "Attribution-required source context",
            "attribution_required",
            1,
        ),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])
    _queue(db, content_id)

    result = check_publish_readiness(db, config=_config())[0]

    assert result.status == "blocked"
    assert result.reasons[0].code == "attribution_guard_blocked"


def test_readiness_checks_do_not_mutate_rows(db):
    content_id = _content(db, "Read-only post")
    queue_id = _queue(db, content_id)
    before = {
        "queue": db.conn.execute("SELECT COUNT(*) FROM publish_queue").fetchone()[0],
        "publication": db.conn.execute(
            "SELECT COUNT(*) FROM content_publications"
        ).fetchone()[0],
        "content": db.conn.execute("SELECT COUNT(*) FROM generated_content").fetchone()[0],
        "queue_row": dict(
            db.conn.execute(
                "SELECT * FROM publish_queue WHERE id = ?",
                (queue_id,),
            ).fetchone()
        ),
    }

    check_publish_readiness(db, config=_config())

    after = {
        "queue": db.conn.execute("SELECT COUNT(*) FROM publish_queue").fetchone()[0],
        "publication": db.conn.execute(
            "SELECT COUNT(*) FROM content_publications"
        ).fetchone()[0],
        "content": db.conn.execute("SELECT COUNT(*) FROM generated_content").fetchone()[0],
        "queue_row": dict(
            db.conn.execute(
                "SELECT * FROM publish_queue WHERE id = ?",
                (queue_id,),
            ).fetchone()
        ),
    }
    assert after == before


def test_platform_and_queue_id_filters(db):
    x_content_id = _content(db, "X post")
    bluesky_content_id = _content(db, "Bluesky post")
    x_queue_id = _queue(db, x_content_id, platform="x")
    bluesky_queue_id = _queue(db, bluesky_content_id, platform="bluesky")

    platform_results = check_publish_readiness(db, config=_config(), platform="bluesky")
    queue_results = check_publish_readiness(db, config=_config(), queue_id=x_queue_id)

    assert [result.queue_id for result in platform_results] == [bluesky_queue_id]
    assert [result.queue_id for result in queue_results] == [x_queue_id]


def test_cli_exits_nonzero_when_blocked(db, capsys):
    content_id = _content(db, "")
    _queue(db, content_id)

    with patch("publish_readiness.script_context") as mock_context:
        mock_context.return_value.__enter__.return_value = (_config(), db)
        mock_context.return_value.__exit__.return_value = False

        exit_code = readiness_main([])

    assert exit_code == 1
    assert "blocked" in capsys.readouterr().out
