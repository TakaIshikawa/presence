"""Tests for publication replay bundle export."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from export_publication_replay_bundle import main  # noqa: E402
from output.publication_replay_bundle import (  # noqa: E402
    REDACTED,
    build_publication_replay_bundle,
    publication_replay_bundle_to_json,
    redact_response_metadata,
)


BASE_TIME = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield None, db


def _insert_content(db, text: str = "Replay this post") -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        source_activity_ids=["repo#1:issue"],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
        content_format="tip",
        image_path="/tmp/replay-card.png",
        image_prompt="A clean product card",
        image_alt_text="A product card with text",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        ((BASE_TIME - timedelta(hours=2)).isoformat(), content_id),
    )
    db.conn.commit()
    return content_id


def _queue_item(db, content_id: int, platform: str = "x") -> int:
    queue_id = db.queue_for_publishing(
        content_id,
        (BASE_TIME - timedelta(minutes=30)).isoformat(),
        platform=platform,
    )
    db.conn.execute(
        "UPDATE publish_queue SET created_at = ? WHERE id = ?",
        ((BASE_TIME - timedelta(hours=1)).isoformat(), queue_id),
    )
    db.conn.commit()
    return queue_id


def _seed_failed_bundle(db) -> dict[str, int]:
    content_id = _insert_content(db)
    queue_id = _queue_item(db, content_id)
    db.upsert_content_variant(
        content_id,
        "x",
        "post",
        "Variant post",
        metadata={"headers": {"Authorization": "Bearer variant-secret"}},
    )
    db.select_content_variant(content_id, "x", "post")
    db.upsert_publication_failure(
        content_id,
        "x",
        "401 invalid token",
        error_category="auth",
    )
    attempt_id = db.record_publication_attempt(
        queue_id,
        content_id,
        "x",
        False,
        attempted_at=(BASE_TIME - timedelta(minutes=10)).isoformat(),
        error="401 invalid token",
        error_category="auth",
        response_metadata={
            "status_code": 401,
            "headers": {
                "authorization": "Bearer platform-token",
                "x-request-id": "req-1",
                "set-cookie": "sid=secret; Path=/",
            },
            "body": {
                "message": "Authorization: Basic dXNlcjpwYXNz failed",
                "nested": [{"refreshToken": "refresh-secret"}],
            },
        },
    )
    return {"content_id": content_id, "queue_id": queue_id, "attempt_id": attempt_id}


def test_redacts_secret_keys_and_auth_values_recursively():
    metadata = {
        "access_token": "abc",
        "headers": {"Cookie": "sid=abc", "x-request-id": "req-1"},
        "events": [
            "POST failed with Bearer secret-token",
            {"note": "Authorization: Basic dXNlcjpwYXNz"},
        ],
    }

    redacted = redact_response_metadata(metadata)

    assert redacted["access_token"] == REDACTED
    assert redacted["headers"]["Cookie"] == REDACTED
    assert redacted["headers"]["x-request-id"] == "req-1"
    assert redacted["events"][0] == f"POST failed with Bearer {REDACTED}"
    assert redacted["events"][1]["note"] == f"Authorization: Basic {REDACTED}"


def test_bundle_defaults_to_failed_attempts_only(db):
    ids = _seed_failed_bundle(db)
    db.record_publication_attempt(
        ids["queue_id"],
        ids["content_id"],
        "x",
        True,
        attempted_at=(BASE_TIME - timedelta(minutes=5)).isoformat(),
        platform_post_id="post-1",
    )

    bundle = build_publication_replay_bundle(
        db,
        generated_at=BASE_TIME,
    )

    assert bundle["bundle_version"] == 1
    assert bundle["filters"]["include_successful"] is False
    assert len(bundle["contents"]) == 1
    attempts = bundle["contents"][0]["attempts"]
    assert [attempt["id"] for attempt in attempts] == [ids["attempt_id"]]
    assert attempts[0]["success"] is False
    assert attempts[0]["response_metadata"]["headers"]["authorization"] == REDACTED
    assert attempts[0]["response_metadata"]["headers"]["set-cookie"] == REDACTED


def test_include_successful_adds_success_attempts_and_platform_filter(db):
    ids = _seed_failed_bundle(db)
    success_id = db.record_publication_attempt(
        ids["queue_id"],
        ids["content_id"],
        "x",
        True,
        attempted_at=(BASE_TIME - timedelta(minutes=5)).isoformat(),
        platform_post_id="post-1",
        response_metadata={"headers": {"Authorization": "Bearer ok"}},
    )
    db.record_publication_attempt(
        None,
        ids["content_id"],
        "bluesky",
        False,
        attempted_at=(BASE_TIME - timedelta(minutes=4)).isoformat(),
    )

    bundle = build_publication_replay_bundle(
        db,
        content_id=ids["content_id"],
        platform="x",
        include_successful=True,
        generated_at=BASE_TIME,
    )

    attempts = bundle["contents"][0]["attempts"]
    assert [attempt["id"] for attempt in attempts] == [ids["attempt_id"], success_id]
    assert attempts[1]["success"] is True
    assert attempts[1]["response_metadata"]["headers"]["Authorization"] == REDACTED
    assert {state["platform"] for state in bundle["contents"][0]["platform_states"]} == {"x"}
    assert {variant["platform"] for variant in bundle["contents"][0]["selected_variants"]} == {"x"}


def test_since_filter_uses_attempted_at(db):
    ids = _seed_failed_bundle(db)
    recent_id = db.record_publication_attempt(
        ids["queue_id"],
        ids["content_id"],
        "x",
        False,
        attempted_at=(BASE_TIME - timedelta(minutes=2)).isoformat(),
        error="recent",
    )

    bundle = build_publication_replay_bundle(
        db,
        since=(BASE_TIME - timedelta(minutes=3)).isoformat(),
        generated_at=BASE_TIME,
    )

    assert [attempt["id"] for attempt in bundle["contents"][0]["attempts"]] == [recent_id]


def test_json_shape_is_stable_and_includes_replay_fields(db):
    ids = _seed_failed_bundle(db)

    bundle = build_publication_replay_bundle(
        db,
        content_id=ids["content_id"],
        generated_at=BASE_TIME,
    )
    payload = json.loads(publication_replay_bundle_to_json(bundle))

    assert list(payload) == ["bundle_version", "contents", "filters", "generated_at"]
    content_bundle = payload["contents"][0]
    assert list(content_bundle) == [
        "attempts",
        "content",
        "media",
        "platform_states",
        "selected_variants",
    ]
    assert content_bundle["content"]["source_commits"] == ["abc123"]
    assert content_bundle["media"] == {
        "image_alt_text": "A product card with text",
        "image_path": "/tmp/replay-card.png",
        "image_prompt": "A clean product card",
    }
    assert content_bundle["platform_states"][0]["status"] == "failed"
    assert content_bundle["selected_variants"][0]["content"] == "Variant post"


def test_cli_writes_json_bundle_to_output_path(db, tmp_path, monkeypatch):
    ids = _seed_failed_bundle(db)
    output_path = tmp_path / "bundle.json"
    monkeypatch.setattr(
        "export_publication_replay_bundle.script_context",
        lambda: _script_context(db),
    )

    result = main(
        [
            "--content-id",
            str(ids["content_id"]),
            "--platform",
            "x",
            "--output",
            str(output_path),
        ]
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert result == 0
    assert payload["filters"]["content_id"] == ids["content_id"]
    assert payload["filters"]["platform"] == "x"
    assert payload["contents"][0]["attempts"][0]["response_metadata"]["headers"][
        "authorization"
    ] == REDACTED
