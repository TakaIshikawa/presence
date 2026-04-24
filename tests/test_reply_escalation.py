"""Tests for reply escalation recommendations."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from engagement.reply_escalation import (
    recommendations_to_jsonable,
    recommend_reply_escalations,
)
from reply_escalations import main


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _mock_script_context(db):
    @contextmanager
    def _ctx():
        yield (SimpleNamespace(), db)

    return _ctx


def _insert_reply(db, tweet_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="Nice post",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="Thanks",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (detected_at, reply_id),
    )
    db.conn.commit()


def test_old_pending_draft_recommends_review_now(db):
    reply_id = _insert_reply(
        db,
        "old-normal",
        quality_score=7.5,
        relationship_context=json.dumps(
            {"stage_name": "Active", "engagement_stage": 3}
        ),
    )
    _set_detected_at(db, reply_id, "2026-04-23 04:00:00")

    rows = db.get_pending_reply_sla(now=NOW)
    recommendations = recommend_reply_escalations(
        rows,
        min_age_hours=6,
        now=NOW,
    )

    assert len(recommendations) == 1
    assert recommendations[0].recommendation == "review_now"
    assert recommendations[0].draft_id == reply_id
    assert recommendations[0].target == "alice"
    assert recommendations[0].age_hours == 8.0
    assert "older than 6h threshold" in recommendations[0].reasons


def test_fresh_pending_draft_waits_until_threshold(db):
    reply_id = _insert_reply(db, "fresh-normal", quality_score=8.0)
    _set_detected_at(db, reply_id, "2026-04-23 10:30:00")

    rows = db.get_pending_reply_sla(now=NOW)
    recommendations = recommend_reply_escalations(
        rows,
        min_age_hours=6,
        now=NOW,
    )

    assert recommendations[0].draft_id == reply_id
    assert recommendations[0].recommendation == "wait"
    assert recommendations[0].reasons == ["younger than 6h threshold"]


def test_low_score_recommends_revision_even_when_fresh(db):
    reply_id = _insert_reply(db, "low-score", quality_score=4.5)
    _set_detected_at(db, reply_id, "2026-04-23 11:30:00")

    rows = db.get_pending_reply_sla(now=NOW)
    recommendations = recommend_reply_escalations(
        rows,
        min_age_hours=6,
        now=NOW,
    )

    assert recommendations[0].draft_id == reply_id
    assert recommendations[0].recommendation == "revise"
    assert recommendations[0].reasons == ["quality score 4.5/10"]


def test_sycophancy_flag_recommends_dismissal(db):
    reply_id = _insert_reply(
        db,
        "sycophantic",
        quality_score=7.0,
        quality_flags='["sycophantic"]',
    )
    _set_detected_at(db, reply_id, "2026-04-23 11:30:00")

    rows = db.get_pending_reply_sla(now=NOW)
    recommendations = recommend_reply_escalations(
        rows,
        min_age_hours=6,
        now=NOW,
    )

    assert recommendations[0].draft_id == reply_id
    assert recommendations[0].recommendation == "dismiss"
    assert recommendations[0].reasons == ["quality flag: sycophantic"]


def test_generic_flag_recommends_revision(db):
    reply_id = _insert_reply(db, "generic", quality_flags='["generic"]')
    _set_detected_at(db, reply_id, "2026-04-23 04:00:00")

    rows = db.get_pending_reply_sla(now=NOW)
    recommendations = recommend_reply_escalations(
        rows,
        min_age_hours=6,
        now=NOW,
    )

    assert recommendations[0].draft_id == reply_id
    assert recommendations[0].recommendation == "revise"
    assert recommendations[0].reasons == ["quality flags: generic"]


def test_low_priority_is_excluded_unless_requested(db):
    low_id = _insert_reply(db, "low", priority="low", quality_score=8.0)
    normal_id = _insert_reply(db, "normal", priority="normal", quality_score=8.0)
    _set_detected_at(db, low_id, "2026-04-23 04:00:00")
    _set_detected_at(db, normal_id, "2026-04-23 04:00:00")

    rows = db.get_pending_reply_sla(now=NOW)
    default_recommendations = recommend_reply_escalations(
        rows,
        min_age_hours=6,
        now=NOW,
    )
    all_recommendations = recommend_reply_escalations(
        rows,
        min_age_hours=6,
        include_low_priority=True,
        now=NOW,
    )

    assert [item.draft_id for item in default_recommendations] == [normal_id]
    assert [item.draft_id for item in all_recommendations] == [normal_id, low_id]


def test_json_output_is_stable_and_includes_required_fields(db):
    reply_id = _insert_reply(
        db,
        "json",
        platform="bluesky",
        priority="high",
        quality_score=8.0,
    )
    _set_detected_at(db, reply_id, "2026-04-23 04:00:00")

    rows = db.get_pending_reply_sla(now=NOW)
    recommendations = recommend_reply_escalations(
        rows,
        min_age_hours=6,
        now=NOW,
    )
    payload = recommendations_to_jsonable(
        recommendations,
        min_age_hours=6,
        include_low_priority=False,
    )
    decoded = json.loads(json.dumps(payload, sort_keys=True))

    assert decoded["filters"] == {
        "include_low_priority": False,
        "min_age_hours": 6,
    }
    assert decoded["total"] == 1
    draft = decoded["drafts"][0]
    assert draft["draft_id"] == reply_id
    assert draft["target"] == "alice"
    assert draft["age_hours"] == 8.0
    assert draft["recommendation"] == "review_now"
    assert draft["reasons"][:1] == ["older than 6h threshold"]


def test_cli_json_output(capsys):
    class FakeDb:
        def get_pending_reply_sla(self):
            return [
                {
                    "id": 1,
                    "status": "pending",
                    "age_hours": 7.25,
                    "priority": "normal",
                    "platform": "x",
                    "inbound_author_handle": "alice",
                    "relationship_context": None,
                    "quality_score": 7.0,
                    "quality_flags": "[]",
                }
            ]

    with patch("reply_escalations.script_context", _mock_script_context(FakeDb())):
        assert main(["--json", "--min-age-hours", "6"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["drafts"][0]["draft_id"] == 1
    assert payload["drafts"][0]["target"] == "alice"
    assert payload["drafts"][0]["age_hours"] == 7.25
    assert payload["drafts"][0]["recommendation"] == "review_now"
