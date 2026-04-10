"""Tests for CultivateBridge — cultivate DB adapter."""

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

from engagement.cultivate_bridge import (
    CultivateBridge,
    PersonContext,
    ProactiveAction,
)

# Cultivate schema (from cultivate/src/cultivate/db/schema.py) — hardcoded to
# avoid importing cultivate as a dependency.
CULTIVATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS people (
    id TEXT PRIMARY KEY,
    x_user_id TEXT UNIQUE NOT NULL,
    x_handle TEXT NOT NULL,
    display_name TEXT NOT NULL,
    bio TEXT,
    followers_count INTEGER,
    following_count INTEGER,
    verified INTEGER DEFAULT 0,
    profile_image_url TEXT,
    is_self INTEGER DEFAULT 0,
    first_seen TEXT NOT NULL,
    last_updated TEXT NOT NULL,
    relationship_strength REAL,
    cluster_id INTEGER,
    centrality_degree REAL,
    centrality_betweenness REAL,
    authenticity_score REAL,
    content_quality_score REAL,
    content_relevance_score REAL,
    suspended INTEGER DEFAULT 0,
    blocked INTEGER DEFAULT 0,
    prev_relationship_strength REAL,
    engagement_stage INTEGER,
    dunbar_tier INTEGER
);

CREATE TABLE IF NOT EXISTS interactions (
    id TEXT PRIMARY KEY,
    actor_person_id TEXT NOT NULL REFERENCES people(id),
    target_person_id TEXT NOT NULL REFERENCES people(id),
    interaction_type TEXT NOT NULL,
    x_tweet_id TEXT,
    content_snippet TEXT,
    occurred_at TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    UNIQUE(x_tweet_id, interaction_type)
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    payload TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    processed INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS actions (
    id TEXT PRIMARY KEY,
    decision_id TEXT,
    action_type TEXT NOT NULL,
    target_person_id TEXT NOT NULL REFERENCES people(id),
    description TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'suggested',
    created_at TEXT NOT NULL,
    completed_at TEXT,
    payload TEXT
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def _init_cultivate_db(conn: sqlite3.Connection) -> None:
    """Initialize in-memory DB with cultivate schema + seed data."""
    conn.executescript(CULTIVATE_SCHEMA)

    # Self person
    conn.execute(
        """INSERT INTO people (id, x_user_id, x_handle, display_name, bio,
           followers_count, following_count, is_self, first_seen, last_updated)
           VALUES ('self-1', '1001', 'taka52ishikawa', 'Taka', 'AI builder',
                   500, 200, 1, '2025-01-01T00:00:00', '2025-01-01T00:00:00')"""
    )

    # Known person with full context
    conn.execute(
        """INSERT INTO people (id, x_user_id, x_handle, display_name, bio,
           followers_count, following_count, is_self, first_seen, last_updated,
           relationship_strength, engagement_stage, dunbar_tier,
           authenticity_score, content_quality_score, content_relevance_score)
           VALUES ('person-a', '2001', 'dev_alice', 'Alice Dev', 'Full-stack engineer',
                   1000, 300, 0, '2025-02-01T00:00:00', '2025-03-01T00:00:00',
                   0.65, 3, 2, 0.85, 0.72, 0.68)"""
    )

    # Known person with minimal data
    conn.execute(
        """INSERT INTO people (id, x_user_id, x_handle, display_name, bio,
           followers_count, following_count, is_self, first_seen, last_updated)
           VALUES ('person-b', '2002', 'tech_bob', 'Bob Tech', NULL,
                   50, 80, 0, '2025-03-01T00:00:00', '2025-03-01T00:00:00')"""
    )

    # Interactions between self and alice
    conn.execute(
        """INSERT INTO interactions (id, actor_person_id, target_person_id,
           interaction_type, x_tweet_id, content_snippet, occurred_at, ingested_at)
           VALUES ('ix-1', 'person-a', 'self-1', 'reply', 'tw-100',
                   'interesting take on agents', '2026-03-28T10:00:00', '2026-03-28T10:01:00')"""
    )
    conn.execute(
        """INSERT INTO interactions (id, actor_person_id, target_person_id,
           interaction_type, x_tweet_id, content_snippet, occurred_at, ingested_at)
           VALUES ('ix-2', 'self-1', 'person-a', 'like', 'tw-101',
                   NULL, '2026-03-20T08:00:00', '2026-03-20T08:01:00')"""
    )

    # Suggested actions
    conn.execute(
        """INSERT INTO actions (id, action_type, target_person_id, description,
           status, created_at, payload)
           VALUES ('act-1', 'engage', 'person-a', '[like] Like latest tweet (@dev_alice)',
                   'suggested', '2026-04-01T00:00:00',
                   '{"tweet_id": "tw-200", "tweet_content": "Just shipped v2"}')"""
    )
    conn.execute(
        """INSERT INTO actions (id, action_type, target_person_id, description,
           status, created_at)
           VALUES ('act-2', 'strengthen', 'person-b', '[reply] Reply to @tech_bob',
                   'suggested', '2026-04-01T01:00:00')"""
    )
    conn.execute(
        """INSERT INTO actions (id, action_type, target_person_id, description,
           status, created_at)
           VALUES ('act-done', 'engage', 'person-a', 'Already completed',
                   'completed', '2026-03-30T00:00:00')"""
    )

    conn.commit()


@pytest.fixture
def cultivate_db():
    """In-memory cultivate DB with test data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _init_cultivate_db(conn)
    yield conn
    conn.close()


@pytest.fixture
def bridge(cultivate_db):
    """CultivateBridge connected to in-memory DB."""
    return CultivateBridge(cultivate_db)


# -- try_connect tests -------------------------------------------------------


class TestTryConnect:
    def test_returns_bridge_when_db_exists(self, tmp_path):
        db_path = tmp_path / "cultivate.db"
        conn = sqlite3.connect(str(db_path))
        conn.executescript(CULTIVATE_SCHEMA)
        conn.close()

        bridge = CultivateBridge.try_connect(str(db_path))
        assert bridge is not None
        bridge.close()

    def test_returns_none_when_db_missing(self, tmp_path):
        bridge = CultivateBridge.try_connect(str(tmp_path / "nonexistent.db"))
        assert bridge is None

    def test_returns_none_when_schema_wrong(self, tmp_path):
        db_path = tmp_path / "bad.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE foo (id TEXT)")
        conn.commit()
        conn.close()

        bridge = CultivateBridge.try_connect(str(db_path))
        assert bridge is None


# -- get_person_context tests ------------------------------------------------


class TestGetPersonContext:
    def test_known_person_full_data(self, bridge):
        ctx = bridge.get_person_context("dev_alice")
        assert ctx is not None
        assert ctx.x_handle == "dev_alice"
        assert ctx.display_name == "Alice Dev"
        assert ctx.bio == "Full-stack engineer"
        assert ctx.relationship_strength == 0.65
        assert ctx.engagement_stage == 3
        assert ctx.stage_name == "Active"
        assert ctx.dunbar_tier == 2
        assert ctx.tier_name == "Key Network"
        assert ctx.authenticity_score == 0.85
        assert ctx.content_quality_score == 0.72
        assert ctx.is_known is True

    def test_known_person_has_interactions(self, bridge):
        ctx = bridge.get_person_context("dev_alice")
        assert len(ctx.recent_interactions) == 2
        # Most recent first
        assert ctx.recent_interactions[0]["type"] == "reply"
        assert ctx.recent_interactions[0]["direction"] == "them \u2192 me"
        assert ctx.recent_interactions[1]["type"] == "like"
        assert ctx.recent_interactions[1]["direction"] == "me \u2192 them"

    def test_known_person_partial_data(self, bridge):
        ctx = bridge.get_person_context("tech_bob")
        assert ctx is not None
        assert ctx.x_handle == "tech_bob"
        assert ctx.bio is None
        assert ctx.relationship_strength is None
        assert ctx.engagement_stage is None
        assert ctx.stage_name == "Unknown"
        assert ctx.dunbar_tier is None
        assert ctx.tier_name == "Unknown"
        assert ctx.recent_interactions == []

    def test_unknown_handle_returns_none(self, bridge):
        ctx = bridge.get_person_context("nonexistent_user")
        assert ctx is None

    def test_strips_at_sign(self, bridge):
        ctx = bridge.get_person_context("@dev_alice")
        assert ctx is not None
        assert ctx.x_handle == "dev_alice"


class TestGetPersonContextByXId:
    def test_known_user_id(self, bridge):
        ctx = bridge.get_person_context_by_x_id("2001")
        assert ctx is not None
        assert ctx.x_handle == "dev_alice"

    def test_unknown_user_id(self, bridge):
        ctx = bridge.get_person_context_by_x_id("9999")
        assert ctx is None


# -- get_pending_proactive_actions tests -------------------------------------


class TestGetPendingProactiveActions:
    def test_returns_suggested_actions(self, bridge):
        actions = bridge.get_pending_proactive_actions()
        assert len(actions) == 2  # act-1 and act-2 (not act-done)

    def test_action_has_person_context(self, bridge):
        actions = bridge.get_pending_proactive_actions()
        act = actions[0]
        assert act.action_id == "act-1"
        assert act.action_type == "engage"
        assert act.target_handle == "dev_alice"
        assert act.person_context is not None
        assert act.person_context.engagement_stage == 3

    def test_action_payload_parsed(self, bridge):
        actions = bridge.get_pending_proactive_actions()
        act = actions[0]
        assert act.payload is not None
        assert act.payload["tweet_id"] == "tw-200"
        assert act.payload["tweet_content"] == "Just shipped v2"

    def test_action_without_payload(self, bridge):
        actions = bridge.get_pending_proactive_actions()
        act = actions[1]  # act-2 has no payload
        assert act.payload is None

    def test_empty_when_none_pending(self, cultivate_db):
        cultivate_db.execute("UPDATE actions SET status = 'completed'")
        cultivate_db.commit()
        b = CultivateBridge(cultivate_db)
        assert b.get_pending_proactive_actions() == []

    def test_respects_limit(self, bridge):
        actions = bridge.get_pending_proactive_actions(limit=1)
        assert len(actions) == 1


# -- record_mention_event tests ----------------------------------------------


class TestRecordMentionEvent:
    def test_inserts_event(self, bridge, cultivate_db):
        bridge.record_mention_event(
            tweet_id="tw-999",
            author_x_id="2001",
            author_handle="dev_alice",
            text="Hey nice post!",
            created_at="2026-04-05T12:00:00",
        )

        row = cultivate_db.execute(
            "SELECT * FROM events WHERE event_type = 'mention'"
        ).fetchone()
        assert row is not None
        assert row["source"] == "presence_poll"
        payload = json.loads(row["payload"])
        assert payload["tweet_id"] == "tw-999"
        assert payload["author_handle"] == "dev_alice"

    def test_inserts_interaction_for_known_person(self, bridge, cultivate_db):
        bridge.record_mention_event(
            tweet_id="tw-999",
            author_x_id="2001",
            author_handle="dev_alice",
            text="Hey nice post!",
            created_at="2026-04-05T12:00:00",
        )

        row = cultivate_db.execute(
            "SELECT * FROM interactions WHERE x_tweet_id = 'tw-999'"
        ).fetchone()
        assert row is not None
        assert row["actor_person_id"] == "person-a"
        assert row["target_person_id"] == "self-1"
        assert row["interaction_type"] == "mention"
        assert row["content_snippet"] == "Hey nice post!"

    def test_no_interaction_for_unknown_person(self, bridge, cultivate_db):
        bridge.record_mention_event(
            tweet_id="tw-888",
            author_x_id="9999",
            author_handle="unknown_user",
            text="Random mention",
            created_at="2026-04-05T12:00:00",
        )

        # Event should exist
        event = cultivate_db.execute(
            "SELECT * FROM events ORDER BY detected_at DESC LIMIT 1"
        ).fetchone()
        assert event is not None

        # But no interaction for unknown person
        ix = cultivate_db.execute(
            "SELECT * FROM interactions WHERE x_tweet_id = 'tw-888'"
        ).fetchone()
        assert ix is None

    def test_no_duplicate_interaction(self, bridge, cultivate_db):
        bridge.record_mention_event(
            tweet_id="tw-777",
            author_x_id="2001",
            author_handle="dev_alice",
            text="First mention",
            created_at="2026-04-05T12:00:00",
        )
        bridge.record_mention_event(
            tweet_id="tw-777",
            author_x_id="2001",
            author_handle="dev_alice",
            text="Duplicate mention",
            created_at="2026-04-05T12:00:00",
        )

        count = cultivate_db.execute(
            "SELECT COUNT(*) as cnt FROM interactions WHERE x_tweet_id = 'tw-777'"
        ).fetchone()["cnt"]
        assert count == 1


# -- mark_action tests -------------------------------------------------------


class TestMarkActionCompleted:
    def test_updates_status_and_timestamp(self, bridge, cultivate_db):
        bridge.mark_action_completed("act-1")

        row = cultivate_db.execute(
            "SELECT status, completed_at FROM actions WHERE id = 'act-1'"
        ).fetchone()
        assert row["status"] == "completed"
        assert row["completed_at"] is not None


class TestMarkActionDismissed:
    def test_updates_status(self, bridge, cultivate_db):
        bridge.mark_action_dismissed("act-2")

        row = cultivate_db.execute(
            "SELECT status FROM actions WHERE id = 'act-2'"
        ).fetchone()
        assert row["status"] == "dismissed"


# -- update_action_payload tests -----------------------------------------------


class TestUpdateActionPayload:
    def test_writes_payload_to_empty_action(self, bridge, cultivate_db):
        bridge.update_action_payload("act-2", {"execution_type": "reply", "tweet_id": "tw-500"})

        row = cultivate_db.execute(
            "SELECT payload FROM actions WHERE id = 'act-2'"
        ).fetchone()
        payload = json.loads(row["payload"])
        assert payload["execution_type"] == "reply"
        assert payload["tweet_id"] == "tw-500"

    def test_merges_with_existing_payload(self, bridge, cultivate_db):
        # act-1 has existing payload: {"tweet_id": "tw-200", "tweet_content": "Just shipped v2"}
        bridge.update_action_payload("act-1", {"execution_type": "like", "resolved_at": "2026-04-09"})

        row = cultivate_db.execute(
            "SELECT payload FROM actions WHERE id = 'act-1'"
        ).fetchone()
        payload = json.loads(row["payload"])
        # Existing fields preserved
        assert payload["tweet_id"] == "tw-200"
        assert payload["tweet_content"] == "Just shipped v2"
        # New fields added
        assert payload["execution_type"] == "like"
        assert payload["resolved_at"] == "2026-04-09"

    def test_overwrites_existing_key(self, bridge, cultivate_db):
        bridge.update_action_payload("act-1", {"tweet_id": "tw-new"})

        row = cultivate_db.execute(
            "SELECT payload FROM actions WHERE id = 'act-1'"
        ).fetchone()
        payload = json.loads(row["payload"])
        assert payload["tweet_id"] == "tw-new"


# -- PersonContext serialization tests ----------------------------------------


class TestPersonContextSerialization:
    def test_round_trip(self):
        ctx = PersonContext(
            x_handle="dev_alice",
            display_name="Alice Dev",
            bio="Engineer",
            relationship_strength=0.65,
            engagement_stage=3,
            dunbar_tier=2,
            authenticity_score=0.85,
            content_quality_score=0.72,
            content_relevance_score=0.68,
            recent_interactions=[
                {"type": "reply", "snippet": "nice", "date": "2026-03-28", "direction": "them \u2192 me"}
            ],
            is_known=True,
        )
        json_str = ctx.to_json()
        restored = PersonContext.from_json(json_str)
        assert restored.x_handle == ctx.x_handle
        assert restored.engagement_stage == ctx.engagement_stage
        assert restored.recent_interactions == ctx.recent_interactions
