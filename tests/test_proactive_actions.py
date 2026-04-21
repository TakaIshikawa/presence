"""Tests for proactive_actions table CRUD and ProactiveConfig."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from config import ProactiveConfig, load_config
from storage.db import IntegrityError


class TestProactiveActionsDB:
    """Test proactive_actions table CRUD via Database methods."""

    def _insert_action(self, db, **overrides):
        defaults = dict(
            action_type="reply",
            target_tweet_id="tweet_001",
            target_tweet_text="Great thread on AI agents",
            target_author_handle="karpathy",
            target_author_id="123456",
            discovery_source="curated_timeline",
            relevance_score=0.75,
            draft_text="Nice insight — we've seen similar patterns.",
        )
        defaults.update(overrides)
        return db.insert_proactive_action(**defaults)

    def test_insert_and_get_pending(self, db):
        row_id = self._insert_action(db)
        assert row_id > 0

        pending = db.get_pending_proactive_actions()
        assert len(pending) == 1
        assert pending[0]["target_tweet_id"] == "tweet_001"
        assert pending[0]["action_type"] == "reply"
        assert pending[0]["status"] == "pending"
        assert pending[0]["relevance_score"] == 0.75

    def test_dedup_same_tweet_and_action_type(self, db):
        self._insert_action(db, target_tweet_id="t1", action_type="reply")
        with pytest.raises(IntegrityError):
            self._insert_action(db, target_tweet_id="t1", action_type="reply")

    def test_different_action_types_same_tweet_allowed(self, db):
        self._insert_action(db, target_tweet_id="t1", action_type="reply")
        row_id = self._insert_action(db, target_tweet_id="t1", action_type="like")
        assert row_id > 0

    def test_mark_posted(self, db):
        action_id = self._insert_action(db)
        db.mark_proactive_posted(action_id, "posted_tweet_999")

        pending = db.get_pending_proactive_actions()
        assert len(pending) == 0

        # Verify posted state
        row = db.conn.execute(
            "SELECT status, posted_tweet_id, posted_at, reviewed_at "
            "FROM proactive_actions WHERE id = ?",
            (action_id,),
        ).fetchone()
        assert row[0] == "posted"
        assert row[1] == "posted_tweet_999"
        assert row[2] is not None  # posted_at set
        assert row[3] is not None  # reviewed_at set

    def test_dismiss(self, db):
        action_id = self._insert_action(db)
        db.dismiss_proactive_action(action_id)

        pending = db.get_pending_proactive_actions()
        assert len(pending) == 0

        row = db.conn.execute(
            "SELECT status, reviewed_at FROM proactive_actions WHERE id = ?",
            (action_id,),
        ).fetchone()
        assert row[0] == "dismissed"
        assert row[1] is not None

    def test_count_daily_proactive_posts(self, db):
        # Insert 3 posted today
        for i in range(3):
            aid = self._insert_action(db, target_tweet_id=f"today_{i}")
            db.mark_proactive_posted(aid, f"posted_{i}")

        # Insert 1 posted yesterday (manually set posted_at)
        aid = self._insert_action(db, target_tweet_id="yesterday_0")
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        db.conn.execute(
            "UPDATE proactive_actions SET status='posted', posted_at=? WHERE id=?",
            (yesterday, aid),
        )
        db.conn.commit()

        assert db.count_daily_proactive_posts("reply") == 3

    def test_count_weekly_replies_to_author(self, db):
        # 2 replies to karpathy this week
        for i in range(2):
            aid = self._insert_action(
                db, target_tweet_id=f"k_{i}", target_author_handle="karpathy"
            )
            db.mark_proactive_posted(aid, f"posted_k_{i}")

        # 1 reply to karpathy 10 days ago (outside weekly window)
        aid = self._insert_action(
            db, target_tweet_id="k_old", target_author_handle="karpathy"
        )
        old_date = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
        db.conn.execute(
            "UPDATE proactive_actions SET status='posted', posted_at=? WHERE id=?",
            (old_date, aid),
        )
        db.conn.commit()

        assert db.count_weekly_replies_to_author("karpathy") == 2

    def test_count_recent_proactive_posts_to_author_counts_all_action_types(self, db):
        for action_type in ("reply", "quote_tweet", "like", "retweet"):
            aid = self._insert_action(
                db,
                action_type=action_type,
                target_tweet_id=f"{action_type}_recent",
                target_author_handle="Karpathy",
            )
            db.mark_proactive_posted(aid, f"posted_{action_type}")

        assert db.count_recent_proactive_posts_to_author("karpathy", 72) == 4
        assert db.count_recent_proactive_posts_to_author("@KARPATHY", 72) == 4

    def test_count_recent_proactive_posts_to_author_respects_cooldown_window(self, db):
        recent_id = self._insert_action(
            db, target_tweet_id="recent", target_author_handle="karpathy"
        )
        db.mark_proactive_posted(recent_id, "posted_recent")

        old_id = self._insert_action(
            db, target_tweet_id="old", target_author_handle="karpathy"
        )
        old_date = (datetime.now(timezone.utc) - timedelta(hours=73)).isoformat()
        db.conn.execute(
            "UPDATE proactive_actions SET status='posted', posted_at=? WHERE id=?",
            (old_date, old_id),
        )
        db.conn.commit()

        assert db.count_recent_proactive_posts_to_author("karpathy", 72) == 1
        assert db.count_recent_proactive_posts_to_author("karpathy", 0) == 0

    def test_proactive_action_exists(self, db):
        assert db.proactive_action_exists("tweet_001", "reply") is False
        self._insert_action(db, target_tweet_id="tweet_001", action_type="reply")
        assert db.proactive_action_exists("tweet_001", "reply") is True
        assert db.proactive_action_exists("tweet_001", "like") is False

    def test_get_pending_orders_by_relevance(self, db):
        self._insert_action(db, target_tweet_id="low", relevance_score=0.3)
        self._insert_action(db, target_tweet_id="high", relevance_score=0.9)
        self._insert_action(db, target_tweet_id="mid", relevance_score=0.6)

        pending = db.get_pending_proactive_actions()
        scores = [p["relevance_score"] for p in pending]
        assert scores == [0.9, 0.6, 0.3]

    def test_get_pending_respects_limit(self, db):
        for i in range(5):
            self._insert_action(db, target_tweet_id=f"t_{i}")
        assert len(db.get_pending_proactive_actions(limit=2)) == 2

    def test_knowledge_ids_stored_as_json(self, db):
        knowledge = json.dumps([(1, 0.85), (3, 0.72)])
        self._insert_action(db, knowledge_ids=knowledge)
        pending = db.get_pending_proactive_actions()
        parsed = json.loads(pending[0]["knowledge_ids"])
        assert parsed == [[1, 0.85], [3, 0.72]]


class TestProactiveConfig:
    """Test ProactiveConfig dataclass and YAML parsing."""

    def test_defaults(self):
        config = ProactiveConfig()
        assert config.enabled is False
        assert config.max_daily_replies == 5
        assert config.account_cooldown_hours == 72
        assert config.min_relevance == 0.50
        assert config.max_tweet_age_hours == 24
        assert config.reply_cap_per_account == 2
        assert config.search_enabled is False
        assert config.search_keywords is None

    def test_load_from_yaml(self, tmp_path):
        yaml_content = """\
github:
  username: test
  token: fake
x:
  api_key: k
  api_secret: s
  access_token: t
  access_token_secret: ts
anthropic:
  api_key: fake
paths:
  claude_logs: /tmp
  static_site: /tmp
  database: /tmp/test.db
synthesis:
  model: test
  eval_threshold: 0.7
polling:
  interval_minutes: 10
  daily_digest_hour: 23
  weekly_digest_day: sunday
proactive:
  enabled: true
  max_daily_replies: 3
  account_cooldown_hours: 48
  min_relevance: 0.60
  search_enabled: true
  search_keywords:
    - "AI agents"
    - "LLM tools"
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml_content)
        config = load_config(str(config_file))

        assert config.proactive is not None
        assert config.proactive.enabled is True
        assert config.proactive.max_daily_replies == 3
        assert config.proactive.account_cooldown_hours == 48
        assert config.proactive.min_relevance == 0.60
        assert config.proactive.search_enabled is True
        assert config.proactive.search_keywords == ["AI agents", "LLM tools"]

    def test_load_without_proactive_section(self, tmp_path):
        yaml_content = """\
github:
  username: test
  token: fake
x:
  api_key: k
  api_secret: s
  access_token: t
  access_token_secret: ts
anthropic:
  api_key: fake
paths:
  claude_logs: /tmp
  static_site: /tmp
  database: /tmp/test.db
synthesis:
  model: test
  eval_threshold: 0.7
polling:
  interval_minutes: 10
  daily_digest_hour: 23
  weekly_digest_day: sunday
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml_content)
        config = load_config(str(config_file))

        assert config.proactive is None
