"""Full pipeline smoke tests — end-to-end lifecycle through shared DB."""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fetch_engagement import backfill_tweet_ids


# --- helpers ---


def _make_post_result(success=True, url="https://x.com/u/status/12345",
                      tweet_id="12345"):
    return SimpleNamespace(success=success, url=url, tweet_id=tweet_id, error=None)


# --- Content Pipeline Smoke ---


class TestContentPipelineSmoke:
    def test_full_lifecycle(self, db):
        """Simulate: create content → publish → fetch engagement → classify.

        This test verifies the data flows correctly through the DB between
        daily_digest → retry_unpublished → fetch_engagement → auto_classify.
        """
        # 1. daily_digest creates content
        content_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=["abc123"],
            source_messages=["uuid-1"],
            content="Building better AI tools with structured evaluation",
            eval_score=8.5,
            eval_feedback="Strong candidate",
        )

        # Verify it's unpublished and findable
        unpublished = db.get_unpublished_content("x_post", min_score=7.0)
        assert len(unpublished) == 1
        assert unpublished[0]["id"] == content_id

        # 2. Content gets published (by daily_digest or retry_unpublished)
        db.mark_published(
            content_id,
            "https://x.com/testuser/status/98765",
            tweet_id="98765",
        )

        # No longer in unpublished
        unpublished = db.get_unpublished_content("x_post", min_score=7.0)
        assert len(unpublished) == 0

        # 3. fetch_engagement finds it needs metrics
        posts = db.get_posts_needing_metrics(max_age_days=30)
        assert len(posts) == 1
        assert posts[0]["tweet_id"] == "98765"

        # 4. Engagement metrics arrive
        db.insert_engagement(
            content_id=content_id,
            tweet_id="98765",
            like_count=25,
            retweet_count=8,
            reply_count=4,
            quote_count=2,
            engagement_score=20.0,
        )

        # 5. Auto-classify after settling period
        # Move published_at to >48h ago
        db.conn.execute(
            "UPDATE generated_content SET published_at = datetime('now', '-50 hours') WHERE id = ?",
            (content_id,),
        )
        db.conn.commit()

        result = db.auto_classify_posts(min_age_hours=48)
        assert result["resonated"] == 1

        # 6. Verify final state
        row = db.conn.execute(
            "SELECT published, tweet_id, auto_quality FROM generated_content WHERE id = ?",
            (content_id,),
        ).fetchone()
        assert row["published"] == 1
        assert row["tweet_id"] == "98765"
        assert row["auto_quality"] == "resonated"


# --- Retry Pipeline Smoke ---


class TestRetryPipelineSmoke:
    def test_failed_publish_retry_succeeds(self, db):
        """Simulate: content fails to publish → retry → success."""
        # 1. Content created (by daily_digest) but not published
        content_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=["def456"],
            source_messages=["uuid-2"],
            content="Understanding code review patterns",
            eval_score=8.0,
            eval_feedback="Good post",
        )

        # 2. First publish attempt fails → retry_unpublished increments
        count = db.increment_retry(content_id)
        assert count == 1

        # Still findable for retry
        unpublished = db.get_unpublished_content("x_post", min_score=7.0)
        assert len(unpublished) == 1

        # 3. Second attempt succeeds
        db.mark_published(
            content_id,
            "https://x.com/testuser/status/55555",
            tweet_id="55555",
        )

        # No longer in unpublished
        unpublished = db.get_unpublished_content("x_post", min_score=7.0)
        assert len(unpublished) == 0

        # Verify published state
        row = db.conn.execute(
            "SELECT published, tweet_id, retry_count FROM generated_content WHERE id = ?",
            (content_id,),
        ).fetchone()
        assert row["published"] == 1
        assert row["tweet_id"] == "55555"
        assert row["retry_count"] == 1


# --- Reply Pipeline Smoke ---


class TestReplyPipelineSmoke:
    def test_reply_lifecycle(self, db):
        """Simulate: our post published → someone replies → draft generated → posted."""
        # 1. Our post exists
        content_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=["ghi789"],
            source_messages=["uuid-3"],
            content="Rethinking developer workflows",
            eval_score=9.0,
            eval_feedback="Strong",
        )
        db.mark_published(content_id, "https://x.com/u/status/77777", tweet_id="77777")

        # 2. Someone replies → poll_replies detects and drafts
        ctx_json = json.dumps({
            "engagement_stage": 3,
            "stage_name": "Active",
            "dunbar_tier": 2,
            "tier_name": "Key Network",
            "relationship_strength": 0.65,
        })
        reply_id = db.insert_reply_draft(
            inbound_tweet_id="reply-999",
            inbound_author_handle="dev_alice",
            inbound_author_id="alice_id",
            inbound_text="Great point! How do you handle edge cases?",
            our_tweet_id="77777",
            our_content_id=content_id,
            our_post_text="Rethinking developer workflows",
            draft_text="Good question! I typically...",
            relationship_context=ctx_json,
            quality_score=7.5,
            quality_flags=json.dumps(["substantive"]),
        )

        # 3. Verify it's pending for review
        pending = db.get_pending_replies()
        assert len(pending) == 1
        reply = pending[0]
        assert reply["inbound_author_handle"] == "dev_alice"
        assert reply["draft_text"] == "Good question! I typically..."
        assert reply["quality_score"] == 7.5

        # Relationship context round-trip
        ctx = json.loads(reply["relationship_context"])
        assert ctx["stage_name"] == "Active"
        assert ctx["dunbar_tier"] == 2

        # 4. User approves → review_replies posts
        db.update_reply_status(reply_id, "posted", posted_tweet_id="reply-posted-123")

        # No longer pending
        pending = db.get_pending_replies()
        assert len(pending) == 0

        # Verify final state
        row = db.conn.execute(
            "SELECT status, posted_tweet_id FROM reply_queue WHERE id = ?",
            (reply_id,),
        ).fetchone()
        assert row["status"] == "posted"
        assert row["posted_tweet_id"] == "reply-posted-123"
