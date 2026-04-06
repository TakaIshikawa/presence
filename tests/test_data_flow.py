"""Cross-script data flow tests — verify DB contracts between scripts."""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fetch_engagement import backfill_tweet_ids
from update_operations_state import sync_operation


# --- helpers ---


def _insert_content(db, content="Test post", eval_score=8.0, content_type="x_post"):
    """Insert a generated_content row and return its ID."""
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=["sha1"],
        source_messages=["uuid1"],
        content=content,
        eval_score=eval_score,
        eval_feedback="Good",
    )


def _insert_reply(db, inbound_tweet_id="tweet-100", our_tweet_id="tweet-1",
                  draft="Thanks!", relationship_context=None,
                  quality_score=None, quality_flags=None):
    """Insert a reply_queue row and return its ID."""
    return db.insert_reply_draft(
        inbound_tweet_id=inbound_tweet_id,
        inbound_author_handle="replier",
        inbound_author_id="replier_id",
        inbound_text="Nice post!",
        our_tweet_id=our_tweet_id,
        our_content_id=None,
        our_post_text="Our original post",
        draft_text=draft,
        relationship_context=relationship_context,
        quality_score=quality_score,
        quality_flags=quality_flags,
    )


# --- Content Pipeline Flow ---
# daily_digest → retry_unpublished → fetch_engagement → curate


class TestContentPipelineFlow:
    def test_generated_content_readable_by_retry(self, db):
        """Content inserted by daily_digest should be found by retry_unpublished."""
        content_id = _insert_content(db, eval_score=8.0)

        unpublished = db.get_unpublished_content("x_post", min_score=7.0)
        assert len(unpublished) == 1
        assert unpublished[0]["id"] == content_id
        assert unpublished[0]["content"] == "Test post"

    def test_mark_published_readable_by_engagement(self, db):
        """Published content with tweet_id should appear in posts needing metrics."""
        content_id = _insert_content(db)
        db.mark_published(content_id, "https://x.com/u/status/12345", tweet_id="12345")

        posts = db.get_posts_needing_metrics(max_age_days=30)
        assert len(posts) == 1
        assert posts[0]["tweet_id"] == "12345"

    def test_engagement_inserted_readable_by_classification(self, db):
        """Engagement data should be usable by auto_classify_posts."""
        content_id = _insert_content(db)
        db.mark_published(content_id, "https://x.com/u/status/100", tweet_id="100")
        # Set published_at far enough in the past using SQL datetime
        # auto_classify uses: published_at <= datetime('now', '-48 hours')
        db.conn.execute(
            "UPDATE generated_content SET published_at = datetime('now', '-50 hours') WHERE id = ?",
            (content_id,),
        )
        db.conn.commit()

        db.insert_engagement(
            content_id=content_id,
            tweet_id="100",
            like_count=20,
            retweet_count=5,
            reply_count=3,
            quote_count=1,
            engagement_score=15.0,
        )

        result = db.auto_classify_posts(min_age_hours=48)
        assert result["resonated"] == 1

    def test_curation_flag_persists_through_lifecycle(self, db):
        """Curation flag set by curate.py should persist."""
        content_id = _insert_content(db)
        db.mark_published(content_id, "https://x.com/u/status/100", tweet_id="100")
        db.set_curation_quality(content_id, "good")

        row = db.conn.execute(
            "SELECT curation_quality FROM generated_content WHERE id = ?",
            (content_id,),
        ).fetchone()
        assert row["curation_quality"] == "good"


# --- Reply Pipeline Flow ---
# poll_replies → review_replies


class TestReplyPipelineFlow:
    def test_draft_inserted_readable_by_review(self, db):
        """Reply drafted by poll_replies should appear in pending for review_replies."""
        ctx = json.dumps({"engagement_stage": 3, "stage_name": "Active"})
        flags = json.dumps(["generic"])
        reply_id = _insert_reply(
            db,
            relationship_context=ctx,
            quality_score=6.5,
            quality_flags=flags,
        )

        pending = db.get_pending_replies()
        assert len(pending) == 1
        assert pending[0]["draft_text"] == "Thanks!"
        assert pending[0]["relationship_context"] == ctx
        assert pending[0]["quality_score"] == 6.5
        assert json.loads(pending[0]["quality_flags"]) == ["generic"]

    def test_posted_reply_not_in_pending(self, db):
        """After review_replies posts a reply, it should not appear in pending."""
        reply_id = _insert_reply(db)
        db.update_reply_status(reply_id, "posted", posted_tweet_id="posted-123")

        pending = db.get_pending_replies()
        assert len(pending) == 0

    def test_dismissed_reply_not_in_pending(self, db):
        """Dismissed replies should not appear in pending."""
        reply_id = _insert_reply(db)
        db.update_reply_status(reply_id, "dismissed")

        pending = db.get_pending_replies()
        assert len(pending) == 0

    def test_reply_state_cursor_persists(self, db):
        """Mention cursor set by poll_replies should be readable on next run."""
        db.set_last_mention_id("mention-999")
        assert db.get_last_mention_id() == "mention-999"

        # Update again
        db.set_last_mention_id("mention-1000")
        assert db.get_last_mention_id() == "mention-1000"


# --- Engagement Feedback Flow ---
# fetch_engagement → curate → analyze_backtest


class TestEngagementFeedbackFlow:
    def test_backfill_tweet_ids_from_published_urls(self, db):
        """backfill_tweet_ids should extract tweet_id from published URLs."""
        content_id = _insert_content(db)
        db.mark_published(content_id, "https://x.com/user/status/54321")

        count = backfill_tweet_ids(db)
        assert count == 1

        row = db.conn.execute(
            "SELECT tweet_id FROM generated_content WHERE id = ?",
            (content_id,),
        ).fetchone()
        assert row["tweet_id"] == "54321"

    def test_auto_classify_only_after_48h(self, db):
        """Posts should only be classified after the 48h settling period."""
        content_id = _insert_content(db)
        db.mark_published(content_id, "https://x.com/u/status/100", tweet_id="100")

        # Set published_at to 24h ago (too recent)
        recent = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        db.conn.execute(
            "UPDATE generated_content SET published_at = ? WHERE id = ?",
            (recent, content_id),
        )
        db.conn.commit()

        db.insert_engagement(
            content_id=content_id, tweet_id="100",
            like_count=50, retweet_count=10, reply_count=5, quote_count=2,
            engagement_score=30.0,
        )

        result = db.auto_classify_posts(min_age_hours=48)
        assert result["resonated"] == 0  # Too recent

    def test_multiple_engagement_snapshots(self, db):
        """Multiple engagement records for same tweet should all be stored."""
        content_id = _insert_content(db)
        db.mark_published(content_id, "https://x.com/u/status/100", tweet_id="100")

        id1 = db.insert_engagement(
            content_id=content_id, tweet_id="100",
            like_count=5, retweet_count=1, reply_count=0, quote_count=0,
            engagement_score=3.0,
        )
        id2 = db.insert_engagement(
            content_id=content_id, tweet_id="100",
            like_count=15, retweet_count=4, reply_count=2, quote_count=1,
            engagement_score=12.0,
        )

        count = db.conn.execute(
            "SELECT COUNT(*) FROM post_engagement WHERE content_id = ?",
            (content_id,),
        ).fetchone()[0]
        assert count == 2

    def test_operations_state_reads_poll_and_pipeline(self, db):
        """sync_operation should read timestamps from presence DB tables."""
        # Use set_last_poll_time to insert poll_state
        db.set_last_poll_time(datetime.now(timezone.utc))

        cursor = db.conn.cursor()
        ops_data = {"runs": []}
        result = sync_operation(cursor, ops_data, "run-poll")

        assert result is True
        assert len(ops_data["runs"]) == 1
        assert ops_data["runs"][0]["operationId"] == "run-poll"
