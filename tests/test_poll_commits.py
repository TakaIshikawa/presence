"""Integration tests for poll_commits readiness gate and pipeline orchestration logic."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

# Add scripts/ so we can import the extracted functions directly
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from poll_commits import (
    check_readiness,
    choose_content_type,
    estimate_tokens,
    get_retryable_content,
    is_daily_cap_reached,
    post_to_x,
)
from storage.db import MAX_RETRIES


# ---------------------------------------------------------------------------
# Unit tests for the pure decision functions
# ---------------------------------------------------------------------------


class TestCheckReadiness:
    """Tests for the readiness gate decision function."""

    def test_ready_when_tokens_exceed_threshold(self):
        assert check_readiness(
            accumulated_tokens=600,
            threshold=500,
            hours_since_post=1.0,
            max_gap_hours=12,
            has_prompts=True,
        ) is True

    def test_ready_when_tokens_equal_threshold(self):
        assert check_readiness(
            accumulated_tokens=500,
            threshold=500,
            hours_since_post=1.0,
            max_gap_hours=12,
            has_prompts=True,
        ) is True

    def test_ready_when_time_gap_exceeded_with_prompts(self):
        assert check_readiness(
            accumulated_tokens=100,
            threshold=500,
            hours_since_post=13.0,
            max_gap_hours=12,
            has_prompts=True,
        ) is True

    def test_not_ready_when_time_gap_exceeded_without_prompts(self):
        """Time cap alone is not enough — there must be prompts to synthesize."""
        assert check_readiness(
            accumulated_tokens=100,
            threshold=500,
            hours_since_post=13.0,
            max_gap_hours=12,
            has_prompts=False,
        ) is False

    def test_not_ready_when_neither_condition_met(self):
        assert check_readiness(
            accumulated_tokens=100,
            threshold=500,
            hours_since_post=2.0,
            max_gap_hours=12,
            has_prompts=True,
        ) is False

    def test_not_ready_zero_tokens_short_gap(self):
        assert check_readiness(
            accumulated_tokens=0,
            threshold=500,
            hours_since_post=0.5,
            max_gap_hours=12,
            has_prompts=False,
        ) is False

    def test_ready_when_gap_exactly_at_cap(self):
        assert check_readiness(
            accumulated_tokens=0,
            threshold=500,
            hours_since_post=12.0,
            max_gap_hours=12,
            has_prompts=True,
        ) is True


class TestIsDailyCapReached:
    """Tests for the daily post cap enforcement."""

    def test_cap_reached_at_limit(self):
        assert is_daily_cap_reached(posts_today=3, max_daily=3) is True

    def test_cap_reached_over_limit(self):
        assert is_daily_cap_reached(posts_today=5, max_daily=3) is True

    def test_cap_not_reached(self):
        assert is_daily_cap_reached(posts_today=2, max_daily=3) is False

    def test_cap_not_reached_zero_posts(self):
        assert is_daily_cap_reached(posts_today=0, max_daily=3) is False


class TestEstimateTokens:
    def test_basic_estimation(self):
        # "abcd" = 4 chars => 1 token
        assert estimate_tokens(["abcd"]) == 1

    def test_multiple_texts(self):
        # 8 chars + 12 chars = 20 chars => 5 tokens
        assert estimate_tokens(["abcdefgh", "abcdefghijkl"]) == 5

    def test_empty_list(self):
        assert estimate_tokens([]) == 0


class TestContentMixHelpers:
    def test_choose_content_type_returns_reason(self, db):
        content_type, reason = choose_content_type(
            db,
            accumulated_tokens=2000,
            has_prompts=True,
        )

        assert content_type == "x_thread"
        assert reason

    def test_post_to_x_uses_single_post_for_x_post(self):
        client = type("Client", (), {})()
        client.post = lambda text: ("post", text)
        client.post_thread = lambda tweets: ("thread", tweets)

        assert post_to_x(client, "x_post", "hello") == ("post", "hello")

    def test_post_to_x_uses_thread_for_x_thread(self):
        client = type("Client", (), {})()
        client.post = lambda text: ("post", text)
        client.post_thread = lambda tweets: ("thread", tweets)

        assert post_to_x(client, "x_thread", "TWEET 1:\nOne\n\nTWEET 2:\nTwo") == (
            "thread",
            ["One", "Two"],
        )


# ---------------------------------------------------------------------------
# Integration tests using the in-memory DB fixture from conftest.py
# ---------------------------------------------------------------------------


class TestDailyCapIntegration:
    """Verify daily cap interacts correctly with the DB layer."""

    def test_cap_prevents_synthesis_when_limit_reached(self, db):
        now_iso = datetime.now(timezone.utc).isoformat()
        for i in range(3):
            cid = db.insert_generated_content(
                "x_thread", ["sha"], ["uuid"], f"post {i}", 8.0, "ok"
            )
            db.conn.execute(
                "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
                (now_iso, cid),
            )
        db.conn.commit()

        posts_today = db.count_posts_today("x_thread")
        assert is_daily_cap_reached(posts_today, max_daily=3) is True

    def test_cap_allows_synthesis_below_limit(self, db):
        now_iso = datetime.now(timezone.utc).isoformat()
        cid = db.insert_generated_content(
            "x_thread", ["sha"], ["uuid"], "post 0", 8.0, "ok"
        )
        db.conn.execute(
            "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
            (now_iso, cid),
        )
        db.conn.commit()

        posts_today = db.count_posts_today("x_thread")
        assert is_daily_cap_reached(posts_today, max_daily=3) is False

    def test_last_published_time_any_uses_latest_across_posts_and_threads(self, db):
        older = datetime.now(timezone.utc) - timedelta(hours=3)
        newer = datetime.now(timezone.utc) - timedelta(hours=1)

        post_id = db.insert_generated_content(
            "x_post", [], [], "post", 8.0, "ok"
        )
        thread_id = db.insert_generated_content(
            "x_thread", [], [], "thread", 8.0, "ok"
        )
        db.conn.execute(
            "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
            (older.isoformat(), thread_id),
        )
        db.conn.execute(
            "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
            (newer.isoformat(), post_id),
        )
        db.conn.commit()

        latest = db.get_last_published_time_any(["x_thread", "x_post"])

        assert latest == newer


class TestRetryLogicIntegration:
    """Verify retry filtering through get_retryable_content + DB layer."""

    def _insert_unpublished(self, db, content="retry me", score=8.0):
        return db.insert_generated_content(
            content_type="x_thread",
            source_commits=["sha1"],
            source_messages=["uuid1"],
            content=content,
            eval_score=score,
            eval_feedback="ok",
        )

    def test_picks_up_unpublished_with_zero_retries(self, db):
        self._insert_unpublished(db)
        results = get_retryable_content(db, min_score=7.0)
        assert len(results) == 1
        assert results[0]["content"] == "retry me"
        assert results[0]["retry_count"] == 0

    def test_picks_up_content_with_retries_below_max(self, db):
        cid = self._insert_unpublished(db)
        db.increment_retry(cid)  # retry_count = 1
        db.increment_retry(cid)  # retry_count = 2

        results = get_retryable_content(db, min_score=7.0)
        assert len(results) == 1
        assert results[0]["retry_count"] == 2

    def test_skips_content_at_max_retries(self, db):
        cid = self._insert_unpublished(db)
        for _ in range(MAX_RETRIES):
            db.increment_retry(cid)

        results = get_retryable_content(db, min_score=7.0)
        assert len(results) == 0

    def test_skips_content_below_min_score(self, db):
        self._insert_unpublished(db, score=5.0)
        results = get_retryable_content(db, min_score=7.0)
        assert len(results) == 0

    def test_skips_already_published_content(self, db):
        cid = self._insert_unpublished(db)
        db.mark_published(cid, "https://x.com/post/1", tweet_id="tw1")

        results = get_retryable_content(db, min_score=7.0)
        assert len(results) == 0

    def test_mixed_retryable_and_abandoned(self, db):
        good_id = self._insert_unpublished(db, content="good post")
        abandoned_id = self._insert_unpublished(db, content="abandoned post")
        for _ in range(MAX_RETRIES):
            db.increment_retry(abandoned_id)

        results = get_retryable_content(db, min_score=7.0)
        assert len(results) == 1
        assert results[0]["content"] == "good post"


class TestReadinessWithDBContext:
    """End-to-end readiness checks combining DB state with decision functions."""

    def test_ready_with_enough_commits_for_tokens(self, db):
        """Insert enough commits that token estimate crosses threshold."""
        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=1)

        # Insert commits with enough message text to exceed threshold
        # threshold=500 tokens => need ~2000 chars of commit messages
        long_msg = "x" * 500  # 500 chars = 125 tokens each
        for i in range(5):
            db.insert_commit(
                repo_name="repo",
                commit_sha=f"sha-{i}",
                commit_message=long_msg,
                timestamp=(one_hour_ago + timedelta(minutes=i)).isoformat(),
                author="dev",
            )

        commits = db.get_commits_in_range(one_hour_ago, now)
        texts = [c["commit_message"] for c in commits]
        tokens = estimate_tokens(texts)

        assert tokens >= 500
        assert check_readiness(
            accumulated_tokens=tokens,
            threshold=500,
            hours_since_post=1.0,
            max_gap_hours=12,
            has_prompts=True,
        ) is True

    def test_not_ready_with_few_commits(self, db):
        """Few short commits should not trigger readiness."""
        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=1)

        db.insert_commit(
            repo_name="repo",
            commit_sha="sha-small",
            commit_message="fix typo",
            timestamp=one_hour_ago.isoformat(),
            author="dev",
        )

        commits = db.get_commits_in_range(one_hour_ago, now)
        texts = [c["commit_message"] for c in commits]
        tokens = estimate_tokens(texts)

        assert tokens < 500
        assert check_readiness(
            accumulated_tokens=tokens,
            threshold=500,
            hours_since_post=1.0,
            max_gap_hours=12,
            has_prompts=True,
        ) is False

    def test_time_cap_forces_readiness(self, db):
        """Even with few tokens, exceeding time cap with prompts triggers readiness."""
        now = datetime.now(timezone.utc)
        long_ago = now - timedelta(hours=13)

        db.insert_commit(
            repo_name="repo",
            commit_sha="sha-old",
            commit_message="small change",
            timestamp=long_ago.isoformat(),
            author="dev",
        )

        commits = db.get_commits_in_range(long_ago, now)
        texts = [c["commit_message"] for c in commits]
        tokens = estimate_tokens(texts)

        assert check_readiness(
            accumulated_tokens=tokens,
            threshold=500,
            hours_since_post=13.0,
            max_gap_hours=12,
            has_prompts=True,
        ) is True

    def test_time_cap_without_prompts_stays_not_ready(self, db):
        """Time cap exceeded but no prompts — should not trigger."""
        assert check_readiness(
            accumulated_tokens=50,
            threshold=500,
            hours_since_post=15.0,
            max_gap_hours=12,
            has_prompts=False,
        ) is False
