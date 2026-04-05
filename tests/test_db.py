"""Tests for the SQLite storage layer (storage/db.py) using in-memory databases."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from storage.db import Database, MAX_RETRIES


# --- Schema initialization ---


class TestSchemaInit:
    def test_tables_created(self, db):
        tables = {
            row[0]
            for row in db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        expected = {
            "claude_messages",
            "github_commits",
            "commit_prompt_links",
            "generated_content",
            "post_engagement",
            "prompt_versions",
            "poll_state",
            "knowledge",
            "curated_sources",
            "content_knowledge_links",
            "pipeline_runs",
        }
        assert expected.issubset(tables)

    def test_migration_columns_exist(self, db):
        cols = {
            row[1]
            for row in db.conn.execute("PRAGMA table_info(generated_content)")
        }
        assert "retry_count" in cols
        assert "last_retry_at" in cols
        assert "tweet_id" in cols
        assert "published_at" in cols
        assert "curation_quality" in cols
        assert "auto_quality" in cols

    def test_pipeline_runs_columns_exist(self, db):
        cols = {
            row[1]
            for row in db.conn.execute("PRAGMA table_info(pipeline_runs)")
        }
        assert "outcome" in cols
        assert "rejection_reason" in cols

    def test_idempotent_init(self, db, schema_path):
        # Running init_schema again should not raise
        db.init_schema(schema_path)


# --- Context manager ---


class TestContextManager:
    def test_context_manager_connects_and_closes(self, schema_path):
        with Database(":memory:") as db:
            db.init_schema(schema_path)
            assert db.conn is not None
            db.conn.execute("SELECT 1")
        assert db.conn is None


# --- Claude messages CRUD ---


class TestClaudeMessages:
    def test_insert_and_check_processed(self, db):
        db.insert_claude_message(
            session_id="session-1",
            message_uuid="uuid-001",
            project_path="/projects/test",
            timestamp="2026-03-30T10:00:00",
            prompt_text="Fix the auth bug",
        )
        assert db.is_message_processed("uuid-001") is True
        assert db.is_message_processed("uuid-999") is False

    def test_duplicate_uuid_raises(self, db):
        db.insert_claude_message("s1", "uuid-dup", "/p", "2026-03-30T10:00:00", "text")
        with pytest.raises(Exception):
            db.insert_claude_message("s2", "uuid-dup", "/p", "2026-03-30T11:00:00", "text2")

    def test_get_messages_in_range(self, db):
        for i in range(5):
            db.insert_claude_message(
                session_id="s1",
                message_uuid=f"uuid-{i}",
                project_path="/p",
                timestamp=f"2026-03-30T{10+i:02d}:00:00",
                prompt_text=f"prompt {i}",
            )

        start = datetime(2026, 3, 30, 11, 0, 0)
        end = datetime(2026, 3, 30, 13, 0, 0)
        results = db.get_messages_in_range(start, end)

        assert len(results) == 2
        assert results[0]["prompt_text"] == "prompt 1"
        assert results[1]["prompt_text"] == "prompt 2"

    def test_get_messages_empty_range(self, db):
        db.insert_claude_message("s1", "uuid-1", "/p", "2026-03-30T10:00:00", "text")
        start = datetime(2026, 3, 31, 0, 0, 0)
        end = datetime(2026, 3, 31, 23, 59, 59)
        assert db.get_messages_in_range(start, end) == []


# --- GitHub commits CRUD ---


class TestGitHubCommits:
    def test_insert_and_check_processed(self, db):
        db.insert_commit(
            repo_name="my-project",
            commit_sha="abc123",
            commit_message="fix: handle timeout",
            timestamp="2026-03-30T10:00:00",
            author="taka",
        )
        assert db.is_commit_processed("abc123") is True
        assert db.is_commit_processed("xyz789") is False

    def test_duplicate_sha_raises(self, db):
        db.insert_commit("repo", "sha-dup", "msg", "2026-03-30T10:00:00", "author")
        with pytest.raises(Exception):
            db.insert_commit("repo", "sha-dup", "msg2", "2026-03-30T11:00:00", "author")

    def test_get_commits_in_range(self, db):
        for i in range(4):
            db.insert_commit(
                repo_name="repo",
                commit_sha=f"sha-{i}",
                commit_message=f"commit {i}",
                timestamp=f"2026-03-30T{10+i:02d}:00:00",
                author="taka",
            )

        start = datetime(2026, 3, 30, 11, 0, 0)
        end = datetime(2026, 3, 30, 13, 0, 0)
        results = db.get_commits_in_range(start, end)

        assert len(results) == 2
        assert results[0]["commit_message"] == "commit 1"


# --- Commit-prompt correlation ---


class TestCommitPromptLinks:
    """Tests for link_commit_to_prompts and get_prompts_for_commit."""

    def _setup_commit_and_prompts(self, db, commit_ts="2026-03-30T12:00:00", prompt_offsets_min=None):
        """Insert a commit and prompts at various offsets from the commit timestamp.

        prompt_offsets_min: list of minute offsets from commit_ts (negative = before).
        Returns (commit_id, commit_sha, commit_datetime).
        """
        if prompt_offsets_min is None:
            prompt_offsets_min = [-10, -5, 0, 5, 10]

        commit_dt = datetime.fromisoformat(commit_ts)
        commit_id = db.insert_commit(
            repo_name="test-repo",
            commit_sha="abc123",
            commit_message="feat: add feature",
            timestamp=commit_ts,
            author="taka",
        )

        for i, offset in enumerate(prompt_offsets_min):
            prompt_ts = commit_dt + timedelta(minutes=offset)
            db.insert_claude_message(
                session_id="session-1",
                message_uuid=f"uuid-{i}",
                project_path="/projects/test",
                timestamp=prompt_ts.isoformat(),
                prompt_text=f"prompt at offset {offset}m",
            )

        return commit_id, "abc123", commit_dt

    def test_links_prompts_within_window(self, db):
        commit_id, _, commit_dt = self._setup_commit_and_prompts(
            db, prompt_offsets_min=[-10, -5, 0, 5, 10]
        )
        link_ids = db.link_commit_to_prompts(commit_id, commit_dt, window_minutes=30)
        assert len(link_ids) == 5

    def test_ignores_prompts_outside_window(self, db):
        commit_id, _, commit_dt = self._setup_commit_and_prompts(
            db, prompt_offsets_min=[-60, -31, 0, 31, 60]
        )
        link_ids = db.link_commit_to_prompts(commit_id, commit_dt, window_minutes=30)
        # Only the prompt at offset 0 is within ±30 min with confidence >= 0.5
        # offset 0: confidence = 1.0
        # offset ±31: confidence = 1 - 31/30 < 0 → clamped to 0 → below 0.5
        # offset ±60: confidence = 1 - 60/30 < 0 → clamped to 0 → below 0.5
        assert len(link_ids) == 1

    def test_confidence_decreases_with_distance(self, db):
        commit_id, sha, commit_dt = self._setup_commit_and_prompts(
            db, prompt_offsets_min=[0, -5, -10]
        )
        db.link_commit_to_prompts(commit_id, commit_dt, window_minutes=30)

        prompts = db.get_prompts_for_commit(sha)
        assert len(prompts) == 3
        # Ordered by confidence descending
        confidences = [p["confidence"] for p in prompts]
        assert confidences == sorted(confidences, reverse=True)
        # Closest (offset 0) should have highest confidence
        assert prompts[0]["confidence"] == 1.0
        assert prompts[0]["prompt_text"] == "prompt at offset 0m"

    def test_get_prompts_for_commit_ordered_by_confidence(self, db):
        commit_id, sha, commit_dt = self._setup_commit_and_prompts(
            db, prompt_offsets_min=[-20, -10, -1, 5, 15]
        )
        db.link_commit_to_prompts(commit_id, commit_dt, window_minutes=30)

        prompts = db.get_prompts_for_commit(sha)
        confidences = [p["confidence"] for p in prompts]
        assert confidences == sorted(confidences, reverse=True)

    def test_no_prompts_in_window_returns_empty(self, db):
        commit_dt = datetime.fromisoformat("2026-03-30T12:00:00")
        commit_id = db.insert_commit(
            "test-repo", "sha-lonely", "fix: solo commit",
            "2026-03-30T12:00:00", "taka",
        )
        # Insert a prompt far outside the window
        db.insert_claude_message(
            "s1", "uuid-far", "/p",
            "2026-03-29T10:00:00",  # ~26 hours before
            "unrelated prompt",
        )
        link_ids = db.link_commit_to_prompts(commit_id, commit_dt, window_minutes=30)
        assert link_ids == []

        prompts = db.get_prompts_for_commit("sha-lonely")
        assert prompts == []

    def test_min_confidence_filters_low_scores(self, db):
        commit_id, sha, commit_dt = self._setup_commit_and_prompts(
            db, prompt_offsets_min=[0, -14, -28]
        )
        # window=30, offset -28 → confidence = 1 - 28/30 ≈ 0.067 → below default 0.5
        # offset -14 → confidence = 1 - 14/30 ≈ 0.533 → above 0.5
        # offset 0 → confidence = 1.0
        link_ids = db.link_commit_to_prompts(commit_id, commit_dt, window_minutes=30, min_confidence=0.5)
        assert len(link_ids) == 2

        prompts = db.get_prompts_for_commit(sha)
        assert all(p["confidence"] >= 0.5 for p in prompts)


# --- Generated content CRUD ---


class TestGeneratedContent:
    def _insert_content(self, db, content="Test post", eval_score=8.0, content_type="x_post"):
        return db.insert_generated_content(
            content_type=content_type,
            source_commits=["sha1", "sha2"],
            source_messages=["uuid1"],
            content=content,
            eval_score=eval_score,
            eval_feedback="Good post",
        )

    def test_insert_and_retrieve(self, db):
        content_id = self._insert_content(db)
        assert content_id > 0

        unpub = db.get_unpublished_content("x_post", min_score=7.0)
        assert len(unpub) == 1
        assert unpub[0]["content"] == "Test post"
        assert json.loads(unpub[0]["source_commits"]) == ["sha1", "sha2"]

    def test_mark_published(self, db):
        content_id = self._insert_content(db)
        db.mark_published(content_id, url="https://x.com/post/1", tweet_id="tweet-001")

        unpub = db.get_unpublished_content("x_post", min_score=7.0)
        assert len(unpub) == 0

        row = db.conn.execute(
            "SELECT published, published_url, tweet_id, published_at FROM generated_content WHERE id = ?",
            (content_id,),
        ).fetchone()
        assert row[0] == 1
        assert row[1] == "https://x.com/post/1"
        assert row[2] == "tweet-001"
        assert row[3] is not None  # published_at set

    def test_get_unpublished_respects_min_score(self, db):
        self._insert_content(db, content="High score", eval_score=9.0)
        self._insert_content(db, content="Low score", eval_score=5.0)

        results = db.get_unpublished_content("x_post", min_score=7.0)
        assert len(results) == 1
        assert results[0]["content"] == "High score"

    def test_get_unpublished_excludes_max_retries(self, db):
        content_id = self._insert_content(db, eval_score=9.0)
        for _ in range(MAX_RETRIES):
            db.increment_retry(content_id)

        results = db.get_unpublished_content("x_post", min_score=7.0)
        assert len(results) == 0

    def test_increment_retry_returns_count(self, db):
        content_id = self._insert_content(db)
        assert db.increment_retry(content_id) == 1
        assert db.increment_retry(content_id) == 2

    def test_increment_retry_abandons_at_max(self, db):
        content_id = self._insert_content(db)
        for _ in range(MAX_RETRIES):
            db.increment_retry(content_id)

        row = db.conn.execute(
            "SELECT published FROM generated_content WHERE id = ?",
            (content_id,),
        ).fetchone()
        assert row[0] == -1  # abandoned

    def test_mark_abandoned(self, db):
        content_id = self._insert_content(db)
        db.mark_abandoned(content_id)

        row = db.conn.execute(
            "SELECT published FROM generated_content WHERE id = ?",
            (content_id,),
        ).fetchone()
        assert row[0] == -1


# --- Poll state ---


class TestPollState:
    def test_get_last_poll_time_initially_none(self, db):
        assert db.get_last_poll_time() is None

    def test_set_and_get_poll_time(self, db):
        t = datetime(2026, 3, 30, 15, 30, 0, tzinfo=timezone.utc)
        db.set_last_poll_time(t)
        result = db.get_last_poll_time()
        assert result == t

    def test_set_poll_time_upserts(self, db):
        t1 = datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 3, 30, 15, 0, 0, tzinfo=timezone.utc)
        db.set_last_poll_time(t1)
        db.set_last_poll_time(t2)

        result = db.get_last_poll_time()
        assert result == t2

        # Only one row in poll_state
        count = db.conn.execute("SELECT COUNT(*) FROM poll_state").fetchone()[0]
        assert count == 1


# --- Last published time ---


class TestLastPublishedTime:
    def test_no_published_returns_none(self, db):
        assert db.get_last_published_time() is None

    def test_returns_most_recent(self, db):
        for i, ts in enumerate(["2026-03-30T10:00:00", "2026-03-30T15:00:00", "2026-03-30T12:00:00"]):
            cid = db.insert_generated_content(
                "x_post", ["sha"], ["uuid"], f"post {i}", 8.0, "ok"
            )
            db.conn.execute(
                "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
                (ts, cid),
            )
        db.conn.commit()

        result = db.get_last_published_time()
        assert result.hour == 15  # Most recent


# --- Daily post cap ---


class TestDailyPostCap:
    def test_count_posts_today_empty(self, db):
        assert db.count_posts_today() == 0

    def test_count_posts_today_counts_published(self, db):
        now_iso = datetime.now(timezone.utc).isoformat()
        for i in range(3):
            cid = db.insert_generated_content(
                "x_post", ["sha"], ["uuid"], f"post {i}", 8.0, "ok"
            )
            db.conn.execute(
                "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
                (now_iso, cid),
            )
        db.conn.commit()

        assert db.count_posts_today("x_post") == 3

    def test_count_excludes_other_content_types(self, db):
        now_iso = datetime.now(timezone.utc).isoformat()
        for ct in ["x_post", "x_thread", "blog_post"]:
            cid = db.insert_generated_content(ct, ["sha"], ["uuid"], "content", 8.0, "ok")
            db.conn.execute(
                "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
                (now_iso, cid),
            )
        db.conn.commit()

        assert db.count_posts_today("x_post") == 1


# --- Engagement tracking ---


class TestEngagement:
    def test_insert_engagement(self, db):
        cid = db.insert_generated_content("x_post", ["sha"], ["uuid"], "post", 8.0, "ok")
        eid = db.insert_engagement(
            content_id=cid,
            tweet_id="tweet-1",
            like_count=10,
            retweet_count=3,
            reply_count=2,
            quote_count=1,
            engagement_score=16.0,
        )
        assert eid > 0

    def test_get_top_performing_posts(self, db):
        # Create 3 posts with different engagement scores
        for i, score in enumerate([5.0, 20.0, 10.0]):
            cid = db.insert_generated_content(
                "x_post", ["sha"], ["uuid"], f"post {i}", 8.0, "ok"
            )
            db.conn.execute(
                "UPDATE generated_content SET published = 1, tweet_id = ? WHERE id = ?",
                (f"tweet-{i}", cid),
            )
            db.conn.commit()
            db.insert_engagement(cid, f"tweet-{i}", 10, 3, 2, 1, score)

        results = db.get_top_performing_posts(limit=2)
        assert len(results) == 2
        assert results[0]["engagement_score"] == 20.0
        assert results[1]["engagement_score"] == 10.0

    def test_top_performing_excludes_too_specific(self, db):
        cid = db.insert_generated_content(
            "x_post", ["sha"], ["uuid"], "too specific post", 8.0, "ok"
        )
        db.conn.execute(
            "UPDATE generated_content SET published = 1, tweet_id = 'tw1', curation_quality = 'too_specific' WHERE id = ?",
            (cid,),
        )
        db.conn.commit()
        db.insert_engagement(cid, "tw1", 100, 50, 30, 10, 100.0)

        results = db.get_top_performing_posts()
        assert len(results) == 0

    def test_top_performing_excludes_low_resonance(self, db):
        cid = db.insert_generated_content(
            "x_post", ["sha"], ["uuid"], "low resonance post", 8.0, "ok"
        )
        db.conn.execute(
            "UPDATE generated_content SET published = 1, tweet_id = 'tw1', auto_quality = 'low_resonance' WHERE id = ?",
            (cid,),
        )
        db.conn.commit()
        db.insert_engagement(cid, "tw1", 100, 50, 30, 10, 100.0)

        results = db.get_top_performing_posts()
        assert len(results) == 0


# --- Curation quality ---


class TestCuration:
    def test_set_and_get_curation_quality(self, db):
        cid = db.insert_generated_content(
            "x_post", ["sha"], ["uuid"], "some post", 8.0, "ok"
        )
        db.set_curation_quality(cid, "too_specific")

        results = db.get_curated_posts("too_specific")
        assert len(results) == 1
        assert results[0]["content"] == "some post"

    def test_clear_curation_quality(self, db):
        cid = db.insert_generated_content(
            "x_post", ["sha"], ["uuid"], "post", 8.0, "ok"
        )
        db.set_curation_quality(cid, "good")
        db.set_curation_quality(cid, None)

        assert db.get_curated_posts("good") == []

    def test_get_curated_filters_by_content_type(self, db):
        for ct in ["x_post", "x_thread"]:
            cid = db.insert_generated_content(ct, ["sha"], ["uuid"], f"{ct} content", 8.0, "ok")
            db.set_curation_quality(cid, "good")

        results = db.get_curated_posts("good", content_type="x_thread")
        assert len(results) == 1
        assert results[0]["content"] == "x_thread content"


# --- Auto-classification ---


class TestAutoClassification:
    def test_get_auto_classified_posts(self, db):
        cid = db.insert_generated_content("x_post", ["sha"], ["uuid"], "post", 8.0, "ok")
        db.conn.execute(
            "UPDATE generated_content SET published = 1, auto_quality = 'resonated' WHERE id = ?",
            (cid,),
        )
        db.conn.commit()

        results = db.get_auto_classified_posts("resonated")
        assert len(results) == 1

        results = db.get_auto_classified_posts("low_resonance")
        assert len(results) == 0


# --- Recent published content ---


class TestRecentPublished:
    def test_get_recent_published_content(self, db):
        for i in range(5):
            cid = db.insert_generated_content(
                "x_post", ["sha"], ["uuid"], f"post {i}", 8.0, "ok"
            )
            db.conn.execute(
                "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
                (f"2026-03-30T{10+i:02d}:00:00", cid),
            )
        db.conn.commit()

        results = db.get_recent_published_content("x_post", limit=3)
        assert len(results) == 3
        # Most recent first
        assert results[0]["content"] == "post 4"
        assert results[2]["content"] == "post 2"

    def test_excludes_unpublished(self, db):
        db.insert_generated_content("x_post", ["sha"], ["uuid"], "unpublished", 8.0, "ok")
        assert db.get_recent_published_content("x_post") == []


# --- Pipeline runs ---


class TestPipelineRuns:
    def test_insert_pipeline_run(self, db):
        run_id = db.insert_pipeline_run(
            batch_id="batch-001",
            content_type="x_post",
            candidates_generated=3,
            best_candidate_index=1,
            best_score_before_refine=7.5,
            best_score_after_refine=8.0,
            refinement_picked="REFINED",
            final_score=8.0,
            published=True,
            content_id=1,
        )
        assert run_id > 0

    def test_insert_pipeline_run_with_outcome(self, db):
        run_id = db.insert_pipeline_run(
            batch_id="batch-002",
            content_type="x_thread",
            candidates_generated=3,
            best_candidate_index=0,
            best_score_before_refine=6.0,
            final_score=6.0,
            outcome="below_threshold",
            rejection_reason="Score 6.0 below threshold 7.0",
        )
        assert run_id > 0

        row = db.conn.execute(
            "SELECT outcome, rejection_reason FROM pipeline_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        assert row[0] == "below_threshold"
        assert row[1] == "Score 6.0 below threshold 7.0"

    def test_insert_pipeline_run_published_no_rejection(self, db):
        run_id = db.insert_pipeline_run(
            batch_id="batch-003",
            content_type="blog_post",
            candidates_generated=3,
            best_candidate_index=1,
            best_score_before_refine=8.5,
            final_score=8.5,
            outcome="published",
        )
        row = db.conn.execute(
            "SELECT outcome, rejection_reason FROM pipeline_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        assert row[0] == "published"
        assert row[1] is None

    def test_insert_pipeline_run_all_filtered(self, db):
        run_id = db.insert_pipeline_run(
            batch_id="batch-004",
            content_type="x_post",
            candidates_generated=0,
            best_candidate_index=0,
            best_score_before_refine=0,
            final_score=0,
            outcome="all_filtered",
            rejection_reason="All candidates filtered (repetitive or stale patterns)",
        )
        row = db.conn.execute(
            "SELECT outcome, rejection_reason FROM pipeline_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        assert row[0] == "all_filtered"
        assert "filtered" in row[1]

    def test_duplicate_batch_id_raises(self, db):
        db.insert_pipeline_run("batch-dup", "x_post", 3, 0, 7.0)
        with pytest.raises(Exception):
            db.insert_pipeline_run("batch-dup", "x_post", 3, 0, 7.0)
