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

    def test_duplicate_batch_id_raises(self, db):
        db.insert_pipeline_run("batch-dup", "x_post", 3, 0, 7.0)
        with pytest.raises(Exception):
            db.insert_pipeline_run("batch-dup", "x_post", 3, 0, 7.0)

    def test_insert_pipeline_run_with_filter_stats(self, db):
        stats = {
            "char_limit_rejected": 1,
            "repetition_rejected": 2,
            "stale_pattern_rejected": 1,
            "stale_patterns_matched": ["(?i)^AI\\s", "(?i)\\bbreakthrough\\b"],
        }
        run_id = db.insert_pipeline_run(
            batch_id="batch-fs",
            content_type="x_post",
            candidates_generated=3,
            best_candidate_index=0,
            best_score_before_refine=7.0,
            filter_stats=stats,
        )
        assert run_id > 0

        row = db.conn.execute(
            "SELECT filter_stats FROM pipeline_runs WHERE id = ?", (run_id,)
        ).fetchone()
        stored = json.loads(row[0])
        assert stored["char_limit_rejected"] == 1
        assert stored["repetition_rejected"] == 2
        assert stored["stale_pattern_rejected"] == 1
        assert stored["stale_patterns_matched"] == ["(?i)^AI\\s", "(?i)\\bbreakthrough\\b"]

    def test_insert_pipeline_run_without_filter_stats(self, db):
        run_id = db.insert_pipeline_run(
            batch_id="batch-nofs",
            content_type="x_post",
            candidates_generated=3,
            best_candidate_index=0,
            best_score_before_refine=7.0,
        )
        row = db.conn.execute(
            "SELECT filter_stats FROM pipeline_runs WHERE id = ?", (run_id,)
        ).fetchone()
        assert row[0] is None

    def test_filter_stats_column_exists(self, db):
        cols = {
            row[1]
            for row in db.conn.execute("PRAGMA table_info(pipeline_runs)")
        }
        assert "filter_stats" in cols

    def test_schema_migration_adds_filter_stats(self, schema_path):
        """Verify migration adds filter_stats to a DB created without it."""
        db = Database(":memory:")
        db.connect()
        # Create pipeline_runs without filter_stats column
        db.conn.executescript("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT UNIQUE NOT NULL,
                content_type TEXT NOT NULL,
                candidates_generated INTEGER,
                best_candidate_index INTEGER,
                best_score_before_refine REAL,
                best_score_after_refine REAL,
                refinement_picked TEXT,
                final_score REAL,
                published INTEGER DEFAULT 0,
                content_id INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # Confirm column does not exist yet
        cols_before = {row[1] for row in db.conn.execute("PRAGMA table_info(pipeline_runs)")}
        assert "filter_stats" not in cols_before

        # Run init_schema which should migrate
        db.init_schema(schema_path)

        cols_after = {row[1] for row in db.conn.execute("PRAGMA table_info(pipeline_runs)")}
        assert "filter_stats" in cols_after
        db.close()
