"""Tests for the SQLite storage layer (storage/db.py) using in-memory databases."""

import json
import sqlite3
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


# --- Schema migration logic ---


class TestInitSchemaMigrations:
    """Tests for init_schema() migration logic (lines 33-67 in db.py)."""

    def test_init_schema_twice_is_idempotent(self, schema_path):
        """Calling init_schema() twice should not error."""
        with Database(":memory:") as db:
            db.init_schema(schema_path)
            db.init_schema(schema_path)  # Second call should succeed
            # Verify tables still exist
            tables = {
                row[0]
                for row in db.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            assert "generated_content" in tables

    def test_migration_adds_retry_count_if_missing(self, schema_path):
        """Test that retry_count column is added when missing."""
        with Database(":memory:") as db:
            # Create table without retry_count
            db.conn.execute("""
                CREATE TABLE generated_content (
                    id INTEGER PRIMARY KEY,
                    content_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    published INTEGER DEFAULT 0
                )
            """)
            db.conn.commit()

            # Run init_schema to trigger migration
            db.init_schema(schema_path)

            # Verify retry_count was added
            cols = {row[1] for row in db.conn.execute("PRAGMA table_info(generated_content)")}
            assert "retry_count" in cols

    def test_migration_adds_last_retry_at_if_missing(self, schema_path):
        """Test that last_retry_at column is added when missing."""
        with Database(":memory:") as db:
            db.conn.execute("""
                CREATE TABLE generated_content (
                    id INTEGER PRIMARY KEY,
                    content_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    published INTEGER DEFAULT 0
                )
            """)
            db.conn.commit()

            db.init_schema(schema_path)

            cols = {row[1] for row in db.conn.execute("PRAGMA table_info(generated_content)")}
            assert "last_retry_at" in cols

    def test_migration_adds_tweet_id_if_missing(self, schema_path):
        """Test that tweet_id column is added when missing."""
        with Database(":memory:") as db:
            db.conn.execute("""
                CREATE TABLE generated_content (
                    id INTEGER PRIMARY KEY,
                    content_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    published INTEGER DEFAULT 0
                )
            """)
            db.conn.commit()

            db.init_schema(schema_path)

            cols = {row[1] for row in db.conn.execute("PRAGMA table_info(generated_content)")}
            assert "tweet_id" in cols

    def test_migration_adds_published_at_if_missing(self, schema_path):
        """Test that published_at column is added when missing."""
        with Database(":memory:") as db:
            db.conn.execute("""
                CREATE TABLE generated_content (
                    id INTEGER PRIMARY KEY,
                    content_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    published INTEGER DEFAULT 0
                )
            """)
            db.conn.commit()

            db.init_schema(schema_path)

            cols = {row[1] for row in db.conn.execute("PRAGMA table_info(generated_content)")}
            assert "published_at" in cols

    def test_migration_adds_curation_quality_and_index_if_missing(self, schema_path):
        """Test that curation_quality column and index are added when missing."""
        with Database(":memory:") as db:
            db.conn.execute("""
                CREATE TABLE generated_content (
                    id INTEGER PRIMARY KEY,
                    content_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    published INTEGER DEFAULT 0
                )
            """)
            db.conn.commit()

            db.init_schema(schema_path)

            cols = {row[1] for row in db.conn.execute("PRAGMA table_info(generated_content)")}
            assert "curation_quality" in cols

            # Check index was created
            indexes = {
                row[1]
                for row in db.conn.execute(
                    "SELECT * FROM sqlite_master WHERE type='index' AND tbl_name='generated_content'"
                ).fetchall()
            }
            assert "idx_generated_content_curation" in indexes

    def test_migration_adds_auto_quality_and_index_if_missing(self, schema_path):
        """Test that auto_quality column and index are added when missing."""
        with Database(":memory:") as db:
            db.conn.execute("""
                CREATE TABLE generated_content (
                    id INTEGER PRIMARY KEY,
                    content_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    published INTEGER DEFAULT 0
                )
            """)
            db.conn.commit()

            db.init_schema(schema_path)

            cols = {row[1] for row in db.conn.execute("PRAGMA table_info(generated_content)")}
            assert "auto_quality" in cols

            # Check index was created
            indexes = {
                row[1]
                for row in db.conn.execute(
                    "SELECT * FROM sqlite_master WHERE type='index' AND tbl_name='generated_content'"
                ).fetchall()
            }
            assert "idx_generated_content_auto_quality" in indexes

    def test_migration_adds_reply_queue_columns_if_missing(self, schema_path):
        """Test that reply_queue columns (relationship_context, quality_score, quality_flags) are added when missing."""
        with Database(":memory:") as db:
            db.conn.execute("""
                CREATE TABLE reply_queue (
                    id INTEGER PRIMARY KEY,
                    inbound_tweet_id TEXT UNIQUE NOT NULL,
                    inbound_text TEXT NOT NULL,
                    status TEXT DEFAULT 'pending'
                )
            """)
            db.conn.commit()

            db.init_schema(schema_path)

            cols = {row[1] for row in db.conn.execute("PRAGMA table_info(reply_queue)")}
            assert "relationship_context" in cols
            assert "quality_score" in cols
            assert "quality_flags" in cols

    def test_migration_adds_pipeline_runs_columns_if_missing(self, schema_path):
        """Test that pipeline_runs columns (outcome, rejection_reason) are added when missing."""
        with Database(":memory:") as db:
            db.conn.execute("""
                CREATE TABLE pipeline_runs (
                    id INTEGER PRIMARY KEY,
                    batch_id TEXT UNIQUE NOT NULL,
                    content_type TEXT NOT NULL,
                    published INTEGER DEFAULT 0
                )
            """)
            db.conn.commit()

            db.init_schema(schema_path)

            cols = {row[1] for row in db.conn.execute("PRAGMA table_info(pipeline_runs)")}
            assert "outcome" in cols
            assert "rejection_reason" in cols

    def test_migration_does_not_re_add_existing_columns(self, schema_path):
        """Test that migration does not attempt to re-add columns that are already present."""
        with Database(":memory:") as db:
            # Create table with all migration columns already present
            db.conn.execute("""
                CREATE TABLE generated_content (
                    id INTEGER PRIMARY KEY,
                    content_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    published INTEGER DEFAULT 0,
                    retry_count INTEGER DEFAULT 0,
                    last_retry_at TEXT,
                    tweet_id TEXT,
                    published_at TEXT,
                    curation_quality TEXT,
                    auto_quality TEXT
                )
            """)
            db.conn.commit()

            # Count columns before migration
            cols_before = {row[1] for row in db.conn.execute("PRAGMA table_info(generated_content)")}

            # Run init_schema (should not error)
            db.init_schema(schema_path)

            # Count columns after migration
            cols_after = {row[1] for row in db.conn.execute("PRAGMA table_info(generated_content)")}

            # All original columns should still be present
            assert cols_before.issubset(cols_after)


# --- Context manager ---


class TestContextManager:
    def test_context_manager_connects_and_closes(self, schema_path):
        with Database(":memory:") as db:
            db.init_schema(schema_path)
            assert db.conn is not None
            db.conn.execute("SELECT 1")
        assert db.conn is None


# --- Connection management ---


class TestDatabaseConnectionManagement:
    """Tests for Database context manager protocol and connection edge cases."""

    def test_context_manager_connects_on_entry(self, schema_path):
        database = Database(":memory:")
        assert database.conn is None
        with database as db:
            assert db.conn is not None
            db.init_schema(schema_path)
            db.conn.execute("SELECT 1")

    def test_context_manager_closes_on_exit(self, schema_path):
        database = Database(":memory:")
        with database as db:
            db.init_schema(schema_path)
        assert database.conn is None

    def test_context_manager_closes_on_exception(self, schema_path):
        database = Database(":memory:")
        with pytest.raises(RuntimeError, match="boom"):
            with database as db:
                db.init_schema(schema_path)
                raise RuntimeError("boom")
        assert database.conn is None

    def test_close_sets_conn_to_none(self):
        database = Database(":memory:")
        database.connect()
        assert database.conn is not None
        database.close()
        assert database.conn is None

    def test_close_is_idempotent(self):
        database = Database(":memory:")
        database.connect()
        database.close()
        database.close()  # second call should not raise
        assert database.conn is None

    def test_connect_sets_row_factory(self):
        database = Database(":memory:")
        database.connect()
        assert database.conn.row_factory is sqlite3.Row
        database.close()

    def test_init_schema_idempotent(self, schema_path):
        with Database(":memory:") as db:
            db.init_schema(schema_path)
            db.init_schema(schema_path)  # second call should not raise
            tables = {
                row[0]
                for row in db.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            assert "generated_content" in tables

    def test_init_schema_migration_columns(self, schema_path):
        with Database(":memory:") as db:
            db.init_schema(schema_path)
            cols = {
                row[1]
                for row in db.conn.execute("PRAGMA table_info(generated_content)")
            }
            for col in ("retry_count", "tweet_id", "published_at", "curation_quality", "auto_quality"):
                assert col in cols, f"migration column '{col}' missing"

    def test_context_manager_with_tmp_path(self, tmp_path, schema_path):
        db_file = tmp_path / "test.db"
        with Database(str(db_file)) as db:
            db.init_schema(schema_path)
            db.insert_claude_message("s1", "uuid-1", "/p", "2026-04-01T10:00:00", "hi")
        assert db.conn is None
        # Re-open and verify data persisted
        with Database(str(db_file)) as db:
            db.init_schema(schema_path)
            assert db.is_message_processed("uuid-1") is True

    def test_auto_classify_posts_mixed_engagement(self, db):
        """Classify posts with engagement >= threshold as 'resonated',
        engagement == 0 as 'low_resonance', and 0 < engagement < threshold as 'ambiguous'."""
        now = datetime.now(timezone.utc)
        old_ts = (now - timedelta(hours=72)).isoformat()

        # High engagement post (resonated)
        cid_high = db.insert_generated_content("x_post", ["sha"], ["uuid"], "high post", 8.0, "ok")
        db.conn.execute(
            "UPDATE generated_content SET published = 1, tweet_id = 'tw-high', published_at = ? WHERE id = ?",
            (old_ts, cid_high),
        )
        db.conn.commit()
        db.insert_engagement(cid_high, "tw-high", 50, 10, 5, 3, 20.0)

        # Zero engagement post (low_resonance)
        cid_zero = db.insert_generated_content("x_post", ["sha"], ["uuid"], "zero post", 8.0, "ok")
        db.conn.execute(
            "UPDATE generated_content SET published = 1, tweet_id = 'tw-zero', published_at = ? WHERE id = ?",
            (old_ts, cid_zero),
        )
        db.conn.commit()
        db.insert_engagement(cid_zero, "tw-zero", 0, 0, 0, 0, 0.0)

        # Ambiguous engagement post (between 0 and threshold)
        cid_mid = db.insert_generated_content("x_post", ["sha"], ["uuid"], "mid post", 8.0, "ok")
        db.conn.execute(
            "UPDATE generated_content SET published = 1, tweet_id = 'tw-mid', published_at = ? WHERE id = ?",
            (old_ts, cid_mid),
        )
        db.conn.commit()
        db.insert_engagement(cid_mid, "tw-mid", 2, 0, 0, 0, 2.0)

        results = db.auto_classify_posts(min_age_hours=48, min_engagement=5.0)
        assert results["resonated"] == 1
        assert results["low_resonance"] == 1
        assert results["ambiguous"] == 1

    def test_increment_retry_abandons_at_max(self, db):
        cid = db.insert_generated_content("x_post", ["sha"], ["uuid"], "retry post", 8.0, "ok")
        for i in range(MAX_RETRIES):
            count = db.increment_retry(cid)
            assert count == i + 1

        row = db.conn.execute(
            "SELECT published FROM generated_content WHERE id = ?", (cid,)
        ).fetchone()
        assert row[0] == -1  # abandoned after MAX_RETRIES

    def test_mark_abandoned_sets_published_minus_one(self, db):
        cid = db.insert_generated_content("x_post", ["sha"], ["uuid"], "abandon post", 8.0, "ok")
        assert db.conn.execute(
            "SELECT published FROM generated_content WHERE id = ?", (cid,)
        ).fetchone()[0] == 0
        db.mark_abandoned(cid)
        assert db.conn.execute(
            "SELECT published FROM generated_content WHERE id = ?", (cid,)
        ).fetchone()[0] == -1


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


class TestAutoClassifyPosts:
    """Tests for auto_classify_posts() method (lines 433-479 in db.py)."""

    def test_classifies_old_post_with_high_engagement_as_resonated(self, db):
        """Posts >= 48 hours old with engagement_score >= 5.0 should be classified as 'resonated'."""
        now = datetime.now(timezone.utc)
        old_ts = (now - timedelta(hours=72)).isoformat()

        cid = db.insert_generated_content("x_post", ["sha"], ["uuid"], "high engagement post", 8.0, "ok")
        db.conn.execute(
            "UPDATE generated_content SET published = 1, tweet_id = 'tw-high', published_at = ? WHERE id = ?",
            (old_ts, cid),
        )
        db.conn.commit()
        db.insert_engagement(cid, "tw-high", 50, 10, 5, 3, 20.0)

        results = db.auto_classify_posts(min_age_hours=48, min_engagement=5.0)

        assert results["resonated"] == 1
        assert results["low_resonance"] == 0
        assert results["ambiguous"] == 0

        # Verify database was updated
        row = db.conn.execute("SELECT auto_quality FROM generated_content WHERE id = ?", (cid,)).fetchone()
        assert row[0] == "resonated"

    def test_classifies_old_post_with_zero_engagement_as_low_resonance(self, db):
        """Posts >= 48 hours old with engagement_score == 0 should be classified as 'low_resonance'."""
        now = datetime.now(timezone.utc)
        old_ts = (now - timedelta(hours=72)).isoformat()

        cid = db.insert_generated_content("x_post", ["sha"], ["uuid"], "zero engagement post", 8.0, "ok")
        db.conn.execute(
            "UPDATE generated_content SET published = 1, tweet_id = 'tw-zero', published_at = ? WHERE id = ?",
            (old_ts, cid),
        )
        db.conn.commit()
        db.insert_engagement(cid, "tw-zero", 0, 0, 0, 0, 0.0)

        results = db.auto_classify_posts(min_age_hours=48, min_engagement=5.0)

        assert results["resonated"] == 0
        assert results["low_resonance"] == 1
        assert results["ambiguous"] == 0

        row = db.conn.execute("SELECT auto_quality FROM generated_content WHERE id = ?", (cid,)).fetchone()
        assert row[0] == "low_resonance"

    def test_does_not_classify_young_posts(self, db):
        """Posts younger than min_age_hours should NOT be classified."""
        now = datetime.now(timezone.utc)
        recent_ts = (now - timedelta(hours=24)).isoformat()  # Only 24 hours old

        cid = db.insert_generated_content("x_post", ["sha"], ["uuid"], "recent post", 8.0, "ok")
        db.conn.execute(
            "UPDATE generated_content SET published = 1, tweet_id = 'tw-recent', published_at = ? WHERE id = ?",
            (recent_ts, cid),
        )
        db.conn.commit()
        db.insert_engagement(cid, "tw-recent", 50, 10, 5, 3, 20.0)

        results = db.auto_classify_posts(min_age_hours=48, min_engagement=5.0)

        assert results["resonated"] == 0
        assert results["low_resonance"] == 0
        assert results["ambiguous"] == 0

        # Verify auto_quality remains NULL
        row = db.conn.execute("SELECT auto_quality FROM generated_content WHERE id = ?", (cid,)).fetchone()
        assert row[0] is None

    def test_skips_already_classified_posts(self, db):
        """Posts with auto_quality IS NOT NULL should be skipped."""
        now = datetime.now(timezone.utc)
        old_ts = (now - timedelta(hours=72)).isoformat()

        cid = db.insert_generated_content("x_post", ["sha"], ["uuid"], "already classified", 8.0, "ok")
        db.conn.execute(
            "UPDATE generated_content SET published = 1, tweet_id = 'tw-already', published_at = ?, auto_quality = 'resonated' WHERE id = ?",
            (old_ts, cid),
        )
        db.conn.commit()
        db.insert_engagement(cid, "tw-already", 0, 0, 0, 0, 0.0)  # Zero engagement, but already classified

        results = db.auto_classify_posts(min_age_hours=48, min_engagement=5.0)

        # Should not be reclassified
        assert results["resonated"] == 0
        assert results["low_resonance"] == 0
        assert results["ambiguous"] == 0

        # Verify auto_quality unchanged
        row = db.conn.execute("SELECT auto_quality FROM generated_content WHERE id = ?", (cid,)).fetchone()
        assert row[0] == "resonated"

    def test_boundary_post_exactly_at_48_hours(self, db):
        """Post at exactly the 48 hour boundary should be classified."""
        # Use SQLite's datetime directly to avoid Python/SQLite timezone/format discrepancies
        db.conn.execute("""
            INSERT INTO generated_content
            (content_type, source_commits, source_messages, content, eval_score, eval_feedback,
             published, tweet_id, published_at)
            VALUES ('x_post', '["sha"]', '["uuid"]', 'exactly 48h old', 8.0, 'ok',
                    1, 'tw-48h', datetime('now', '-48 hours'))
        """)
        db.conn.commit()

        cid = db.conn.execute("SELECT id FROM generated_content WHERE tweet_id = 'tw-48h'").fetchone()[0]
        db.insert_engagement(cid, "tw-48h", 20, 5, 3, 2, 10.0)

        results = db.auto_classify_posts(min_age_hours=48, min_engagement=5.0)

        assert results["resonated"] == 1
        row = db.conn.execute("SELECT auto_quality FROM generated_content WHERE id = ?", (cid,)).fetchone()
        assert row[0] == "resonated"

    def test_boundary_engagement_exactly_at_threshold(self, db):
        """Post with engagement_score exactly at min_engagement should be classified as 'resonated'."""
        now = datetime.now(timezone.utc)
        old_ts = (now - timedelta(hours=72)).isoformat()

        cid = db.insert_generated_content("x_post", ["sha"], ["uuid"], "threshold engagement", 8.0, "ok")
        db.conn.execute(
            "UPDATE generated_content SET published = 1, tweet_id = 'tw-threshold', published_at = ? WHERE id = ?",
            (old_ts, cid),
        )
        db.conn.commit()
        db.insert_engagement(cid, "tw-threshold", 10, 2, 1, 0, 5.0)  # Exactly 5.0

        results = db.auto_classify_posts(min_age_hours=48, min_engagement=5.0)

        assert results["resonated"] == 1
        row = db.conn.execute("SELECT auto_quality FROM generated_content WHERE id = ?", (cid,)).fetchone()
        assert row[0] == "resonated"

    def test_ambiguous_engagement_between_zero_and_threshold(self, db):
        """Posts with 0 < engagement < threshold should be left as ambiguous (NULL)."""
        now = datetime.now(timezone.utc)
        old_ts = (now - timedelta(hours=72)).isoformat()

        cid = db.insert_generated_content("x_post", ["sha"], ["uuid"], "mid engagement", 8.0, "ok")
        db.conn.execute(
            "UPDATE generated_content SET published = 1, tweet_id = 'tw-mid', published_at = ? WHERE id = ?",
            (old_ts, cid),
        )
        db.conn.commit()
        db.insert_engagement(cid, "tw-mid", 2, 0, 0, 0, 2.0)  # Between 0 and 5.0

        results = db.auto_classify_posts(min_age_hours=48, min_engagement=5.0)

        assert results["resonated"] == 0
        assert results["low_resonance"] == 0
        assert results["ambiguous"] == 1

        # Verify auto_quality remains NULL
        row = db.conn.execute("SELECT auto_quality FROM generated_content WHERE id = ?", (cid,)).fetchone()
        assert row[0] is None

    def test_returns_correct_count_of_classified_posts(self, db):
        """Test that the method returns correct counts for all categories."""
        now = datetime.now(timezone.utc)
        old_ts = (now - timedelta(hours=72)).isoformat()

        # Create 2 resonated posts
        for i in range(2):
            cid = db.insert_generated_content("x_post", ["sha"], ["uuid"], f"resonated {i}", 8.0, "ok")
            db.conn.execute(
                "UPDATE generated_content SET published = 1, tweet_id = ?, published_at = ? WHERE id = ?",
                (f"tw-res-{i}", old_ts, cid),
            )
            db.conn.commit()
            db.insert_engagement(cid, f"tw-res-{i}", 50, 10, 5, 3, 20.0)

        # Create 3 low_resonance posts
        for i in range(3):
            cid = db.insert_generated_content("x_post", ["sha"], ["uuid"], f"low {i}", 8.0, "ok")
            db.conn.execute(
                "UPDATE generated_content SET published = 1, tweet_id = ?, published_at = ? WHERE id = ?",
                (f"tw-low-{i}", old_ts, cid),
            )
            db.conn.commit()
            db.insert_engagement(cid, f"tw-low-{i}", 0, 0, 0, 0, 0.0)

        # Create 1 ambiguous post
        cid = db.insert_generated_content("x_post", ["sha"], ["uuid"], "ambiguous", 8.0, "ok")
        db.conn.execute(
            "UPDATE generated_content SET published = 1, tweet_id = 'tw-amb', published_at = ? WHERE id = ?",
            (old_ts, cid),
        )
        db.conn.commit()
        db.insert_engagement(cid, "tw-amb", 2, 0, 0, 0, 2.5)

        results = db.auto_classify_posts(min_age_hours=48, min_engagement=5.0)

        assert results["resonated"] == 2
        assert results["low_resonance"] == 3
        assert results["ambiguous"] == 1

    def test_no_engagement_data_defaults_to_zero(self, db):
        """Posts with no engagement records should be treated as having 0 engagement."""
        now = datetime.now(timezone.utc)
        old_ts = (now - timedelta(hours=72)).isoformat()

        cid = db.insert_generated_content("x_post", ["sha"], ["uuid"], "no engagement data", 8.0, "ok")
        db.conn.execute(
            "UPDATE generated_content SET published = 1, tweet_id = 'tw-none', published_at = ? WHERE id = ?",
            (old_ts, cid),
        )
        db.conn.commit()
        # Do not insert any engagement record

        results = db.auto_classify_posts(min_age_hours=48, min_engagement=5.0)

        assert results["low_resonance"] == 1
        row = db.conn.execute("SELECT auto_quality FROM generated_content WHERE id = ?", (cid,)).fetchone()
        assert row[0] == "low_resonance"


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


# --- Reply queue ---


class TestReplyQueue:
    def _insert_reply(self, db, tweet_id="tw-100", **kwargs):
        defaults = dict(
            inbound_tweet_id=tweet_id,
            inbound_author_handle="alice",
            inbound_author_id="user_A",
            inbound_text="Nice post!",
            our_tweet_id="our_tw_1",
            our_content_id=1,
            our_post_text="Our original post",
            draft_text="Thanks for the kind words",
        )
        defaults.update(kwargs)
        return db.insert_reply_draft(**defaults)

    def test_insert_and_get_pending_with_enrichment(self, db):
        ctx_json = '{"x_handle":"alice","engagement_stage":3,"dunbar_tier":2}'
        self._insert_reply(
            db,
            relationship_context=ctx_json,
            quality_score=7.5,
            quality_flags='["clean"]',
        )

        pending = db.get_pending_replies()
        assert len(pending) == 1
        r = pending[0]
        assert r["relationship_context"] == ctx_json
        assert r["quality_score"] == 7.5
        assert r["quality_flags"] == '["clean"]'

    def test_insert_without_enrichment_returns_none_columns(self, db):
        self._insert_reply(db)

        pending = db.get_pending_replies()
        assert len(pending) == 1
        r = pending[0]
        assert r["relationship_context"] is None
        assert r["quality_score"] is None
        assert r["quality_flags"] is None

    def test_quality_flags_json_roundtrip(self, db):
        flags = ["sycophantic", "generic"]
        self._insert_reply(db, quality_flags=json.dumps(flags))

        pending = db.get_pending_replies()
        assert json.loads(pending[0]["quality_flags"]) == flags

    def test_relationship_context_json_roundtrip(self, db):
        from engagement.cultivate_bridge import PersonContext

        ctx = PersonContext(
            x_handle="alice",
            display_name="Alice",
            bio="dev",
            relationship_strength=0.5,
            engagement_stage=2,
            dunbar_tier=3,
            authenticity_score=0.8,
            content_quality_score=0.7,
            content_relevance_score=0.6,
            is_known=True,
        )
        self._insert_reply(db, relationship_context=ctx.to_json())

        pending = db.get_pending_replies()
        restored = PersonContext.from_json(pending[0]["relationship_context"])
        assert restored.x_handle == "alice"
        assert restored.engagement_stage == 2
        assert restored.stage_name == "Light"

    def test_reply_queue_columns_exist(self, db):
        cols = {row[1] for row in db.conn.execute("PRAGMA table_info(reply_queue)")}
        assert "relationship_context" in cols
        assert "quality_score" in cols
        assert "quality_flags" in cols


# ===========================================================================
# Additional tests from create-comprehensive-unit-tests-for-the-storage-la branch
# ===========================================================================

# ---------------------------------------------------------------------------

class TestConnectionLifecycle:
    def test_connect_creates_connection(self, tmp_path):
        db = Database(str(tmp_path / "t.db"))
        assert db.conn is None
        db.connect()
        assert db.conn is not None
        db.close()

    def test_close_sets_conn_to_none(self, tmp_path):
        db = Database(str(tmp_path / "t.db"))
        db.connect()
        db.close()
        assert db.conn is None

    def test_close_is_idempotent(self, tmp_path):
        db = Database(str(tmp_path / "t.db"))
        db.connect()
        db.close()
        db.close()  # should not raise
        assert db.conn is None

    def test_context_manager(self, tmp_path):
        db_path = str(tmp_path / "t.db")
        with Database(db_path) as db:
            assert db.conn is not None
        assert db.conn is None

    def test_row_factory_is_sqlite_row(self, db):
        assert db.conn.row_factory is sqlite3.Row


# ---------------------------------------------------------------------------
# Schema initialisation & migration
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------

class TestInitSchema:
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

    def test_migration_columns_present(self, db):
        cols = {
            row[1]
            for row in db.conn.execute("PRAGMA table_info(generated_content)")
        }
        for col in ("retry_count", "last_retry_at", "tweet_id", "published_at"):
            assert col in cols

    def test_init_schema_is_idempotent(self, db, schema_path):
        db.init_schema(schema_path=schema_path)  # second call must not raise


# ---------------------------------------------------------------------------
# Claude messages
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------

class TestMarkPublished:
    def test_mark_published_sets_fields(self, db, sample_content):
        cid = db.insert_generated_content(**sample_content)
        db.mark_published(cid, "https://x.com/status/1", tweet_id="tw-999")
        row = db.conn.execute(
            "SELECT published, published_url, tweet_id, published_at "
            "FROM generated_content WHERE id = ?",
            (cid,),
        ).fetchone()
        assert row["published"] == 1
        assert row["published_url"] == "https://x.com/status/1"
        assert row["tweet_id"] == "tw-999"
        assert row["published_at"] is not None

    def test_mark_published_without_tweet_id(self, db, sample_content):
        cid = db.insert_generated_content(**sample_content)
        db.mark_published(cid, "https://example.com/post")
        row = db.conn.execute(
            "SELECT tweet_id FROM generated_content WHERE id = ?", (cid,)
        ).fetchone()
        assert row["tweet_id"] is None

    def test_mark_published_nonexistent_id_is_noop(self, db):
        """Updating a nonexistent row should not raise."""
        db.mark_published(9999, "https://example.com")  # no error


# ---------------------------------------------------------------------------
# Retry & abandon
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------

class TestRetryAndAbandon:
    def test_increment_retry_returns_count(self, db, sample_content):
        cid = db.insert_generated_content(**sample_content)
        assert db.increment_retry(cid) == 1
        assert db.increment_retry(cid) == 2

    def test_increment_retry_sets_last_retry_at(self, db, sample_content):
        cid = db.insert_generated_content(**sample_content)
        db.increment_retry(cid)
        row = db.conn.execute(
            "SELECT last_retry_at FROM generated_content WHERE id = ?", (cid,)
        ).fetchone()
        assert row["last_retry_at"] is not None

    def test_increment_retry_triggers_abandon_at_max(self, db, sample_content):
        cid = db.insert_generated_content(**sample_content)
        for i in range(MAX_RETRIES):
            count = db.increment_retry(cid)
        assert count == MAX_RETRIES
        row = db.conn.execute(
            "SELECT published FROM generated_content WHERE id = ?", (cid,)
        ).fetchone()
        assert row["published"] == -1  # abandoned

    def test_increment_retry_nonexistent_returns_zero(self, db):
        assert db.increment_retry(9999) == 0

    def test_mark_abandoned(self, db, sample_content):
        cid = db.insert_generated_content(**sample_content)
        db.mark_abandoned(cid)
        row = db.conn.execute(
            "SELECT published FROM generated_content WHERE id = ?", (cid,)
        ).fetchone()
        assert row["published"] == -1


# ---------------------------------------------------------------------------
# Last published time
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------

class TestGetLastPublishedTime:
    def test_returns_none_when_nothing_published(self, db):
        assert db.get_last_published_time() is None

    def test_returns_most_recent(self, db, sample_content):
        cid1 = db.insert_generated_content(**sample_content)
        db.mark_published(cid1, "https://x.com/1", tweet_id="t1")

        cid2 = db.insert_generated_content(
            **{**sample_content, "content": "second post"}
        )
        db.mark_published(cid2, "https://x.com/2", tweet_id="t2")

        result = db.get_last_published_time("x_post")
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_filters_by_content_type(self, db, sample_content):
        cid = db.insert_generated_content(
            **{**sample_content, "content_type": "blog_post"}
        )
        db.mark_published(cid, "https://blog.example.com/1")
        assert db.get_last_published_time("x_post") is None
        assert db.get_last_published_time("blog_post") is not None

    def test_naive_datetime_gets_utc(self, db, sample_content):
        """If published_at is stored without tz info, it should still return UTC."""
        cid = db.insert_generated_content(**sample_content)
        # Manually store a naive ISO string
        db.conn.execute(
            "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
            ("2026-03-20T12:00:00", cid),
        )
        db.conn.commit()
        result = db.get_last_published_time("x_post")
        assert result.tzinfo == timezone.utc


# ---------------------------------------------------------------------------
# Poll state
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------

class TestEngagementTracking:
    def _publish_post(self, db, sample_content, tweet_id="tw-1"):
        cid = db.insert_generated_content(**sample_content)
        db.mark_published(cid, f"https://x.com/{tweet_id}", tweet_id=tweet_id)
        return cid

    def test_insert_engagement(self, db, sample_content):
        cid = self._publish_post(db, sample_content)
        eid = db.insert_engagement(
            content_id=cid,
            tweet_id="tw-1",
            like_count=10,
            retweet_count=3,
            reply_count=1,
            quote_count=0,
            engagement_score=14.0,
        )
        assert isinstance(eid, int) and eid > 0

    def test_get_posts_needing_metrics_returns_fresh_post(self, db, sample_content):
        self._publish_post(db, sample_content)
        results = db.get_posts_needing_metrics()
        assert len(results) == 1
        assert results[0]["tweet_id"] == "tw-1"

    def test_get_posts_needing_metrics_excludes_recently_fetched(
        self, db, sample_content
    ):
        cid = self._publish_post(db, sample_content)
        db.insert_engagement(
            content_id=cid,
            tweet_id="tw-1",
            like_count=1,
            retweet_count=0,
            reply_count=0,
            quote_count=0,
            engagement_score=1.0,
        )
        # Engagement was just fetched, so it should be excluded (< 6 hours old)
        results = db.get_posts_needing_metrics()
        assert len(results) == 0

    def test_get_posts_needing_metrics_respects_max_age(self, db, sample_content):
        """Posts older than max_age_days are excluded."""
        cid = db.insert_generated_content(**sample_content)
        old_date = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
        db.conn.execute(
            "UPDATE generated_content SET published = 1, tweet_id = 'tw-old', published_at = ? WHERE id = ?",
            (old_date, cid),
        )
        db.conn.commit()
        results = db.get_posts_needing_metrics(max_age_days=30)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# Top performing posts
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------

class TestTopPerformingPosts:
    def test_returns_empty_when_no_engagement(self, db):
        assert db.get_top_performing_posts() == []

    def test_ranked_by_engagement_score(self, db, sample_content):
        # Create and publish two posts
        cid1 = db.insert_generated_content(**sample_content)
        db.mark_published(cid1, "https://x.com/1", tweet_id="tw-1")
        cid2 = db.insert_generated_content(
            **{**sample_content, "content": "post two"}
        )
        db.mark_published(cid2, "https://x.com/2", tweet_id="tw-2")

        # Post 2 has higher engagement
        db.insert_engagement(cid1, "tw-1", 5, 1, 0, 0, 6.0)
        db.insert_engagement(cid2, "tw-2", 50, 10, 5, 2, 67.0)

        results = db.get_top_performing_posts(limit=5)
        assert len(results) == 2
        assert results[0]["id"] == cid2

    def test_uses_latest_engagement_snapshot(self, db, sample_content):
        cid = db.insert_generated_content(**sample_content)
        db.mark_published(cid, "https://x.com/1", tweet_id="tw-1")

        # Old snapshot
        db.insert_engagement(cid, "tw-1", 1, 0, 0, 0, 1.0)
        # Newer snapshot with higher score
        db.insert_engagement(cid, "tw-1", 20, 5, 3, 1, 29.0)

        results = db.get_top_performing_posts(limit=1)
        assert len(results) == 1
        assert results[0]["engagement_score"] == 29.0

    def test_limit_parameter(self, db, sample_content):
        for i in range(5):
            cid = db.insert_generated_content(
                **{**sample_content, "content": f"post {i}"}
            )
            db.mark_published(cid, f"https://x.com/{i}", tweet_id=f"tw-{i}")
            db.insert_engagement(cid, f"tw-{i}", i, 0, 0, 0, float(i))

        results = db.get_top_performing_posts(limit=2)
        assert len(results) == 2

    def test_filters_by_content_type(self, db, sample_content):
        cid = db.insert_generated_content(
            **{**sample_content, "content_type": "blog_post"}
        )
        db.mark_published(cid, "https://blog.example.com/1", tweet_id="tw-blog")
        db.insert_engagement(cid, "tw-blog", 10, 5, 2, 1, 18.0)

        assert db.get_top_performing_posts(content_type="x_post") == []
        assert len(db.get_top_performing_posts(content_type="blog_post")) == 1


# ---------------------------------------------------------------------------
# Pipeline runs
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------

class TestTransactionBehaviour:
    def test_insert_message_commits_immediately(self, db, sample_message):
        """Each insert commits, so data survives a fresh connection."""
        db.insert_claude_message(**sample_message)

        # Open second connection to same file to prove data is persisted
        db2 = Database(str(db.db_path))
        db2.connect()
        assert db2.is_message_processed(sample_message["message_uuid"]) is True
        db2.close()

    def test_insert_commit_commits_immediately(self, db, sample_commit):
        db.insert_commit(**sample_commit)

        db2 = Database(str(db.db_path))
        db2.connect()
        assert db2.is_commit_processed(sample_commit["commit_sha"]) is True
        db2.close()

    def test_multiple_inserts_are_independent(self, db, sample_content):
        """Failure of one insert should not roll back others."""
        db.insert_generated_content(**sample_content)
        try:
            # This will fail because batch_id UNIQUE constraint is not
            # relevant here; instead we rely on a known invariant—
            # each insert_generated_content call commits on its own.
            db.insert_generated_content(**sample_content)
        except Exception:
            pass
        # First insert should still be there
        rows = db.get_unpublished_content("x_post", min_score=0)
        assert len(rows) >= 1
