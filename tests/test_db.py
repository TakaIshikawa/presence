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
            "github_activity",
            "commit_prompt_links",
            "generated_content",
            "post_engagement",
            "prompt_versions",
            "poll_state",
            "knowledge",
            "curated_sources",
            "content_knowledge_links",
            "pipeline_runs",
            "content_publications",
            "content_variants",
            "content_ideas",
            "eval_batches",
            "eval_results",
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
        assert "source_activity_ids" in cols
        assert "curation_quality" in cols
        assert "auto_quality" in cols
        assert "bluesky_uri" in cols

    def test_pipeline_runs_columns_exist(self, db):
        cols = {
            row[1]
            for row in db.conn.execute("PRAGMA table_info(pipeline_runs)")
        }
        assert "outcome" in cols
        assert "rejection_reason" in cols

    def test_content_publications_columns_exist(self, db):
        cols = {
            row[1]
            for row in db.conn.execute("PRAGMA table_info(content_publications)")
        }
        expected = {
            "content_id",
            "platform",
            "status",
            "platform_post_id",
            "platform_url",
            "error",
            "error_category",
            "attempt_count",
            "next_retry_at",
            "last_error_at",
            "published_at",
            "updated_at",
        }
        assert expected.issubset(cols)

    def test_publish_queue_error_category_column_exists(self, db):
        cols = {
            row[1]
            for row in db.conn.execute("PRAGMA table_info(publish_queue)")
        }
        assert "error_category" in cols

    def test_content_variants_columns_exist(self, db):
        cols = {
            row[1]
            for row in db.conn.execute("PRAGMA table_info(content_variants)")
        }
        expected = {
            "content_id",
            "platform",
            "variant_type",
            "content",
            "metadata",
            "created_at",
        }
        assert expected.issubset(cols)

    def test_content_ideas_columns_exist(self, db):
        cols = {
            row[1]
            for row in db.conn.execute("PRAGMA table_info(content_ideas)")
        }
        expected = {
            "note",
            "topic",
            "priority",
            "status",
            "source",
            "created_at",
            "updated_at",
        }
        assert expected.issubset(cols)

    def test_newsletter_subscriber_metrics_table_exists(self, db):
        cols = {
            row[1]
            for row in db.conn.execute(
                "PRAGMA table_info(newsletter_subscriber_metrics)"
            )
        }
        expected = {
            "subscriber_count",
            "active_subscriber_count",
            "unsubscribes",
            "churn_rate",
            "new_subscribers",
            "net_subscriber_change",
            "raw_metrics",
            "fetched_at",
        }
        assert expected.issubset(cols)

    def test_newsletter_subject_candidates_table_exists(self, db):
        cols = {
            row[1]
            for row in db.conn.execute(
                "PRAGMA table_info(newsletter_subject_candidates)"
            )
        }
        expected = {
            "newsletter_send_id",
            "issue_id",
            "subject",
            "score",
            "rationale",
            "source",
            "rank",
            "selected",
            "source_content_ids",
            "week_start",
            "week_end",
            "metadata",
        }
        assert expected.issubset(cols)

    def test_idempotent_init(self, db, schema_path):
        # Running init_schema again should not raise
        db.init_schema(schema_path)


class TestPromptVersions:
    def test_prompt_versions_schema_has_hash(self, db):
        cols = {
            row[1]
            for row in db.conn.execute("PRAGMA table_info(prompt_versions)")
        }
        assert "prompt_hash" in cols

    def test_register_prompt_version_is_deterministic(self, db):
        first = db.register_prompt_version("x_post", "Prompt text")
        second = db.register_prompt_version("x_post", "Prompt text")

        assert first["id"] == second["id"]
        assert first["version"] == 1
        assert second["usage_count"] == 2
        assert len(second["prompt_hash"]) == 64

        rows = db.conn.execute(
            "SELECT * FROM prompt_versions WHERE prompt_type = ?",
            ("x_post",),
        ).fetchall()
        assert len(rows) == 1

    def test_register_prompt_version_increments_version_for_changed_hash(self, db):
        first = db.register_prompt_version("x_post", "Prompt text")
        second = db.register_prompt_version("x_post", "Changed prompt text")

        assert first["version"] == 1
        assert second["version"] == 2
        assert first["prompt_hash"] != second["prompt_hash"]


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
            assert "intent" in cols
            assert "priority" in cols

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

    def test_migration_creates_content_publications_for_existing_schema(self, schema_path):
        """Test that content_publications is added to an existing in-memory schema."""
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

            tables = {
                row[0]
                for row in db.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            assert "content_publications" in tables

            indexes = {
                row[1]
                for row in db.conn.execute(
                    "SELECT * FROM sqlite_master WHERE type='index' AND tbl_name='content_publications'"
                ).fetchall()
            }
            assert "idx_content_publications_content" in indexes
            assert "idx_content_publications_platform_status" in indexes
            assert "idx_content_publications_retry" in indexes

    def test_migration_creates_content_variants_for_existing_schema(self, schema_path):
        """Test that content_variants is added to an existing in-memory schema."""
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

            tables = {
                row[0]
                for row in db.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            assert "content_variants" in tables

            indexes = {
                row[1]
                for row in db.conn.execute("PRAGMA index_list(content_variants)")
            }
            assert "idx_content_variants_content" in indexes

    def test_migration_adds_campaign_id_before_schema_indexes(self, schema_path):
        """Old DBs without planned_topics.campaign_id should initialize cleanly."""
        with Database(":memory:") as db:
            db.conn.execute("""
                CREATE TABLE planned_topics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    angle TEXT,
                    source_material TEXT,
                    target_date TEXT,
                    status TEXT DEFAULT 'planned',
                    content_id INTEGER,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            db.conn.commit()

            db.init_schema(schema_path)

            cols = {row[1] for row in db.conn.execute("PRAGMA table_info(planned_topics)")}
            assert "campaign_id" in cols
            indexes = {
                row[1]
                for row in db.conn.execute("PRAGMA index_list(planned_topics)")
            }
            assert "idx_planned_topics_campaign" in indexes

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


class TestGitHubActivity:
    def test_upsert_and_check_processed(self, db):
        activity_id = db.upsert_github_activity(
            repo_name="owner/repo",
            activity_type="issue",
            number=7,
            title="Fix flaky tests",
            body="Details",
            state="open",
            author="taka",
            url="https://github.com/owner/repo/issues/7",
            updated_at="2026-04-01T12:00:00+00:00",
            created_at="2026-04-01T10:00:00+00:00",
            labels=["bug", "tests"],
        )

        assert activity_id
        assert db.is_github_activity_processed("owner/repo", "issue", 7) is True
        assert db.is_github_activity_processed(
            "owner/repo", "issue", 7, "2026-04-01T12:00:00+00:00"
        ) is True
        assert db.is_github_activity_processed(
            "owner/repo", "issue", 7, "2026-04-01T13:00:00+00:00"
        ) is False

    def test_upsert_updates_existing_activity(self, db):
        first_id = db.upsert_github_activity(
            repo_name="repo",
            activity_type="pull_request",
            number=3,
            title="Old title",
            state="open",
            author="taka",
            url="url",
            updated_at="2026-04-01T12:00:00+00:00",
        )
        second_id = db.upsert_github_activity(
            repo_name="repo",
            activity_type="pull_request",
            number=3,
            title="New title",
            state="closed",
            author="taka",
            url="url",
            updated_at="2026-04-01T13:00:00+00:00",
            merged_at="2026-04-01T13:00:00+00:00",
        )

        assert second_id == first_id
        rows = db.get_github_activity_in_range(
            datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 2, 0, 0, tzinfo=timezone.utc),
        )
        assert len(rows) == 1
        assert rows[0]["title"] == "New title"
        assert rows[0]["merged_at"] == "2026-04-01T13:00:00+00:00"

    def test_get_github_activity_in_range_parses_labels(self, db):
        db.upsert_github_activity(
            repo_name="repo",
            activity_type="issue",
            number=1,
            title="Issue",
            state="open",
            author="taka",
            url="url",
            updated_at="2026-04-01T12:00:00+00:00",
            labels=["bug"],
            metadata={"source": "test"},
        )

        rows = db.get_github_activity_in_range(
            datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 2, 0, 0, tzinfo=timezone.utc),
            activity_type="issue",
        )

        assert rows[0]["labels"] == ["bug"]
        assert rows[0]["metadata"] == {"source": "test"}
        assert rows[0]["activity_id"] == "repo#1:issue"

    def test_github_activity_recent_and_unresolved_helpers(self, db):
        db.upsert_github_activity(
            repo_name="repo",
            activity_type="issue",
            number=1,
            title="Open issue",
            state="open",
            author="taka",
            url="url",
            updated_at="2026-04-01T12:00:00+00:00",
            labels=["bug"],
        )
        db.upsert_github_activity(
            repo_name="repo",
            activity_type="pull_request",
            number=2,
            title="Closed PR",
            state="closed",
            author="taka",
            url="url",
            updated_at="2026-03-20T12:00:00+00:00",
        )

        now = datetime(2026, 4, 3, 0, 0, tzinfo=timezone.utc)
        recent = db.get_recent_github_activity(days=7, now=now)
        unresolved = db.get_unresolved_github_activity()

        assert [row["activity_id"] for row in recent] == ["repo#1:issue"]
        assert [row["activity_id"] for row in unresolved] == ["repo#1:issue"]


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

    def test_insert_and_retrieve_source_activity_ids(self, db):
        db.upsert_github_activity(
            repo_name="repo",
            activity_type="issue",
            number=9,
            title="Keep retries visible",
            state="open",
            author="taka",
            url="url",
            updated_at="2026-04-01T12:00:00+00:00",
        )
        content_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            source_activity_ids=["repo#9:issue"],
            content="Post from issue context",
            eval_score=8.0,
            eval_feedback="ok",
        )

        content = db.get_generated_content(content_id)
        activity = db.get_source_github_activity_for_content(content_id)

        assert content["source_activity_ids"] == ["repo#9:issue"]
        assert activity[0]["matched"] is True
        assert activity[0]["title"] == "Keep retries visible"

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

        publication = db.get_publication_state(content_id, "x")
        assert publication["status"] == "published"
        assert publication["platform_post_id"] == "tweet-001"
        assert publication["platform_url"] == "https://x.com/post/1"
        assert publication["attempt_count"] == 1

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

    def test_publication_failure_is_per_platform(self, db):
        content_id = self._insert_content(db)
        db.upsert_publication_success(
            content_id,
            "x",
            platform_post_id="tweet-001",
            platform_url="https://x.com/post/1",
        )
        db.upsert_publication_failure(
            content_id,
            "bluesky",
            "Authentication failed",
        )

        x_state = db.get_publication_state(content_id, "x")
        bsky_state = db.get_publication_state(content_id, "bluesky")

        assert x_state["status"] == "published"
        assert x_state["error"] is None
        assert bsky_state["status"] == "failed"
        assert bsky_state["error"] == "Authentication failed"
        assert bsky_state["error_category"] == "auth"
        assert bsky_state["attempt_count"] == 1

    def test_publication_failure_retry_increments_attempt_count(self, db):
        content_id = self._insert_content(db)
        db.upsert_publication_failure(content_id, "bluesky", "temporary error")
        db.upsert_publication_failure(content_id, "bluesky", "rate limit")

        state = db.get_publication_state(content_id, "bluesky")
        assert state["status"] == "failed"
        assert state["error"] == "rate limit"
        assert state["error_category"] == "rate_limit"
        assert state["attempt_count"] == 2

    def test_publication_first_failure_sets_retry_backoff(self, db):
        content_id = self._insert_content(db)
        db.upsert_publication_failure(content_id, "x", "rate limit")

        state = db.get_publication_state(content_id, "x")
        last_error_at = datetime.fromisoformat(state["last_error_at"])
        next_retry_at = datetime.fromisoformat(state["next_retry_at"])

        assert state["attempt_count"] == 1
        assert 299 <= (next_retry_at - last_error_at).total_seconds() <= 301

    def test_publication_second_failure_doubles_retry_backoff(self, db):
        content_id = self._insert_content(db)
        db.upsert_publication_failure(content_id, "x", "rate limit")
        db.upsert_publication_failure(content_id, "x", "still limited")

        state = db.get_publication_state(content_id, "x")
        last_error_at = datetime.fromisoformat(state["last_error_at"])
        next_retry_at = datetime.fromisoformat(state["next_retry_at"])

        assert state["attempt_count"] == 2
        assert 599 <= (next_retry_at - last_error_at).total_seconds() <= 601

    def test_publication_failure_retry_backoff_respects_max_delay(self, db):
        content_id = self._insert_content(db)
        db.upsert_publication_failure(
            content_id,
            "x",
            "rate limit",
            max_retry_delay_minutes=3,
        )

        state = db.get_publication_state(content_id, "x")
        last_error_at = datetime.fromisoformat(state["last_error_at"])
        next_retry_at = datetime.fromisoformat(state["next_retry_at"])

        assert (next_retry_at - last_error_at).total_seconds() == 180

    def test_queue_for_publishing_seeds_platform_states(self, db):
        content_id = self._insert_content(db)
        db.queue_for_publishing(content_id, "2026-04-17T12:00:00+00:00", platform="all")

        states = {
            row["platform"]: row
            for row in db.get_latest_publication_states(content_id)
        }
        assert states["x"]["status"] == "queued"
        assert states["bluesky"]["status"] == "queued"
        assert states["x"]["attempt_count"] == 0
        assert states["bluesky"]["attempt_count"] == 0

    def test_insert_content_variant(self, db):
        content_id = self._insert_content(db)

        variant_id = db.upsert_content_variant(
            content_id=content_id,
            platform="x",
            variant_type="post",
            content="Short X copy",
            metadata={"source": "generator", "score": 8.4},
        )

        variant = db.get_content_variant(content_id, "x", "post")
        assert variant["id"] == variant_id
        assert variant["content"] == "Short X copy"
        assert variant["metadata"] == {"source": "generator", "score": 8.4}

        row = db.conn.execute(
            "SELECT metadata FROM content_variants WHERE id = ?",
            (variant_id,),
        ).fetchone()
        assert json.loads(row["metadata"]) == {"source": "generator", "score": 8.4}

    def test_update_content_variant_preserves_unique_row(self, db):
        content_id = self._insert_content(db)
        first_id = db.upsert_content_variant(
            content_id, "newsletter", "summary", "Original summary", {"version": 1}
        )

        second_id = db.upsert_content_variant(
            content_id, "newsletter", "summary", "Updated summary", {"version": 2}
        )

        assert second_id == first_id
        variant = db.get_content_variant(content_id, "newsletter", "summary")
        assert variant["content"] == "Updated summary"
        assert variant["metadata"] == {"version": 2}

    def test_content_variant_uniqueness_is_per_platform_and_type(self, db):
        content_id = self._insert_content(db)

        db.upsert_content_variant(content_id, "x", "post", "X copy")
        db.upsert_content_variant(content_id, "bluesky", "post", "Bluesky copy")
        db.upsert_content_variant(content_id, "linkedin", "post", "LinkedIn copy")
        db.upsert_content_variant(content_id, "x", "thread", "X thread")

        variants = db.list_content_variants(content_id)
        keys = {(v["platform"], v["variant_type"]) for v in variants}
        assert keys == {
            ("x", "post"),
            ("bluesky", "post"),
            ("linkedin", "post"),
            ("x", "thread"),
        }
        assert len(variants) == 4

    def test_list_generated_content_for_variant_refresh_returns_supported_content(self, db):
        x_post_id = self._insert_content(db, content_type="x_post", content="X post")
        thread_id = self._insert_content(db, content_type="x_thread", content="Thread")
        blog_seed_id = self._insert_content(db, content_type="blog_seed", content="Seed")
        self._insert_content(db, content_type="newsletter", content="Newsletter")

        rows = db.list_generated_content_for_variant_refresh(limit=10)

        ids = {row["id"] for row in rows}
        assert {x_post_id, thread_id, blog_seed_id}.issubset(ids)
        assert all(row["content_type"] in {"x_post", "x_thread", "blog_seed"} for row in rows)

    def test_missing_content_variant_lookup_returns_none(self, db):
        content_id = self._insert_content(db)

        assert db.get_content_variant(content_id, "blog", "draft") is None

    def test_get_content_provenance_returns_single_item_details(self, db):
        commit_id = db.insert_commit(
            "presence",
            "sha1",
            "fix: tighten retry provenance",
            "2026-04-22T12:00:00+00:00",
            "taka",
        )
        message_id = db.insert_claude_message(
            "session-1",
            "uuid1",
            "/repo",
            "2026-04-22T11:58:00+00:00",
            "Add provenance reporting",
        )
        content_id = self._insert_content(db)
        db.upsert_content_variant(content_id, "x", "post", "Variant copy")
        db.upsert_publication_success(
            content_id,
            "x",
            platform_post_id="tweet-1",
            platform_url="https://x.test/tweet-1",
            published_at="2026-04-22T13:00:00+00:00",
        )
        db.insert_engagement(content_id, "tweet-1", 5, 2, 1, 0, 9.0)
        db.insert_pipeline_run(
            batch_id="batch-1",
            content_type="x_post",
            candidates_generated=3,
            best_candidate_index=1,
            best_score_before_refine=7.8,
            final_score=8.0,
            published=True,
            content_id=content_id,
            outcome="published",
            filter_stats={"kept": 1},
        )
        knowledge_id = db.conn.execute(
            """INSERT INTO knowledge
               (source_type, source_id, source_url, author, content, insight)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("curated_article", "article-1", "https://example.test/a", "Ada", "Long source", "Sharp insight"),
        ).lastrowid
        db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.91)])

        provenance = db.get_content_provenance(content_id)

        assert provenance["content"]["id"] == content_id
        assert provenance["content"]["source_commits"] == ["sha1", "sha2"]
        assert provenance["source_commits"][0]["id"] == commit_id
        assert provenance["source_commits"][0]["matched"] is True
        assert provenance["source_commits"][1]["commit_sha"] == "sha2"
        assert provenance["source_commits"][1]["matched"] is False
        assert provenance["source_messages"][0]["id"] == message_id
        assert provenance["knowledge_links"][0]["insight"] == "Sharp insight"
        assert provenance["variants"][0]["content"] == "Variant copy"
        assert provenance["publications"][0]["platform_post_id"] == "tweet-1"
        assert provenance["engagement_snapshots"][0]["platform"] == "x"
        assert provenance["pipeline_runs"][0]["filter_stats"] == {"kept": 1}

    def test_get_content_provenance_missing_content_returns_none(self, db):
        assert db.get_content_provenance(9999) is None


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
        assert "intent" in cols
        assert "priority" in cols

    def test_insert_reply_draft_stores_intent_priority_and_status(self, db):
        self._insert_reply(
            db,
            tweet_id="classified",
            intent="bug_report",
            priority="high",
            status="pending",
        )

        row = db.conn.execute(
            "SELECT intent, priority, status FROM reply_queue WHERE inbound_tweet_id = ?",
            ("classified",),
        ).fetchone()
        assert row["intent"] == "bug_report"
        assert row["priority"] == "high"
        assert row["status"] == "pending"

    def test_update_reply_classification_helpers(self, db):
        reply_id = self._insert_reply(db, tweet_id="needs-classification")

        db.update_reply_classification(reply_id, "question", "normal")
        db.update_reply_priority(reply_id, "high")

        row = db.conn.execute(
            "SELECT intent, priority FROM reply_queue WHERE id = ?",
            (reply_id,),
        ).fetchone()
        assert row["intent"] == "question"
        assert row["priority"] == "high"

    def test_get_expired_reply_drafts_returns_old_pending_x_and_bluesky(self, db):
        now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
        x_id = self._insert_reply(db, tweet_id="x-old", platform="x")
        bluesky_id = self._insert_reply(
            db,
            tweet_id="bsky-old",
            platform="bluesky",
            inbound_url="https://bsky.app/profile/alice/post/abc",
        )
        fresh_id = self._insert_reply(db, tweet_id="x-fresh", platform="x")
        posted_id = self._insert_reply(db, tweet_id="x-posted", platform="x")
        db.conn.execute(
            "UPDATE reply_queue SET detected_at = ? WHERE id IN (?, ?)",
            ("2026-04-20 12:00:00", x_id, bluesky_id),
        )
        db.conn.execute(
            "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
            ("2026-04-23 10:00:00", fresh_id),
        )
        db.conn.execute(
            "UPDATE reply_queue SET detected_at = ?, status = 'posted' WHERE id = ?",
            ("2026-04-20 12:00:00", posted_id),
        )
        db.conn.commit()

        expired = db.get_expired_reply_drafts(48, now=now)

        assert [row["inbound_tweet_id"] for row in expired] == ["x-old", "bsky-old"]
        assert {row["platform"] for row in expired} == {"x", "bluesky"}

    def test_dismiss_expired_reply_drafts_marks_only_old_pending(self, db):
        now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
        old_id = self._insert_reply(db, tweet_id="old", platform="x")
        fresh_id = self._insert_reply(db, tweet_id="fresh", platform="bluesky")
        db.conn.execute(
            "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
            ("2026-04-20 12:00:00", old_id),
        )
        db.conn.execute(
            "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
            ("2026-04-23 10:00:00", fresh_id),
        )
        db.conn.commit()

        dismissed = db.dismiss_expired_reply_drafts(48, now=now)

        assert dismissed == 1
        rows = {
            row["inbound_tweet_id"]: dict(row)
            for row in db.conn.execute(
                "SELECT inbound_tweet_id, status, reviewed_at FROM reply_queue"
            )
        }
        assert rows["old"]["status"] == "dismissed"
        assert rows["old"]["reviewed_at"] == "2026-04-23T12:00:00+00:00"
        assert rows["fresh"]["status"] == "pending"
        assert rows["fresh"]["reviewed_at"] is None

    def test_expired_reply_drafts_rejects_non_positive_ttl(self, db):
        with pytest.raises(ValueError, match="draft_ttl_hours"):
            db.get_expired_reply_drafts(0)
        with pytest.raises(ValueError, match="draft_ttl_hours"):
            db.dismiss_expired_reply_drafts(-1)


# ====================================================================

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


class TestContentFeedback:
    def test_table_created(self, db):
        tables = {
            row[0]
            for row in db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "content_feedback" in tables

    def test_add_content_feedback(self, db, sample_content):
        cid = db.insert_generated_content(**sample_content)
        feedback_id = db.add_content_feedback(
            cid,
            "reject",
            "too vague",
            "Make the failure mode concrete.",
        )
        row = db.conn.execute(
            """SELECT content_id, feedback_type, notes, replacement_text
               FROM content_feedback WHERE id = ?""",
            (feedback_id,),
        ).fetchone()

        assert row["content_id"] == cid
        assert row["feedback_type"] == "reject"
        assert row["notes"] == "too vague"
        assert row["replacement_text"] == "Make the failure mode concrete."

    def test_add_content_feedback_rejects_invalid_type(self, db, sample_content):
        cid = db.insert_generated_content(**sample_content)
        with pytest.raises(ValueError):
            db.add_content_feedback(cid, "unclear", "bad label")

    def test_get_recent_content_feedback_filters_by_type_and_content_type(
        self, db, sample_content
    ):
        post_id = db.insert_generated_content(**sample_content)
        thread_id = db.insert_generated_content(
            **{**sample_content, "content_type": "x_thread", "content": "thread"}
        )
        db.add_content_feedback(post_id, "reject", "avoid abstract framing")
        db.add_content_feedback(thread_id, "prefer", "thread worked")

        rows = db.get_recent_content_feedback(
            content_type="x_post",
            feedback_types=["reject"],
        )

        assert len(rows) == 1
        assert rows[0]["content_id"] == post_id
        assert rows[0]["content"] == sample_content["content"]


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
        count = db.conn.execute(
            "SELECT COUNT(*) FROM content_publications WHERE content_id = 9999"
        ).fetchone()[0]
        assert count == 0

    def test_mark_published_bluesky_sets_uri(self, db, sample_content):
        """Test marking content as published to Bluesky."""
        cid = db.insert_generated_content(**sample_content)
        db.mark_published_bluesky(cid, "at://did:plc:xyz/app.bsky.feed.post/abc123")

        row = db.conn.execute(
            "SELECT bluesky_uri FROM generated_content WHERE id = ?", (cid,)
        ).fetchone()
        assert row[0] == "at://did:plc:xyz/app.bsky.feed.post/abc123"

    def test_mark_published_bluesky_nonexistent_id_is_noop(self, db):
        """Updating a nonexistent row should not raise."""
        db.mark_published_bluesky(9999, "at://did:plc:xyz/app.bsky.feed.post/abc")
        count = db.conn.execute(
            "SELECT COUNT(*) FROM content_publications WHERE content_id = 9999"
        ).fetchone()[0]
        assert count == 0


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
    def test_insert_message_commits_immediately(self, file_db, sample_message):
        """Each insert commits, so data survives a fresh connection."""
        file_db.insert_claude_message(**sample_message)

        # Open second connection to same file to prove data is persisted
        db2 = Database(str(file_db.db_path))
        db2.connect()
        assert db2.is_message_processed(sample_message["message_uuid"]) is True
        db2.close()

    def test_insert_commit_commits_immediately(self, file_db, sample_commit):
        file_db.insert_commit(**sample_commit)

        db2 = Database(str(file_db.db_path))
        db2.connect()
        assert db2.is_commit_processed(sample_commit["commit_sha"]) is True
        db2.close()

    def test_multiple_inserts_are_independent(self, file_db, sample_content):
        """Failure of one insert should not roll back others."""
        file_db.insert_generated_content(**sample_content)
        try:
            # This will fail because batch_id UNIQUE constraint is not
            # relevant here; instead we rely on a known invariant—
            # each insert_generated_content call commits on its own.
            file_db.insert_generated_content(**sample_content)
        except Exception:
            pass
        # First insert should still be there
        rows = file_db.get_unpublished_content("x_post", min_score=0)
        assert len(rows) >= 1

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

    def test_eval_batch_round_trip(self, db):
        batch_id = db.create_eval_batch(
            label="prompt tuning",
            content_type="x_thread",
            generator_model="claude-sonnet-4.5",
            evaluator_model="claude-opus-4.6",
            threshold=0.7,
        )
        result_id = db.add_eval_result(
            batch_id=batch_id,
            content_type="x_thread",
            generator_model="claude-sonnet-4.5",
            evaluator_model="claude-opus-4.6",
            threshold=0.7,
            source_window_hours=8,
            prompt_count=3,
            commit_count=2,
            candidate_count=4,
            final_score=8.2,
            rejection_reason=None,
            filter_stats={"repetition_rejected": 1},
            final_content="redacted content",
        )

        payload = db.get_eval_batch(batch_id)
        assert payload["batch"]["label"] == "prompt tuning"
        assert payload["batch"]["content_type"] == "x_thread"
        assert payload["results"][0]["id"] == result_id
        assert payload["results"][0]["source_window_hours"] == 8
        assert payload["results"][0]["filter_stats"] == {"repetition_rejected": 1}
        assert payload["results"][0]["final_content"] == "redacted content"

    def test_list_eval_batches_summarizes_recent_batches(self, db):
        batch_id = db.create_eval_batch(
            label="compare models",
            content_type="x_post",
            generator_model="model-a",
            evaluator_model="model-b",
            threshold=0.75,
        )
        db.add_eval_result(
            batch_id=batch_id,
            content_type="x_post",
            generator_model="model-a",
            evaluator_model="model-b",
            threshold=0.75,
            source_window_hours=8,
            prompt_count=1,
            commit_count=1,
            candidate_count=2,
            final_score=7.0,
        )
        db.add_eval_result(
            batch_id=batch_id,
            content_type="x_post",
            generator_model="model-a",
            evaluator_model="model-b",
            threshold=0.75,
            source_window_hours=16,
            prompt_count=2,
            commit_count=2,
            candidate_count=3,
            final_score=9.0,
        )

        batches = db.list_eval_batches()
        assert batches[0]["id"] == batch_id
        assert batches[0]["result_count"] == 2
        assert batches[0]["average_score"] == 8.0
        assert batches[0]["best_score"] == 9.0

    def test_get_eval_batch_missing_returns_none(self, db):
        assert db.get_eval_batch(9999) is None


# ---------------------------------------------------------------------------
# Content embedding
# ---------------------------------------------------------------------------


class TestSetContentEmbedding:
    """Tests for set_content_embedding() method (~line 421 in db.py)."""

    def test_set_content_embedding_stores_blob(self, db, sample_content):
        """Test that embedding blob is stored correctly for a content item."""
        cid = db.insert_generated_content(**sample_content)
        embedding_blob = b'\x00\x01\x02\x03\x04\x05'

        db.set_content_embedding(cid, embedding_blob)

        # Verify blob was stored
        row = db.conn.execute(
            "SELECT content_embedding FROM generated_content WHERE id = ?",
            (cid,)
        ).fetchone()
        assert row[0] == embedding_blob

    def test_set_content_embedding_updates_existing(self, db, sample_content):
        """Test that embedding can be updated for existing content."""
        cid = db.insert_generated_content(**sample_content)
        embedding_blob_1 = b'\x01\x02\x03'
        embedding_blob_2 = b'\x04\x05\x06\x07'

        db.set_content_embedding(cid, embedding_blob_1)
        db.set_content_embedding(cid, embedding_blob_2)

        # Verify second blob overwrote the first
        row = db.conn.execute(
            "SELECT content_embedding FROM generated_content WHERE id = ?",
            (cid,)
        ).fetchone()
        assert row[0] == embedding_blob_2

    def test_set_content_embedding_for_nonexistent_id(self, db):
        """Test that setting embedding for nonexistent ID is a no-op."""
        embedding_blob = b'\x00\x01\x02'
        db.set_content_embedding(9999, embedding_blob)
        # Should not raise, just no-op


# ---------------------------------------------------------------------------
# Content lookup by tweet ID
# ---------------------------------------------------------------------------


class TestGetContentByTweetId:
    """Tests for get_content_by_tweet_id() method (~line 623 in db.py)."""

    def test_get_content_by_tweet_id_returns_published_content(self, db, sample_content):
        """Test retrieval of published content by tweet ID."""
        cid = db.insert_generated_content(**sample_content)
        db.mark_published(cid, "https://x.com/status/123", tweet_id="tweet-123")

        result = db.get_content_by_tweet_id("tweet-123")

        assert result is not None
        assert result["id"] == cid
        assert result["content"] == sample_content["content"]
        assert result["content_type"] == "x_post"

    def test_get_content_by_tweet_id_returns_none_for_unknown_id(self, db):
        """Test that unknown tweet ID returns None."""
        result = db.get_content_by_tweet_id("nonexistent-tweet-id")
        assert result is None

    def test_get_content_by_tweet_id_returns_none_for_unpublished(self, db, sample_content):
        """Test that unpublished content without tweet_id is not found."""
        db.insert_generated_content(**sample_content)
        result = db.get_content_by_tweet_id("tweet-999")
        assert result is None


# ---------------------------------------------------------------------------
# Published content in date range
# ---------------------------------------------------------------------------


class TestGetPublishedContentInRange:
    """Tests for get_published_content_in_range() method (~line 665 in db.py)."""

    def test_get_published_content_in_range_filters_by_dates(self, db, sample_content):
        """Test that content is filtered by date range."""
        # Insert content at different times
        cid1 = db.insert_generated_content(**{**sample_content, "content": "post 1"})
        cid2 = db.insert_generated_content(**{**sample_content, "content": "post 2"})
        cid3 = db.insert_generated_content(**{**sample_content, "content": "post 3"})

        # Publish at different times
        db.conn.execute(
            "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
            ("2026-04-01T10:00:00", cid1)
        )
        db.conn.execute(
            "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
            ("2026-04-05T10:00:00", cid2)
        )
        db.conn.execute(
            "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
            ("2026-04-10T10:00:00", cid3)
        )
        db.conn.commit()

        # Query for mid-range
        start = datetime(2026, 4, 3, 0, 0, 0)
        end = datetime(2026, 4, 8, 0, 0, 0)
        results = db.get_published_content_in_range("x_post", start, end)

        assert len(results) == 1
        assert results[0]["content"] == "post 2"

    def test_get_published_content_in_range_filters_by_content_type(self, db, sample_content):
        """Test that content_type filter works."""
        cid1 = db.insert_generated_content(**{**sample_content, "content_type": "x_post"})
        cid2 = db.insert_generated_content(**{**sample_content, "content_type": "x_thread"})

        ts = "2026-04-05T10:00:00"
        for cid in [cid1, cid2]:
            db.conn.execute(
                "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
                (ts, cid)
            )
        db.conn.commit()

        start = datetime(2026, 4, 1, 0, 0, 0)
        end = datetime(2026, 4, 10, 0, 0, 0)
        results = db.get_published_content_in_range("x_post", start, end)

        assert len(results) == 1
        assert results[0]["content_type"] == "x_post"

    def test_get_published_content_in_range_excludes_unpublished(self, db, sample_content):
        """Test that unpublished content is not returned."""
        cid = db.insert_generated_content(**sample_content)
        # Don't mark as published

        start = datetime(2026, 4, 1, 0, 0, 0)
        end = datetime(2026, 4, 10, 0, 0, 0)
        results = db.get_published_content_in_range("x_post", start, end)

        assert len(results) == 0

    def test_get_published_content_in_range_empty_range(self, db, sample_content):
        """Test that empty date range returns no results."""
        cid = db.insert_generated_content(**sample_content)
        db.mark_published(cid, "https://x.com/1", tweet_id="tw-1")

        # Query for a range that doesn't include the published date
        start = datetime(2026, 1, 1, 0, 0, 0)
        end = datetime(2026, 1, 2, 0, 0, 0)
        results = db.get_published_content_in_range("x_post", start, end)

        assert len(results) == 0


# ---------------------------------------------------------------------------
# Commits by repository
# ---------------------------------------------------------------------------


class TestGetCommitsByRepo:
    """Tests for get_commits_by_repo() method (~line 684 in db.py)."""

    def test_get_commits_by_repo_filters_by_repo_name(self, db):
        """Test that commits are filtered by repository name."""
        # Insert commits for different repos at appropriate ages
        db.conn.execute(
            """INSERT INTO github_commits (repo_name, commit_sha, commit_message, timestamp, author)
               VALUES ('repo-a', 'sha-1', 'commit 1', datetime('now', '-60 days'), 'author1')"""
        )
        db.conn.execute(
            """INSERT INTO github_commits (repo_name, commit_sha, commit_message, timestamp, author)
               VALUES ('repo-b', 'sha-2', 'commit 2', datetime('now', '-60 days'), 'author2')"""
        )
        db.conn.execute(
            """INSERT INTO github_commits (repo_name, commit_sha, commit_message, timestamp, author)
               VALUES ('repo-a', 'sha-3', 'commit 3', datetime('now', '-90 days'), 'author1')"""
        )
        db.conn.commit()

        results = db.get_commits_by_repo("repo-a")

        assert len(results) == 2
        assert all(row["repo_name"] == "repo-a" for row in results)

    def test_get_commits_by_repo_respects_age_filters(self, db):
        """Test that min_age_days and max_age_days filter correctly."""
        # Insert commits at different ages
        db.conn.execute(
            """INSERT INTO github_commits (repo_name, commit_sha, commit_message, timestamp, author)
               VALUES ('repo-a', 'sha-recent', 'recent', datetime('now', '-10 days'), 'author1')"""
        )
        db.conn.execute(
            """INSERT INTO github_commits (repo_name, commit_sha, commit_message, timestamp, author)
               VALUES ('repo-a', 'sha-mid', 'mid', datetime('now', '-60 days'), 'author1')"""
        )
        db.conn.execute(
            """INSERT INTO github_commits (repo_name, commit_sha, commit_message, timestamp, author)
               VALUES ('repo-a', 'sha-old', 'old', datetime('now', '-400 days'), 'author1')"""
        )
        db.conn.commit()

        # Query for commits between 30 and 365 days old
        results = db.get_commits_by_repo("repo-a", min_age_days=30, max_age_days=365)

        # Should only return the mid-age commit
        assert len(results) == 1
        assert results[0]["commit_sha"] == "sha-mid"

    def test_get_commits_by_repo_respects_limit(self, db):
        """Test that limit parameter works."""
        # Insert multiple commits for same repo
        for i in range(10):
            db.conn.execute(
                """INSERT INTO github_commits (repo_name, commit_sha, commit_message, timestamp, author)
                   VALUES ('repo-a', ?, 'commit', datetime('now', '-60 days'), 'author1')""",
                (f"sha-{i}",)
            )
        db.conn.commit()

        results = db.get_commits_by_repo("repo-a", limit=5)

        assert len(results) == 5

    def test_get_commits_by_repo_empty_for_unknown_repo(self, db):
        """Test that unknown repo returns empty list."""
        results = db.get_commits_by_repo("nonexistent-repo")
        assert len(results) == 0


# ---------------------------------------------------------------------------
# Pipeline run counting
# ---------------------------------------------------------------------------


class TestCountPipelineRuns:
    """Tests for count_pipeline_runs() method (~line 703 in db.py)."""

    def test_count_pipeline_runs_counts_within_window(self, db):
        """Test that pipeline runs within the time window are counted."""
        # Insert recent pipeline runs
        db.conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, candidates_generated, best_candidate_index,
                best_score_before_refine, created_at)
               VALUES ('batch-1', 'x_post', 3, 0, 7.0, datetime('now', '-10 days'))"""
        )
        db.conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, candidates_generated, best_candidate_index,
                best_score_before_refine, created_at)
               VALUES ('batch-2', 'x_post', 3, 1, 8.0, datetime('now', '-20 days'))"""
        )
        db.conn.commit()

        count = db.count_pipeline_runs("x_post", since_days=30)

        assert count == 2

    def test_count_pipeline_runs_excludes_old_runs(self, db):
        """Test that runs outside the time window are excluded."""
        # Insert old pipeline run
        db.conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, candidates_generated, best_candidate_index,
                best_score_before_refine, created_at)
               VALUES ('batch-old', 'x_post', 3, 0, 7.0, datetime('now', '-60 days'))"""
        )
        # Insert recent pipeline run
        db.conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, candidates_generated, best_candidate_index,
                best_score_before_refine, created_at)
               VALUES ('batch-recent', 'x_post', 3, 1, 8.0, datetime('now', '-10 days'))"""
        )
        db.conn.commit()

        count = db.count_pipeline_runs("x_post", since_days=30)

        assert count == 1

    def test_count_pipeline_runs_filters_by_content_type(self, db):
        """Test that content_type filter works."""
        # Insert runs for different content types
        db.conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, candidates_generated, best_candidate_index,
                best_score_before_refine, created_at)
               VALUES ('batch-1', 'x_post', 3, 0, 7.0, datetime('now', '-10 days'))"""
        )
        db.conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, candidates_generated, best_candidate_index,
                best_score_before_refine, created_at)
               VALUES ('batch-2', 'x_thread', 3, 1, 8.0, datetime('now', '-10 days'))"""
        )
        db.conn.commit()

        count = db.count_pipeline_runs("x_post", since_days=30)

        assert count == 1

    def test_count_pipeline_runs_returns_zero_for_no_matches(self, db):
        """Test that count returns 0 when no runs match."""
        count = db.count_pipeline_runs("x_post", since_days=30)
        assert count == 0


# ---------------------------------------------------------------------------
# Reply methods: count_replies_today, get_last_mention_id, set_last_mention_id
# ---------------------------------------------------------------------------


class TestReplyMethods:
    """Tests for reply-related methods in Database."""

    def test_count_replies_today_returns_zero_when_no_replies(self, db):
        """count_replies_today should return 0 when no replies exist."""
        assert db.count_replies_today() == 0

    def test_count_replies_today_counts_posted_replies(self, db):
        """count_replies_today should count replies with status='posted' from today."""
        # Insert a posted reply today
        db.conn.execute(
            """INSERT INTO reply_queue (
                inbound_tweet_id, inbound_text, our_tweet_id, draft_text,
                status, posted_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now'))""",
            ("tw-in-1", "Nice post!", "tw-our-1", "Thanks!", "posted")
        )
        db.conn.commit()

        assert db.count_replies_today() == 1

    def test_count_replies_today_excludes_pending_replies(self, db):
        """count_replies_today should not count pending replies."""
        # Insert a pending reply
        db.conn.execute(
            """INSERT INTO reply_queue (
                inbound_tweet_id, inbound_text, our_tweet_id, draft_text,
                status
            ) VALUES (?, ?, ?, ?, ?)""",
            ("tw-in-2", "Interesting!", "tw-our-2", "Glad you liked it!", "pending")
        )
        db.conn.commit()

        assert db.count_replies_today() == 0

    def test_count_replies_today_excludes_old_replies(self, db):
        """count_replies_today should not count replies posted yesterday."""
        # Insert a reply posted yesterday
        db.conn.execute(
            """INSERT INTO reply_queue (
                inbound_tweet_id, inbound_text, our_tweet_id, draft_text,
                status, posted_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now', '-1 day'))""",
            ("tw-in-3", "Great!", "tw-our-3", "Thank you!", "posted")
        )
        db.conn.commit()

        assert db.count_replies_today() == 0

    def test_count_replies_today_multiple_posted_today(self, db):
        """count_replies_today should count multiple posted replies from today."""
        # Insert 3 posted replies today
        for i in range(3):
            db.conn.execute(
                """INSERT INTO reply_queue (
                    inbound_tweet_id, inbound_text, our_tweet_id, draft_text,
                    status, posted_at
                ) VALUES (?, ?, ?, ?, ?, datetime('now'))""",
                (f"tw-in-{i}", f"Comment {i}", f"tw-our-{i}", f"Reply {i}", "posted")
            )
        db.conn.commit()

        assert db.count_replies_today() == 3

    def test_get_last_mention_id_returns_none_initially(self, db):
        """get_last_mention_id should return None when no mention ID is stored."""
        assert db.get_last_mention_id() is None

    def test_set_and_get_last_mention_id_roundtrip(self, db):
        """set_last_mention_id followed by get_last_mention_id should retrieve the same ID."""
        mention_id = "mention-12345"
        db.set_last_mention_id(mention_id)

        result = db.get_last_mention_id()
        assert result == mention_id

    def test_set_last_mention_id_updates_existing(self, db):
        """set_last_mention_id should update the existing record (upsert behavior)."""
        db.set_last_mention_id("mention-old")
        db.set_last_mention_id("mention-new")

        result = db.get_last_mention_id()
        assert result == "mention-new"

        # Verify only one row exists in reply_state
        count = db.conn.execute("SELECT COUNT(*) FROM reply_state").fetchone()[0]
        assert count == 1


# ---------------------------------------------------------------------------
# Newsletter methods: insert_newsletter_send, get_last_newsletter_send
# ---------------------------------------------------------------------------


class TestNewsletterMethods:
    """Tests for newsletter-related methods in Database."""

    def test_insert_newsletter_send_returns_id(self, db):
        """insert_newsletter_send should return a valid row ID."""
        send_id = db.insert_newsletter_send(
            issue_id="issue-001",
            subject="Weekly Update",
            content_ids=[1, 2, 3],
            subscriber_count=100,
            status="sent"
        )
        assert isinstance(send_id, int)
        assert send_id > 0

    def test_insert_newsletter_send_stores_data(self, db):
        """insert_newsletter_send should store all provided data correctly."""
        send_id = db.insert_newsletter_send(
            issue_id="issue-002",
            subject="Monthly Digest",
            content_ids=[10, 20],
            subscriber_count=250,
            status="sent"
        )

        # Verify the data was stored
        row = db.conn.execute(
            "SELECT issue_id, subject, source_content_ids, subscriber_count, status FROM newsletter_sends WHERE id = ?",
            (send_id,)
        ).fetchone()

        assert row["issue_id"] == "issue-002"
        assert row["subject"] == "Monthly Digest"
        assert json.loads(row["source_content_ids"]) == [10, 20]
        assert row["subscriber_count"] == 250
        assert row["status"] == "sent"

    def test_insert_newsletter_send_stores_metadata(self, db):
        """insert_newsletter_send should persist optional metadata JSON."""
        send_id = db.insert_newsletter_send(
            issue_id="issue-meta",
            subject="Attributed Digest",
            content_ids=[10],
            subscriber_count=25,
            metadata={"utm_campaign": "weekly-20260423"},
        )

        row = db.conn.execute(
            "SELECT metadata FROM newsletter_sends WHERE id = ?",
            (send_id,),
        ).fetchone()

        assert json.loads(row["metadata"]) == {"utm_campaign": "weekly-20260423"}

    def test_get_last_newsletter_send_returns_none_when_empty(self, db):
        """get_last_newsletter_send should return None when no newsletters exist."""
        assert db.get_last_newsletter_send() is None

    def test_insert_and_get_last_newsletter_send_roundtrip(self, db):
        """insert_newsletter_send followed by get_last_newsletter_send should return a timestamp."""
        db.insert_newsletter_send(
            issue_id="issue-003",
            subject="Test Newsletter",
            content_ids=[5],
            subscriber_count=50
        )

        result = db.get_last_newsletter_send()
        assert result is not None
        assert isinstance(result, datetime)
        assert result.tzinfo is not None  # Should have timezone info

    def test_get_last_newsletter_send_returns_most_recent(self, db):
        """get_last_newsletter_send should return the most recent send timestamp."""
        # Insert older newsletter
        db.conn.execute(
            """INSERT INTO newsletter_sends (issue_id, subject, source_content_ids, subscriber_count, sent_at)
               VALUES (?, ?, ?, ?, datetime('now', '-2 days'))""",
            ("issue-old", "Old Newsletter", "[]", 10)
        )

        # Insert newer newsletter
        db.insert_newsletter_send(
            issue_id="issue-new",
            subject="New Newsletter",
            content_ids=[1],
            subscriber_count=20
        )

        result = db.get_last_newsletter_send()
        assert result is not None

        # The most recent send should be from today (much more recent than 2 days ago)
        # We can verify it's within the last minute
        now = datetime.now(timezone.utc)
        time_diff = (now - result).total_seconds()
        assert time_diff < 60  # Should be less than 60 seconds old

    def test_insert_newsletter_engagement_classifies_send(self, db):
        """Stored Buttondown metrics update newsletter_sends.status."""
        send_id = db.insert_newsletter_send(
            issue_id="issue-high",
            subject="High Engagement",
            content_ids=[1],
            subscriber_count=100,
        )

        db.insert_newsletter_engagement(
            newsletter_send_id=send_id,
            issue_id="issue-high",
            opens=42,
            clicks=1,
            unsubscribes=0,
        )

        status = db.conn.execute(
            "SELECT status FROM newsletter_sends WHERE id = ?",
            (send_id,),
        ).fetchone()["status"]
        assert status == "resonated"

    def test_insert_and_list_newsletter_subscriber_metrics(self, db):
        """Newsletter subscriber snapshots are persisted newest-first."""
        first_id = db.insert_newsletter_subscriber_metrics(
            subscriber_count=100,
            active_subscriber_count=95,
            unsubscribes=5,
            churn_rate=0.05,
            new_subscribers=8,
            net_subscriber_change=3,
            raw_metrics={"count": 100},
        )
        second_id = db.insert_newsletter_subscriber_metrics(
            subscriber_count=102,
            active_subscriber_count=97,
            unsubscribes=5,
            raw_metrics={"count": 102},
        )

        rows = db.list_newsletter_subscriber_metrics(limit=10)

        assert [row["id"] for row in rows] == [second_id, first_id]
        assert rows[0]["subscriber_count"] == 102
        assert rows[0]["active_subscriber_count"] == 97
        assert rows[0]["unsubscribes"] == 5
        assert rows[0]["raw_metrics"] == {"count": 102}
        assert rows[1]["churn_rate"] == 0.05

    def test_insert_and_list_newsletter_subject_candidates(self, db):
        """Newsletter subject candidates are persisted with scores and selection."""
        now = datetime(2026, 4, 23, tzinfo=timezone.utc)
        ids = db.insert_newsletter_subject_candidates(
            [
                {
                    "subject": "Specific AI notes",
                    "score": 8.5,
                    "rationale": "issue-specific",
                    "source": "heuristic",
                    "metadata": {"source": "heuristic"},
                },
                {
                    "subject": "Weekly Digest",
                    "score": 6.0,
                    "rationale": "baseline",
                },
            ],
            content_ids=[10, 20],
            week_start=now - timedelta(days=7),
            week_end=now,
            selected_subject="Specific AI notes",
        )

        rows = db.list_newsletter_subject_candidates(limit=10)

        assert len(ids) == 2
        assert rows[0]["subject"] == "Weekly Digest"
        assert rows[1]["subject"] == "Specific AI notes"
        assert rows[1]["selected"] is True
        assert rows[1]["source"] == "heuristic"
        assert rows[1]["source_content_ids"] == [10, 20]
        assert rows[1]["metadata"] == {"source": "heuristic"}

    def test_resonant_newsletter_source_patterns_uses_source_content(self, db):
        """Source content from resonant sends becomes future assembly preference data."""
        tip_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Tip post",
            eval_score=8.0,
            eval_feedback="Good",
            content_format="tip",
        )
        thread_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=[],
            source_messages=[],
            content="Thread",
            eval_score=8.0,
            eval_feedback="Good",
            content_format="contrarian_thread",
        )
        db.insert_newsletter_send(
            issue_id="issue-resonated",
            subject="Resonated",
            content_ids=[tip_id, thread_id],
            subscriber_count=100,
            status="resonated",
        )

        patterns = db.get_resonant_newsletter_source_patterns()

        assert {
            (pattern["content_type"], pattern["content_format"])
            for pattern in patterns
        } == {("x_post", "tip"), ("x_thread", "contrarian_thread")}
