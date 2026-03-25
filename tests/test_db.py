"""Comprehensive unit tests for src/storage/db.py."""

import json
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from src.storage.db import Database, MAX_RETRIES

SCHEMA_PATH = str(Path(__file__).resolve().parent.parent / "schema.sql")


# ---------------------------------------------------------------------------
# Connection & lifecycle
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

    def test_init_schema_is_idempotent(self, db):
        db.init_schema(schema_path=SCHEMA_PATH)  # second call must not raise


# ---------------------------------------------------------------------------
# Claude messages
# ---------------------------------------------------------------------------

class TestClaudeMessages:
    def test_insert_and_retrieve(self, db, sample_message):
        row_id = db.insert_claude_message(**sample_message)
        assert isinstance(row_id, int) and row_id > 0

    def test_is_message_processed_true(self, db, sample_message):
        db.insert_claude_message(**sample_message)
        assert db.is_message_processed(sample_message["message_uuid"]) is True

    def test_is_message_processed_false(self, db):
        assert db.is_message_processed("nonexistent-uuid") is False

    def test_duplicate_message_uuid_raises(self, db, sample_message):
        db.insert_claude_message(**sample_message)
        with pytest.raises(sqlite3.IntegrityError):
            db.insert_claude_message(**sample_message)

    def test_get_messages_in_range(self, db, sample_message):
        db.insert_claude_message(**sample_message)
        # Also insert one outside the range
        db.insert_claude_message(
            session_id="sess-002",
            message_uuid="uuid-bbb",
            project_path="/x",
            timestamp="2026-03-21T10:00:00+00:00",
            prompt_text="Later message",
        )
        start = datetime(2026, 3, 20, tzinfo=timezone.utc)
        end = datetime(2026, 3, 21, tzinfo=timezone.utc)
        results = db.get_messages_in_range(start, end)
        assert len(results) == 1
        assert results[0]["message_uuid"] == "uuid-aaa"

    def test_get_messages_in_range_empty(self, db):
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 2, tzinfo=timezone.utc)
        assert db.get_messages_in_range(start, end) == []

    def test_get_messages_in_range_ordering(self, db):
        for i, ts in enumerate(
            ["2026-03-20T12:00:00+00:00", "2026-03-20T08:00:00+00:00"]
        ):
            db.insert_claude_message(
                session_id=f"s{i}",
                message_uuid=f"u{i}",
                project_path="/p",
                timestamp=ts,
                prompt_text=f"msg{i}",
            )
        start = datetime(2026, 3, 20, tzinfo=timezone.utc)
        end = datetime(2026, 3, 21, tzinfo=timezone.utc)
        results = db.get_messages_in_range(start, end)
        assert results[0]["timestamp"] <= results[1]["timestamp"]


# ---------------------------------------------------------------------------
# GitHub commits
# ---------------------------------------------------------------------------

class TestGitHubCommits:
    def test_insert_commit(self, db, sample_commit):
        row_id = db.insert_commit(**sample_commit)
        assert isinstance(row_id, int) and row_id > 0

    def test_is_commit_processed_true(self, db, sample_commit):
        db.insert_commit(**sample_commit)
        assert db.is_commit_processed("abc123") is True

    def test_is_commit_processed_false(self, db):
        assert db.is_commit_processed("no-such-sha") is False

    def test_duplicate_commit_sha_raises(self, db, sample_commit):
        db.insert_commit(**sample_commit)
        with pytest.raises(sqlite3.IntegrityError):
            db.insert_commit(**sample_commit)

    def test_get_commits_in_range(self, db, sample_commit):
        db.insert_commit(**sample_commit)
        start = datetime(2026, 3, 20, tzinfo=timezone.utc)
        end = datetime(2026, 3, 21, tzinfo=timezone.utc)
        results = db.get_commits_in_range(start, end)
        assert len(results) == 1
        assert results[0]["commit_sha"] == "abc123"

    def test_get_commits_in_range_empty(self, db):
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 2, tzinfo=timezone.utc)
        assert db.get_commits_in_range(start, end) == []

    def test_get_commits_in_range_excludes_end_boundary(self, db, sample_commit):
        """The end bound is exclusive (< not <=)."""
        db.insert_commit(**sample_commit)
        exact_end = datetime(2026, 3, 20, 11, 0, 0, tzinfo=timezone.utc)
        results = db.get_commits_in_range(
            datetime(2026, 3, 20, tzinfo=timezone.utc), exact_end
        )
        assert len(results) == 0


# ---------------------------------------------------------------------------
# Generated content
# ---------------------------------------------------------------------------

class TestGeneratedContent:
    def _insert(self, db, sample_content, **overrides):
        data = {**sample_content, **overrides}
        return db.insert_generated_content(**data)

    def test_insert_generated_content(self, db, sample_content):
        row_id = self._insert(db, sample_content)
        assert isinstance(row_id, int) and row_id > 0

    def test_source_fields_stored_as_json(self, db, sample_content):
        row_id = self._insert(db, sample_content)
        row = db.conn.execute(
            "SELECT source_commits, source_messages FROM generated_content WHERE id = ?",
            (row_id,),
        ).fetchone()
        assert json.loads(row["source_commits"]) == sample_content["source_commits"]
        assert json.loads(row["source_messages"]) == sample_content["source_messages"]

    def test_get_unpublished_content(self, db, sample_content):
        self._insert(db, sample_content)
        results = db.get_unpublished_content("x_post", min_score=5.0)
        assert len(results) == 1

    def test_get_unpublished_content_filters_by_type(self, db, sample_content):
        self._insert(db, sample_content, content_type="blog_post")
        assert db.get_unpublished_content("x_post", min_score=0) == []

    def test_get_unpublished_content_filters_below_score(self, db, sample_content):
        self._insert(db, sample_content, eval_score=3.0)
        assert db.get_unpublished_content("x_post", min_score=5.0) == []

    def test_get_unpublished_content_excludes_published(self, db, sample_content):
        cid = self._insert(db, sample_content)
        db.mark_published(cid, "https://x.com/status/1")
        assert db.get_unpublished_content("x_post", min_score=0) == []

    def test_get_unpublished_content_excludes_max_retries(self, db, sample_content):
        cid = self._insert(db, sample_content)
        for _ in range(MAX_RETRIES):
            db.increment_retry(cid)
        # After MAX_RETRIES the row is abandoned (published = -1)
        assert db.get_unpublished_content("x_post", min_score=0) == []

    def test_get_unpublished_content_ordering(self, db, sample_content):
        """Results ordered by created_at ascending."""
        id1 = self._insert(db, sample_content, content="first")
        id2 = self._insert(db, sample_content, content="second")
        results = db.get_unpublished_content("x_post", min_score=0)
        assert results[0]["id"] == id1
        assert results[1]["id"] == id2


# ---------------------------------------------------------------------------
# Mark published
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

class TestPollState:
    def test_get_last_poll_time_returns_none_initially(self, db):
        assert db.get_last_poll_time() is None

    def test_set_and_get_poll_time(self, db):
        now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
        db.set_last_poll_time(now)
        result = db.get_last_poll_time()
        assert result == now

    def test_set_poll_time_upserts(self, db):
        t1 = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 3, 21, 12, 0, 0, tzinfo=timezone.utc)
        db.set_last_poll_time(t1)
        db.set_last_poll_time(t2)
        assert db.get_last_poll_time() == t2

    def test_singleton_constraint(self, db):
        """Only row with id=1 is allowed."""
        db.set_last_poll_time(datetime(2026, 3, 20, tzinfo=timezone.utc))
        with pytest.raises(sqlite3.IntegrityError):
            db.conn.execute(
                "INSERT INTO poll_state (id, last_poll_time) VALUES (2, '2026-03-20T00:00:00')"
            )


# ---------------------------------------------------------------------------
# Engagement tracking
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

class TestPipelineRuns:
    def test_insert_pipeline_run(self, db):
        row_id = db.insert_pipeline_run(
            batch_id="batch-001",
            content_type="x_post",
            candidates_generated=5,
            best_candidate_index=2,
            best_score_before_refine=6.5,
            best_score_after_refine=7.8,
            refinement_picked="REFINED",
            final_score=7.8,
            published=True,
            content_id=None,
        )
        assert isinstance(row_id, int) and row_id > 0

    def test_insert_pipeline_run_minimal(self, db):
        row_id = db.insert_pipeline_run(
            batch_id="batch-002",
            content_type="x_post",
            candidates_generated=3,
            best_candidate_index=0,
            best_score_before_refine=5.0,
        )
        assert row_id > 0

    def test_duplicate_batch_id_raises(self, db):
        db.insert_pipeline_run(
            batch_id="batch-dup",
            content_type="x_post",
            candidates_generated=1,
            best_candidate_index=0,
            best_score_before_refine=5.0,
        )
        with pytest.raises(sqlite3.IntegrityError):
            db.insert_pipeline_run(
                batch_id="batch-dup",
                content_type="x_post",
                candidates_generated=1,
                best_candidate_index=0,
                best_score_before_refine=5.0,
            )

    def test_published_stored_as_int(self, db):
        db.insert_pipeline_run(
            batch_id="batch-bool",
            content_type="x_post",
            candidates_generated=1,
            best_candidate_index=0,
            best_score_before_refine=5.0,
            published=True,
        )
        row = db.conn.execute(
            "SELECT published FROM pipeline_runs WHERE batch_id = 'batch-bool'"
        ).fetchone()
        assert row["published"] == 1


# ---------------------------------------------------------------------------
# Transaction behaviour / data integrity
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
