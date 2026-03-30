"""SQLite storage layer for Presence."""

import sqlite3
import json
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone

MAX_RETRIES = 3


class Database:
    def __init__(self, db_path: str = "./presence.db"):
        self.db_path = Path(db_path).expanduser()
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def init_schema(self, schema_path: str = "./schema.sql") -> None:
        """Initialize database with schema."""
        schema = Path(schema_path).read_text()
        self.conn.executescript(schema)
        # Migrate: add columns if missing (existing DBs)
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(generated_content)")}
        if "retry_count" not in cols:
            self.conn.execute("ALTER TABLE generated_content ADD COLUMN retry_count INTEGER DEFAULT 0")
        if "last_retry_at" not in cols:
            self.conn.execute("ALTER TABLE generated_content ADD COLUMN last_retry_at TEXT")
        if "tweet_id" not in cols:
            self.conn.execute("ALTER TABLE generated_content ADD COLUMN tweet_id TEXT")
        if "published_at" not in cols:
            self.conn.execute("ALTER TABLE generated_content ADD COLUMN published_at TEXT")
        if "curation_quality" not in cols:
            self.conn.execute("ALTER TABLE generated_content ADD COLUMN curation_quality TEXT")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_generated_content_curation ON generated_content(curation_quality)")
        self.conn.commit()

    # Claude messages
    def is_message_processed(self, message_uuid: str) -> bool:
        cursor = self.conn.execute(
            "SELECT 1 FROM claude_messages WHERE message_uuid = ?",
            (message_uuid,)
        )
        return cursor.fetchone() is not None

    def insert_claude_message(
        self,
        session_id: str,
        message_uuid: str,
        project_path: str,
        timestamp: str,
        prompt_text: str
    ) -> int:
        cursor = self.conn.execute(
            """INSERT INTO claude_messages
               (session_id, message_uuid, project_path, timestamp, prompt_text)
               VALUES (?, ?, ?, ?, ?)""",
            (session_id, message_uuid, project_path, timestamp, prompt_text)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_messages_in_range(self, start: datetime, end: datetime) -> list[dict]:
        cursor = self.conn.execute(
            """SELECT * FROM claude_messages
               WHERE timestamp >= ? AND timestamp < ?
               ORDER BY timestamp""",
            (start.isoformat(), end.isoformat())
        )
        return [dict(row) for row in cursor.fetchall()]

    # GitHub commits
    def is_commit_processed(self, commit_sha: str) -> bool:
        cursor = self.conn.execute(
            "SELECT 1 FROM github_commits WHERE commit_sha = ?",
            (commit_sha,)
        )
        return cursor.fetchone() is not None

    def insert_commit(
        self,
        repo_name: str,
        commit_sha: str,
        commit_message: str,
        timestamp: str,
        author: str
    ) -> int:
        cursor = self.conn.execute(
            """INSERT INTO github_commits
               (repo_name, commit_sha, commit_message, timestamp, author)
               VALUES (?, ?, ?, ?, ?)""",
            (repo_name, commit_sha, commit_message, timestamp, author)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_commits_in_range(self, start: datetime, end: datetime) -> list[dict]:
        cursor = self.conn.execute(
            """SELECT * FROM github_commits
               WHERE timestamp >= ? AND timestamp < ?
               ORDER BY timestamp""",
            (start.isoformat(), end.isoformat())
        )
        return [dict(row) for row in cursor.fetchall()]

    # Generated content
    def insert_generated_content(
        self,
        content_type: str,
        source_commits: list[str],
        source_messages: list[str],
        content: str,
        eval_score: float,
        eval_feedback: str
    ) -> int:
        cursor = self.conn.execute(
            """INSERT INTO generated_content
               (content_type, source_commits, source_messages, content, eval_score, eval_feedback)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                content_type,
                json.dumps(source_commits),
                json.dumps(source_messages),
                content,
                eval_score,
                eval_feedback
            )
        )
        self.conn.commit()
        return cursor.lastrowid

    def mark_published(self, content_id: int, url: str, tweet_id: str = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE generated_content
               SET published = 1, published_url = ?, tweet_id = ?, published_at = ?
               WHERE id = ?""",
            (url, tweet_id, now, content_id)
        )
        self.conn.commit()

    def get_unpublished_content(self, content_type: str, min_score: float) -> list[dict]:
        cursor = self.conn.execute(
            """SELECT * FROM generated_content
               WHERE content_type = ? AND published = 0
               AND eval_score >= ? AND COALESCE(retry_count, 0) < ?
               ORDER BY created_at""",
            (content_type, min_score, MAX_RETRIES)
        )
        return [dict(row) for row in cursor.fetchall()]

    def increment_retry(self, content_id: int) -> int:
        """Increment retry count and return new count. Abandons if max exceeded."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE generated_content
               SET retry_count = COALESCE(retry_count, 0) + 1, last_retry_at = ?
               WHERE id = ?""",
            (now, content_id)
        )
        self.conn.commit()
        row = self.conn.execute(
            "SELECT retry_count FROM generated_content WHERE id = ?", (content_id,)
        ).fetchone()
        count = row[0] if row else 0
        if count >= MAX_RETRIES:
            self.mark_abandoned(content_id)
        return count

    def mark_abandoned(self, content_id: int) -> None:
        """Mark content as abandoned (published = -1)."""
        self.conn.execute(
            "UPDATE generated_content SET published = -1 WHERE id = ?",
            (content_id,)
        )
        self.conn.commit()

    def get_last_published_time(self, content_type: str = "x_post") -> Optional[datetime]:
        """Get the most recent published_at timestamp for a content type."""
        cursor = self.conn.execute(
            "SELECT published_at FROM generated_content "
            "WHERE content_type = ? AND published = 1 AND published_at IS NOT NULL "
            "ORDER BY published_at DESC LIMIT 1",
            (content_type,)
        )
        row = cursor.fetchone()
        if row and row[0]:
            dt = datetime.fromisoformat(row[0])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        return None

    # Poll state
    def get_last_poll_time(self) -> Optional[datetime]:
        """Get the last successful poll time."""
        cursor = self.conn.execute(
            "SELECT last_poll_time FROM poll_state WHERE id = 1"
        )
        row = cursor.fetchone()
        if row:
            return datetime.fromisoformat(row[0])
        return None

    def set_last_poll_time(self, poll_time: datetime) -> None:
        """Update the last poll time."""
        self.conn.execute(
            """INSERT INTO poll_state (id, last_poll_time, updated_at)
               VALUES (1, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(id) DO UPDATE SET
               last_poll_time = excluded.last_poll_time,
               updated_at = CURRENT_TIMESTAMP""",
            (poll_time.isoformat(),)
        )
        self.conn.commit()

    # Engagement tracking
    def get_posts_needing_metrics(self, max_age_days: int = 30) -> list[dict]:
        """Get published posts with tweet_ids that need engagement metrics fetched."""
        cursor = self.conn.execute(
            """SELECT gc.id, gc.tweet_id, gc.content, gc.published_at,
                      pe.fetched_at AS last_fetched
               FROM generated_content gc
               LEFT JOIN (
                   SELECT content_id, MAX(fetched_at) AS fetched_at
                   FROM post_engagement
                   GROUP BY content_id
               ) pe ON pe.content_id = gc.id
               WHERE gc.published = 1
                 AND gc.tweet_id IS NOT NULL
                 AND gc.published_at >= datetime('now', ?)
                 AND (pe.fetched_at IS NULL
                      OR pe.fetched_at < datetime('now', '-6 hours'))
               ORDER BY gc.published_at DESC""",
            (f'-{max_age_days} days',)
        )
        return [dict(row) for row in cursor.fetchall()]

    def insert_engagement(
        self,
        content_id: int,
        tweet_id: str,
        like_count: int,
        retweet_count: int,
        reply_count: int,
        quote_count: int,
        engagement_score: float
    ) -> int:
        """Insert an engagement metrics snapshot."""
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.conn.execute(
            """INSERT INTO post_engagement
               (content_id, tweet_id, like_count, retweet_count,
                reply_count, quote_count, engagement_score, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (content_id, tweet_id, like_count, retweet_count,
             reply_count, quote_count, engagement_score, now)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_top_performing_posts(
        self,
        limit: int = 5,
        content_type: str = "x_post"
    ) -> list[dict]:
        """Get top-performing published posts ranked by latest engagement score."""
        cursor = self.conn.execute(
            """SELECT gc.id, gc.content, gc.eval_score, gc.tweet_id,
                      pe.engagement_score, pe.like_count, pe.retweet_count,
                      pe.reply_count, pe.quote_count
               FROM generated_content gc
               INNER JOIN (
                   SELECT content_id, engagement_score, like_count,
                          retweet_count, reply_count, quote_count,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC
                          ) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               WHERE gc.published = 1 AND gc.content_type = ?
                 AND COALESCE(gc.curation_quality, '') != 'too_specific'
               ORDER BY pe.engagement_score DESC
               LIMIT ?""",
            (content_type, limit)
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_recent_published_content(
        self,
        content_type: str = "x_post",
        limit: int = 10,
    ) -> list[dict]:
        """Get most recently published posts by timestamp."""
        cursor = self.conn.execute(
            """SELECT id, content, published_at
               FROM generated_content
               WHERE content_type = ? AND published = 1
               ORDER BY published_at DESC
               LIMIT ?""",
            (content_type, limit)
        )
        return [dict(row) for row in cursor.fetchall()]

    # Curation
    def set_curation_quality(self, content_id: int, quality: str) -> None:
        """Flag a post's curation quality ('good', 'too_specific', or None to clear)."""
        self.conn.execute(
            "UPDATE generated_content SET curation_quality = ? WHERE id = ?",
            (quality, content_id)
        )
        self.conn.commit()

    def get_curated_posts(
        self,
        quality: str,
        content_type: str = "x_post",
        limit: int = 5,
    ) -> list[dict]:
        """Get posts flagged with a specific curation quality."""
        cursor = self.conn.execute(
            """SELECT id, content, eval_score, curation_quality
               FROM generated_content
               WHERE curation_quality = ? AND content_type = ?
               ORDER BY created_at DESC
               LIMIT ?""",
            (quality, content_type, limit)
        )
        return [dict(row) for row in cursor.fetchall()]

    # Pipeline runs
    def insert_pipeline_run(
        self,
        batch_id: str,
        content_type: str,
        candidates_generated: int,
        best_candidate_index: int,
        best_score_before_refine: float,
        best_score_after_refine: float = None,
        refinement_picked: str = None,
        final_score: float = None,
        published: bool = False,
        content_id: int = None,
    ) -> int:
        """Record a pipeline run for observability."""
        cursor = self.conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, candidates_generated,
                best_candidate_index, best_score_before_refine,
                best_score_after_refine, refinement_picked,
                final_score, published, content_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (batch_id, content_type, candidates_generated,
             best_candidate_index, best_score_before_refine,
             best_score_after_refine, refinement_picked,
             final_score, 1 if published else 0, content_id)
        )
        self.conn.commit()
        return cursor.lastrowid
