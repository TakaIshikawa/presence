"""SQLite storage layer for Presence."""

import sqlite3
import json
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta, timezone

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
        if "auto_quality" not in cols:
            self.conn.execute("ALTER TABLE generated_content ADD COLUMN auto_quality TEXT")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_generated_content_auto_quality ON generated_content(auto_quality)")
        # Migrate reply_queue for cultivate enrichment
        rq_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(reply_queue)")}
        if rq_cols and "relationship_context" not in rq_cols:
            self.conn.execute("ALTER TABLE reply_queue ADD COLUMN relationship_context TEXT")
        if rq_cols and "quality_score" not in rq_cols:
            self.conn.execute("ALTER TABLE reply_queue ADD COLUMN quality_score REAL")
        if rq_cols and "quality_flags" not in rq_cols:
            self.conn.execute("ALTER TABLE reply_queue ADD COLUMN quality_flags TEXT")
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

    # Commit-prompt correlation
    def link_commit_to_prompts(
        self,
        commit_id: int,
        commit_timestamp: datetime,
        window_minutes: int = 30,
        min_confidence: float = 0.5,
    ) -> list[int]:
        """Find claude_messages within ±window_minutes of the commit and insert links.

        Confidence = 1.0 - (time_delta_minutes / window_minutes), clamped to [0, 1].
        Only links with confidence >= min_confidence are inserted.

        Returns list of inserted link IDs.
        """
        window_start = commit_timestamp - timedelta(minutes=window_minutes)
        window_end = commit_timestamp + timedelta(minutes=window_minutes)

        cursor = self.conn.execute(
            """SELECT id, timestamp FROM claude_messages
               WHERE timestamp >= ? AND timestamp <= ?""",
            (window_start.isoformat(), window_end.isoformat())
        )

        link_ids = []
        for row in cursor.fetchall():
            msg_ts = datetime.fromisoformat(row["timestamp"])
            delta_minutes = abs((commit_timestamp - msg_ts).total_seconds()) / 60
            confidence = 1.0 - (delta_minutes / window_minutes)
            confidence = max(0.0, min(1.0, confidence))

            if confidence < min_confidence:
                continue

            result = self.conn.execute(
                """INSERT INTO commit_prompt_links (commit_id, message_id, confidence)
                   VALUES (?, ?, ?)""",
                (commit_id, row["id"], round(confidence, 4))
            )
            link_ids.append(result.lastrowid)

        if link_ids:
            self.conn.commit()
        return link_ids

    def get_prompts_for_commit(self, commit_sha: str) -> list[dict]:
        """Return linked prompts for a commit, ordered by confidence descending."""
        cursor = self.conn.execute(
            """SELECT cm.id, cm.session_id, cm.message_uuid, cm.project_path,
                      cm.timestamp, cm.prompt_text, cpl.confidence
               FROM commit_prompt_links cpl
               JOIN claude_messages cm ON cm.id = cpl.message_id
               JOIN github_commits gc ON gc.id = cpl.commit_id
               WHERE gc.commit_sha = ?
               ORDER BY cpl.confidence DESC""",
            (commit_sha,)
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
                 AND COALESCE(gc.auto_quality, '') != 'low_resonance'
               ORDER BY pe.engagement_score DESC
               LIMIT ?""",
            (content_type, limit)
        )
        return [dict(row) for row in cursor.fetchall()]

    def count_posts_today(self, content_type: str = "x_post") -> int:
        """Count posts published today (UTC)."""
        cursor = self.conn.execute(
            """SELECT COUNT(*) FROM generated_content
               WHERE content_type = ? AND published = 1
                 AND published_at >= datetime('now', 'start of day')""",
            (content_type,)
        )
        return cursor.fetchone()[0]

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

    # Auto-classification
    def auto_classify_posts(
        self, min_age_hours: int = 48, min_engagement: float = 5.0
    ) -> dict:
        """Auto-classify published posts based on engagement after settling period.

        Posts >= min_age_hours old with auto_quality IS NULL get classified:
        - 'resonated' if latest engagement_score >= min_engagement
        - 'low_resonance' if latest engagement_score == 0
        - Left as NULL if 0 < engagement_score < min_engagement (ambiguous)
        """
        cursor = self.conn.execute(
            """SELECT gc.id, gc.content,
                      COALESCE(pe.engagement_score, 0) AS latest_score
               FROM generated_content gc
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC
                          ) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               WHERE gc.published = 1
                 AND gc.auto_quality IS NULL
                 AND gc.published_at IS NOT NULL
                 AND gc.published_at <= datetime('now', ?)""",
            (f'-{min_age_hours} hours',)
        )

        results = {"resonated": 0, "low_resonance": 0, "ambiguous": 0}
        for row in cursor.fetchall():
            score = row[2]
            if score >= min_engagement:
                quality = "resonated"
            elif score == 0:
                quality = "low_resonance"
            else:
                results["ambiguous"] += 1
                continue  # Leave as NULL — don't use for calibration
            self.conn.execute(
                "UPDATE generated_content SET auto_quality = ? WHERE id = ?",
                (quality, row[0])
            )
            results[quality] += 1

        if results["resonated"] or results["low_resonance"]:
            self.conn.commit()
        return results

    def get_auto_classified_posts(
        self,
        quality: str,
        content_type: str = "x_post",
        limit: int = 3,
    ) -> list[dict]:
        """Get posts with a specific auto_quality classification."""
        cursor = self.conn.execute(
            """SELECT id, content, eval_score, auto_quality
               FROM generated_content
               WHERE auto_quality = ? AND content_type = ?
               ORDER BY created_at DESC
               LIMIT ?""",
            (quality, content_type, limit)
        )
        return [dict(row) for row in cursor.fetchall()]

    # Reply queue
    def is_reply_processed(self, inbound_tweet_id: str) -> bool:
        """Check if we've already processed a reply."""
        cursor = self.conn.execute(
            "SELECT 1 FROM reply_queue WHERE inbound_tweet_id = ?",
            (inbound_tweet_id,)
        )
        return cursor.fetchone() is not None

    def insert_reply_draft(
        self,
        inbound_tweet_id: str,
        inbound_author_handle: str,
        inbound_author_id: str,
        inbound_text: str,
        our_tweet_id: str,
        our_content_id: Optional[int],
        our_post_text: str,
        draft_text: str,
        relationship_context: Optional[str] = None,
        quality_score: Optional[float] = None,
        quality_flags: Optional[str] = None,
    ) -> int:
        """Insert a drafted reply into the queue."""
        cursor = self.conn.execute(
            """INSERT INTO reply_queue
               (inbound_tweet_id, inbound_author_handle, inbound_author_id,
                inbound_text, our_tweet_id, our_content_id, our_post_text,
                draft_text, relationship_context, quality_score, quality_flags)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (inbound_tweet_id, inbound_author_handle, inbound_author_id,
             inbound_text, our_tweet_id, our_content_id, our_post_text,
             draft_text, relationship_context, quality_score, quality_flags)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_pending_replies(self) -> list[dict]:
        """Get all reply drafts awaiting review."""
        cursor = self.conn.execute(
            """SELECT * FROM reply_queue
               WHERE status = 'pending'
               ORDER BY detected_at ASC"""
        )
        return [dict(row) for row in cursor.fetchall()]

    def update_reply_status(
        self,
        reply_id: int,
        status: str,
        posted_tweet_id: Optional[str] = None,
    ) -> None:
        """Update a reply's status (approved, posted, dismissed)."""
        now = datetime.now(timezone.utc).isoformat()
        if status == "posted" and posted_tweet_id:
            self.conn.execute(
                """UPDATE reply_queue
                   SET status = ?, posted_tweet_id = ?, posted_at = ?, reviewed_at = ?
                   WHERE id = ?""",
                (status, posted_tweet_id, now, now, reply_id)
            )
        elif status == "dismissed":
            self.conn.execute(
                "UPDATE reply_queue SET status = ?, reviewed_at = ? WHERE id = ?",
                (status, now, reply_id)
            )
        else:
            self.conn.execute(
                "UPDATE reply_queue SET status = ?, reviewed_at = ? WHERE id = ?",
                (status, now, reply_id)
            )
        self.conn.commit()

    def count_replies_today(self) -> int:
        """Count replies posted today (UTC)."""
        cursor = self.conn.execute(
            """SELECT COUNT(*) FROM reply_queue
               WHERE status = 'posted'
                 AND posted_at >= datetime('now', 'start of day')"""
        )
        return cursor.fetchone()[0]

    def get_last_mention_id(self) -> Optional[str]:
        """Get the last processed mention ID for reply polling."""
        cursor = self.conn.execute(
            "SELECT last_mention_id FROM reply_state WHERE id = 1"
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def set_last_mention_id(self, mention_id: str) -> None:
        """Update the last processed mention ID."""
        self.conn.execute(
            """INSERT INTO reply_state (id, last_mention_id, updated_at)
               VALUES (1, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(id) DO UPDATE SET
               last_mention_id = excluded.last_mention_id,
               updated_at = CURRENT_TIMESTAMP""",
            (mention_id,)
        )
        self.conn.commit()

    def get_content_by_tweet_id(self, tweet_id: str) -> Optional[dict]:
        """Look up generated content by its published tweet ID."""
        cursor = self.conn.execute(
            "SELECT id, content, content_type FROM generated_content WHERE tweet_id = ?",
            (tweet_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    # Newsletter
    def insert_newsletter_send(
        self,
        issue_id: str,
        subject: str,
        content_ids: list[int],
        subscriber_count: int = 0,
        status: str = "sent",
    ) -> int:
        """Record a newsletter send."""
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.conn.execute(
            """INSERT INTO newsletter_sends
               (issue_id, subject, source_content_ids, subscriber_count, status, sent_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (issue_id, subject, json.dumps(content_ids), subscriber_count, status, now)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_last_newsletter_send(self) -> Optional[datetime]:
        """Get the most recent newsletter send timestamp."""
        cursor = self.conn.execute(
            "SELECT sent_at FROM newsletter_sends ORDER BY sent_at DESC LIMIT 1"
        )
        row = cursor.fetchone()
        if row and row[0]:
            dt = datetime.fromisoformat(row[0])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        return None

    def get_published_content_in_range(
        self,
        content_type: str,
        start: datetime,
        end: datetime,
    ) -> list[dict]:
        """Get published content within a date range."""
        cursor = self.conn.execute(
            """SELECT id, content, content_type, eval_score, published_url,
                      tweet_id, published_at
               FROM generated_content
               WHERE content_type = ? AND published = 1
                 AND published_at >= ? AND published_at < ?
               ORDER BY published_at DESC""",
            (content_type, start.isoformat(), end.isoformat())
        )
        return [dict(row) for row in cursor.fetchall()]

    # Historical commit queries
    def get_commits_by_repo(
        self,
        repo_name: str,
        limit: int = 20,
        min_age_days: int = 30,
        max_age_days: int = 365,
    ) -> list[dict]:
        """Get historical commits for a repository, filtered by age."""
        cursor = self.conn.execute(
            """SELECT * FROM github_commits
               WHERE repo_name = ?
                 AND timestamp <= datetime('now', ?)
                 AND timestamp >= datetime('now', ?)
               ORDER BY timestamp DESC
               LIMIT ?""",
            (repo_name, f'-{min_age_days} days', f'-{max_age_days} days', limit)
        )
        return [dict(row) for row in cursor.fetchall()]

    def count_pipeline_runs(self, content_type: str, since_days: int = 30) -> int:
        """Count pipeline runs for a content type within a period."""
        cursor = self.conn.execute(
            """SELECT COUNT(*) FROM pipeline_runs
               WHERE content_type = ?
                 AND created_at >= datetime('now', ?)""",
            (content_type, f'-{since_days} days')
        )
        return cursor.fetchone()[0]

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
