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
        if "content_embedding" not in cols:
            self.conn.execute("ALTER TABLE generated_content ADD COLUMN content_embedding BLOB")
        if "repurposed_from" not in cols:
            self.conn.execute("ALTER TABLE generated_content ADD COLUMN repurposed_from INTEGER REFERENCES generated_content(id)")
        # Migrate reply_queue for cultivate enrichment
        rq_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(reply_queue)")}
        if rq_cols and "relationship_context" not in rq_cols:
            self.conn.execute("ALTER TABLE reply_queue ADD COLUMN relationship_context TEXT")
        if rq_cols and "quality_score" not in rq_cols:
            self.conn.execute("ALTER TABLE reply_queue ADD COLUMN quality_score REAL")
        if rq_cols and "quality_flags" not in rq_cols:
            self.conn.execute("ALTER TABLE reply_queue ADD COLUMN quality_flags TEXT")
        # Migrate pipeline_runs for outcome tracking
        pr_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(pipeline_runs)")}
        if pr_cols and "outcome" not in pr_cols:
            self.conn.execute("ALTER TABLE pipeline_runs ADD COLUMN outcome TEXT")
        if pr_cols and "rejection_reason" not in pr_cols:
            self.conn.execute("ALTER TABLE pipeline_runs ADD COLUMN rejection_reason TEXT")
        if pr_cols and "filter_stats" not in pr_cols:
            self.conn.execute("ALTER TABLE pipeline_runs ADD COLUMN filter_stats TEXT")
        # Migrate: create meta table if missing
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
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

    def get_recent_published_content_all(self, limit: int = 30) -> list[dict]:
        """Get most recently published posts across x_post and x_thread."""
        cursor = self.conn.execute(
            """SELECT id, content, content_type, content_embedding, published_at
               FROM generated_content
               WHERE content_type IN ('x_post', 'x_thread') AND published = 1
               ORDER BY published_at DESC LIMIT ?""",
            (limit,)
        )
        return [dict(row) for row in cursor.fetchall()]

    def set_content_embedding(self, content_id: int, embedding_blob: bytes) -> None:
        """Store embedding vector for a content item."""
        self.conn.execute(
            "UPDATE generated_content SET content_embedding = ? WHERE id = ?",
            (embedding_blob, content_id)
        )
        self.conn.commit()

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

    # Engagement calibration stats
    def get_engagement_calibration_stats(self, content_type: str = "x_post") -> dict:
        """Quantitative correlation between eval scores and real engagement.

        Returns dict with total counts, average eval scores per outcome,
        and accuracy metrics for posts scored 7+.
        """
        stats = {
            "total_classified": 0,
            "resonated_count": 0,
            "low_resonance_count": 0,
            "avg_eval_score_resonated": None,
            "avg_eval_score_low_resonance": None,
            "scored_7plus_total": 0,
            "scored_7plus_zero_engagement": 0,
            "scored_7plus_zero_pct": 0.0,
        }

        # Average eval_score by auto_quality
        cursor = self.conn.execute(
            """SELECT auto_quality, COUNT(*) AS cnt, AVG(eval_score) AS avg_score
               FROM generated_content
               WHERE content_type = ? AND auto_quality IS NOT NULL
               GROUP BY auto_quality""",
            (content_type,)
        )
        for row in cursor.fetchall():
            quality = row[0]
            count = row[1]
            avg = row[2]
            stats["total_classified"] += count
            if quality == "resonated":
                stats["resonated_count"] = count
                stats["avg_eval_score_resonated"] = round(avg, 2) if avg else None
            elif quality == "low_resonance":
                stats["low_resonance_count"] = count
                stats["avg_eval_score_low_resonance"] = round(avg, 2) if avg else None

        # Accuracy of 7+ scores
        cursor = self.conn.execute(
            """SELECT
                   COUNT(*) AS total_7plus,
                   SUM(CASE WHEN auto_quality = 'low_resonance' THEN 1 ELSE 0 END)
                       AS zero_engagement_7plus
               FROM generated_content
               WHERE content_type = ? AND eval_score >= 7.0
                 AND auto_quality IS NOT NULL""",
            (content_type,)
        )
        row = cursor.fetchone()
        if row and row[0]:
            stats["scored_7plus_total"] = row[0]
            stats["scored_7plus_zero_engagement"] = row[1] or 0
            stats["scored_7plus_zero_pct"] = round(
                (row[1] or 0) / row[0] * 100, 1
            ) if row[0] > 0 else 0.0

        return stats

    def get_all_classified_posts(self, content_type: str = "x_post") -> dict:
        """Get all auto-classified posts grouped by quality for pattern analysis.

        Returns {"resonated": [...], "low_resonance": [...]} with latest engagement scores.
        """
        result = {"resonated": [], "low_resonance": []}
        for quality in ("resonated", "low_resonance"):
            cursor = self.conn.execute(
                """SELECT gc.id, gc.content, gc.eval_score, gc.auto_quality,
                          COALESCE(pe.engagement_score, 0) AS engagement_score
                   FROM generated_content gc
                   LEFT JOIN (
                       SELECT content_id, engagement_score,
                              ROW_NUMBER() OVER (
                                  PARTITION BY content_id ORDER BY fetched_at DESC
                              ) AS rn
                       FROM post_engagement
                   ) pe ON pe.content_id = gc.id AND pe.rn = 1
                   WHERE gc.auto_quality = ? AND gc.content_type = ?
                   ORDER BY pe.engagement_score DESC""",
                (quality, content_type)
            )
            result[quality] = [dict(row) for row in cursor.fetchall()]
        return result

    # Meta key-value store
    def get_meta(self, key: str) -> Optional[str]:
        """Get a meta value by key."""
        cursor = self.conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        """Set a meta value (upsert)."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """INSERT INTO meta (key, value, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT(key) DO UPDATE SET
               value = excluded.value, updated_at = excluded.updated_at""",
            (key, value, now)
        )
        self.conn.commit()

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

    def get_pipeline_runs(self, content_type: str, since_days: int = 30) -> list[dict]:
        """Get pipeline runs with parsed filter_stats."""
        cursor = self.conn.execute(
            """SELECT * FROM pipeline_runs
               WHERE content_type = ? AND created_at >= datetime('now', ?)
               ORDER BY created_at DESC""",
            (content_type, f'-{since_days} days')
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
        outcome: str = None,
        rejection_reason: str = None,
        filter_stats: dict = None,
    ) -> int:
        """Record a pipeline run for observability."""
        cursor = self.conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, candidates_generated,
                best_candidate_index, best_score_before_refine,
                best_score_after_refine, refinement_picked,
                final_score, published, content_id,
                outcome, rejection_reason, filter_stats)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (batch_id, content_type, candidates_generated,
             best_candidate_index, best_score_before_refine,
             best_score_after_refine, refinement_picked,
             final_score, 1 if published else 0, content_id,
             outcome, rejection_reason,
             json.dumps(filter_stats) if filter_stats else None)
        )
        self.conn.commit()
        return cursor.lastrowid

    # Content repurposing
    def get_repurpose_candidates(
        self, min_engagement: float = 10.0, max_age_days: int = 14
    ) -> list[dict]:
        """Find published posts with high engagement that haven't been repurposed yet.

        Returns posts where:
        - auto_quality = 'resonated' OR latest engagement_score >= min_engagement
        - No existing generated_content has repurposed_from pointing to this id
        - Published within max_age_days
        """
        cursor = self.conn.execute(
            """SELECT gc.id, gc.content, gc.content_type, gc.eval_score,
                      gc.published_at, pe.engagement_score
               FROM generated_content gc
               INNER JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY fetched_at DESC) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               WHERE gc.published = 1
                 AND (gc.auto_quality = 'resonated' OR pe.engagement_score >= ?)
                 AND gc.published_at >= datetime('now', ?)
                 AND gc.id NOT IN (SELECT repurposed_from FROM generated_content WHERE repurposed_from IS NOT NULL)
               ORDER BY pe.engagement_score DESC""",
            (min_engagement, f'-{max_age_days} days')
        )
        return [dict(row) for row in cursor.fetchall()]

    def insert_repurposed_content(
        self,
        content_type: str,
        source_content_id: int,
        content: str,
        eval_score: float,
        eval_feedback: str,
    ) -> int:
        """Insert content that was repurposed from an existing post."""
        cursor = self.conn.execute(
            """INSERT INTO generated_content
               (content_type, source_commits, source_messages, content,
                eval_score, eval_feedback, repurposed_from)
               VALUES (?, '[]', '[]', ?, ?, ?, ?)""",
            (content_type, content, eval_score, eval_feedback, source_content_id)
        )
        self.conn.commit()
        return cursor.lastrowid

    # Engagement predictions
    def insert_prediction(
        self,
        content_id: int,
        predicted_score: float,
        hook_strength: float = None,
        specificity: float = None,
        emotional_resonance: float = None,
        novelty: float = None,
        actionability: float = None,
        prompt_version: str = None,
    ) -> int:
        """Store an engagement prediction for content."""
        cursor = self.conn.execute(
            """INSERT INTO engagement_predictions
               (content_id, predicted_score, hook_strength, specificity,
                emotional_resonance, novelty, actionability, prompt_version)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (content_id, predicted_score, hook_strength, specificity,
             emotional_resonance, novelty, actionability, prompt_version)
        )
        self.conn.commit()
        return cursor.lastrowid

    def backfill_prediction_actuals(
        self, content_id: int, actual_score: float
    ) -> None:
        """Update prediction with actual engagement score and error."""
        # Get the predicted score
        cursor = self.conn.execute(
            """SELECT predicted_score FROM engagement_predictions
               WHERE content_id = ?
               ORDER BY created_at DESC LIMIT 1""",
            (content_id,)
        )
        row = cursor.fetchone()
        if not row:
            return

        predicted = row[0]
        error = actual_score - predicted

        self.conn.execute(
            """UPDATE engagement_predictions
               SET actual_engagement_score = ?, prediction_error = ?
               WHERE content_id = ?""",
            (actual_score, error, content_id)
        )
        self.conn.commit()

    def get_prediction_accuracy(self, days: int = 30) -> dict:
        """Calculate prediction accuracy metrics for the period.

        Returns dict with:
        - count: number of predictions with actuals
        - mae: mean absolute error
        - correlation: pearson correlation coefficient (if >= 3 samples)
        - avg_predicted: average predicted score
        - avg_actual: average actual score
        """
        cursor = self.conn.execute(
            """SELECT predicted_score, actual_engagement_score, prediction_error,
                      hook_strength, specificity, emotional_resonance,
                      novelty, actionability
               FROM engagement_predictions
               WHERE actual_engagement_score IS NOT NULL
                 AND created_at >= datetime('now', ?)
               ORDER BY created_at DESC""",
            (f'-{days} days',)
        )
        rows = cursor.fetchall()

        if not rows:
            return {
                "count": 0,
                "mae": None,
                "correlation": None,
                "avg_predicted": None,
                "avg_actual": None,
            }

        predicted_scores = [row[0] for row in rows]
        actual_scores = [row[1] for row in rows]
        errors = [abs(row[2]) for row in rows]

        mae = sum(errors) / len(errors)
        avg_predicted = sum(predicted_scores) / len(predicted_scores)
        avg_actual = sum(actual_scores) / len(actual_scores)

        # Calculate Pearson correlation if we have enough samples
        correlation = None
        if len(rows) >= 3:
            import statistics
            try:
                correlation = statistics.correlation(predicted_scores, actual_scores)
            except statistics.StatisticsError:
                correlation = None

        # Per-criteria breakdown
        criteria_breakdown = {}
        for criterion, idx in [
            ("hook_strength", 3),
            ("specificity", 4),
            ("emotional_resonance", 5),
            ("novelty", 6),
            ("actionability", 7),
        ]:
            values = [row[idx] for row in rows if row[idx] is not None]
            if values:
                criteria_breakdown[criterion] = {
                    "avg": sum(values) / len(values),
                    "count": len(values),
                }

        return {
            "count": len(rows),
            "mae": round(mae, 2),
            "correlation": round(correlation, 3) if correlation is not None else None,
            "avg_predicted": round(avg_predicted, 2),
            "avg_actual": round(avg_actual, 2),
            "criteria_breakdown": criteria_breakdown,
        }

    # Content topics and planning
    def insert_content_topics(
        self,
        content_id: int,
        topics: list[tuple[str, str, float]]
    ) -> list[int]:
        """Store topic extractions for a piece of content.

        Args:
            content_id: ID of the generated content
            topics: List of (topic, subtopic, confidence) tuples

        Returns:
            List of inserted topic IDs
        """
        topic_ids = []
        for topic, subtopic, confidence in topics:
            cursor = self.conn.execute(
                """INSERT INTO content_topics (content_id, topic, subtopic, confidence)
                   VALUES (?, ?, ?, ?)""",
                (content_id, topic, subtopic, confidence)
            )
            topic_ids.append(cursor.lastrowid)

        if topic_ids:
            self.conn.commit()
        return topic_ids

    def get_topic_frequency(self, days: int = 30) -> list[dict]:
        """Get topic frequency for published content in the period.

        Args:
            days: Number of days to look back

        Returns:
            List of dicts with topic, count, last_published_at, ordered by count desc
        """
        cursor = self.conn.execute(
            """SELECT ct.topic,
                      COUNT(*) AS count,
                      MAX(gc.published_at) AS last_published_at
               FROM content_topics ct
               INNER JOIN generated_content gc ON gc.id = ct.content_id
               WHERE gc.published = 1
                 AND gc.published_at >= datetime('now', ?)
               GROUP BY ct.topic
               ORDER BY count DESC, last_published_at DESC""",
            (f'-{days} days',)
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_topic_gaps(self, days: int = 30, min_gap_days: int = 7) -> list[str]:
        """Return topics not covered in the last N days.

        Args:
            days: Total period to consider
            min_gap_days: Minimum days since last coverage to count as a gap

        Returns:
            List of topic names that haven't been covered recently
        """
        # Get all topics covered in the period
        cursor = self.conn.execute(
            """SELECT DISTINCT ct.topic,
                      MAX(gc.published_at) AS last_published_at
               FROM content_topics ct
               INNER JOIN generated_content gc ON gc.id = ct.content_id
               WHERE gc.published = 1
                 AND gc.published_at >= datetime('now', ?)
               GROUP BY ct.topic""",
            (f'-{days} days',)
        )

        covered_topics = {}
        for row in cursor.fetchall():
            topic = row[0]
            last_date = row[1]
            covered_topics[topic] = last_date

        # Find topics with gaps
        gaps = []
        cutoff = datetime.now(timezone.utc) - timedelta(days=min_gap_days)
        cutoff_iso = cutoff.isoformat()

        # Import taxonomy to check all possible topics
        from evaluation.topic_extractor import TOPIC_TAXONOMY

        for topic in TOPIC_TAXONOMY:
            if topic == "other":
                continue  # Skip "other" category

            last_date = covered_topics.get(topic)
            if last_date is None or last_date < cutoff_iso:
                gaps.append(topic)

        return sorted(gaps)

    def insert_planned_topic(
        self,
        topic: str,
        angle: str = None,
        target_date: str = None,
        source_material: str = None
    ) -> int:
        """Plan a future topic for content generation.

        Args:
            topic: Topic label from taxonomy
            angle: Specific angle or approach to cover
            target_date: Target publication date (ISO format)
            source_material: Optional commit SHAs or session IDs to draw from

        Returns:
            ID of the planned topic
        """
        cursor = self.conn.execute(
            """INSERT INTO planned_topics (topic, angle, target_date, source_material)
               VALUES (?, ?, ?, ?)""",
            (topic, angle, target_date, source_material)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_planned_topics(self, status: str = "planned") -> list[dict]:
        """Get planned topics by status.

        Args:
            status: Filter by status ('planned', 'generated', 'skipped')

        Returns:
            List of planned topic dicts
        """
        cursor = self.conn.execute(
            """SELECT * FROM planned_topics
               WHERE status = ?
               ORDER BY target_date ASC NULLS LAST, created_at ASC""",
            (status,)
        )
        return [dict(row) for row in cursor.fetchall()]

    def mark_planned_topic_generated(
        self,
        planned_id: int,
        content_id: int
    ) -> None:
        """Link a planned topic to generated content and mark as generated.

        Args:
            planned_id: ID of the planned topic
            content_id: ID of the generated content
        """
        self.conn.execute(
            """UPDATE planned_topics
               SET status = 'generated', content_id = ?
               WHERE id = ?""",
            (content_id, planned_id)
        )
        self.conn.commit()

    def get_content_without_topics(self) -> list[dict]:
        """Get published content that doesn't have topic entries yet.

        Returns:
            List of content dicts that need topic extraction
        """
        cursor = self.conn.execute(
            """SELECT gc.id, gc.content, gc.content_type, gc.published_at
               FROM generated_content gc
               WHERE gc.published = 1
                 AND gc.id NOT IN (SELECT DISTINCT content_id FROM content_topics)
               ORDER BY gc.published_at DESC"""
        )
        return [dict(row) for row in cursor.fetchall()]
