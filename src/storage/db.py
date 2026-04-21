"""SQLite storage layer for Presence."""

from __future__ import annotations

import sqlite3
import json
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta, timezone

MAX_RETRIES = 3


class DatabaseError(Exception):
    """Base exception for database errors."""
    pass


class ConnectionError(DatabaseError):
    """Raised when database connection fails."""
    pass


class IntegrityError(DatabaseError):
    """Raised when database integrity constraint is violated."""
    pass


class Database:
    def __init__(self, db_path: str = "./presence.db") -> None:
        self.db_path = Path(db_path).expanduser()
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
        except sqlite3.OperationalError as e:
            raise ConnectionError(
                f"Failed to connect to database at {self.db_path}: {e}"
            ) from e

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self) -> Database:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def init_schema(self, schema_path: str = "./schema.sql") -> None:
        """Initialize database with schema."""
        try:
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
            if "bluesky_uri" not in cols:
                self.conn.execute("ALTER TABLE generated_content ADD COLUMN bluesky_uri TEXT")
            if "content_format" not in cols:
                self.conn.execute("ALTER TABLE generated_content ADD COLUMN content_format TEXT")
            if "image_path" not in cols:
                self.conn.execute("ALTER TABLE generated_content ADD COLUMN image_path TEXT")
            if "image_prompt" not in cols:
                self.conn.execute("ALTER TABLE generated_content ADD COLUMN image_prompt TEXT")
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
            # Migrate: create content calendar campaign support if missing
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS content_campaigns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    goal TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    status TEXT DEFAULT 'planned',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_content_campaigns_status ON content_campaigns(status)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_content_campaigns_dates ON content_campaigns(start_date, end_date)")
            pt_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(planned_topics)")}
            if pt_cols and "campaign_id" not in pt_cols:
                self.conn.execute("ALTER TABLE planned_topics ADD COLUMN campaign_id INTEGER REFERENCES content_campaigns(id)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_planned_topics_campaign ON planned_topics(campaign_id)")
            # Migrate curated_sources for account discovery
            cs_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(curated_sources)")}
            if cs_cols:
                if "status" not in cs_cols:
                    self.conn.execute("ALTER TABLE curated_sources ADD COLUMN status TEXT DEFAULT 'active'")
                if "discovery_source" not in cs_cols:
                    self.conn.execute("ALTER TABLE curated_sources ADD COLUMN discovery_source TEXT")
                if "relevance_score" not in cs_cols:
                    self.conn.execute("ALTER TABLE curated_sources ADD COLUMN relevance_score REAL")
                if "sample_count" not in cs_cols:
                    self.conn.execute("ALTER TABLE curated_sources ADD COLUMN sample_count INTEGER DEFAULT 0")
                if "reviewed_at" not in cs_cols:
                    self.conn.execute("ALTER TABLE curated_sources ADD COLUMN reviewed_at TEXT")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_curated_sources_status ON curated_sources(status)")
            # Migrate: create bluesky_engagement table if missing
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS bluesky_engagement (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    content_id INTEGER NOT NULL REFERENCES generated_content(id),
                    bluesky_uri TEXT NOT NULL,
                    like_count INTEGER DEFAULT 0,
                    repost_count INTEGER DEFAULT 0,
                    reply_count INTEGER DEFAULT 0,
                    quote_count INTEGER DEFAULT 0,
                    engagement_score REAL,
                    fetched_at TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_bluesky_engagement_content ON bluesky_engagement(content_id)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_bluesky_engagement_uri ON bluesky_engagement(bluesky_uri)")
            self.conn.commit()
        except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
            raise DatabaseError(
                f"Failed to initialize database schema at {self.db_path}: {e}"
            ) from e

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
        eval_feedback: str,
        content_format: Optional[str] = None,
        image_path: Optional[str] = None,
        image_prompt: Optional[str] = None,
    ) -> int:
        cursor = self.conn.execute(
            """INSERT INTO generated_content
               (content_type, source_commits, source_messages, content, eval_score, eval_feedback,
                content_format, image_path, image_prompt)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                content_type,
                json.dumps(source_commits),
                json.dumps(source_messages),
                content,
                eval_score,
                eval_feedback,
                content_format,
                image_path,
                image_prompt,
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

    def mark_published_bluesky(self, content_id: int, uri: str) -> None:
        """Mark content as cross-posted to Bluesky by storing its AT URI."""
        self.conn.execute(
            "UPDATE generated_content SET bluesky_uri = ? WHERE id = ?",
            (uri, content_id)
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

    def get_last_published_time_any(
        self, content_types: list[str]
    ) -> Optional[datetime]:
        """Get the most recent published_at timestamp across content types."""
        if not content_types:
            return None
        placeholders = ",".join("?" for _ in content_types)
        cursor = self.conn.execute(
            f"""SELECT published_at FROM generated_content
                WHERE content_type IN ({placeholders})
                  AND published = 1
                  AND published_at IS NOT NULL
                ORDER BY published_at DESC
                LIMIT 1""",
            tuple(content_types),
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

    def insert_profile_metrics(
        self,
        platform: str,
        follower_count: int,
        following_count: int,
        tweet_count: int,
        listed_count: int = None,
    ) -> int:
        """Insert a profile metrics snapshot."""
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.conn.execute(
            """INSERT INTO profile_metrics
               (platform, follower_count, following_count, tweet_count,
                listed_count, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (platform, follower_count, following_count, tweet_count,
             listed_count, now),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_latest_profile_metrics(self, platform: str = "x") -> dict | None:
        """Get the most recent profile metrics snapshot."""
        cursor = self.conn.execute(
            """SELECT follower_count, following_count, tweet_count,
                      listed_count, fetched_at
               FROM profile_metrics
               WHERE platform = ?
               ORDER BY fetched_at DESC
               LIMIT 1""",
            (platform,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "follower_count": row[0],
            "following_count": row[1],
            "tweet_count": row[2],
            "listed_count": row[3],
            "fetched_at": row[4],
        }

    # Proactive actions
    def insert_proactive_action(
        self,
        action_type: str,
        target_tweet_id: str,
        target_tweet_text: str,
        target_author_handle: str,
        target_author_id: Optional[str] = None,
        discovery_source: Optional[str] = None,
        relevance_score: Optional[float] = None,
        draft_text: Optional[str] = None,
        relationship_context: Optional[str] = None,
        knowledge_ids: Optional[str] = None,
    ) -> int:
        """Insert a proactive engagement action (reply/like/quote opportunity)."""
        try:
            cursor = self.conn.execute(
                """INSERT INTO proactive_actions
                   (action_type, target_tweet_id, target_tweet_text,
                    target_author_handle, target_author_id, discovery_source,
                    relevance_score, draft_text, relationship_context, knowledge_ids)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (action_type, target_tweet_id, target_tweet_text,
                 target_author_handle, target_author_id, discovery_source,
                 relevance_score, draft_text, relationship_context, knowledge_ids),
            )
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError as e:
            raise IntegrityError(str(e)) from e

    def get_pending_proactive_actions(self, limit: int = 20) -> list[dict]:
        """Get pending proactive actions awaiting review."""
        cursor = self.conn.execute(
            """SELECT * FROM proactive_actions
               WHERE status = 'pending'
               ORDER BY relevance_score DESC, created_at ASC
               LIMIT ?""",
            (limit,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def mark_proactive_posted(self, action_id: int, posted_tweet_id: str) -> None:
        """Mark a proactive action as posted."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE proactive_actions
               SET status = 'posted', posted_tweet_id = ?, posted_at = ?, reviewed_at = ?
               WHERE id = ?""",
            (posted_tweet_id, now, now, action_id),
        )
        self.conn.commit()

    def dismiss_proactive_action(self, action_id: int) -> None:
        """Dismiss a proactive action."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE proactive_actions
               SET status = 'dismissed', reviewed_at = ?
               WHERE id = ?""",
            (now, action_id),
        )
        self.conn.commit()

    def count_daily_proactive_posts(self, action_type: str = "reply") -> int:
        """Count proactive actions posted today (UTC)."""
        cursor = self.conn.execute(
            """SELECT COUNT(*) FROM proactive_actions
               WHERE action_type = ? AND status = 'posted'
                 AND posted_at >= datetime('now', 'start of day')""",
            (action_type,),
        )
        return cursor.fetchone()[0]

    def count_weekly_replies_to_author(self, handle: str) -> int:
        """Count proactive replies posted to a specific author this week."""
        cursor = self.conn.execute(
            """SELECT COUNT(*) FROM proactive_actions
               WHERE target_author_handle = ? AND action_type = 'reply'
                 AND status = 'posted'
                 AND posted_at >= datetime('now', '-7 days')""",
            (handle,),
        )
        return cursor.fetchone()[0]

    def proactive_action_exists(self, tweet_id: str, action_type: str) -> bool:
        """Check if a proactive action already exists for this tweet+type."""
        cursor = self.conn.execute(
            """SELECT 1 FROM proactive_actions
               WHERE target_tweet_id = ? AND action_type = ?""",
            (tweet_id, action_type),
        )
        return cursor.fetchone() is not None

    # Curated sources (account discovery)
    def sync_config_sources(
        self, sources: list[dict], source_type: str
    ) -> int:
        """Upsert config-driven sources into curated_sources table.

        Args:
            sources: List of dicts with 'identifier', 'name', 'license' keys
            source_type: e.g. 'x_account', 'blog'

        Returns:
            Number of sources synced
        """
        for src in sources:
            self.conn.execute(
                """INSERT INTO curated_sources
                   (source_type, identifier, name, license, status, discovery_source)
                   VALUES (?, ?, ?, ?, 'active', 'config')
                   ON CONFLICT(source_type, identifier) DO UPDATE SET
                   name = excluded.name,
                   license = excluded.license""",
                (source_type, src["identifier"], src.get("name", ""),
                 src.get("license", "attribution_required")),
            )
        self.conn.commit()
        return len(sources)

    def get_active_curated_sources(self, source_type: str) -> list[dict]:
        """Get curated sources with status='active'."""
        cursor = self.conn.execute(
            """SELECT * FROM curated_sources
               WHERE source_type = ? AND status = 'active'
               ORDER BY created_at ASC""",
            (source_type,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def insert_candidate_source(
        self,
        source_type: str,
        identifier: str,
        name: str = "",
        discovery_source: str = "proactive_mining",
        relevance_score: float = None,
        sample_count: int = 0,
    ) -> int | None:
        """Insert a discovered candidate source. Returns id or None if duplicate."""
        try:
            cursor = self.conn.execute(
                """INSERT INTO curated_sources
                   (source_type, identifier, name, status, discovery_source,
                    relevance_score, sample_count)
                   VALUES (?, ?, ?, 'candidate', ?, ?, ?)""",
                (source_type, identifier, name, discovery_source,
                 relevance_score, sample_count),
            )
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None

    def get_candidate_sources(
        self, source_type: str, limit: int = 20
    ) -> list[dict]:
        """Get candidate sources pending review."""
        cursor = self.conn.execute(
            """SELECT * FROM curated_sources
               WHERE source_type = ? AND status = 'candidate'
               ORDER BY relevance_score DESC, created_at ASC
               LIMIT ?""",
            (source_type, limit),
        )
        return [dict(row) for row in cursor.fetchall()]

    def approve_candidate(self, source_id: int) -> None:
        """Approve a candidate source (sets status='active')."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE curated_sources
               SET status = 'active', reviewed_at = ?
               WHERE id = ?""",
            (now, source_id),
        )
        self.conn.commit()

    def reject_candidate(self, source_id: int) -> None:
        """Reject a candidate source."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE curated_sources
               SET status = 'rejected', reviewed_at = ?
               WHERE id = ?""",
            (now, source_id),
        )
        self.conn.commit()

    def sync_following_sources(
        self, accounts: list[dict], source_type: str = "x_account"
    ) -> int:
        """Insert followed accounts as active sources, skipping existing entries.

        Unlike sync_config_sources, this does NOT overwrite existing rows —
        if an account already exists (from config, mining, or prior follow sync),
        it is left untouched.

        Returns count of newly inserted accounts.
        """
        inserted = 0
        for acc in accounts:
            try:
                self.conn.execute(
                    """INSERT INTO curated_sources
                       (source_type, identifier, name, status, discovery_source)
                       VALUES (?, ?, ?, 'active', 'following')""",
                    (source_type, acc["username"], acc.get("name", acc["username"])),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                pass  # Already exists — skip
        self.conn.commit()
        return inserted

    def candidate_exists(self, source_type: str, identifier: str) -> bool:
        """Check if a source already exists (any status)."""
        cursor = self.conn.execute(
            """SELECT 1 FROM curated_sources
               WHERE source_type = ? AND identifier = ?""",
            (source_type, identifier),
        )
        return cursor.fetchone() is not None

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

    def get_format_engagement_stats(self, days: int = 90) -> list[dict]:
        """Get engagement stats grouped by content_format.

        Returns list of dicts with:
            - format: str - format name (e.g., 'micro_story', 'bold_claim')
            - count: int - number of posts using this format
            - avg_engagement: float - average engagement score
            - resonated_count: int - number classified as 'resonated'
            - total_classified: int - number with auto_quality classification

        Args:
            days: Lookback window for published content (default 90)
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        cursor = self.conn.execute(
            """SELECT
                   gc.content_format AS format,
                   COUNT(*) AS count,
                   AVG(COALESCE(pe.engagement_score, 0)) AS avg_engagement,
                   SUM(CASE WHEN gc.auto_quality = 'resonated' THEN 1 ELSE 0 END) AS resonated_count,
                   SUM(CASE WHEN gc.auto_quality IS NOT NULL THEN 1 ELSE 0 END) AS total_classified
               FROM generated_content gc
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY fetched_at DESC) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               WHERE gc.content_format IS NOT NULL
                 AND gc.published = 1
                 AND gc.published_at >= ?
               GROUP BY gc.content_format
               ORDER BY avg_engagement DESC""",
            (cutoff.isoformat(),)
        )
        return [dict(row) for row in cursor.fetchall()]

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

    def insert_reply_knowledge_links(
        self, reply_queue_id: int, knowledge_ids: list[tuple[int, float]]
    ) -> None:
        """Bulk insert knowledge item links for reply drafts.

        Args:
            reply_queue_id: ID of the reply in reply_queue
            knowledge_ids: List of (knowledge_id, relevance_score) tuples
        """
        if not knowledge_ids:
            return

        for knowledge_id, relevance_score in knowledge_ids:
            self.conn.execute(
                """INSERT INTO reply_knowledge_links (reply_queue_id, knowledge_id, relevance_score)
                   VALUES (?, ?, ?)""",
                (reply_queue_id, knowledge_id, relevance_score)
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

    def get_predictions_with_actuals(self, days: int = 30) -> list[dict]:
        """Get predictions with actual engagement scores for calibration.

        Args:
            days: Number of days to look back

        Returns:
            List of prediction dicts with all fields
        """
        cursor = self.conn.execute(
            """SELECT predicted_score, actual_engagement_score, prediction_error,
                      hook_strength, specificity, emotional_resonance,
                      novelty, actionability, content_id, created_at
               FROM engagement_predictions
               WHERE actual_engagement_score IS NOT NULL
                 AND created_at >= datetime('now', ?)
               ORDER BY created_at DESC""",
            (f'-{days} days',)
        )
        rows = cursor.fetchall()

        return [
            {
                "predicted_score": row[0],
                "actual_engagement_score": row[1],
                "prediction_error": row[2],
                "hook_strength": row[3],
                "specificity": row[4],
                "emotional_resonance": row[5],
                "novelty": row[6],
                "actionability": row[7],
                "content_id": row[8],
                "created_at": row[9],
            }
            for row in rows
        ]

    def get_predictions_by_criterion(
        self, criterion: str, days: int = 30
    ) -> list[tuple[float, float]]:
        """Get (criterion_score, actual_engagement) pairs for correlation analysis.

        Args:
            criterion: One of hook_strength, specificity, emotional_resonance,
                      novelty, actionability
            days: Number of days to look back

        Returns:
            List of (criterion_value, actual_engagement) tuples
        """
        valid_criteria = [
            "hook_strength",
            "specificity",
            "emotional_resonance",
            "novelty",
            "actionability",
        ]
        if criterion not in valid_criteria:
            raise ValueError(f"Invalid criterion: {criterion}")

        cursor = self.conn.execute(
            f"""SELECT {criterion}, actual_engagement_score
                FROM engagement_predictions
                WHERE actual_engagement_score IS NOT NULL
                  AND {criterion} IS NOT NULL
                  AND created_at >= datetime('now', ?)
                ORDER BY created_at DESC""",
            (f'-{days} days',)
        )
        return cursor.fetchall()

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
        source_material: str = None,
        campaign_id: int = None
    ) -> int:
        """Plan a future topic for content generation.

        Args:
            topic: Topic label from taxonomy
            angle: Specific angle or approach to cover
            target_date: Target publication date (ISO format)
            source_material: Optional commit SHAs or session IDs to draw from
            campaign_id: Optional campaign to group this planned topic under

        Returns:
            ID of the planned topic
        """
        cursor = self.conn.execute(
            """INSERT INTO planned_topics
               (topic, angle, target_date, source_material, campaign_id)
               VALUES (?, ?, ?, ?, ?)""",
            (topic, angle, target_date, source_material, campaign_id)
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
            """SELECT pt.*,
                      cc.name AS campaign_name,
                      cc.goal AS campaign_goal,
                      cc.start_date AS campaign_start_date,
                      cc.end_date AS campaign_end_date,
                      cc.status AS campaign_status
               FROM planned_topics pt
               LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
               WHERE pt.status = ?
               ORDER BY cc.start_date ASC NULLS LAST,
                        pt.campaign_id ASC NULLS LAST,
                        pt.target_date ASC NULLS LAST,
                        pt.created_at ASC""",
            (status,)
        )
        return [dict(row) for row in cursor.fetchall()]

    def create_campaign(
        self,
        name: str,
        goal: str = None,
        start_date: str = None,
        end_date: str = None,
        status: str = "planned",
    ) -> int:
        """Create a content campaign for grouping planned topics."""
        cursor = self.conn.execute(
            """INSERT INTO content_campaigns (name, goal, start_date, end_date, status)
               VALUES (?, ?, ?, ?, ?)""",
            (name, goal, start_date, end_date, status)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_campaign(self, campaign_id: int) -> dict | None:
        """Get a single content campaign by ID."""
        cursor = self.conn.execute(
            "SELECT * FROM content_campaigns WHERE id = ?",
            (campaign_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_campaigns(self, status: str = None) -> list[dict]:
        """List content campaigns, optionally filtered by status."""
        if status:
            cursor = self.conn.execute(
                """SELECT * FROM content_campaigns
                   WHERE status = ?
                   ORDER BY start_date ASC NULLS LAST, created_at ASC""",
                (status,)
            )
        else:
            cursor = self.conn.execute(
                """SELECT * FROM content_campaigns
                   ORDER BY start_date ASC NULLS LAST, created_at ASC"""
            )
        return [dict(row) for row in cursor.fetchall()]

    def attach_planned_topic_to_campaign(
        self,
        planned_id: int,
        campaign_id: int | None
    ) -> None:
        """Attach a planned topic to a campaign, or detach it with None."""
        if campaign_id is not None and self.get_campaign(campaign_id) is None:
            raise ValueError(f"Campaign {campaign_id} does not exist")

        cursor = self.conn.execute(
            """UPDATE planned_topics
               SET campaign_id = ?
               WHERE id = ?""",
            (campaign_id, planned_id)
        )
        if cursor.rowcount == 0:
            raise ValueError(f"Planned topic {planned_id} does not exist")
        self.conn.commit()

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

    # Publish queue for scheduled posting
    def queue_for_publishing(
        self,
        content_id: int,
        scheduled_at: str,
        platform: str = 'all'
    ) -> int:
        """Queue content for publishing at a scheduled time.

        Args:
            content_id: ID of generated content to publish
            scheduled_at: ISO timestamp for when to publish
            platform: Target platform ('x', 'bluesky', 'all')

        Returns:
            ID of the queue entry
        """
        cursor = self.conn.execute(
            """INSERT INTO publish_queue (content_id, scheduled_at, platform)
               VALUES (?, ?, ?)""",
            (content_id, scheduled_at, platform)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_due_queue_items(self, now: str) -> list[dict]:
        """Get queued items that are ready to publish.

        Args:
            now: Current ISO timestamp to compare against

        Returns:
            List of queue item dicts where scheduled_at <= now and status = 'queued'
        """
        cursor = self.conn.execute(
            """SELECT pq.id, pq.content_id, pq.scheduled_at, pq.platform,
                      gc.content, gc.content_type
               FROM publish_queue pq
               INNER JOIN generated_content gc ON gc.id = pq.content_id
               WHERE pq.status = 'queued'
                 AND pq.scheduled_at <= ?
               ORDER BY pq.scheduled_at ASC""",
            (now,)
        )
        return [dict(row) for row in cursor.fetchall()]

    def mark_queue_published(self, queue_id: int) -> None:
        """Mark a queue item as successfully published.

        Args:
            queue_id: ID of the publish_queue entry
        """
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE publish_queue
               SET status = 'published', published_at = ?
               WHERE id = ?""",
            (now, queue_id)
        )
        self.conn.commit()

    def mark_queue_failed(self, queue_id: int, error: str) -> None:
        """Mark a queue item as failed with error message.

        Args:
            queue_id: ID of the publish_queue entry
            error: Error message describing the failure
        """
        self.conn.execute(
            """UPDATE publish_queue
               SET status = 'failed', error = ?
               WHERE id = ?""",
            (error, queue_id)
        )
        self.conn.commit()

    def cancel_queued(self, content_id: int) -> None:
        """Cancel all queued items for a content ID.

        Args:
            content_id: ID of the generated content
        """
        self.conn.execute(
            """UPDATE publish_queue
               SET status = 'cancelled'
               WHERE content_id = ? AND status = 'queued'""",
            (content_id,)
        )
        self.conn.commit()

    # Bluesky engagement tracking
    def insert_bluesky_engagement(
        self,
        content_id: int,
        bluesky_uri: str,
        like_count: int,
        repost_count: int,
        reply_count: int,
        quote_count: int,
        engagement_score: float
    ) -> int:
        """Insert a Bluesky engagement metrics snapshot.

        Args:
            content_id: ID of the generated content
            bluesky_uri: AT Protocol URI of the post
            like_count: Number of likes
            repost_count: Number of reposts
            reply_count: Number of replies
            quote_count: Number of quote posts
            engagement_score: Computed engagement score

        Returns:
            ID of the inserted engagement record
        """
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.conn.execute(
            """INSERT INTO bluesky_engagement
               (content_id, bluesky_uri, like_count, repost_count,
                reply_count, quote_count, engagement_score, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (content_id, bluesky_uri, like_count, repost_count,
             reply_count, quote_count, engagement_score, now)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_bluesky_engagement(self, content_id: int) -> list[dict]:
        """Get time-series of Bluesky engagement metrics for a post.

        Args:
            content_id: ID of the generated content

        Returns:
            List of engagement snapshots ordered by fetched_at
        """
        cursor = self.conn.execute(
            """SELECT * FROM bluesky_engagement
               WHERE content_id = ?
               ORDER BY fetched_at ASC""",
            (content_id,)
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_content_needing_bluesky_engagement(
        self,
        max_age_days: int = 7
    ) -> list[dict]:
        """Get content with bluesky_uri but no recent engagement fetch.

        Args:
            max_age_days: Only fetch for posts published within this many days

        Returns:
            List of content dicts needing Bluesky engagement fetch
        """
        cursor = self.conn.execute(
            """SELECT gc.id, gc.bluesky_uri, gc.content, gc.published_at,
                      be.fetched_at AS last_fetched
               FROM generated_content gc
               LEFT JOIN (
                   SELECT content_id, MAX(fetched_at) AS fetched_at
                   FROM bluesky_engagement
                   GROUP BY content_id
               ) be ON be.content_id = gc.id
               WHERE gc.published = 1
                 AND gc.bluesky_uri IS NOT NULL
                 AND gc.published_at >= datetime('now', ?)
                 AND (be.fetched_at IS NULL
                      OR be.fetched_at < datetime('now', '-6 hours'))
               ORDER BY gc.published_at DESC""",
            (f'-{max_age_days} days',)
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_combined_engagement(self, content_id: int) -> dict:
        """Get unified engagement view combining X and Bluesky metrics.

        Args:
            content_id: ID of the generated content

        Returns:
            Dict with latest X metrics, Bluesky metrics, and combined score
        """
        # Get latest X engagement
        x_cursor = self.conn.execute(
            """SELECT like_count, retweet_count, reply_count, quote_count, engagement_score
               FROM post_engagement
               WHERE content_id = ?
               ORDER BY fetched_at DESC LIMIT 1""",
            (content_id,)
        )
        x_row = x_cursor.fetchone()

        # Get latest Bluesky engagement
        bsky_cursor = self.conn.execute(
            """SELECT like_count, repost_count, reply_count, quote_count, engagement_score
               FROM bluesky_engagement
               WHERE content_id = ?
               ORDER BY fetched_at DESC LIMIT 1""",
            (content_id,)
        )
        bsky_row = bsky_cursor.fetchone()

        result = {
            'content_id': content_id,
            'x_engagement': None,
            'bluesky_engagement': None,
            'combined_score': 0.0,
        }

        if x_row:
            result['x_engagement'] = {
                'like_count': x_row[0],
                'retweet_count': x_row[1],
                'reply_count': x_row[2],
                'quote_count': x_row[3],
                'engagement_score': x_row[4],
            }
            result['combined_score'] += x_row[4]

        if bsky_row:
            result['bluesky_engagement'] = {
                'like_count': bsky_row[0],
                'repost_count': bsky_row[1],
                'reply_count': bsky_row[2],
                'quote_count': bsky_row[3],
                'engagement_score': bsky_row[4],
            }
            result['combined_score'] += bsky_row[4]

        return result

    # Knowledge lineage tracking
    def insert_content_knowledge_links(
        self, content_id: int, links: list[tuple[int, float]]
    ) -> None:
        """Bulk insert knowledge item links for generated content.

        Args:
            content_id: ID of the generated content
            links: List of (knowledge_id, relevance_score) tuples
        """
        if not links:
            return

        for knowledge_id, relevance_score in links:
            self.conn.execute(
                """INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score)
                   VALUES (?, ?, ?)""",
                (content_id, knowledge_id, relevance_score)
            )
        self.conn.commit()

    def get_knowledge_usage_stats(self, days: int = 30) -> list[dict]:
        """Get knowledge usage statistics for the period.

        Returns for each knowledge item used: knowledge_id, source_type, author,
        usage_count, avg_relevance_score, avg_engagement_of_linked_content.
        """
        cursor = self.conn.execute(
            """SELECT k.id, k.source_type, k.author, k.content,
                      COUNT(DISTINCT ckl.content_id) AS usage_count,
                      AVG(ckl.relevance_score) AS avg_relevance,
                      AVG(COALESCE(pe.engagement_score, 0)) AS avg_engagement
               FROM knowledge k
               INNER JOIN content_knowledge_links ckl ON ckl.knowledge_id = k.id
               INNER JOIN generated_content gc ON gc.id = ckl.content_id
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY fetched_at DESC) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               WHERE ckl.created_at >= datetime('now', ?)
                 AND gc.published = 1
               GROUP BY k.id
               ORDER BY usage_count DESC, avg_engagement DESC""",
            (f'-{days} days',)
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_most_valuable_sources(
        self, days: int = 90, min_uses: int = 3
    ) -> list[dict]:
        """Get curated sources ranked by average engagement of content they contributed to.

        Args:
            days: Number of days to look back
            min_uses: Minimum number of uses to be included in results

        Returns:
            List of dicts with source_type, author, usage_count, avg_engagement,
            ordered by avg_engagement descending
        """
        cursor = self.conn.execute(
            """SELECT k.source_type, k.author,
                      COUNT(DISTINCT ckl.content_id) AS usage_count,
                      AVG(COALESCE(pe.engagement_score, 0)) AS avg_engagement
               FROM knowledge k
               INNER JOIN content_knowledge_links ckl ON ckl.knowledge_id = k.id
               INNER JOIN generated_content gc ON gc.id = ckl.content_id
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY fetched_at DESC) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               WHERE ckl.created_at >= datetime('now', ?)
                 AND gc.published = 1
                 AND k.source_type IN ('curated_x', 'curated_article')
               GROUP BY k.source_type, k.author
               HAVING usage_count >= ?
               ORDER BY avg_engagement DESC""",
            (f'-{days} days', min_uses)
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_source_engagement_details(
        self, days: int = 90, min_uses: int = 2
    ) -> list[dict]:
        """Get detailed engagement stats for curated sources including resonance data.

        Similar to get_most_valuable_sources but also includes:
        - resonated_count: number of posts using this source classified as 'resonated'
        - classified_count: total number of classified posts using this source
        - total_uses: total times knowledge from this source was used

        Args:
            days: Number of days to look back
            min_uses: Minimum number of uses to be included in results

        Returns:
            List of dicts with source_type, author, total_uses, avg_engagement,
            resonated_count, classified_count, ordered by avg_engagement descending
        """
        cursor = self.conn.execute(
            """SELECT k.source_type, k.author,
                      COUNT(DISTINCT ckl.content_id) AS total_uses,
                      AVG(COALESCE(pe.engagement_score, 0)) AS avg_engagement,
                      SUM(CASE WHEN gc.auto_quality = 'resonated' THEN 1 ELSE 0 END) AS resonated_count,
                      SUM(CASE WHEN gc.auto_quality IS NOT NULL THEN 1 ELSE 0 END) AS classified_count
               FROM knowledge k
               INNER JOIN content_knowledge_links ckl ON ckl.knowledge_id = k.id
               INNER JOIN generated_content gc ON gc.id = ckl.content_id
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY fetched_at DESC) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               WHERE ckl.created_at >= datetime('now', ?)
                 AND gc.published = 1
                 AND k.source_type IN ('curated_x', 'curated_article')
               GROUP BY k.source_type, k.author
               HAVING total_uses >= ?
               ORDER BY avg_engagement DESC""",
            (f'-{days} days', min_uses)
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_content_lineage(self, content_id: int) -> list[dict]:
        """Get all knowledge items that contributed to a specific post.

        Returns list of dicts with knowledge details and relevance scores.
        """
        cursor = self.conn.execute(
            """SELECT k.id, k.source_type, k.source_id, k.source_url,
                      k.author, k.content, k.insight, k.attribution_required,
                      ckl.relevance_score, ckl.created_at AS linked_at
               FROM content_knowledge_links ckl
               INNER JOIN knowledge k ON k.id = ckl.knowledge_id
               WHERE ckl.content_id = ?
               ORDER BY ckl.relevance_score DESC""",
            (content_id,)
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_unused_knowledge(self, days: int = 30) -> list[dict]:
        """Get knowledge items ingested but never used in generation.

        Args:
            days: Look for knowledge items created in the last N days

        Returns:
            List of knowledge item dicts that have never been linked to content
        """
        cursor = self.conn.execute(
            """SELECT k.id, k.source_type, k.source_id, k.source_url,
                      k.author, k.content, k.insight, k.created_at
               FROM knowledge k
               WHERE k.created_at >= datetime('now', ?)
                 AND k.approved = 1
                 AND k.id NOT IN (
                     SELECT DISTINCT knowledge_id
                     FROM content_knowledge_links
                 )
               ORDER BY k.created_at DESC""",
            (f'-{days} days',)
        )
        return [dict(row) for row in cursor.fetchall()]

    # Platform divergence analysis
    def get_cross_platform_engagement(self, days: int = 60) -> list[dict]:
        """Get engagement data for content published to both X and Bluesky.

        Args:
            days: Number of days to look back

        Returns:
            List of dicts with content_id, content_type, content_preview,
            x_score, and bluesky_score for cross-posted content
        """
        cursor = self.conn.execute(
            """SELECT gc.id AS content_id,
                      gc.content_type,
                      SUBSTR(gc.content, 1, 100) AS content_preview,
                      COALESCE(pe.engagement_score, 0) AS x_score,
                      COALESCE(be.engagement_score, 0) AS bluesky_score
               FROM generated_content gc
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY fetched_at DESC) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY fetched_at DESC) AS rn
                   FROM bluesky_engagement
               ) be ON be.content_id = gc.id AND be.rn = 1
               WHERE gc.published = 1
                 AND gc.tweet_id IS NOT NULL
                 AND gc.bluesky_uri IS NOT NULL
                 AND gc.published_at >= datetime('now', ?)
                 AND (pe.engagement_score IS NOT NULL OR be.engagement_score IS NOT NULL)
               ORDER BY gc.published_at DESC""",
            (f'-{days} days',)
        )
        return [dict(row) for row in cursor.fetchall()]
