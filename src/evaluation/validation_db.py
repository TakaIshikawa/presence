"""SQLite storage for evaluator backtesting validation data."""

import sqlite3
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone


VALIDATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT UNIQUE NOT NULL,
    username TEXT NOT NULL,
    display_name TEXT,
    bio TEXT,
    follower_count INTEGER,
    following_count INTEGER,
    tweet_count INTEGER,
    collected_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tweets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tweet_id TEXT UNIQUE NOT NULL,
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    text TEXT NOT NULL,
    like_count INTEGER DEFAULT 0,
    retweet_count INTEGER DEFAULT 0,
    reply_count INTEGER DEFAULT 0,
    quote_count INTEGER DEFAULT 0,
    engagement_score REAL DEFAULT 0,
    tweet_created_at TEXT,
    collected_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS evaluations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tweet_id TEXT NOT NULL,
    evaluator_version TEXT NOT NULL,
    model TEXT NOT NULL,
    predicted_score REAL,
    hook_strength REAL,
    specificity REAL,
    emotional_resonance REAL,
    novelty REAL,
    actionability REAL,
    raw_response TEXT,
    evaluated_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tweet_id, evaluator_version)
);

CREATE TABLE IF NOT EXISTS backtest_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT UNIQUE NOT NULL,
    evaluator_version TEXT NOT NULL,
    model TEXT NOT NULL,
    num_tweets INTEGER,
    num_accounts INTEGER,
    spearman_overall REAL,
    spearman_within_account REAL,
    pearson_log REAL,
    top_quartile_precision REAL,
    bottom_quartile_precision REAL,
    notes TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tweets_account ON tweets(account_id);
CREATE INDEX IF NOT EXISTS idx_tweets_engagement ON tweets(engagement_score);
CREATE INDEX IF NOT EXISTS idx_evaluations_tweet ON evaluations(tweet_id);
CREATE INDEX IF NOT EXISTS idx_evaluations_version ON evaluations(evaluator_version);
"""


class ValidationDatabase:
    """SQLite storage for evaluator backtesting data."""

    def __init__(self, db_path: str = "./validation.db") -> None:
        self.db_path = Path(db_path).expanduser()
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self) -> "ValidationDatabase":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def init_schema(self) -> None:
        self.conn.executescript(VALIDATION_SCHEMA)

    # --- Account operations ---

    def upsert_account(
        self,
        user_id: str,
        username: str,
        display_name: str,
        bio: str,
        follower_count: int,
        following_count: int,
        tweet_count: int,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.conn.execute(
            """INSERT INTO accounts
               (user_id, username, display_name, bio,
                follower_count, following_count, tweet_count, collected_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET
                 username=excluded.username,
                 display_name=excluded.display_name,
                 bio=excluded.bio,
                 follower_count=excluded.follower_count,
                 following_count=excluded.following_count,
                 tweet_count=excluded.tweet_count,
                 collected_at=excluded.collected_at""",
            (user_id, username, display_name, bio,
             follower_count, following_count, tweet_count, now),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_account_by_user_id(self, user_id: str) -> Optional[dict]:
        cursor = self.conn.execute(
            "SELECT * FROM accounts WHERE user_id = ?", (user_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_all_accounts(self) -> list[dict]:
        cursor = self.conn.execute("SELECT * FROM accounts ORDER BY username")
        return [dict(row) for row in cursor.fetchall()]

    # --- Tweet operations ---

    def insert_tweet(
        self,
        tweet_id: str,
        account_id: int,
        text: str,
        like_count: int,
        retweet_count: int,
        reply_count: int,
        quote_count: int,
        engagement_score: float,
        tweet_created_at: str,
    ) -> Optional[int]:
        """Insert tweet, returns None if already exists."""
        now = datetime.now(timezone.utc).isoformat()
        try:
            cursor = self.conn.execute(
                """INSERT INTO tweets
                   (tweet_id, account_id, text, like_count, retweet_count,
                    reply_count, quote_count, engagement_score,
                    tweet_created_at, collected_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (tweet_id, account_id, text, like_count, retweet_count,
                 reply_count, quote_count, engagement_score,
                 tweet_created_at, now),
            )
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None

    def get_tweets_for_account(self, account_id: int) -> list[dict]:
        cursor = self.conn.execute(
            """SELECT * FROM tweets WHERE account_id = ?
               ORDER BY engagement_score DESC""",
            (account_id,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_unevaluated_tweets(
        self, evaluator_version: str, limit: int = 500
    ) -> list[dict]:
        """Get tweets not yet evaluated by a specific evaluator version."""
        cursor = self.conn.execute(
            """SELECT t.*, a.username, a.follower_count, a.bio
               FROM tweets t
               JOIN accounts a ON a.id = t.account_id
               LEFT JOIN evaluations e
                 ON e.tweet_id = t.tweet_id AND e.evaluator_version = ?
               WHERE e.id IS NULL
               ORDER BY t.account_id, t.tweet_created_at DESC
               LIMIT ?""",
            (evaluator_version, limit),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_purged_tweet_ids(self, limit: int = 500) -> list[str]:
        """Get tweet IDs that have been purged (empty text)."""
        cursor = self.conn.execute(
            "SELECT tweet_id FROM tweets WHERE text = '' LIMIT ?",
            (limit,),
        )
        return [row["tweet_id"] for row in cursor.fetchall()]

    def update_tweet_text(self, tweet_id: str, text: str) -> None:
        """Restore text for a previously purged tweet."""
        self.conn.execute(
            "UPDATE tweets SET text = ? WHERE tweet_id = ?",
            (text, tweet_id),
        )
        self.conn.commit()

    def get_all_tweets_with_accounts(self) -> list[dict]:
        cursor = self.conn.execute(
            """SELECT t.*, a.username, a.follower_count
               FROM tweets t
               JOIN accounts a ON a.id = t.account_id
               ORDER BY t.account_id, t.engagement_score DESC"""
        )
        return [dict(row) for row in cursor.fetchall()]

    # --- Evaluation operations ---

    def insert_evaluation(
        self,
        tweet_id: str,
        evaluator_version: str,
        model: str,
        predicted_score: float,
        hook_strength: float,
        specificity: float,
        emotional_resonance: float,
        novelty: float,
        actionability: float,
        raw_response: str,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.conn.execute(
            """INSERT INTO evaluations
               (tweet_id, evaluator_version, model, predicted_score,
                hook_strength, specificity, emotional_resonance,
                novelty, actionability, raw_response, evaluated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tweet_id, evaluator_version) DO UPDATE SET
                 model=excluded.model,
                 predicted_score=excluded.predicted_score,
                 hook_strength=excluded.hook_strength,
                 specificity=excluded.specificity,
                 emotional_resonance=excluded.emotional_resonance,
                 novelty=excluded.novelty,
                 actionability=excluded.actionability,
                 raw_response=excluded.raw_response,
                 evaluated_at=excluded.evaluated_at""",
            (tweet_id, evaluator_version, model, predicted_score,
             hook_strength, specificity, emotional_resonance,
             novelty, actionability, raw_response, now),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_evaluations_for_version(self, evaluator_version: str) -> list[dict]:
        cursor = self.conn.execute(
            """SELECT e.*, t.text, t.engagement_score, t.like_count,
                      t.retweet_count, t.reply_count, t.quote_count,
                      a.username, a.follower_count
               FROM evaluations e
               JOIN tweets t ON t.tweet_id = e.tweet_id
               JOIN accounts a ON a.id = t.account_id
               WHERE e.evaluator_version = ?
               ORDER BY a.username, e.predicted_score DESC""",
            (evaluator_version,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def purge_tweet_text(self) -> int:
        """Clear stored tweet text, keeping only IDs and metrics.

        Call after evaluation to comply with X data retention policy.
        Text can be refetched via API if needed for future evaluations.
        Returns number of rows updated.
        """
        cursor = self.conn.execute(
            "UPDATE tweets SET text = '' WHERE text != ''"
        )
        self.conn.commit()
        return cursor.rowcount

    # --- Backtest run operations ---

    def insert_backtest_run(
        self,
        run_id: str,
        evaluator_version: str,
        model: str,
        num_tweets: int,
        num_accounts: int,
        spearman_overall: float,
        spearman_within_account: float,
        pearson_log: float,
        top_quartile_precision: float,
        bottom_quartile_precision: float,
        notes: str = "",
    ) -> int:
        cursor = self.conn.execute(
            """INSERT INTO backtest_runs
               (run_id, evaluator_version, model, num_tweets, num_accounts,
                spearman_overall, spearman_within_account, pearson_log,
                top_quartile_precision, bottom_quartile_precision, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, evaluator_version, model, num_tweets, num_accounts,
             spearman_overall, spearman_within_account, pearson_log,
             top_quartile_precision, bottom_quartile_precision, notes),
        )
        self.conn.commit()
        return cursor.lastrowid
