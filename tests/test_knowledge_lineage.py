"""Tests for knowledge lineage tracking system."""

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from storage.db import Database


@pytest.fixture
def db():
    """In-memory database with schema and test data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Create schema
    conn.executescript("""
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_url TEXT,
            author TEXT,
            content TEXT,
            insight TEXT,
            embedding BLOB,
            attribution_required INTEGER DEFAULT 1,
            approved INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_type, source_id)
        );

        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_type TEXT NOT NULL,
            source_commits TEXT,
            source_messages TEXT,
            content TEXT NOT NULL,
            eval_score REAL,
            eval_feedback TEXT,
            published INTEGER DEFAULT 0,
            published_url TEXT,
            tweet_id TEXT,
            published_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE content_knowledge_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_id INTEGER REFERENCES generated_content(id),
            knowledge_id INTEGER REFERENCES knowledge(id),
            relevance_score REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE post_engagement (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_id INTEGER REFERENCES generated_content(id),
            tweet_id TEXT NOT NULL,
            like_count INTEGER DEFAULT 0,
            retweet_count INTEGER DEFAULT 0,
            reply_count INTEGER DEFAULT 0,
            quote_count INTEGER DEFAULT 0,
            engagement_score REAL,
            fetched_at TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Create Database instance with the test connection
    database = Database(":memory:")
    database.conn = conn

    yield database

    conn.close()


def test_insert_content_knowledge_links(db):
    """Test bulk inserting knowledge links."""
    # Insert test knowledge items
    cursor = db.conn.execute(
        "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
        ("curated_x", "tweet1", "alice", "Knowledge 1", 1)
    )
    k1_id = cursor.lastrowid

    cursor = db.conn.execute(
        "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
        ("curated_x", "tweet2", "bob", "Knowledge 2", 1)
    )
    k2_id = cursor.lastrowid

    # Insert test content
    cursor = db.conn.execute(
        "INSERT INTO generated_content (content_type, content, eval_score, published) VALUES (?, ?, ?, ?)",
        ("x_post", "Test post", 8.0, 1)
    )
    content_id = cursor.lastrowid

    # Insert links
    links = [(k1_id, 0.8), (k2_id, 0.6)]
    db.insert_content_knowledge_links(content_id, links)

    # Verify links were created
    cursor = db.conn.execute(
        "SELECT knowledge_id, relevance_score FROM content_knowledge_links WHERE content_id = ? ORDER BY relevance_score DESC",
        (content_id,)
    )
    rows = cursor.fetchall()

    assert len(rows) == 2
    assert rows[0][0] == k1_id
    assert rows[0][1] == 0.8
    assert rows[1][0] == k2_id
    assert rows[1][1] == 0.6


def test_insert_content_knowledge_links_empty(db):
    """Test that empty links list does nothing."""
    db.insert_content_knowledge_links(123, [])

    cursor = db.conn.execute("SELECT COUNT(*) FROM content_knowledge_links")
    assert cursor.fetchone()[0] == 0


def test_get_knowledge_usage_stats(db):
    """Test knowledge usage statistics aggregation."""
    # Create knowledge items
    cursor = db.conn.execute(
        "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
        ("curated_x", "tweet1", "alice", "Popular knowledge", 1)
    )
    k1_id = cursor.lastrowid

    cursor = db.conn.execute(
        "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
        ("curated_x", "tweet2", "bob", "Less popular", 1)
    )
    k2_id = cursor.lastrowid

    # Create content items
    now = datetime.now(timezone.utc)
    for i in range(3):
        cursor = db.conn.execute(
            "INSERT INTO generated_content (content_type, content, eval_score, published, published_at) VALUES (?, ?, ?, ?, ?)",
            ("x_post", f"Post {i}", 8.0, 1, now.isoformat())
        )
        content_id = cursor.lastrowid

        # Link knowledge - k1 is used in all posts, k2 only in first post
        cursor = db.conn.execute(
            "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
            (content_id, k1_id, 0.7 + i * 0.1)
        )
        if i == 0:
            cursor = db.conn.execute(
                "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
                (content_id, k2_id, 0.5)
            )

        # Add engagement
        cursor = db.conn.execute(
            "INSERT INTO post_engagement (content_id, tweet_id, like_count, retweet_count, engagement_score, fetched_at) VALUES (?, ?, ?, ?, ?, ?)",
            (content_id, f"tweet{i}", 10 * (i + 1), 2 * (i + 1), 5.0 * (i + 1), now.isoformat())
        )

    db.conn.commit()

    # Get usage stats
    stats = db.get_knowledge_usage_stats(days=30)

    assert len(stats) == 2
    # k1 should be first (used 3 times)
    assert stats[0]["id"] == k1_id
    assert stats[0]["usage_count"] == 3
    assert stats[0]["avg_relevance"] == pytest.approx(0.8, abs=0.01)  # (0.7 + 0.8 + 0.9) / 3
    assert stats[0]["avg_engagement"] == pytest.approx(10.0, abs=0.01)  # (5 + 10 + 15) / 3

    # k2 should be second (used 1 time)
    assert stats[1]["id"] == k2_id
    assert stats[1]["usage_count"] == 1
    assert stats[1]["avg_relevance"] == 0.5
    assert stats[1]["avg_engagement"] == 5.0


def test_get_most_valuable_sources(db):
    """Test source ranking by engagement."""
    # Create knowledge from different sources
    now = datetime.now(timezone.utc)

    # Alice's knowledge - high engagement
    for i in range(3):
        cursor = db.conn.execute(
            "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
            ("curated_x", f"alice_{i}", "alice", f"Alice knowledge {i}", 1)
        )
        k_id = cursor.lastrowid

        cursor = db.conn.execute(
            "INSERT INTO generated_content (content_type, content, eval_score, published, published_at) VALUES (?, ?, ?, ?, ?)",
            ("x_post", f"Post using alice {i}", 8.0, 1, now.isoformat())
        )
        c_id = cursor.lastrowid

        cursor = db.conn.execute(
            "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
            (c_id, k_id, 0.7)
        )

        cursor = db.conn.execute(
            "INSERT INTO post_engagement (content_id, tweet_id, engagement_score, fetched_at) VALUES (?, ?, ?, ?)",
            (c_id, f"tweet_a{i}", 15.0, now.isoformat())
        )

    # Bob's knowledge - lower engagement
    for i in range(3):
        cursor = db.conn.execute(
            "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
            ("curated_x", f"bob_{i}", "bob", f"Bob knowledge {i}", 1)
        )
        k_id = cursor.lastrowid

        cursor = db.conn.execute(
            "INSERT INTO generated_content (content_type, content, eval_score, published, published_at) VALUES (?, ?, ?, ?, ?)",
            ("x_post", f"Post using bob {i}", 8.0, 1, now.isoformat())
        )
        c_id = cursor.lastrowid

        cursor = db.conn.execute(
            "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
            (c_id, k_id, 0.6)
        )

        cursor = db.conn.execute(
            "INSERT INTO post_engagement (content_id, tweet_id, engagement_score, fetched_at) VALUES (?, ?, ?, ?)",
            (c_id, f"tweet_b{i}", 5.0, now.isoformat())
        )

    db.conn.commit()

    # Get valuable sources
    sources = db.get_most_valuable_sources(days=90, min_uses=3)

    assert len(sources) == 2
    # Alice should be first (higher engagement)
    assert sources[0]["author"] == "alice"
    assert sources[0]["usage_count"] == 3
    assert sources[0]["avg_engagement"] == 15.0

    # Bob should be second
    assert sources[1]["author"] == "bob"
    assert sources[1]["usage_count"] == 3
    assert sources[1]["avg_engagement"] == 5.0


def test_get_content_lineage(db):
    """Test retrieving all knowledge items for a post."""
    # Create knowledge
    cursor = db.conn.execute(
        "INSERT INTO knowledge (source_type, source_id, author, content, insight, attribution_required, approved) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("curated_x", "tweet1", "alice", "Original content", "Key insight", 1, 1)
    )
    k1_id = cursor.lastrowid

    cursor = db.conn.execute(
        "INSERT INTO knowledge (source_type, source_id, author, content, attribution_required, approved, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("curated_article", "article1", "bob", "Article content", 0, 1, "https://example.com/article")
    )
    k2_id = cursor.lastrowid

    # Create content
    cursor = db.conn.execute(
        "INSERT INTO generated_content (content_type, content, eval_score) VALUES (?, ?, ?)",
        ("x_post", "Generated post", 8.5)
    )
    content_id = cursor.lastrowid

    # Link knowledge
    cursor = db.conn.execute(
        "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
        (content_id, k1_id, 0.9)
    )
    cursor = db.conn.execute(
        "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
        (content_id, k2_id, 0.4)
    )
    db.conn.commit()

    # Get lineage
    lineage = db.get_content_lineage(content_id)

    assert len(lineage) == 2
    # Should be ordered by relevance desc
    assert lineage[0]["id"] == k1_id
    assert lineage[0]["relevance_score"] == 0.9
    assert lineage[0]["author"] == "alice"
    assert lineage[0]["insight"] == "Key insight"
    assert lineage[0]["attribution_required"] == 1

    assert lineage[1]["id"] == k2_id
    assert lineage[1]["relevance_score"] == 0.4
    assert lineage[1]["source_url"] == "https://example.com/article"
    assert lineage[1]["attribution_required"] == 0


def test_get_content_lineage_empty(db):
    """Test lineage for content with no knowledge links."""
    cursor = db.conn.execute(
        "INSERT INTO generated_content (content_type, content, eval_score) VALUES (?, ?, ?)",
        ("x_post", "Post without knowledge", 7.0)
    )
    content_id = cursor.lastrowid
    db.conn.commit()

    lineage = db.get_content_lineage(content_id)
    assert len(lineage) == 0


def test_get_unused_knowledge(db):
    """Test finding knowledge items never used in generation."""
    now = datetime.now(timezone.utc)
    old_time = (now - timedelta(days=40)).isoformat()
    recent_time = now.isoformat()

    # Recent unused knowledge
    cursor = db.conn.execute(
        "INSERT INTO knowledge (source_type, source_id, author, content, approved, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        ("curated_x", "unused1", "alice", "Unused recent", 1, recent_time)
    )
    unused_id = cursor.lastrowid

    # Recent used knowledge
    cursor = db.conn.execute(
        "INSERT INTO knowledge (source_type, source_id, author, content, approved, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        ("curated_x", "used1", "bob", "Used recent", 1, recent_time)
    )
    used_id = cursor.lastrowid

    # Old unused knowledge
    cursor = db.conn.execute(
        "INSERT INTO knowledge (source_type, source_id, author, content, approved, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        ("curated_x", "unused_old", "charlie", "Unused old", 1, old_time)
    )

    # Create content and link the used knowledge
    cursor = db.conn.execute(
        "INSERT INTO generated_content (content_type, content, eval_score) VALUES (?, ?, ?)",
        ("x_post", "Post", 8.0)
    )
    content_id = cursor.lastrowid

    cursor = db.conn.execute(
        "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
        (content_id, used_id, 0.7)
    )
    db.conn.commit()

    # Get unused knowledge from last 30 days
    unused = db.get_unused_knowledge(days=30)

    # Should only return the recent unused item
    assert len(unused) == 1
    assert unused[0]["id"] == unused_id
    assert unused[0]["author"] == "alice"
    assert unused[0]["content"] == "Unused recent"


def test_pipeline_integration(db):
    """Test full pipeline flow: generate content, link knowledge, query lineage."""
    # Simulate pipeline: create knowledge, content, and links
    now = datetime.now(timezone.utc)

    # Add knowledge from trend context
    trend_items = []
    for i in range(3):
        cursor = db.conn.execute(
            "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
            ("curated_x", f"trend{i}", f"author{i}", f"Trend knowledge {i}", 1)
        )
        trend_items.append(cursor.lastrowid)

    # Generate content
    cursor = db.conn.execute(
        "INSERT INTO generated_content (content_type, content, eval_score, published, published_at) VALUES (?, ?, ?, ?, ?)",
        ("x_thread", "Thread about trends", 8.5, 1, now.isoformat())
    )
    content_id = cursor.lastrowid

    # Link knowledge (simulating pipeline behavior: default relevance 0.3 for trend items)
    knowledge_ids = [(kid, 0.3) for kid in trend_items]
    db.insert_content_knowledge_links(content_id, knowledge_ids)

    # Add engagement
    cursor = db.conn.execute(
        "INSERT INTO post_engagement (content_id, tweet_id, engagement_score, fetched_at) VALUES (?, ?, ?, ?)",
        (content_id, "thread_tweet", 12.0, now.isoformat())
    )
    db.conn.commit()

    # Verify lineage
    lineage = db.get_content_lineage(content_id)
    assert len(lineage) == 3
    for item in lineage:
        assert item["relevance_score"] == 0.3
        assert item["source_type"] == "curated_x"

    # Verify usage stats
    stats = db.get_knowledge_usage_stats(days=30)
    assert len(stats) == 3
    for stat in stats:
        assert stat["usage_count"] == 1
        assert stat["avg_engagement"] == 12.0
        assert stat["avg_relevance"] == 0.3
