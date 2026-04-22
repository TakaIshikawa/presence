"""Tests for source quality scoring system."""

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from storage.db import Database
from knowledge.source_scorer import SourceScorer, SourceScore


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
            auto_quality TEXT,
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


def test_compute_scores_empty(db):
    """Test compute_scores with no data returns empty list."""
    scorer = SourceScorer(db)
    scores = scorer.compute_scores(days=90, min_uses=2)
    assert scores == []


def test_compute_scores_basic(db):
    """Test basic score computation with varied engagement."""
    now = datetime.now(timezone.utc).isoformat()

    # Create knowledge from two sources
    # Alice - high engagement, high hit rate
    for i in range(5):
        cursor = db.conn.execute(
            "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
            ("curated_x", f"alice_{i}", "alice", f"Alice content {i}", 1)
        )
        k_id = cursor.lastrowid

        cursor = db.conn.execute(
            "INSERT INTO generated_content (content_type, content, eval_score, published, published_at, auto_quality) VALUES (?, ?, ?, ?, ?, ?)",
            ("x_post", f"Post {i}", 8.0, 1, now, "resonated")
        )
        c_id = cursor.lastrowid

        db.conn.execute(
            "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
            (c_id, k_id, 0.7)
        )

        db.conn.execute(
            "INSERT INTO post_engagement (content_id, tweet_id, engagement_score, fetched_at) VALUES (?, ?, ?, ?)",
            (c_id, f"tweet_a{i}", 15.0, now)
        )

    # Bob - lower engagement, lower hit rate
    for i in range(5):
        cursor = db.conn.execute(
            "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
            ("curated_x", f"bob_{i}", "bob", f"Bob content {i}", 1)
        )
        k_id = cursor.lastrowid

        cursor = db.conn.execute(
            "INSERT INTO generated_content (content_type, content, eval_score, published, published_at, auto_quality) VALUES (?, ?, ?, ?, ?, ?)",
            ("x_post", f"Post b{i}", 8.0, 1, now, "low_resonance" if i < 4 else "resonated")
        )
        c_id = cursor.lastrowid

        db.conn.execute(
            "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
            (c_id, k_id, 0.6)
        )

        db.conn.execute(
            "INSERT INTO post_engagement (content_id, tweet_id, engagement_score, fetched_at) VALUES (?, ?, ?, ?)",
            (c_id, f"tweet_b{i}", 5.0, now)
        )

    db.conn.commit()

    scorer = SourceScorer(db)
    scores = scorer.compute_scores(days=90, min_uses=2)

    assert len(scores) == 2

    # Alice should be first (higher quality score)
    assert scores[0].author == "alice"
    assert scores[0].source_type == "curated_x"
    assert scores[0].usage_count == 5
    assert scores[0].avg_engagement == 15.0
    assert scores[0].hit_rate == 1.0  # 5/5 resonated
    assert scores[0].quality_score > scores[1].quality_score

    # Bob should be second
    assert scores[1].author == "bob"
    assert scores[1].usage_count == 5
    assert scores[1].avg_engagement == 5.0
    assert scores[1].hit_rate == 0.2  # 1/5 resonated


def test_tier_assignment(db):
    """Test gold/silver/bronze tier assignment based on percentiles."""
    now = datetime.now(timezone.utc).isoformat()

    # Create 10 sources with varying quality
    for author_idx in range(10):
        author = f"author{author_idx}"
        engagement = 10.0 - author_idx  # Decreasing engagement
        resonated_ratio = (10 - author_idx) / 10.0  # Decreasing hit rate

        for i in range(3):
            cursor = db.conn.execute(
                "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
                ("curated_x", f"{author}_{i}", author, f"Content {i}", 1)
            )
            k_id = cursor.lastrowid

            auto_quality = "resonated" if i < int(resonated_ratio * 3) else "low_resonance"
            cursor = db.conn.execute(
                "INSERT INTO generated_content (content_type, content, eval_score, published, published_at, auto_quality) VALUES (?, ?, ?, ?, ?, ?)",
                ("x_post", f"Post {author_idx}_{i}", 8.0, 1, now, auto_quality)
            )
            c_id = cursor.lastrowid

            db.conn.execute(
                "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
                (c_id, k_id, 0.7)
            )

            db.conn.execute(
                "INSERT INTO post_engagement (content_id, tweet_id, engagement_score, fetched_at) VALUES (?, ?, ?, ?)",
                (c_id, f"tweet_{author_idx}_{i}", engagement, now)
            )

    db.conn.commit()

    scorer = SourceScorer(db)
    scores = scorer.compute_scores(days=90, min_uses=2)

    assert len(scores) == 10

    # Check tier distribution
    tier_counts = {'gold': 0, 'silver': 0, 'bronze': 0}
    for score in scores:
        tier_counts[score.tier] += 1

    # Top 20% (2 sources) should be gold
    assert tier_counts['gold'] == 2
    # 20-60% (4 sources) should be silver
    assert tier_counts['silver'] == 4
    # Bottom 40% (4 sources) should be bronze
    assert tier_counts['bronze'] == 4

    # Verify gold tier has highest quality scores
    gold_scores = [s for s in scores if s.tier == 'gold']
    bronze_scores = [s for s in scores if s.tier == 'bronze']

    assert all(g.quality_score > b.quality_score for g in gold_scores for b in bronze_scores)


def test_quality_score_weighting(db):
    """Test that quality_score uses 60% engagement, 40% hit rate weighting."""
    now = datetime.now(timezone.utc).isoformat()

    # Source A: High engagement (10.0), low hit rate (0.0)
    for i in range(5):
        cursor = db.conn.execute(
            "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
            ("curated_x", f"source_a_{i}", "source_a", f"Content {i}", 1)
        )
        k_id = cursor.lastrowid

        cursor = db.conn.execute(
            "INSERT INTO generated_content (content_type, content, eval_score, published, published_at, auto_quality) VALUES (?, ?, ?, ?, ?, ?)",
            ("x_post", f"Post a{i}", 8.0, 1, now, "low_resonance")
        )
        c_id = cursor.lastrowid

        db.conn.execute(
            "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
            (c_id, k_id, 0.7)
        )

        db.conn.execute(
            "INSERT INTO post_engagement (content_id, tweet_id, engagement_score, fetched_at) VALUES (?, ?, ?, ?)",
            (c_id, f"tweet_a{i}", 10.0, now)
        )

    # Source B: Low engagement (0.0), high hit rate (1.0)
    for i in range(5):
        cursor = db.conn.execute(
            "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
            ("curated_x", f"source_b_{i}", "source_b", f"Content {i}", 1)
        )
        k_id = cursor.lastrowid

        cursor = db.conn.execute(
            "INSERT INTO generated_content (content_type, content, eval_score, published, published_at, auto_quality) VALUES (?, ?, ?, ?, ?, ?)",
            ("x_post", f"Post b{i}", 8.0, 1, now, "resonated")
        )
        c_id = cursor.lastrowid

        db.conn.execute(
            "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
            (c_id, k_id, 0.7)
        )

        db.conn.execute(
            "INSERT INTO post_engagement (content_id, tweet_id, engagement_score, fetched_at) VALUES (?, ?, ?, ?)",
            (c_id, f"tweet_b{i}", 0.0, now)
        )

    db.conn.commit()

    scorer = SourceScorer(db)
    scores = scorer.compute_scores(days=90, min_uses=2)

    assert len(scores) == 2

    # Source A: normalized_eng=1.0 (max), hit_rate=0.0 → score = 0.6*1.0 + 0.4*0.0 = 0.6
    # Source B: normalized_eng=0.0 (min), hit_rate=1.0 → score = 0.6*0.0 + 0.4*1.0 = 0.4
    # So source A should rank higher despite lower hit rate

    assert scores[0].author == "source_a"
    assert scores[0].quality_score == pytest.approx(0.6, abs=0.01)

    assert scores[1].author == "source_b"
    assert scores[1].quality_score == pytest.approx(0.4, abs=0.01)


def test_hit_rate_calculation(db):
    """Test hit_rate calculation with mix of resonated/low_resonance posts."""
    now = datetime.now(timezone.utc).isoformat()

    # Create 10 posts from same source:
    # - 3 resonated
    # - 5 low_resonance
    # - 2 unclassified (NULL auto_quality)
    # Expected hit_rate = 3/8 = 0.375

    for i in range(10):
        cursor = db.conn.execute(
            "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
            ("curated_x", f"test_{i}", "test_source", f"Content {i}", 1)
        )
        k_id = cursor.lastrowid

        if i < 3:
            auto_quality = "resonated"
        elif i < 8:
            auto_quality = "low_resonance"
        else:
            auto_quality = None  # Unclassified

        cursor = db.conn.execute(
            "INSERT INTO generated_content (content_type, content, eval_score, published, published_at, auto_quality) VALUES (?, ?, ?, ?, ?, ?)",
            ("x_post", f"Post {i}", 8.0, 1, now, auto_quality)
        )
        c_id = cursor.lastrowid

        db.conn.execute(
            "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
            (c_id, k_id, 0.7)
        )

        db.conn.execute(
            "INSERT INTO post_engagement (content_id, tweet_id, engagement_score, fetched_at) VALUES (?, ?, ?, ?)",
            (c_id, f"tweet_{i}", 5.0, now)
        )

    db.conn.commit()

    scorer = SourceScorer(db)
    scores = scorer.compute_scores(days=90, min_uses=2)

    assert len(scores) == 1
    assert scores[0].hit_rate == pytest.approx(3.0 / 8.0, abs=0.01)


def test_get_source_tier(db):
    """Test get_source_tier lookup functionality."""
    now = datetime.now(timezone.utc).isoformat()

    # Create sources with different qualities
    for author_idx in range(5):
        author = f"author{author_idx}"
        engagement = 10.0 - author_idx * 2

        for i in range(3):
            cursor = db.conn.execute(
                "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
                ("curated_x", f"{author}_{i}", author, f"Content {i}", 1)
            )
            k_id = cursor.lastrowid

            cursor = db.conn.execute(
                "INSERT INTO generated_content (content_type, content, eval_score, published, published_at, auto_quality) VALUES (?, ?, ?, ?, ?, ?)",
                ("x_post", f"Post {author_idx}_{i}", 8.0, 1, now, "resonated")
            )
            c_id = cursor.lastrowid

            db.conn.execute(
                "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
                (c_id, k_id, 0.7)
            )

            db.conn.execute(
                "INSERT INTO post_engagement (content_id, tweet_id, engagement_score, fetched_at) VALUES (?, ?, ?, ?)",
                (c_id, f"tweet_{author_idx}_{i}", engagement, now)
            )

    db.conn.commit()

    scorer = SourceScorer(db)

    # Test lookup (should compute scores first time)
    tier = scorer.get_source_tier("author0", "curated_x")
    assert tier == "gold"

    tier = scorer.get_source_tier("author4", "curated_x")
    assert tier == "bronze"

    # Test non-existent source
    tier = scorer.get_source_tier("nonexistent", "curated_x")
    assert tier is None


def test_generate_retrieval_boost_context(db):
    """Test generate_retrieval_boost_context output format."""
    now = datetime.now(timezone.utc).isoformat()

    # Create 6 sources to ensure proper tier distribution
    # Top 20% (1-2) = gold, 20-60% (3-4) = silver, bottom 40% (5-6) = bronze
    sources = [
        ("gold_author", "curated_x", 20.0, "resonated"),
        ("silver_author", "curated_x", 12.0, "resonated"),
        ("mid_author", "curated_x", 8.0, "resonated"),
        ("bronze_x", "curated_x", 5.0, "low_resonance"),
        ("bronze_blog", "curated_article", 3.0, "low_resonance"),
        ("lowest_author", "curated_x", 1.0, "low_resonance"),
    ]

    for author, source_type, engagement, quality in sources:
        for i in range(3):
            cursor = db.conn.execute(
                "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
                (source_type, f"{author}_{i}", author, f"Content {i}", 1)
            )
            k_id = cursor.lastrowid

            cursor = db.conn.execute(
                "INSERT INTO generated_content (content_type, content, eval_score, published, published_at, auto_quality) VALUES (?, ?, ?, ?, ?, ?)",
                ("x_post", f"Post {author}_{i}", 8.0, 1, now, quality)
            )
            c_id = cursor.lastrowid

            db.conn.execute(
                "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
                (c_id, k_id, 0.7)
            )

            db.conn.execute(
                "INSERT INTO post_engagement (content_id, tweet_id, engagement_score, fetched_at) VALUES (?, ?, ?, ?)",
                (c_id, f"tweet_{author}{i}", engagement, now)
            )

    db.conn.commit()

    scorer = SourceScorer(db)
    context = scorer.generate_retrieval_boost_context(days=90)

    assert "Gold-tier sources" in context
    assert "@gold_author" in context  # X account should have @ prefix
    assert "bronze_blog" in context  # Article should not have @ prefix
    assert "Bronze-tier sources" in context
    assert "consistently drive engagement" in context
    assert "low engagement correlation" in context


def test_generate_retrieval_boost_context_empty(db):
    """Test generate_retrieval_boost_context returns empty string with no data."""
    scorer = SourceScorer(db)
    context = scorer.generate_retrieval_boost_context(days=90)
    assert context == ""


def test_get_source_engagement_details(db):
    """Test get_source_engagement_details DB query method."""
    now = datetime.now(timezone.utc).isoformat()

    # Create test data
    for i in range(4):
        cursor = db.conn.execute(
            "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
            ("curated_x", f"test_{i}", "test_author", f"Content {i}", 1)
        )
        k_id = cursor.lastrowid

        auto_quality = "resonated" if i < 2 else "low_resonance"
        cursor = db.conn.execute(
            "INSERT INTO generated_content (content_type, content, eval_score, published, published_at, auto_quality) VALUES (?, ?, ?, ?, ?, ?)",
            ("x_post", f"Post {i}", 8.0, 1, now, auto_quality)
        )
        c_id = cursor.lastrowid

        db.conn.execute(
            "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
            (c_id, k_id, 0.7)
        )

        db.conn.execute(
            "INSERT INTO post_engagement (content_id, tweet_id, engagement_score, fetched_at) VALUES (?, ?, ?, ?)",
            (c_id, f"tweet_{i}", 10.0, now)
        )

    db.conn.commit()

    # Query source engagement details
    details = db.get_source_engagement_details(days=90, min_uses=2)

    assert len(details) == 1
    assert details[0]['author'] == 'test_author'
    assert details[0]['source_type'] == 'curated_x'
    assert details[0]['total_uses'] == 4
    assert details[0]['avg_engagement'] == 10.0
    assert details[0]['resonated_count'] == 2
    assert details[0]['classified_count'] == 4


def test_min_uses_filter(db):
    """Test that min_uses parameter filters out low-usage sources."""
    now = datetime.now(timezone.utc).isoformat()

    # Source with 1 use
    cursor = db.conn.execute(
        "INSERT INTO knowledge (source_type, source_id, author, content, approved) VALUES (?, ?, ?, ?, ?)",
        ("curated_x", "single_use", "single_author", "Content", 1)
    )
    k_id = cursor.lastrowid

    cursor = db.conn.execute(
        "INSERT INTO generated_content (content_type, content, eval_score, published, published_at, auto_quality) VALUES (?, ?, ?, ?, ?, ?)",
        ("x_post", "Post", 8.0, 1, now, "resonated")
    )
    c_id = cursor.lastrowid

    db.conn.execute(
        "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
        (c_id, k_id, 0.7)
    )

    db.conn.execute(
        "INSERT INTO post_engagement (content_id, tweet_id, engagement_score, fetched_at) VALUES (?, ?, ?, ?)",
        (c_id, "tweet", 10.0, now)
    )

    db.conn.commit()

    scorer = SourceScorer(db)

    # With min_uses=2, should return empty
    scores = scorer.compute_scores(days=90, min_uses=2)
    assert len(scores) == 0

    # With min_uses=1, should return the source
    scores = scorer.compute_scores(days=90, min_uses=1)
    assert len(scores) == 1
    assert scores[0].author == "single_author"


def test_compute_scores_with_freshness_boost(db):
    """Fresh source timestamps can boost score without disabling quality ranking."""
    db.conn.execute("ALTER TABLE knowledge ADD COLUMN published_at TEXT")
    db.conn.execute("ALTER TABLE knowledge ADD COLUMN ingested_at TEXT")
    now = datetime.now(timezone.utc)

    sources = [
        ("evergreen", 20.0, "low_resonance", now - timedelta(days=45)),
        ("fresh", 10.0, "resonated", now),
    ]
    for author, engagement, quality, published_at in sources:
        for i in range(3):
            cursor = db.conn.execute(
                """INSERT INTO knowledge
                   (source_type, source_id, author, content, approved, published_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    "curated_x",
                    f"{author}_{i}",
                    author,
                    f"Content {i}",
                    1,
                    published_at.isoformat(),
                )
            )
            k_id = cursor.lastrowid
            cursor = db.conn.execute(
                """INSERT INTO generated_content
                   (content_type, content, eval_score, published, published_at, auto_quality)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                ("x_post", f"Post {author}_{i}", 8.0, 1, now.isoformat(), quality)
            )
            c_id = cursor.lastrowid
            db.conn.execute(
                "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, ?)",
                (c_id, k_id, 0.7)
            )
            db.conn.execute(
                "INSERT INTO post_engagement (content_id, tweet_id, engagement_score, fetched_at) VALUES (?, ?, ?, ?)",
                (c_id, f"tweet_{author}_{i}", engagement, now.isoformat())
            )

    db.conn.commit()

    scorer = SourceScorer(db)
    disabled = scorer.compute_scores(days=90, min_uses=2)
    enabled = scorer.compute_scores(days=90, min_uses=2, freshness_half_life_days=14)

    assert [score.author for score in disabled] == ["evergreen", "fresh"]
    assert enabled[0].author == "fresh"
    assert enabled[0].freshness_score > enabled[1].freshness_score
