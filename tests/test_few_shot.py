"""Tests for few-shot example selection (synthesis/few_shot.py)."""

import pytest

from synthesis.few_shot import FewShotSelector, FewShotExample, _has_stale_pattern


# --- Helpers ---


def _insert_published_post(db, content, eval_score=7.0, content_type="x_post",
                           curation_quality=None, auto_quality=None, post_id=None):
    """Insert a published post into generated_content. Returns the row id."""
    cursor = db.conn.execute(
        """INSERT INTO generated_content
           (content_type, content, eval_score, published, tweet_id,
            curation_quality, auto_quality)
           VALUES (?, ?, ?, 1, ?, ?, ?)""",
        (content_type, content, eval_score, f"tweet_{post_id or 'auto'}",
         curation_quality, auto_quality),
    )
    return cursor.lastrowid


def _insert_engagement(db, content_id, engagement_score, fetched_at="2025-01-01T00:00:00",
                       like_count=0, retweet_count=0, reply_count=0, quote_count=0):
    """Insert an engagement record for a post."""
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, engagement_score, like_count, retweet_count,
            reply_count, quote_count, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (content_id, f"tweet_{content_id}", engagement_score,
         like_count, retweet_count, reply_count, quote_count, fetched_at),
    )


# --- FewShotExample dataclass ---


class TestFewShotExample:
    def test_fields(self):
        ex = FewShotExample(content="hello world", engagement_score=4.2)
        assert ex.content == "hello world"
        assert ex.engagement_score == 4.2

    def test_zero_engagement_score(self):
        ex = FewShotExample(content="cold start post", engagement_score=0.0)
        assert ex.engagement_score == 0.0


# --- Stale pattern filtering ---


class TestStalePatternFiltering:
    """Verify each regex in _STALE_PATTERNS correctly rejects matching content."""

    def test_starts_with_ai(self):
        assert _has_stale_pattern("AI is transforming everything") is True

    def test_starts_with_ai_case_insensitive(self):
        assert _has_stale_pattern("ai models are getting better") is True

    def test_ai_mid_sentence_not_matched(self):
        # Pattern requires ^AI\s — only at start of string
        assert _has_stale_pattern("The AI model works well") is False

    def test_isnt_about_its_about(self):
        assert _has_stale_pattern(
            "Engineering isn't about writing code—it's about solving problems"
        ) is True

    def test_isnt_about_its_about_with_dash(self):
        assert _has_stale_pattern(
            "Success isn't about perfection-it's about progress"
        ) is True

    def test_breakthrough(self):
        assert _has_stale_pattern("This is a breakthrough in AI") is True

    def test_breakthrough_case_insensitive(self):
        assert _has_stale_pattern("BREAKTHROUGH discovery in science") is True

    def test_perfect_prompts(self):
        assert _has_stale_pattern("You don't need perfect prompts") is True

    def test_perfect_memory(self):
        assert _has_stale_pattern("No system has perfect memory") is True

    def test_perfect_agents(self):
        assert _has_stale_pattern("There are no perfect agents") is True

    def test_perfect_handoffs(self):
        assert _has_stale_pattern("Forget about perfect handoffs") is True

    def test_perfect_context(self):
        assert _has_stale_pattern("You won't get perfect context") is True

    def test_commits_across_pattern(self):
        assert _has_stale_pattern("47 commits across 3 repos today") is True

    def test_todays_insight(self):
        assert _has_stale_pattern("Today's insight on building agents") is True

    def test_todays_breakthrough(self):
        assert _has_stale_pattern("Today's breakthrough in memory systems") is True

    def test_todays_lesson(self):
        assert _has_stale_pattern("Today's lesson about error handling") is True

    def test_tweet_prefix_todays_insight(self):
        assert _has_stale_pattern("TWEET 1:\nToday's insight on testing") is True

    def test_clean_content_not_flagged(self):
        assert _has_stale_pattern(
            "Spent the morning debugging a race condition in the event loop"
        ) is False

    def test_empty_string_not_flagged(self):
        assert _has_stale_pattern("") is False


# --- select() with engagement data ---


class TestSelectWithEngagement:
    """Test get_examples() when engagement data exists."""

    def test_returns_examples_ranked_by_engagement(self, db):
        ids = []
        for i, score in enumerate([5.0, 9.0, 7.0]):
            pid = _insert_published_post(db, f"Post {i}", post_id=i)
            _insert_engagement(db, pid, engagement_score=score)
            ids.append(pid)

        selector = FewShotSelector(db)
        examples = selector.get_examples(limit=3)

        assert len(examples) == 3
        # Highest engagement first
        assert examples[0].engagement_score == 9.0
        assert examples[1].engagement_score == 7.0
        assert examples[2].engagement_score == 5.0

    def test_returns_few_shot_example_instances(self, db):
        pid = _insert_published_post(db, "Good post")
        _insert_engagement(db, pid, engagement_score=8.5)

        selector = FewShotSelector(db)
        examples = selector.get_examples(limit=1)

        assert len(examples) == 1
        assert isinstance(examples[0], FewShotExample)
        assert examples[0].content == "Good post"
        assert examples[0].engagement_score == 8.5

    def test_respects_limit(self, db):
        for i in range(5):
            pid = _insert_published_post(db, f"Post {i}", post_id=i)
            _insert_engagement(db, pid, engagement_score=float(i))

        selector = FewShotSelector(db)
        examples = selector.get_examples(limit=2)

        assert len(examples) == 2

    def test_excludes_ids(self, db):
        pid1 = _insert_published_post(db, "Keep this", post_id=1)
        pid2 = _insert_published_post(db, "Exclude this", post_id=2)
        _insert_engagement(db, pid1, engagement_score=5.0)
        _insert_engagement(db, pid2, engagement_score=10.0)

        selector = FewShotSelector(db)
        examples = selector.get_examples(exclude_ids={pid2})

        assert len(examples) == 1
        assert examples[0].content == "Keep this"

    def test_filters_stale_patterns(self, db):
        pid_stale = _insert_published_post(db, "AI is the future of everything", post_id=1)
        pid_good = _insert_published_post(db, "Debugged a tricky race condition today", post_id=2)
        _insert_engagement(db, pid_stale, engagement_score=10.0)
        _insert_engagement(db, pid_good, engagement_score=3.0)

        selector = FewShotSelector(db)
        examples = selector.get_examples(limit=5)

        assert len(examples) == 1
        assert examples[0].content == "Debugged a tricky race condition today"

    def test_filters_too_specific_posts(self, db):
        pid = _insert_published_post(
            db, "A very niche post", curation_quality="too_specific", post_id=1
        )
        _insert_engagement(db, pid, engagement_score=10.0)

        pid_good = _insert_published_post(db, "A broadly relevant post", post_id=2)
        _insert_engagement(db, pid_good, engagement_score=2.0)

        selector = FewShotSelector(db)
        examples = selector.get_examples(limit=5)

        # too_specific is filtered by the SQL query in get_top_performing_posts
        assert len(examples) == 1
        assert examples[0].content == "A broadly relevant post"

    def test_filters_low_resonance_posts(self, db):
        pid = _insert_published_post(
            db, "Low engagement post", auto_quality="low_resonance", post_id=1
        )
        _insert_engagement(db, pid, engagement_score=10.0)

        pid_good = _insert_published_post(db, "Resonant post", post_id=2)
        _insert_engagement(db, pid_good, engagement_score=2.0)

        selector = FewShotSelector(db)
        examples = selector.get_examples(limit=5)

        assert len(examples) == 1
        assert examples[0].content == "Resonant post"

    def test_content_type_filtering(self, db):
        pid_post = _insert_published_post(db, "A tweet", content_type="x_post", post_id=1)
        pid_thread = _insert_published_post(db, "A thread", content_type="x_thread", post_id=2)
        _insert_engagement(db, pid_post, engagement_score=5.0)
        _insert_engagement(db, pid_thread, engagement_score=8.0)

        selector = FewShotSelector(db)

        post_examples = selector.get_examples(content_type="x_post")
        assert len(post_examples) == 1
        assert post_examples[0].content == "A tweet"

        thread_examples = selector.get_examples(content_type="x_thread")
        assert len(thread_examples) == 1
        assert thread_examples[0].content == "A thread"


# --- Fallback to eval score ---


class TestFallbackScoring:
    """Test fallback when no engagement data exists."""

    def test_falls_back_to_eval_score_when_no_engagement(self, db):
        _insert_published_post(db, "Low eval", eval_score=5.0, post_id=1)
        _insert_published_post(db, "High eval", eval_score=9.0, post_id=2)
        _insert_published_post(db, "Mid eval", eval_score=7.0, post_id=3)

        selector = FewShotSelector(db)
        examples = selector.get_examples(limit=3)

        assert len(examples) == 3
        # Ordered by eval_score DESC
        assert examples[0].content == "High eval"
        assert examples[1].content == "Mid eval"
        assert examples[2].content == "Low eval"

    def test_fallback_sets_engagement_score_to_zero(self, db):
        _insert_published_post(db, "Eval only post", eval_score=8.0, post_id=1)

        selector = FewShotSelector(db)
        examples = selector.get_examples(limit=1)

        assert len(examples) == 1
        assert examples[0].engagement_score == 0.0

    def test_fallback_excludes_too_specific(self, db):
        _insert_published_post(
            db, "Niche post", eval_score=9.0, curation_quality="too_specific", post_id=1
        )
        _insert_published_post(db, "Good post", eval_score=5.0, post_id=2)

        selector = FewShotSelector(db)
        examples = selector.get_examples(limit=5)

        assert len(examples) == 1
        assert examples[0].content == "Good post"

    def test_fallback_excludes_stale_patterns(self, db):
        _insert_published_post(
            db, "Today's insight on building tools", eval_score=9.0, post_id=1
        )
        _insert_published_post(db, "Clean post about debugging", eval_score=5.0, post_id=2)

        selector = FewShotSelector(db)
        examples = selector.get_examples(limit=5)

        assert len(examples) == 1
        assert examples[0].content == "Clean post about debugging"

    def test_fallback_respects_limit(self, db):
        for i in range(5):
            _insert_published_post(db, f"Post {i}", eval_score=float(i), post_id=i)

        selector = FewShotSelector(db)
        examples = selector.get_examples(limit=2)

        assert len(examples) == 2

    def test_fallback_excludes_ids(self, db):
        pid1 = _insert_published_post(db, "Keep", eval_score=5.0, post_id=1)
        pid2 = _insert_published_post(db, "Exclude", eval_score=9.0, post_id=2)

        selector = FewShotSelector(db)
        examples = selector.get_examples(exclude_ids={pid2})

        assert len(examples) == 1
        assert examples[0].content == "Keep"

    def test_fallback_only_includes_published_posts(self, db):
        _insert_published_post(db, "Published post", eval_score=7.0, post_id=1)
        # Insert an unpublished post directly
        db.conn.execute(
            """INSERT INTO generated_content
               (content_type, content, eval_score, published)
               VALUES (?, ?, ?, 0)""",
            ("x_post", "Unpublished post", 9.0),
        )

        selector = FewShotSelector(db)
        examples = selector.get_examples(limit=5)

        assert len(examples) == 1
        assert examples[0].content == "Published post"


# --- Empty database ---


class TestEmptyDatabase:
    def test_returns_empty_list(self, db):
        selector = FewShotSelector(db)
        examples = selector.get_examples()
        assert examples == []

    def test_returns_empty_list_with_limit(self, db):
        selector = FewShotSelector(db)
        examples = selector.get_examples(limit=10)
        assert examples == []

    def test_returns_empty_list_with_exclude_ids(self, db):
        selector = FewShotSelector(db)
        examples = selector.get_examples(exclude_ids={1, 2, 3})
        assert examples == []


# --- format_examples ---


class TestFormatExamples:
    def test_format_numbered_list(self):
        examples = [
            FewShotExample(content="First post", engagement_score=5.0),
            FewShotExample(content="Second post", engagement_score=3.0),
        ]
        selector = FewShotSelector(db=None)
        result = selector.format_examples(examples)

        assert result == "1. First post\n\n2. Second post"

    def test_format_single_example(self):
        examples = [FewShotExample(content="Only post", engagement_score=1.0)]
        selector = FewShotSelector(db=None)
        result = selector.format_examples(examples)

        assert result == "1. Only post"

    def test_format_empty_list(self):
        selector = FewShotSelector(db=None)
        result = selector.format_examples([])

        assert result == ""


# --- Edge cases ---


class TestEdgeCases:
    def test_all_posts_are_stale(self, db):
        """When all engagement posts match stale patterns, falls back to eval score."""
        pid = _insert_published_post(db, "AI is changing everything", post_id=1)
        _insert_engagement(db, pid, engagement_score=10.0)

        # Add a clean post with eval score only (no engagement)
        _insert_published_post(db, "Solid engineering post", eval_score=7.0, post_id=2)

        selector = FewShotSelector(db)
        examples = selector.get_examples(limit=3)

        # Engagement path yields nothing after stale filter -> fallback kicks in
        assert len(examples) == 1
        assert examples[0].content == "Solid engineering post"

    def test_all_engagement_posts_excluded_by_id(self, db):
        """When all engagement posts are excluded, falls back to eval score."""
        pid = _insert_published_post(db, "Excluded post", post_id=1)
        _insert_engagement(db, pid, engagement_score=10.0)

        _insert_published_post(db, "Fallback post", eval_score=6.0, post_id=2)

        selector = FewShotSelector(db)
        examples = selector.get_examples(exclude_ids={pid})

        assert len(examples) == 1
        assert examples[0].content == "Fallback post"

    def test_uses_latest_engagement_snapshot(self, db):
        """When multiple engagement records exist, the latest one is used."""
        pid = _insert_published_post(db, "Evolving post", post_id=1)
        _insert_engagement(db, pid, engagement_score=2.0, fetched_at="2025-01-01T00:00:00")
        _insert_engagement(db, pid, engagement_score=8.0, fetched_at="2025-01-02T00:00:00")

        selector = FewShotSelector(db)
        examples = selector.get_examples(limit=1)

        assert len(examples) == 1
        assert examples[0].engagement_score == 8.0
