"""Tests for stale rhetorical pattern detection in few-shot filtering."""

import pytest

from synthesis.few_shot import _has_stale_pattern, FewShotSelector, FewShotExample
from synthesis.pipeline import SynthesisPipeline


# ---------------------------------------------------------------------------
# Helper: run the same text against both the few_shot module-level patterns
# and the pipeline class-level patterns, verifying they agree.
# ---------------------------------------------------------------------------


def _both_detect(text: str) -> bool:
    """Return True only if BOTH pattern lists flag the text as stale."""
    few_shot_hit = _has_stale_pattern(text)
    pipeline_hit = any(p.search(text) for p in SynthesisPipeline.STALE_PATTERNS)
    assert few_shot_hit == pipeline_hit, (
        f"Pattern lists diverge on: {text!r} "
        f"(few_shot={few_shot_hit}, pipeline={pipeline_hit})"
    )
    return few_shot_hit


# ===========================================================================
# 1. Unpopular opinion / Controversial take
# ===========================================================================


class TestUnpopularOpinionPattern:
    @pytest.mark.parametrize("text", [
        "Unpopular opinion: most AI wrappers are fine",
        "unpopular opinion - LLMs peaked last year",
        "Unpopular opinion — nobody cares about your framework",
        "UNPOPULAR OPINION: hot takes only",
        "Controversial take: TypeScript is overrated",
        "controversial take - testing is a waste of time",
        "Controversial take — microservices hurt more than they help",
    ])
    def test_matches(self, text):
        assert _both_detect(text)

    @pytest.mark.parametrize("text", [
        "I have an unpopular opinion about error handling",
        "That's a controversial take on caching strategies",
        "My opinion is unpopular among backend devs",
    ])
    def test_no_false_positive(self, text):
        assert not _both_detect(text)


# ===========================================================================
# 2. Nobody talks about / Nobody is talking about
# ===========================================================================


class TestNobodyTalksAboutPattern:
    @pytest.mark.parametrize("text", [
        "Nobody talks about the cost of context switching",
        "Nobody is talking about how fragile CI pipelines are",
        "nobody mentions the memory overhead",
        "Nobody talks about error budgets enough",
    ])
    def test_matches(self, text):
        assert _both_detect(text)

    @pytest.mark.parametrize("text", [
        "I noticed nobody at the standup raised the latency issue",
        "The talk about distributed systems was great",
    ])
    def test_no_false_positive(self, text):
        assert not _both_detect(text)


# ===========================================================================
# 3. The secret to / The trick to
# ===========================================================================


class TestSecretTrickPattern:
    @pytest.mark.parametrize("text", [
        "The secret to good prompts is specificity",
        "The trick to fast deploys is caching layers",
        "THE SECRET TO reliable agents is structured output",
        "The trick to debugging race conditions",
    ])
    def test_matches(self, text):
        assert _both_detect(text)

    @pytest.mark.parametrize("text", [
        "I learned the secret to this module's behavior by reading the source",
        "There's a neat trick to rebase without conflicts",
        "Discovered the secret behind the flaky test",
    ])
    def test_no_false_positive(self, text):
        assert not _both_detect(text)


# ===========================================================================
# 4. Stop doing X. Start doing Y.
# ===========================================================================


class TestStopStartPattern:
    @pytest.mark.parametrize("text", [
        "Stop writing unit tests. Start writing integration tests.",
        "Stop using REST. Start using GraphQL.",
        "Stop chasing metrics. Start shipping value.",
        "stop refactoring everything. start shipping.",
    ])
    def test_matches(self, text):
        assert _both_detect(text)

    @pytest.mark.parametrize("text", [
        "We had to stop the deploy and start the rollback",
        "I decided to stop and rethink the architecture",
        "The service will start after the migration completes",
    ])
    def test_no_false_positive(self, text):
        assert not _both_detect(text)


# ===========================================================================
# 5. X is dead. Long live Y.
# ===========================================================================


class TestIsDeadLongLivePattern:
    @pytest.mark.parametrize("text", [
        "REST is dead. Long live GraphQL.",
        "Monoliths are dead. Long live microservices.",
        "jQuery is dead. Long live vanilla JS.",
        "OOP is dead. Long live functional programming.",
    ])
    def test_matches(self, text):
        assert _both_detect(text)

    @pytest.mark.parametrize("text", [
        "The process is dead after an OOM kill",
        "Long live the king of merge conflicts",
        "That branch is dead code we should remove",
    ])
    def test_no_false_positive(self, text):
        assert not _both_detect(text)


# ===========================================================================
# 6. I spent X hours/days/weeks (effort-brag framing)
# ===========================================================================


class TestEffortBragPattern:
    @pytest.mark.parametrize("text", [
        "I spent 10 hours debugging a single test",
        "I spent 3 days rewriting the auth module",
        "I spent 2 weeks building an agent framework",
        "I spent 6 months on this side project",
    ])
    def test_matches(self, text):
        assert _both_detect(text)

    @pytest.mark.parametrize("text", [
        "The team spent 3 days on the migration",
        "After I spent the afternoon pairing, we found the bug",
        "We spent 2 hours in a design review",
    ])
    def test_no_false_positive(self, text):
        assert not _both_detect(text)


# ===========================================================================
# 7. Most people don't / Most developers don't
# ===========================================================================


class TestMostPeopleDontPattern:
    @pytest.mark.parametrize("text", [
        "Most people don't understand event loops",
        "Most developers don't test their error paths",
        "Most devs don't read the docs",
        "Most engineers don't profile before optimizing",
        "most people don't realize how slow DNS can be",
    ])
    def test_matches(self, text):
        assert _both_detect(text)

    @pytest.mark.parametrize("text", [
        "I think most people would agree this API is clunky",
        "Most of the developers on our team prefer Rust",
        "Unlike most people, I enjoy writing Makefiles",
    ])
    def test_no_false_positive(self, text):
        assert not _both_detect(text)


# ===========================================================================
# Existing patterns still work (regression)
# ===========================================================================


class TestExistingPatternsRegression:
    @pytest.mark.parametrize("text", [
        "AI is changing everything",
        "Coding isn't about syntax—it's about thinking",
        "This is a major breakthrough for LLMs",
        "perfect prompts are a myth",
        "42 commits across 8 repos",
        "Today's insight on agent design",
    ])
    def test_existing_patterns_still_match(self, text):
        assert _both_detect(text)


# ===========================================================================
# 8. Everyone says / Everyone thinks
# ===========================================================================


class TestEveryonePattern:
    @pytest.mark.parametrize("text", [
        "Everyone says AI will replace developers",
        "Everyone preaches microservices but nobody runs them well",
        "Everyone thinks testing is easy",
        "Everyone knows the basics but few master them",
        "Everyone believes in trunk-based development",
    ])
    def test_matches(self, text):
        assert _both_detect(text)

    @pytest.mark.parametrize("text", [
        "I asked everyone on the team about their preferences",
        "The feature works for everyone except Safari users",
        "Everyone at the standup agreed on the approach",
    ])
    def test_no_false_positive(self, text):
        assert not _both_detect(text)


# ===========================================================================
# Comprehensive stale pattern detection tests
# ===========================================================================


class TestStalePatternDetection:
    """Verify each category of stale pattern is caught."""

    def test_ai_prefix_pattern(self):
        assert _has_stale_pattern("AI is transforming everything")
        assert _has_stale_pattern("AI will change how we code")
        assert _has_stale_pattern("AI agents are the future")

    def test_isnt_about_its_about_pattern(self):
        assert _has_stale_pattern("Coding isn't about syntax—it's about thinking")
        assert _has_stale_pattern("This isn't about syntax - it's about clarity")
        assert _has_stale_pattern("Testing isn't about test coverage — it's about confidence")

    def test_unpopular_opinion_engagement_bait(self):
        assert _has_stale_pattern("Unpopular opinion: tests are overrated")
        assert _has_stale_pattern("Controversial take: microservices are harmful")

    def test_i_spent_effort_brag(self):
        assert _has_stale_pattern("I spent 3 hours debugging this")
        assert _has_stale_pattern("I spent 2 weeks building this framework")

    def test_most_people_dont_pattern(self):
        assert _has_stale_pattern("Most developers don't understand async")
        assert _has_stale_pattern("Most people don't test error paths")

    def test_everyone_pattern(self):
        assert _has_stale_pattern("Everyone thinks AI is magic")
        assert _has_stale_pattern("Everyone says unit tests are essential")

    def test_secret_trick_pattern(self):
        assert _has_stale_pattern("The secret to clean code")
        assert _has_stale_pattern("The trick to fast builds")

    def test_stop_start_pattern(self):
        assert _has_stale_pattern("Stop writing tests. Start writing types")
        assert _has_stale_pattern("Stop using REST. Start using GraphQL")

    def test_non_stale_text_returns_false(self):
        assert not _has_stale_pattern("Built a caching layer for our API today")
        assert not _has_stale_pattern("Refactored the auth module to use JWT")
        assert not _has_stale_pattern("Fixed a race condition in the queue processor")

    def test_case_insensitivity(self):
        assert _has_stale_pattern("AI IS AMAZING")
        assert _has_stale_pattern("ai is changing everything")
        assert _has_stale_pattern("UNPOPULAR OPINION: testing is waste")


# ===========================================================================
# FewShotSelector.get_examples with engagement data
# ===========================================================================


class TestFewShotSelectorWithEngagement:
    """Test FewShotSelector.get_examples when engagement data exists."""

    def test_returns_posts_ranked_by_engagement(self, db):
        """Verify get_examples returns posts ranked by engagement score."""
        selector = FewShotSelector(db)

        # Insert published posts with varying engagement
        post1_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Low engagement post",
            eval_score=7.0,
            eval_feedback="good"
        )
        db.mark_published(post1_id, "http://x.com/1", "tweet1")
        db.insert_engagement(post1_id, "tweet1", 5, 1, 0, 0, 2.0)

        post2_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="High engagement post",
            eval_score=6.0,
            eval_feedback="good"
        )
        db.mark_published(post2_id, "http://x.com/2", "tweet2")
        db.insert_engagement(post2_id, "tweet2", 50, 10, 5, 2, 25.0)

        post3_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Medium engagement post",
            eval_score=8.0,
            eval_feedback="good"
        )
        db.mark_published(post3_id, "http://x.com/3", "tweet3")
        db.insert_engagement(post3_id, "tweet3", 15, 3, 1, 0, 8.0)

        # Get examples - should be ranked by engagement, not eval_score
        examples = selector.get_examples(content_type="x_post", limit=3)

        assert len(examples) == 3
        assert examples[0].content == "High engagement post"
        assert examples[0].engagement_score == 25.0
        assert examples[1].content == "Medium engagement post"
        assert examples[1].engagement_score == 8.0
        assert examples[2].content == "Low engagement post"
        assert examples[2].engagement_score == 2.0

    def test_filters_stale_patterns(self, db):
        """Verify posts with stale patterns are excluded even with high engagement."""
        selector = FewShotSelector(db)

        # Insert post with high engagement but stale pattern
        stale_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="AI is transforming everything about development",
            eval_score=8.0,
            eval_feedback="good"
        )
        db.mark_published(stale_id, "http://x.com/stale", "tweet_stale")
        db.insert_engagement(stale_id, "tweet_stale", 100, 20, 10, 5, 50.0)

        # Insert normal post with lower engagement
        good_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Built a caching layer that reduced latency by 50%",
            eval_score=7.0,
            eval_feedback="good"
        )
        db.mark_published(good_id, "http://x.com/good", "tweet_good")
        db.insert_engagement(good_id, "tweet_good", 20, 5, 2, 1, 10.0)

        examples = selector.get_examples(content_type="x_post", limit=2)

        # Should only return the non-stale post
        assert len(examples) == 1
        assert examples[0].content == "Built a caching layer that reduced latency by 50%"

    def test_excludes_posts_via_exclude_ids(self, db):
        """Verify exclude_ids parameter filters out specified posts."""
        selector = FewShotSelector(db)

        post1_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="First post",
            eval_score=7.0,
            eval_feedback="good"
        )
        db.mark_published(post1_id, "http://x.com/1", "tweet1")
        db.insert_engagement(post1_id, "tweet1", 20, 5, 2, 1, 10.0)

        post2_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Second post - too specific",
            eval_score=8.0,
            eval_feedback="good"
        )
        db.mark_published(post2_id, "http://x.com/2", "tweet2")
        db.insert_engagement(post2_id, "tweet2", 30, 8, 3, 2, 15.0)
        db.set_curation_quality(post2_id, "too_specific")

        post3_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Third post",
            eval_score=6.0,
            eval_feedback="good"
        )
        db.mark_published(post3_id, "http://x.com/3", "tweet3")
        db.insert_engagement(post3_id, "tweet3", 25, 6, 2, 1, 12.0)

        # Exclude post2 via exclude_ids
        examples = selector.get_examples(
            content_type="x_post",
            limit=2,
            exclude_ids={post2_id}
        )

        # Should return post3 and post1, skipping post2
        assert len(examples) == 2
        assert examples[0].content == "Third post"
        assert examples[1].content == "First post"

    def test_respects_limit_parameter(self, db):
        """Verify exactly limit posts are returned when more exist."""
        selector = FewShotSelector(db)

        # Insert 5 posts
        for i in range(5):
            post_id = db.insert_generated_content(
                content_type="x_post",
                source_commits=[],
                source_messages=[],
                content=f"Post {i+1}",
                eval_score=7.0,
                eval_feedback="good"
            )
            db.mark_published(post_id, f"http://x.com/{i+1}", f"tweet{i+1}")
            db.insert_engagement(
                post_id, f"tweet{i+1}",
                like_count=10 * (5 - i),  # Decreasing engagement
                retweet_count=2 * (5 - i),
                reply_count=1,
                quote_count=0,
                engagement_score=float(5 - i)
            )

        examples = selector.get_examples(content_type="x_post", limit=2)

        assert len(examples) == 2
        assert examples[0].content == "Post 1"
        assert examples[1].content == "Post 2"


# ===========================================================================
# FewShotSelector._fallback_by_eval_score cold-start path
# ===========================================================================


class TestFewShotSelectorFallback:
    """Test FewShotSelector fallback when no engagement data exists."""

    def test_fallback_returns_posts_by_eval_score(self, db):
        """Verify fallback uses eval_score when no engagement data."""
        selector = FewShotSelector(db)

        # Insert published posts with no engagement data
        post1_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Low eval score post",
            eval_score=5.0,
            eval_feedback="okay"
        )
        db.mark_published(post1_id, "http://x.com/1", "tweet1")

        post2_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="High eval score post",
            eval_score=9.0,
            eval_feedback="excellent"
        )
        db.mark_published(post2_id, "http://x.com/2", "tweet2")

        post3_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Medium eval score post",
            eval_score=7.0,
            eval_feedback="good"
        )
        db.mark_published(post3_id, "http://x.com/3", "tweet3")

        examples = selector.get_examples(content_type="x_post", limit=3)

        # Should be ordered by eval_score DESC
        assert len(examples) == 3
        assert examples[0].content == "High eval score post"
        assert examples[0].engagement_score == 0.0  # Fallback sets to 0
        assert examples[1].content == "Medium eval score post"
        assert examples[2].content == "Low eval score post"

    def test_fallback_filters_stale_patterns(self, db):
        """Verify stale pattern filtering works in fallback path."""
        selector = FewShotSelector(db)

        # Insert post with stale pattern
        stale_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Everyone thinks testing is essential",
            eval_score=9.0,
            eval_feedback="excellent"
        )
        db.mark_published(stale_id, "http://x.com/stale", "tweet_stale")

        # Insert normal post
        good_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Implemented retry logic with exponential backoff",
            eval_score=8.0,
            eval_feedback="good"
        )
        db.mark_published(good_id, "http://x.com/good", "tweet_good")

        examples = selector.get_examples(content_type="x_post", limit=2)

        # Should only return non-stale post
        assert len(examples) == 1
        assert examples[0].content == "Implemented retry logic with exponential backoff"

    def test_fallback_excludes_too_specific_via_sql(self, db):
        """Verify curation_quality='too_specific' posts excluded via SQL."""
        selector = FewShotSelector(db)

        # Insert too_specific post
        specific_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Too specific content",
            eval_score=9.0,
            eval_feedback="excellent"
        )
        db.mark_published(specific_id, "http://x.com/specific", "tweet_specific")
        db.set_curation_quality(specific_id, "too_specific")

        # Insert normal post
        good_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="General insight about code quality",
            eval_score=7.0,
            eval_feedback="good"
        )
        db.mark_published(good_id, "http://x.com/good", "tweet_good")

        examples = selector.get_examples(content_type="x_post", limit=2)

        # Should exclude too_specific via SQL
        assert len(examples) == 1
        assert examples[0].content == "General insight about code quality"

    def test_fallback_respects_exclude_ids(self, db):
        """Verify exclude_ids parameter works in fallback path."""
        selector = FewShotSelector(db)

        post1_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="First post",
            eval_score=9.0,
            eval_feedback="excellent"
        )
        db.mark_published(post1_id, "http://x.com/1", "tweet1")

        post2_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Second post",
            eval_score=8.0,
            eval_feedback="good"
        )
        db.mark_published(post2_id, "http://x.com/2", "tweet2")

        post3_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Third post",
            eval_score=7.0,
            eval_feedback="good"
        )
        db.mark_published(post3_id, "http://x.com/3", "tweet3")

        # Exclude post1
        examples = selector.get_examples(
            content_type="x_post",
            limit=2,
            exclude_ids={post1_id}
        )

        assert len(examples) == 2
        assert examples[0].content == "Second post"
        assert examples[1].content == "Third post"


# ===========================================================================
# FewShotSelector.format_examples output
# ===========================================================================


class TestFormatExamples:
    """Test format_examples output formatting."""

    def test_empty_list_returns_empty_string(self):
        selector = FewShotSelector(None)  # db not needed for formatting
        result = selector.format_examples([])
        assert result == ""

    def test_single_example_formatting(self):
        selector = FewShotSelector(None)
        examples = [FewShotExample(content="Single post", engagement_score=10.0)]
        result = selector.format_examples(examples)
        assert result == "1. Single post"

    def test_multiple_examples_formatting(self):
        selector = FewShotSelector(None)
        examples = [
            FewShotExample(content="First post", engagement_score=20.0),
            FewShotExample(content="Second post", engagement_score=15.0),
            FewShotExample(content="Third post", engagement_score=10.0),
        ]
        result = selector.format_examples(examples)
        expected = "1. First post\n\n2. Second post\n\n3. Third post"
        assert result == expected

    def test_formatting_preserves_content_exactly(self):
        selector = FewShotSelector(None)
        examples = [
            FewShotExample(
                content="Post with\nmultiple lines\nand special chars: @#$%",
                engagement_score=10.0
            ),
        ]
        result = selector.format_examples(examples)
        assert result == "1. Post with\nmultiple lines\nand special chars: @#$%"


# ===========================================================================
# Tests from add-unit-tests-for-synthesis-few-shot-py-fewshotse branch
# ===========================================================================

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
