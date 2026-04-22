"""Tests for deterministic platform text adaptation."""

from output.platform_adapter import (
    BLUESKY_GRAPHEME_LIMIT,
    LINKEDIN_GRAPHEME_LIMIT,
    LINKEDIN_MAX_HASHTAGS,
    BlueskyPlatformAdapter,
    LinkedInPlatformAdapter,
    count_graphemes,
)


class TestBlueskyPlatformAdapter:
    def test_preserves_text_at_grapheme_limit(self):
        adapter = BlueskyPlatformAdapter()
        text = "👩‍💻" * BLUESKY_GRAPHEME_LIMIT

        assert count_graphemes(text) == BLUESKY_GRAPHEME_LIMIT
        assert adapter.adapt(text) == text

    def test_truncates_without_splitting_grapheme_cluster(self):
        adapter = BlueskyPlatformAdapter()
        text = "👩‍💻" * (BLUESKY_GRAPHEME_LIMIT + 20)

        result = adapter.adapt(text)

        assert count_graphemes(result) == BLUESKY_GRAPHEME_LIMIT
        assert result.endswith("...")
        assert "\u200d..." not in result

    def test_truncates_at_sentence_boundary_when_possible(self):
        adapter = BlueskyPlatformAdapter()
        first_sentence = "This complete sentence should survive."
        second_sentence = "This second sentence should be removed before it is cut midstream " * 10 + "."
        text = first_sentence + " " + second_sentence * 10

        result = adapter.adapt(text)

        assert result == first_sentence + "..."
        assert count_graphemes(result) <= BLUESKY_GRAPHEME_LIMIT

    def test_preserves_link_when_truncating(self):
        adapter = BlueskyPlatformAdapter()
        link = "https://example.com/release-notes"
        text = (
            "This post has a lot of background detail before the important link. "
            * 8
        ) + link

        result = adapter.adapt(text)

        assert link in result
        assert count_graphemes(result) <= BLUESKY_GRAPHEME_LIMIT
        assert not result.endswith("https://example.com/release-not...")

    def test_removes_x_specific_wording(self):
        adapter = BlueskyPlatformAdapter()
        text = "Tweeting this on X: quote tweet the Twitter thread and retweet if useful."

        result = adapter.adapt(text)

        assert result == "Posting this quote post the Bluesky thread and repost if useful."
        assert "tweet" not in result.lower()
        assert "twitter" not in result.lower()
        assert " X" not in result

    def test_uses_divergence_context_when_available(self):
        class ContextProvider:
            def __init__(self):
                self.called = False

            def generate_adaptation_context(self, days=60):
                self.called = True
                return "PLATFORM NOTES:\n- Posts get more engagement on Bluesky"

        provider = ContextProvider()
        adapter = BlueskyPlatformAdapter(context_provider=provider)
        text = "Shipping today #python #buildinpublic #ai #launch"

        result = adapter.adapt(text)

        assert provider.called is True
        assert result == "Shipping today #python #buildinpublic"


class TestLinkedInPlatformAdapter:
    def test_formats_thread_as_linkedin_paragraphs_and_removes_thread_markers(self):
        adapter = LinkedInPlatformAdapter()
        text = (
            "TWEET 1: Tweeting this on X about a launch. #python #ai\n"
            "TWEET 2: Retweet if this Twitter thread helps. #buildinpublic"
        )

        result = adapter.adapt(text, content_type="x_thread")

        assert "TWEET" not in result
        assert "tweet" not in result.lower()
        assert "Twitter" not in result
        assert "LinkedIn" in result
        assert "\n\n" in result
        assert result.endswith("#python #ai #buildinpublic")

    def test_limits_hashtags_for_linkedin_conventions(self):
        adapter = LinkedInPlatformAdapter()
        text = "Shipping today #one #two #three #four #five #six #seven"

        result = adapter.adapt(text)

        hashtags = [word for word in result.split() if word.startswith("#")]
        assert hashtags == ["#one", "#two", "#three", "#four", "#five"]
        assert len(hashtags) == LINKEDIN_MAX_HASHTAGS

    def test_truncates_to_linkedin_limit_without_splitting_graphemes(self):
        adapter = LinkedInPlatformAdapter()
        text = ("This is a complete sentence. " * 180) + "https://example.com/details " + (
            "👩‍💻" * 200
        )

        result = adapter.adapt(text)

        assert count_graphemes(result) <= LINKEDIN_GRAPHEME_LIMIT
        assert result.endswith("https://example.com/details")
        assert "\u200d..." not in result
