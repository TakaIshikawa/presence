"""Tests for deterministic hashtag suggestions."""

from synthesis.hashtag_suggester import suggest_hashtags


def test_suggests_hashtags_from_existing_topics_and_keywords():
    suggestions = suggest_hashtags(
        "Shipping a Python pytest workflow for faster API testing. #launch",
        topics=[
            {"topic": "testing", "subtopic": "integration tests", "confidence": 0.9},
            {"topic": "developer-tools", "subtopic": "CLI automation", "confidence": 0.8},
        ],
    )

    assert suggestions.x == ("#launch", "#Testing", "#DevTools")
    assert suggestions.bluesky == ("#launch", "#Testing")
    assert suggestions.linkedin == (
        "#launch",
        "#Testing",
        "#DevTools",
        "#CLI",
        "#Automation",
    )


def test_deduplicates_and_ignores_low_confidence_topics():
    suggestions = suggest_hashtags(
        "Testing tests need better observability for debugging.",
        topics=[
            ("testing", "", 0.9),
            ("debugging", "", 0.2),
            "testing",
        ],
    )

    assert suggestions.linkedin == ("#Testing", "#Observability", "#Debugging")
    assert suggestions.x == ("#Testing", "#Observability", "#Debugging")
