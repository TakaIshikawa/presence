import json
from types import SimpleNamespace
from unittest.mock import MagicMock

from engagement.reply_classifier import ReplyClassifier


def test_classifies_questions():
    result = ReplyClassifier().classify("How does this handle retries?")

    assert result.intent == "question"
    assert result.priority == "normal"


def test_classifies_appreciation_as_low_priority():
    result = ReplyClassifier().classify("Thanks, this was helpful")

    assert result.intent == "appreciation"
    assert result.priority == "low"
    assert result.is_low_value


def test_classifies_disagreement():
    result = ReplyClassifier().classify("I disagree, that seems too optimistic")

    assert result.intent == "disagreement"
    assert result.priority == "normal"


def test_classifies_bug_report_as_high_priority():
    result = ReplyClassifier().classify("This crashes with a traceback on startup")

    assert result.intent == "bug_report"
    assert result.priority == "high"


def test_classifies_spam():
    result = ReplyClassifier().classify("Free crypto giveaway, click https://example.com")

    assert result.intent == "spam"
    assert result.priority == "low"


def test_uses_anthropic_fallback_for_heuristic_misses():
    classifier = ReplyClassifier(anthropic_fallback=False)
    classifier.client = MagicMock()
    classifier.model = "claude-test"
    classifier.client.messages.create.return_value = SimpleNamespace(
        content=[
            SimpleNamespace(
                text=json.dumps(
                    {
                        "intent": "question",
                        "priority": "normal",
                        "reason": "asks for clarification",
                    }
                )
            )
        ]
    )

    result = classifier.classify("Retries maybe later", our_post="post", author_handle="alice")

    assert result.intent == "question"
    classifier.client.messages.create.assert_called_once()
