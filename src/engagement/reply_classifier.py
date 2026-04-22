"""Classify inbound replies before drafting a response."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Literal, Optional

import anthropic

logger = logging.getLogger(__name__)

ReplyIntent = Literal[
    "question",
    "appreciation",
    "disagreement",
    "bug_report",
    "spam",
    "other",
]

ReplyPriority = Literal["high", "normal", "low"]

VALID_INTENTS: set[str] = {
    "question",
    "appreciation",
    "disagreement",
    "bug_report",
    "spam",
    "other",
}

CLASSIFIER_SYSTEM_PROMPT = """\
Classify an inbound social reply to one of:
question, appreciation, disagreement, bug_report, spam, other.

Return ONLY valid JSON:
{"intent":"question","priority":"normal","reason":"brief reason"}
"""

_URL_RE = re.compile(r"https?://|www\.", re.IGNORECASE)
_MENTION_RE = re.compile(r"@\w+")
_TOKEN_RE = re.compile(r"[a-z0-9']+")

_QUESTION_WORDS = {
    "what",
    "why",
    "how",
    "when",
    "where",
    "who",
    "which",
    "can",
    "could",
    "would",
    "should",
    "do",
    "does",
    "did",
    "is",
    "are",
}

_APPRECIATION_PHRASES = (
    "thank you",
    "thanks",
    "appreciate",
    "helpful",
    "great point",
    "great post",
    "love this",
    "nice post",
    "well said",
    "awesome",
)

_DISAGREEMENT_PHRASES = (
    "i disagree",
    "disagree",
    "not sure",
    "i don't think",
    "dont think",
    "doesn't seem",
    "doesnt seem",
    "wrong",
    "false",
    "actually",
    "but ",
    "however",
)

_BUG_PHRASES = (
    "bug",
    "broken",
    "crash",
    "crashes",
    "error",
    "exception",
    "traceback",
    "doesn't work",
    "doesnt work",
    "not working",
    "fails",
    "failure",
    "regression",
    "repro",
)

_SPAM_PHRASES = (
    "crypto",
    "airdrop",
    "giveaway",
    "dm me",
    "check my profile",
    "follow back",
    "earn money",
    "work from home",
    "onlyfans",
    "forex",
    "investment opportunity",
)


@dataclass(frozen=True)
class ReplyClassification:
    intent: ReplyIntent
    priority: ReplyPriority
    reason: str
    confidence: float = 1.0

    @property
    def is_low_value(self) -> bool:
        return self.intent in {"appreciation", "other", "spam"}


class ReplyClassifier:
    """Deterministic reply classifier with an optional Anthropic fallback."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        timeout: float = 300.0,
        anthropic_fallback: bool = False,
    ) -> None:
        self.model = model
        self.client = None
        if anthropic_fallback and api_key and model:
            self.client = anthropic.Anthropic(api_key=api_key, timeout=timeout)

    def classify(
        self,
        inbound_text: str,
        our_post: str = "",
        author_handle: str = "",
    ) -> ReplyClassification:
        """Classify inbound text, using Anthropic only for heuristic misses."""
        heuristic = self._classify_heuristic(inbound_text)
        if heuristic.intent != "other" or self.client is None:
            return heuristic
        return self._classify_with_anthropic(inbound_text, our_post, author_handle)

    def _classify_heuristic(self, text: str) -> ReplyClassification:
        normalized = _normalize(text)
        tokens = _tokens(normalized)

        if not normalized:
            return ReplyClassification("other", "low", "empty reply", 0.8)

        if _looks_like_spam(normalized, tokens):
            return ReplyClassification("spam", "low", "spam pattern", 0.95)

        if _contains_any(normalized, _BUG_PHRASES):
            return ReplyClassification("bug_report", "high", "bug report terms", 0.9)

        if "?" in text or (tokens and tokens[0] in _QUESTION_WORDS):
            return ReplyClassification("question", "normal", "question marker", 0.9)

        if _contains_any(normalized, _DISAGREEMENT_PHRASES):
            return ReplyClassification("disagreement", "normal", "disagreement terms", 0.85)

        if _contains_any(normalized, _APPRECIATION_PHRASES):
            return ReplyClassification("appreciation", "low", "appreciation terms", 0.9)

        return ReplyClassification("other", "low", "no deterministic match", 0.5)

    def _classify_with_anthropic(
        self,
        inbound_text: str,
        our_post: str,
        author_handle: str,
    ) -> ReplyClassification:
        prompt = (
            f"Our post: {our_post!r}\n"
            f"@{author_handle}'s reply: {inbound_text!r}\n\n"
            "Classify the reply."
        )
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=120,
                system=CLASSIFIER_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text.strip()
            return _parse_llm_response(raw)
        except (anthropic.APIError, anthropic.APIConnectionError, anthropic.APITimeoutError) as e:
            logger.warning("Reply classification fallback failed: %s", e)
            return ReplyClassification("other", "low", "fallback_error", 0.0)


def _normalize(text: str) -> str:
    return " ".join(text.lower().strip().split())


def _tokens(text: str) -> list[str]:
    return _TOKEN_RE.findall(text)


def _contains_any(text: str, phrases: tuple[str, ...]) -> bool:
    return any(phrase in text for phrase in phrases)


def _looks_like_spam(text: str, tokens: list[str]) -> bool:
    if _contains_any(text, _SPAM_PHRASES):
        return True
    if _URL_RE.search(text) and ("buy" in tokens or "free" in tokens or "click" in tokens):
        return True
    if len(_MENTION_RE.findall(text)) >= 4:
        return True
    if len(tokens) <= 4 and _URL_RE.search(text):
        return True
    return False


def _parse_llm_response(raw: str) -> ReplyClassification:
    text = raw.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        text = text.rsplit("```", 1)[0]
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start < 0 or end <= start:
            return ReplyClassification("other", "low", "parse_error", 0.0)
        try:
            data = json.loads(text[start:end])
        except json.JSONDecodeError:
            return ReplyClassification("other", "low", "parse_error", 0.0)

    intent = str(data.get("intent", "other"))
    if intent not in VALID_INTENTS:
        intent = "other"
    priority = str(data.get("priority") or _default_priority(intent))
    if priority not in {"high", "normal", "low"}:
        priority = _default_priority(intent)
    reason = str(data.get("reason", "anthropic_fallback"))
    return ReplyClassification(intent, priority, reason, 0.7)


def _default_priority(intent: str) -> ReplyPriority:
    if intent == "bug_report":
        return "high"
    if intent in {"appreciation", "spam", "other"}:
        return "low"
    return "normal"
