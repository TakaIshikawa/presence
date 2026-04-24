"""Recommend review actions for inbound mention reply drafts."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Literal

from engagement.reply_classifier import ReplyClassifier


ReplyAction = Literal[
    "reply_now",
    "save_for_later",
    "quote_candidate",
    "no_response",
    "needs_manual_review",
]

ACTION_ORDER: tuple[ReplyAction, ...] = (
    "reply_now",
    "quote_candidate",
    "needs_manual_review",
    "save_for_later",
    "no_response",
)

_TOKEN_RE = re.compile(r"[a-z0-9']+")
_URL_RE = re.compile(r"https?://|www\.", re.IGNORECASE)

_PRAISE_PHRASES = (
    "thanks",
    "thank you",
    "helpful",
    "great post",
    "nice post",
    "love this",
    "well said",
    "awesome",
)

_SUPPORT_PHRASES = (
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
    "can't install",
    "cant install",
    "fails",
    "failure",
    "regression",
)

_SPAM_PHRASES = (
    "airdrop",
    "crypto giveaway",
    "dm me",
    "follow back",
    "check my profile",
    "earn money",
    "work from home",
    "forex",
    "investment opportunity",
    "onlyfans",
)

_QUOTE_HOOK_PHRASES = (
    "hot take",
    "unpopular opinion",
    "the future of",
    "the biggest",
    "the real problem",
    "research shows",
    "data shows",
    "i learned",
    "lesson learned",
    "everyone thinks",
    "nobody talks about",
    "people underestimate",
    "broadly true",
    "always",
    "never",
)

_HIGH_CONTEXT_PHRASES = (
    "can you review",
    "could you review",
    "what do you think about my",
    "please check",
    "take a look at my",
    "would you audit",
    "can we hop on",
    "book a call",
    "send me details",
    "i need help with my",
)

_HIGH_RISK_FLAGS = {
    "generic",
    "sycophantic",
    "hashtags",
    "stage_mismatch",
    "parse_error",
    "eval_error",
}


@dataclass(frozen=True)
class ReplyActionRecommendation:
    reply_id: int | None
    action: ReplyAction
    reason: str
    confidence: float
    platform: str
    status: str
    author: str | None
    intent: str
    priority: str
    inbound_text: str
    draft_text: str | None
    quality_score: float | None = None
    quality_flags: list[str] | None = None
    detected_at: str | None = None
    inbound_tweet_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ReplyActionRecommender:
    """Turn stored reply classifier/evaluator outputs into triage actions."""

    def __init__(self, classifier: ReplyClassifier | None = None) -> None:
        self.classifier = classifier or ReplyClassifier()

    def recommend(self, row: dict[str, Any]) -> ReplyActionRecommendation:
        inbound_text = str(row.get("inbound_text") or "")
        draft_text = row.get("draft_text")
        intent = str(row.get("intent") or "").strip() or "other"
        priority = str(row.get("priority") or "").strip() or "normal"

        if intent == "other" or not intent:
            classification = self.classifier.classify(
                inbound_text,
                our_post=str(row.get("our_post_text") or ""),
                author_handle=str(row.get("inbound_author_handle") or ""),
            )
            intent = classification.intent
            priority = priority if row.get("priority") else classification.priority

        flags = _parse_flags(row.get("quality_flags"))
        quality_score = _coerce_score(row.get("quality_score"))
        action, reason, confidence = self._decide(
            inbound_text=inbound_text,
            draft_text=str(draft_text or ""),
            intent=intent,
            priority=priority,
            quality_score=quality_score,
            quality_flags=flags,
        )
        return ReplyActionRecommendation(
            reply_id=_coerce_int(row.get("id")),
            action=action,
            reason=reason,
            confidence=confidence,
            platform=str(row.get("platform") or "x"),
            status=str(row.get("status") or "pending"),
            author=row.get("inbound_author_handle"),
            intent=intent,
            priority=priority,
            inbound_text=inbound_text,
            draft_text=draft_text,
            quality_score=quality_score,
            quality_flags=flags,
            detected_at=row.get("detected_at"),
            inbound_tweet_id=row.get("inbound_tweet_id"),
        )

    def recommend_many(self, rows: list[dict[str, Any]]) -> list[ReplyActionRecommendation]:
        recommendations: list[ReplyActionRecommendation] = []
        seen_mentions: set[tuple[str, str]] = set()
        for row in rows:
            recommendation = self.recommend(row)
            duplicate_key = (
                (recommendation.author or "").lower(),
                _fingerprint(recommendation.inbound_text),
            )
            if duplicate_key[0] and duplicate_key[1] and duplicate_key in seen_mentions:
                recommendation = _replace_action(
                    recommendation,
                    "no_response",
                    "duplicate mention from same author",
                    0.95,
                )
            seen_mentions.add(duplicate_key)
            recommendations.append(recommendation)
        return sorted(
            recommendations,
            key=lambda item: (
                ACTION_ORDER.index(item.action),
                _priority_rank(item.priority),
                item.detected_at or "",
                item.reply_id or 0,
            ),
        )

    def _decide(
        self,
        *,
        inbound_text: str,
        draft_text: str,
        intent: str,
        priority: str,
        quality_score: float | None,
        quality_flags: list[str],
    ) -> tuple[ReplyAction, str, float]:
        normalized = _normalize(inbound_text)
        tokens = _tokens(normalized)
        has_draft = bool(draft_text.strip())
        high_risk = _has_high_evaluator_risk(quality_score, quality_flags)

        if _looks_duplicate(normalized, quality_flags):
            return "no_response", "duplicate mention", 0.95
        if intent == "spam" or _looks_spam(normalized, tokens):
            return "no_response", "spam pattern", 0.95
        if not normalized:
            return "no_response", "empty mention", 0.9
        if _is_low_signal(normalized, tokens, intent):
            return "no_response", "low-signal mention", 0.85

        if high_risk:
            return "needs_manual_review", "evaluator flagged draft risk", 0.85

        if _is_high_context_ask(normalized, tokens):
            return "needs_manual_review", "high-context ask", 0.8

        if _contains_any(normalized, _QUOTE_HOOK_PHRASES):
            return "quote_candidate", "quote-worthy claim or topical hook", 0.82

        if intent in {"question", "bug_report"} or _is_question(normalized, tokens):
            if has_draft:
                return "reply_now", "direct question or support issue", 0.9
            return "needs_manual_review", "direct question without draft", 0.78

        if _is_quote_candidate(normalized, tokens):
            return "quote_candidate", "quote-worthy claim or topical hook", 0.82

        if intent == "disagreement":
            return "reply_now" if has_draft else "needs_manual_review", "substantive disagreement", 0.78

        if priority == "high" and has_draft:
            return "reply_now", "high-priority mention with draft", 0.75

        return "save_for_later", "substantive but not urgent", 0.65


def recommendations_to_dict(
    recommendations: list[ReplyActionRecommendation],
) -> list[dict[str, Any]]:
    return [item.to_dict() for item in recommendations]


def group_recommendations(
    recommendations: list[ReplyActionRecommendation],
) -> dict[ReplyAction, list[ReplyActionRecommendation]]:
    return {
        action: [item for item in recommendations if item.action == action]
        for action in ACTION_ORDER
    }


def _replace_action(
    item: ReplyActionRecommendation,
    action: ReplyAction,
    reason: str,
    confidence: float,
) -> ReplyActionRecommendation:
    data = item.to_dict()
    data.update({"action": action, "reason": reason, "confidence": confidence})
    return ReplyActionRecommendation(**data)


def _normalize(text: str) -> str:
    return " ".join(text.lower().strip().split())


def _tokens(text: str) -> list[str]:
    return _TOKEN_RE.findall(text)


def _contains_any(text: str, phrases: tuple[str, ...]) -> bool:
    return any(phrase in text for phrase in phrases)


def _parse_flags(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    try:
        parsed = json.loads(str(value))
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed]


def _coerce_score(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _has_high_evaluator_risk(score: float | None, flags: list[str]) -> bool:
    normalized_flags = {flag.lower() for flag in flags}
    return (score is not None and score < 5.0) or bool(normalized_flags & _HIGH_RISK_FLAGS)


def _looks_spam(text: str, tokens: list[str]) -> bool:
    if _contains_any(text, _SPAM_PHRASES):
        return True
    if _URL_RE.search(text) and {"free", "buy", "click", "subscribe"} & set(tokens):
        return True
    return len(tokens) <= 4 and bool(_URL_RE.search(text))


def _looks_duplicate(text: str, flags: list[str]) -> bool:
    if any("duplicate" in flag.lower() for flag in flags):
        return True
    return "duplicate reply" in text or "already asked" in text


def _is_low_signal(text: str, tokens: list[str], intent: str) -> bool:
    if intent == "appreciation" and len(tokens) <= 8:
        return True
    if len(tokens) <= 3 and not _is_question(text, tokens):
        return True
    return len(tokens) <= 8 and _contains_any(text, _PRAISE_PHRASES) and not _is_question(text, tokens)


def _is_question(text: str, tokens: list[str]) -> bool:
    question_words = {"what", "why", "how", "when", "where", "who", "which", "can", "could", "would", "should", "do", "does", "did", "is", "are"}
    return "?" in text or bool(tokens and tokens[0] in question_words)


def _is_high_context_ask(text: str, tokens: list[str]) -> bool:
    if _contains_any(text, _HIGH_CONTEXT_PHRASES):
        return True
    return _is_question(text, tokens) and len(tokens) > 35


def _is_quote_candidate(text: str, tokens: list[str]) -> bool:
    if _contains_any(text, _QUOTE_HOOK_PHRASES):
        return True
    if re.search(r"\b\d+(?:\.\d+)?\s?%|\b\d+x\b", text):
        return True
    if _contains_any(text, _SUPPORT_PHRASES):
        return False
    return len(tokens) >= 18 and any(word in tokens for word in ("because", "trend", "teams", "developers", "agents", "testing"))


def _fingerprint(text: str) -> str:
    tokens = _tokens(_normalize(text))
    return " ".join(tokens[:32])


def _priority_rank(priority: str) -> int:
    return {"high": 0, "normal": 1, "low": 2}.get(priority, 3)
