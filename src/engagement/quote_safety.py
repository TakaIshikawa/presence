"""Deterministic safety checks for proactive quote opportunities."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any

from engagement.quote_opportunities import QuoteOpportunity


RESTRICTED_LICENSE = "restricted"
DEFAULT_PLATFORM_LIMIT = 280
SOURCE_QUOTE_CHAR_LIMIT = 500
SOURCE_QUOTE_WORD_LIMIT = 80
MIN_RELEVANCE_SCORE = 0.2

INFLAMMATORY_TERMS = {
    "idiot",
    "moron",
    "stupid",
    "fraud",
    "scam",
    "garbage",
    "trash",
    "liar",
    "hate",
    "destroy",
    "clown",
}
TOKEN_RE = re.compile(r"[a-z0-9+#.-]+")


@dataclass(frozen=True)
class QuoteSafetyReview:
    """Safety result for one quote opportunity."""

    score: float
    blocking_flags: list[str]
    reasons: list[str]
    checks: dict[str, bool]

    @property
    def blocked(self) -> bool:
        return bool(self.blocking_flags)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["blocked"] = self.blocked
        return payload


def _tokens(text: str | None) -> set[str]:
    return {
        token
        for token in TOKEN_RE.findall((text or "").lower())
        if len(token) > 2 and token not in {"the", "and", "for", "with", "that", "this"}
    }


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    if hasattr(row, "keys"):
        return row[key] if key in row.keys() else default
    if isinstance(row, dict):
        return row.get(key, default)
    return default


class QuoteSafetyReviewer:
    """Review quote opportunity drafts before human export/review."""

    def __init__(self, db: Any | None = None, *, platform_limit: int = DEFAULT_PLATFORM_LIMIT) -> None:
        self.db = db
        self.platform_limit = platform_limit

    def review(self, opportunity: QuoteOpportunity) -> QuoteSafetyReview:
        license_value = self._knowledge_license(opportunity.knowledge_id)
        checks = {
            "attribution": self._has_attribution(opportunity),
            "quote_length": self._has_acceptable_quote_length(opportunity),
            "inflammatory_language": not self._has_inflammatory_language(opportunity),
            "relevance": self._has_relevance(opportunity),
            "restricted_license": str(license_value or "").lower() != RESTRICTED_LICENSE,
            "platform_length": len(opportunity.draft_text or "") <= self.platform_limit,
        }

        blocking_flags: list[str] = []
        reasons: list[str] = []
        if not checks["attribution"]:
            blocking_flags.append("missing_attribution")
            reasons.append("Missing source attribution in the draft or source metadata.")
        if not checks["quote_length"]:
            blocking_flags.append("excessive_quote_length")
            reasons.append("Source text is too long to quote safely without stronger summarization.")
        if not checks["inflammatory_language"]:
            blocking_flags.append("inflammatory_language")
            reasons.append("Draft or source contains inflammatory phrasing.")
        if not checks["relevance"]:
            blocking_flags.append("weak_relevance")
            reasons.append("Opportunity has weak topic or draft-to-source relevance.")
        if not checks["restricted_license"]:
            blocking_flags.append("restricted_license")
            reasons.append("Knowledge reference is marked with a restricted license.")
        if not checks["platform_length"]:
            blocking_flags.append("platform_length")
            reasons.append(f"Draft exceeds the {self.platform_limit}-character platform limit.")

        penalty = 0.16 * len(blocking_flags)
        if not blocking_flags:
            reasons.append("Safety checks passed.")
        return QuoteSafetyReview(
            score=round(max(0.0, 1.0 - penalty), 4),
            blocking_flags=blocking_flags,
            reasons=reasons,
            checks=checks,
        )

    def review_many(self, opportunities: list[QuoteOpportunity]) -> dict[int, QuoteSafetyReview]:
        return {opportunity.knowledge_id: self.review(opportunity) for opportunity in opportunities}

    def _knowledge_license(self, knowledge_id: int) -> str | None:
        if self.db is None:
            return None
        row = self.db.conn.execute(
            "SELECT license FROM knowledge WHERE id = ?",
            (knowledge_id,),
        ).fetchone()
        return _row_value(row, "license")

    def _has_attribution(self, opportunity: QuoteOpportunity) -> bool:
        author = (opportunity.author or "").strip().lstrip("@")
        if not author and not opportunity.source_url:
            return False
        draft = (opportunity.draft_text or "").lower()
        if author and (f"@{author.lower()}" in draft or author.lower() in draft):
            return True
        return bool(opportunity.source_url and opportunity.source_url.lower() in draft)

    def _has_acceptable_quote_length(self, opportunity: QuoteOpportunity) -> bool:
        source_text = " ".join((opportunity.content or "").split())
        return (
            len(source_text) <= SOURCE_QUOTE_CHAR_LIMIT
            and len(source_text.split()) <= SOURCE_QUOTE_WORD_LIMIT
        )

    def _has_inflammatory_language(self, opportunity: QuoteOpportunity) -> bool:
        text = f"{opportunity.draft_text} {opportunity.content}".lower()
        tokens = _tokens(text)
        return bool(tokens & INFLAMMATORY_TERMS)

    def _has_relevance(self, opportunity: QuoteOpportunity) -> bool:
        if opportunity.topical_relevance >= MIN_RELEVANCE_SCORE:
            return True
        draft_tokens = _tokens(opportunity.draft_text)
        source_tokens = _tokens(opportunity.content)
        return bool(draft_tokens & source_tokens & set(opportunity.topics))
