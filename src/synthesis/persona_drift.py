"""Deterministic preview-time persona drift detection."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from synthesis.persona_guard import (
    ABSTRACT_TERMS,
    BANNED_TONE_MARKERS,
    FIRST_PERSON_TERMS,
    WORK_ARTIFACT_RE,
)


_WORD_RE = re.compile(r"[a-z0-9][a-z0-9_./#'-]*")
_THREAD_LABEL_RE = re.compile(r"^TWEET\s+\d+:\s*", re.IGNORECASE | re.MULTILINE)

_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "from",
    "has",
    "have",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "this",
    "to",
    "was",
    "with",
}

HYPE_MARKERS = tuple(
    sorted(
        set(BANNED_TONE_MARKERS)
        | {
            "amazing",
            "breakthrough",
            "crush it",
            "delighted",
            "disruptive",
            "excited to announce",
            "finally here",
            "incredible",
            "massive",
            "next-level",
            "proud to announce",
            "thrilled",
            "transform",
            "viral",
        }
    )
)

CERTAINTY_MARKERS = (
    "always",
    "guaranteed",
    "must",
    "never",
    "no doubt",
    "obviously",
    "proven",
    "the only",
    "undeniable",
)

UNCERTAINTY_MARKERS = (
    "appears",
    "I think",
    "I am not sure",
    "I'm not sure",
    "likely",
    "might",
    "probably",
    "roughly",
    "seems",
    "so far",
)

SELF_PROMO_MARKERS = (
    "book a call",
    "dm me",
    "follow me",
    "my brand",
    "my course",
    "my framework",
    "my newsletter",
    "my product",
    "my startup",
    "subscribe",
    "work with me",
)

GENERIC_MARKERS = (
    "digital transformation",
    "drive impact",
    "future of",
    "high-performing teams",
    "move fast",
    "ship faster",
    "strategic advantage",
)


@dataclass
class PersonaDriftResult:
    """Preview-friendly persona drift summary."""

    score: float
    level: str
    reasons: list[str] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "score": self.score,
            "level": self.level,
            "reasons": self.reasons,
            "metrics": self.metrics,
        }


def _normalize(text: str) -> str:
    return _THREAD_LABEL_RE.sub("", text or "").lower()


def _tokens(text: str) -> list[str]:
    return [token for token in _WORD_RE.findall(_normalize(text)) if token]


def _content_tokens(text: str) -> list[str]:
    return [token for token in _tokens(text) if token not in _STOPWORDS]


def _recent_texts(recent_posts: list[dict | str] | None, *, limit: int) -> list[str]:
    texts: list[str] = []
    for post in recent_posts or []:
        text = post if isinstance(post, str) else post.get("content", "")
        if text.strip():
            texts.append(text.strip())
    return texts[:limit]


def _phrase_set(tokens: list[str], n: int = 2) -> set[tuple[str, ...]]:
    return {tuple(tokens[index : index + n]) for index in range(max(0, len(tokens) - n + 1))}


def _marker_hits(normalized: str, markers: tuple[str, ...]) -> list[str]:
    return [marker for marker in markers if marker.lower() in normalized]


def _ratio(count: int, total: int) -> float:
    return round(count / total, 4) if total else 0.0


def _baseline_metrics(recent_texts: list[str]) -> dict:
    baseline = " ".join(recent_texts)
    tokens = _content_tokens(baseline)
    first_person = [token for token in _tokens(baseline) if token in FIRST_PERSON_TERMS]
    artifact_hits = list(WORK_ARTIFACT_RE.finditer(baseline))
    return {
        "recent_posts": len(recent_texts),
        "self_reference_ratio": _ratio(len(first_person), len(tokens)),
        "technical_grounding_ratio": _ratio(len(artifact_hits), len(tokens)),
    }


def detect_persona_drift(
    content: str,
    recent_posts: list[dict | str] | None = None,
    *,
    recent_limit: int = 20,
) -> PersonaDriftResult:
    """Score preview content for abrupt persona shifts without LLM calls.

    The score is a drift score: 0.0 means no observed drift, 1.0 means high drift.
    """
    normalized = _normalize(content)
    tokens = _content_tokens(content)
    token_count = len(tokens)
    recent_texts = _recent_texts(recent_posts, limit=recent_limit)

    hype_hits = _marker_hits(normalized, HYPE_MARKERS)
    certainty_hits = _marker_hits(normalized, CERTAINTY_MARKERS)
    uncertainty_hits = _marker_hits(normalized, UNCERTAINTY_MARKERS)
    self_promo_hits = _marker_hits(normalized, SELF_PROMO_MARKERS)
    generic_hits = _marker_hits(normalized, GENERIC_MARKERS)

    first_person_count = sum(1 for token in _tokens(content) if token in FIRST_PERSON_TERMS)
    self_reference_ratio = _ratio(first_person_count, token_count)
    abstract_count = sum(1 for token in tokens if token in ABSTRACT_TERMS)
    abstraction_ratio = _ratio(abstract_count, token_count)
    artifact_hits = sorted({match.group(0) for match in WORK_ARTIFACT_RE.finditer(content)})
    technical_grounding_ratio = _ratio(len(artifact_hits), token_count)

    candidate_phrases = _phrase_set(tokens)
    recent_tokens = _content_tokens(" ".join(recent_texts))
    recent_phrases = _phrase_set(recent_tokens)
    phrase_overlap = (
        round(len(candidate_phrases & recent_phrases) / len(candidate_phrases), 4)
        if candidate_phrases and recent_phrases
        else 0.0
    )

    baseline = _baseline_metrics(recent_texts)
    self_reference_delta = round(
        max(0.0, self_reference_ratio - baseline["self_reference_ratio"]),
        4,
    )

    penalties: list[tuple[float, str]] = []
    if hype_hits:
        penalties.append((min(0.42, 0.18 + len(hype_hits) * 0.08), "hype-heavy tone"))
    if self_promo_hits:
        penalties.append((min(0.35, 0.2 + len(self_promo_hits) * 0.05), "self-promotional language"))
    if generic_hits or abstraction_ratio >= 0.18:
        penalties.append((0.18 if abstraction_ratio < 0.28 else 0.28, "generic abstract language"))
    if certainty_hits and not uncertainty_hits:
        penalties.append((min(0.24, 0.12 + len(certainty_hits) * 0.04), "unusually absolute certainty"))
    if self_reference_delta >= 0.12 and not artifact_hits:
        penalties.append((0.18, "self-reference is higher than recent posts without grounding"))
    if recent_texts and phrase_overlap < 0.04 and not artifact_hits:
        penalties.append((0.12, "little overlap with recent accepted technical voice"))
    if token_count >= 8 and not artifact_hits and abstraction_ratio >= 0.1:
        penalties.append((0.1, "missing concrete technical grounding"))

    score = min(1.0, sum(amount for amount, _reason in penalties))
    if score >= 0.6:
        level = "high"
    elif score >= 0.3:
        level = "medium"
    else:
        level = "low"

    reasons = []
    for _amount, reason in penalties:
        if reason not in reasons:
            reasons.append(reason)
    if uncertainty_hits and score < 0.3:
        reasons.append("measured uncertainty lowers drift risk")
    if artifact_hits and score < 0.3:
        reasons.append("technical specifics match normal voice")

    metrics = {
        "recent_posts": len(recent_texts),
        "hype_markers": hype_hits,
        "certainty_markers": certainty_hits,
        "uncertainty_markers": uncertainty_hits,
        "self_promo_markers": self_promo_hits,
        "generic_markers": generic_hits,
        "abstraction_ratio": abstraction_ratio,
        "self_reference_ratio": self_reference_ratio,
        "self_reference_delta": self_reference_delta,
        "technical_grounding_ratio": technical_grounding_ratio,
        "artifact_hits": artifact_hits[:8],
        "phrase_overlap": phrase_overlap,
        "baseline": baseline,
    }
    return PersonaDriftResult(
        score=round(score, 4),
        level=level,
        reasons=reasons,
        metrics=metrics,
    )
