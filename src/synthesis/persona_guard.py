"""Deterministic persona drift guard for generated content."""

from __future__ import annotations

import re
from dataclasses import dataclass, field


_WORD_RE = re.compile(r"[a-z0-9][a-z0-9_./#-]*")
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

BANNED_TONE_MARKERS = (
    "10x",
    "best-in-class",
    "game changer",
    "game-changing",
    "growth hack",
    "leverage",
    "revolutionary",
    "sales funnel",
    "scale your",
    "supercharge",
    "thought leader",
    "unlock",
    "world-class",
)

ABSTRACT_TERMS = {
    "alignment",
    "architecture",
    "capabilities",
    "clarity",
    "efficiency",
    "enablement",
    "framework",
    "friction",
    "impact",
    "innovation",
    "leverage",
    "momentum",
    "optimize",
    "paradigm",
    "quality",
    "resilience",
    "scalable",
    "strategy",
    "streamline",
    "systems",
    "value",
}

FIRST_PERSON_TERMS = {
    "i",
    "i'd",
    "i'll",
    "i'm",
    "i've",
    "me",
    "my",
    "we",
    "we'd",
    "we'll",
    "we're",
    "we've",
}

WORK_ARTIFACT_RE = re.compile(
    r"""
    \b(?:api|branch|bug|build|cli|commit|config|cron|db|debug|deploy|diff|endpoint|
       error|file|fixture|function|github|handler|issue|job|json|log|migration|
       model|module|patch|pipeline|pr|query|queue|refactor|repo|schema|script|
       sqlite|table|test|timeout|trace|worker|yaml)\b
    |[a-z0-9_./-]+\.(?:py|ts|tsx|js|jsx|sql|yaml|yml|json|md|txt)\b
    |\#[0-9]+\b
    |[a-f0-9]{7,40}\b
    """,
    re.IGNORECASE | re.VERBOSE,
)


@dataclass
class PersonaGuardConfig:
    enabled: bool = True
    min_score: float = 0.55
    min_phrase_overlap: float = 0.08
    max_banned_markers: int = 0
    max_abstraction_ratio: float = 0.18
    min_grounding_score: float = 0.5
    recent_limit: int = 20
    min_recent_posts: int = 3


@dataclass
class PersonaGuardResult:
    passed: bool
    score: float
    reasons: list[str] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)
    checked: bool = True
    status: str = "passed"

    def to_summary(self) -> dict:
        return {
            "checked": self.checked,
            "passed": self.passed,
            "status": self.status,
            "score": self.score,
            "reasons": self.reasons,
            "metrics": self.metrics,
        }


def _normalize(text: str) -> str:
    text = _THREAD_LABEL_RE.sub("", text or "")
    return text.lower()


def _tokens(text: str) -> list[str]:
    return [token for token in _WORD_RE.findall(_normalize(text)) if token]


def _content_tokens(text: str) -> list[str]:
    return [token for token in _tokens(text) if token not in _STOPWORDS]


def _phrases(tokens: list[str], n: int = 2) -> set[tuple[str, ...]]:
    if len(tokens) < n:
        return set()
    return {tuple(tokens[i : i + n]) for i in range(len(tokens) - n + 1)}


class PersonaGuard:
    """Score final content against recent author voice using cheap heuristics."""

    def __init__(self, config: PersonaGuardConfig | None = None) -> None:
        self.config = config or PersonaGuardConfig()

    def check(self, content: str, recent_posts: list[dict | str] | None) -> PersonaGuardResult:
        if not self.config.enabled:
            return PersonaGuardResult(
                passed=True,
                score=1.0,
                reasons=["persona guard disabled"],
                metrics={},
                checked=False,
                status="disabled",
            )

        recent_texts = self._recent_texts(recent_posts)
        if len(recent_texts) < self.config.min_recent_posts:
            return PersonaGuardResult(
                passed=True,
                score=1.0,
                reasons=["not enough recent published posts for persona comparison"],
                metrics={"recent_posts": len(recent_texts)},
                checked=False,
                status="skipped",
            )

        metrics = self._metrics(content, recent_texts)
        reasons = self._failure_reasons(metrics)
        score = self._score(metrics)
        if score < self.config.min_score:
            reasons.append(
                f"persona score {score:.2f} below minimum {self.config.min_score:.2f}"
            )

        passed = not reasons
        return PersonaGuardResult(
            passed=passed,
            score=score,
            reasons=reasons,
            metrics=metrics,
            checked=True,
            status="passed" if passed else "failed",
        )

    def _recent_texts(self, recent_posts: list[dict | str] | None) -> list[str]:
        texts = []
        for post in recent_posts or []:
            if isinstance(post, str):
                text = post
            elif isinstance(post, dict):
                text = post.get("content") or ""
            else:
                text = ""
            text = text.strip()
            if text:
                texts.append(text)
        return texts[: self.config.recent_limit]

    def _metrics(self, content: str, recent_texts: list[str]) -> dict:
        candidate_tokens = _content_tokens(content)
        recent_tokens = _content_tokens(" ".join(recent_texts))
        candidate_phrases = _phrases(candidate_tokens)
        recent_phrases = _phrases(recent_tokens)
        phrase_overlap = (
            len(candidate_phrases & recent_phrases) / len(candidate_phrases)
            if candidate_phrases
            else 0.0
        )

        normalized = _normalize(content)
        banned_hits = [
            marker
            for marker in BANNED_TONE_MARKERS
            if marker in normalized
        ]
        abstract_count = sum(1 for token in candidate_tokens if token in ABSTRACT_TERMS)
        abstraction_ratio = (
            abstract_count / len(candidate_tokens) if candidate_tokens else 0.0
        )
        first_person_hits = sorted(set(_tokens(content)) & FIRST_PERSON_TERMS)
        artifact_hits = sorted({match.group(0) for match in WORK_ARTIFACT_RE.finditer(content)})
        grounding_score = min(1.0, (0.5 if first_person_hits else 0.0) + (0.5 if artifact_hits else 0.0))

        return {
            "recent_posts": len(recent_texts),
            "phrase_overlap": round(phrase_overlap, 4),
            "banned_markers": banned_hits,
            "banned_marker_count": len(banned_hits),
            "abstraction_ratio": round(abstraction_ratio, 4),
            "abstract_term_count": abstract_count,
            "grounding_score": grounding_score,
            "first_person_hits": first_person_hits,
            "artifact_hits": artifact_hits[:8],
        }

    def _failure_reasons(self, metrics: dict) -> list[str]:
        reasons = []
        if metrics["phrase_overlap"] < self.config.min_phrase_overlap:
            reasons.append(
                f"phrase overlap {metrics['phrase_overlap']:.2f} below minimum "
                f"{self.config.min_phrase_overlap:.2f}"
            )
        if metrics["banned_marker_count"] > self.config.max_banned_markers:
            reasons.append(
                "banned tone markers: " + ", ".join(metrics["banned_markers"])
            )
        if metrics["abstraction_ratio"] > self.config.max_abstraction_ratio:
            reasons.append(
                f"abstraction ratio {metrics['abstraction_ratio']:.2f} above maximum "
                f"{self.config.max_abstraction_ratio:.2f}"
            )
        if metrics["grounding_score"] < self.config.min_grounding_score:
            reasons.append(
                f"grounding score {metrics['grounding_score']:.2f} below minimum "
                f"{self.config.min_grounding_score:.2f}"
            )
        return reasons

    @staticmethod
    def _score(metrics: dict) -> float:
        phrase_score = min(1.0, metrics["phrase_overlap"] / 0.2)
        tone_score = max(0.0, 1.0 - (metrics["banned_marker_count"] * 0.4))
        abstraction_score = max(0.0, 1.0 - metrics["abstraction_ratio"])
        grounding_score = metrics["grounding_score"]
        return round(
            (phrase_score * 0.35)
            + (tone_score * 0.25)
            + (abstraction_score * 0.2)
            + (grounding_score * 0.2),
            4,
        )
