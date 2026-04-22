"""Deterministic hashtag suggestions for generated content variants."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable


X_MAX_HASHTAGS = 3
BLUESKY_MAX_HASHTAGS = 2
LINKEDIN_MAX_HASHTAGS = 5

_HASHTAG_RE = re.compile(r"(?<!\w)#[A-Za-z][A-Za-z0-9_]*")
_URL_RE = re.compile(r"https?://[^\s<>()]+")
_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9+._-]*")

_STOPWORDS = {
    "about",
    "after",
    "again",
    "because",
    "before",
    "being",
    "between",
    "build",
    "could",
    "every",
    "from",
    "have",
    "into",
    "just",
    "more",
    "most",
    "need",
    "over",
    "post",
    "shipping",
    "should",
    "that",
    "their",
    "there",
    "these",
    "this",
    "through",
    "today",
    "tweet",
    "with",
    "without",
    "work",
    "would",
    "your",
}

_TOPIC_HASHTAGS = {
    "architecture": "#Architecture",
    "testing": "#Testing",
    "debugging": "#Debugging",
    "ai-agents": "#AIAgents",
    "developer-tools": "#DevTools",
    "performance": "#Performance",
    "data-modeling": "#DataModeling",
    "devops": "#DevOps",
    "open-source": "#OpenSource",
    "product-thinking": "#ProductThinking",
    "workflow": "#Workflow",
}

_KEYWORD_HASHTAGS = {
    "agent": "#AIAgents",
    "agents": "#AIAgents",
    "ai": "#AI",
    "api": "#API",
    "apis": "#API",
    "architecture": "#Architecture",
    "automation": "#Automation",
    "benchmark": "#Benchmarking",
    "benchmarks": "#Benchmarking",
    "cli": "#CLI",
    "database": "#Databases",
    "databases": "#Databases",
    "debug": "#Debugging",
    "debugging": "#Debugging",
    "deploy": "#DevOps",
    "deployment": "#DevOps",
    "developer": "#DeveloperTools",
    "developers": "#DeveloperTools",
    "devops": "#DevOps",
    "docs": "#Documentation",
    "documentation": "#Documentation",
    "eval": "#Evals",
    "evals": "#Evals",
    "github": "#GitHub",
    "latency": "#Performance",
    "llm": "#LLM",
    "llms": "#LLM",
    "observability": "#Observability",
    "open-source": "#OpenSource",
    "opensource": "#OpenSource",
    "performance": "#Performance",
    "postgres": "#Postgres",
    "pytest": "#Pytest",
    "python": "#Python",
    "release": "#ReleaseEngineering",
    "releases": "#ReleaseEngineering",
    "sqlite": "#SQLite",
    "test": "#Testing",
    "testing": "#Testing",
    "tests": "#Testing",
    "typescript": "#TypeScript",
    "workflow": "#Workflow",
    "workflows": "#Workflow",
}


@dataclass(frozen=True)
class HashtagSuggestions:
    """Platform-specific hashtag suggestions."""

    x: tuple[str, ...]
    bluesky: tuple[str, ...]
    linkedin: tuple[str, ...]

    def for_platform(self, platform: str) -> tuple[str, ...]:
        """Return suggestions for a known platform key."""
        if platform == "x":
            return self.x
        if platform == "bluesky":
            return self.bluesky
        if platform == "linkedin":
            return self.linkedin
        return ()

    def as_dict(self) -> dict[str, list[str]]:
        """Return JSON-friendly platform suggestions."""
        return {
            "x": list(self.x),
            "bluesky": list(self.bluesky),
            "linkedin": list(self.linkedin),
        }


def suggest_hashtags(
    text: str,
    topics: Iterable[Any] | None = None,
) -> HashtagSuggestions:
    """Suggest small deterministic hashtag sets for each publishing platform."""
    candidates = _unique_hashtags(
        [
            *_existing_hashtags(text),
            *_topic_hashtags(topics or []),
            *_keyword_hashtags(text),
        ]
    )
    return HashtagSuggestions(
        x=tuple(candidates[:X_MAX_HASHTAGS]),
        bluesky=tuple(candidates[:BLUESKY_MAX_HASHTAGS]),
        linkedin=tuple(candidates[:LINKEDIN_MAX_HASHTAGS]),
    )


def _existing_hashtags(text: str) -> list[str]:
    return [_normalize_hashtag(match.group(0)) for match in _HASHTAG_RE.finditer(text)]


def _topic_hashtags(topics: Iterable[Any]) -> list[str]:
    hashtags: list[str] = []
    for item in topics:
        topic, subtopic, confidence = _topic_parts(item)
        if confidence is not None and confidence < 0.35:
            continue
        if topic:
            hashtag = _TOPIC_HASHTAGS.get(topic.lower())
            if hashtag:
                hashtags.append(hashtag)
        if subtopic:
            hashtags.extend(_keyword_hashtags(subtopic))
    return hashtags


def _topic_parts(item: Any) -> tuple[str, str, float | None]:
    if isinstance(item, dict):
        return (
            str(item.get("topic") or "").strip(),
            str(item.get("subtopic") or "").strip(),
            _optional_float(item.get("confidence")),
        )
    if isinstance(item, (list, tuple)):
        topic = str(item[0] if len(item) > 0 else "").strip()
        subtopic = str(item[1] if len(item) > 1 else "").strip()
        confidence = _optional_float(item[2] if len(item) > 2 else None)
        return topic, subtopic, confidence
    return str(item or "").strip(), "", None


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _keyword_hashtags(text: str) -> list[str]:
    cleaned = _HASHTAG_RE.sub(" ", _URL_RE.sub(" ", text.lower()))
    scored: dict[str, int] = {}
    first_seen: dict[str, int] = {}

    for index, match in enumerate(_WORD_RE.finditer(cleaned)):
        token = match.group(0).strip("._-")
        if len(token) < 3 or token in _STOPWORDS:
            continue
        hashtag = _KEYWORD_HASHTAGS.get(token)
        if not hashtag:
            continue
        scored[hashtag] = scored.get(hashtag, 0) + 1
        first_seen.setdefault(hashtag, index)

    return [
        hashtag
        for hashtag, _score in sorted(
            scored.items(),
            key=lambda item: (-item[1], first_seen[item[0]], item[0].lower()),
        )
    ]


def _unique_hashtags(hashtags: Iterable[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for hashtag in hashtags:
        normalized = _normalize_hashtag(hashtag)
        key = normalized.lower()
        if not normalized or key in seen:
            continue
        seen.add(key)
        unique.append(normalized)
    return unique


def _normalize_hashtag(hashtag: str) -> str:
    label = hashtag.strip()
    if label.startswith("#"):
        label = label[1:]
    label = re.sub(r"[^A-Za-z0-9_]+", "", label)
    if not label or not label[0].isalpha():
        return ""
    return f"#{label}"
