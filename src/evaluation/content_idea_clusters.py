"""Cluster content idea backlog rows into reviewable themes."""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from dataclasses import dataclass
from typing import Iterable, Sequence


STOPWORDS = {
    "about",
    "after",
    "again",
    "against",
    "also",
    "and",
    "are",
    "because",
    "been",
    "being",
    "build",
    "can",
    "could",
    "from",
    "have",
    "how",
    "idea",
    "into",
    "its",
    "make",
    "maybe",
    "more",
    "note",
    "one",
    "our",
    "out",
    "post",
    "show",
    "that",
    "the",
    "their",
    "them",
    "then",
    "this",
    "turn",
    "use",
    "when",
    "with",
    "write",
    "your",
}

PRIORITY_ORDER = {"high": 0, "normal": 1, "low": 2}
SOURCE_ID_KEYS = {
    "activity_id",
    "gap_fingerprint",
    "planned_topic_id",
    "release_id",
    "source_id",
}
SOURCE_ID_EXCLUSIONS = {
    "campaign_id",
    "content_id",
    "content_idea_id",
}
TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_+-]*")


@dataclass(frozen=True)
class ContentIdeaCluster:
    """One deterministic cluster of related content ideas."""

    label: str
    idea_ids: list[int]
    representative_note: str
    shared_terms: list[str]
    sources: list[str]
    priority_mix: dict[str, int]


@dataclass(frozen=True)
class _Idea:
    id: int
    note: str
    topic: str
    source: str
    priority: str
    topic_key: str
    source_key: str
    terms: frozenset[str]
    source_ids: frozenset[tuple[str, str]]


class _UnionFind:
    def __init__(self, ids: Iterable[int]) -> None:
        self.parent = {idea_id: idea_id for idea_id in ids}

    def find(self, idea_id: int) -> int:
        parent = self.parent[idea_id]
        if parent != idea_id:
            self.parent[idea_id] = self.find(parent)
        return self.parent[idea_id]

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        if left_root < right_root:
            self.parent[right_root] = left_root
        else:
            self.parent[left_root] = right_root


def cluster_content_ideas(
    ideas: Sequence[dict],
    *,
    min_cluster_size: int = 1,
    embeddings: dict[int, Sequence[float]] | None = None,
    lexical_threshold: float = 0.32,
    embedding_threshold: float = 0.86,
) -> list[ContentIdeaCluster]:
    """Group content idea rows by topic/source identity and lexical similarity.

    Embeddings are optional and keyed by content idea ID. They are only used to add
    links; deterministic lexical/topic/source clustering is always available.
    """
    if min_cluster_size <= 0:
        raise ValueError("min_cluster_size must be positive")

    normalized = [_normalize_idea(row) for row in ideas]
    normalized.sort(key=lambda idea: idea.id)
    if not normalized:
        return []

    uf = _UnionFind(idea.id for idea in normalized)
    for index, left in enumerate(normalized):
        for right in normalized[index + 1 :]:
            if _ideas_match(
                left,
                right,
                embeddings=embeddings,
                lexical_threshold=lexical_threshold,
                embedding_threshold=embedding_threshold,
            ):
                uf.union(left.id, right.id)

    groups: dict[int, list[_Idea]] = {}
    for idea in normalized:
        groups.setdefault(uf.find(idea.id), []).append(idea)

    clusters = [
        _build_cluster(group)
        for group in groups.values()
        if len(group) >= min_cluster_size
    ]
    return sorted(clusters, key=lambda cluster: (cluster.label, cluster.idea_ids))


def clusters_to_dicts(clusters: Sequence[ContentIdeaCluster]) -> list[dict]:
    """Return JSON-serializable cluster dictionaries."""
    return [
        {
            "label": cluster.label,
            "idea_ids": cluster.idea_ids,
            "representative_note": cluster.representative_note,
            "shared_terms": cluster.shared_terms,
            "sources": cluster.sources,
            "priority_mix": cluster.priority_mix,
        }
        for cluster in clusters
    ]


def _ideas_match(
    left: _Idea,
    right: _Idea,
    *,
    embeddings: dict[int, Sequence[float]] | None,
    lexical_threshold: float,
    embedding_threshold: float,
) -> bool:
    if left.topic_key and left.topic_key == right.topic_key:
        return True
    if left.source_ids and left.source_ids.intersection(right.source_ids):
        return True

    lexical_score = _jaccard(left.terms, right.terms)
    if lexical_score >= lexical_threshold:
        return True
    if left.source_key and left.source_key == right.source_key and lexical_score >= 0.18:
        return True

    if embeddings and left.id in embeddings and right.id in embeddings:
        return _cosine(embeddings[left.id], embeddings[right.id]) >= embedding_threshold
    return False


def _build_cluster(ideas: Sequence[_Idea]) -> ContentIdeaCluster:
    ordered = sorted(ideas, key=lambda idea: idea.id)
    shared_terms = _shared_terms(ordered)
    representative = min(
        ordered,
        key=lambda idea: (PRIORITY_ORDER.get(idea.priority, 3), idea.id),
    )
    priority_counter = Counter(idea.priority or "normal" for idea in ordered)
    priority_mix = {
        priority: priority_counter[priority]
        for priority in ("high", "normal", "low")
        if priority_counter[priority]
    }

    return ContentIdeaCluster(
        label=_cluster_label(ordered, shared_terms),
        idea_ids=[idea.id for idea in ordered],
        representative_note=representative.note,
        shared_terms=shared_terms,
        sources=sorted({idea.source for idea in ordered if idea.source}),
        priority_mix=priority_mix,
    )


def _cluster_label(ideas: Sequence[_Idea], shared_terms: Sequence[str]) -> str:
    topics = Counter(idea.topic_key for idea in ideas if idea.topic_key)
    if topics:
        topic_key, _count = min(
            topics.items(),
            key=lambda item: (-item[1], item[0]),
        )
        for idea in sorted(ideas, key=lambda item: item.id):
            if idea.topic_key == topic_key:
                return idea.topic.strip()

    common_source_ids = set(ideas[0].source_ids)
    for idea in ideas[1:]:
        common_source_ids.intersection_update(idea.source_ids)
    sources = sorted({idea.source for idea in ideas if idea.source})
    if common_source_ids and len(sources) == 1:
        return sources[0]

    if shared_terms:
        return " / ".join(shared_terms[:3])

    if sources:
        return sources[0]
    return f"Idea {min(idea.id for idea in ideas)}"


def _shared_terms(ideas: Sequence[_Idea]) -> list[str]:
    if not ideas:
        return []

    term_counts = Counter(term for idea in ideas for term in idea.terms)
    if len(ideas) == 1:
        candidates = term_counts
    else:
        candidates = Counter(
            {
                term: count
                for term, count in term_counts.items()
                if count >= 2 or count == len(ideas)
            }
        )
    return [
        term
        for term, _count in sorted(candidates.items(), key=lambda item: (-item[1], item[0]))
    ][:8]


def _normalize_idea(row: dict) -> _Idea:
    note = _clean_text(row.get("note"))
    topic = _clean_text(row.get("topic"))
    source = _clean_text(row.get("source"))
    priority = _clean_text(row.get("priority")) or "normal"
    text_for_terms = " ".join(part for part in (topic, note) if part)
    return _Idea(
        id=int(row["id"]),
        note=note,
        topic=topic,
        source=source,
        priority=priority,
        topic_key=_normalize_key(topic),
        source_key=_normalize_key(source),
        terms=frozenset(_tokens(text_for_terms)),
        source_ids=frozenset(_source_identity(row.get("source_metadata")).items()),
    )


def _tokens(text: str) -> set[str]:
    return {
        token
        for token in TOKEN_RE.findall(text.lower())
        if len(token) >= 3 and token not in STOPWORDS
    }


def _source_identity(source_metadata: dict | str | None) -> dict[str, str]:
    if not source_metadata:
        return {}
    if isinstance(source_metadata, str):
        try:
            metadata = json.loads(source_metadata)
        except (TypeError, ValueError):
            return {}
    elif isinstance(source_metadata, dict):
        metadata = source_metadata
    else:
        return {}
    if not isinstance(metadata, dict):
        return {}

    ids: dict[str, str] = {}
    for key, value in metadata.items():
        if value in (None, ""):
            continue
        normalized_key = str(key).strip().lower()
        is_identity_key = (
            normalized_key in SOURCE_ID_KEYS
            or (
                normalized_key.endswith("_id")
                and normalized_key not in SOURCE_ID_EXCLUSIONS
            )
        )
        if is_identity_key:
            ids[normalized_key] = _metadata_value(value)
    return ids


def _metadata_value(value: object) -> str:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return str(value).strip().lower()


def _jaccard(left: frozenset[str], right: frozenset[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left.intersection(right)) / len(left.union(right))


def _cosine(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right) or not left:
        return 0.0
    dot = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return dot / (left_norm * right_norm)


def _clean_text(value: object | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _normalize_key(value: object | None) -> str:
    return _clean_text(value).lower()
