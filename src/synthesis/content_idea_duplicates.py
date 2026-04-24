"""Duplicate cluster reporting for open content ideas."""

from __future__ import annotations

import json
import re
import string
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any

from storage.db import (
    CONTENT_IDEA_METADATA_ID_EXCLUSIONS,
    CONTENT_IDEA_METADATA_ID_KEYS,
)


_PUNCT_TRANSLATION = str.maketrans({char: " " for char in string.punctuation})
_PRIORITY_RANK = {"high": 0, "normal": 1, "low": 2}


@dataclass(frozen=True)
class ContentIdeaDuplicateMember:
    """One idea included in a duplicate cluster."""

    id: int
    note: str
    topic: str | None
    priority: str
    source: str | None
    created_at: str | None
    matching_reasons: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class ContentIdeaDuplicateCluster:
    """A set of open ideas that appear to represent the same opportunity."""

    primary_idea_id: int
    idea_ids: tuple[int, ...]
    members: tuple[ContentIdeaDuplicateMember, ...]
    reasons: tuple[str, ...]
    max_similarity: float
    shared_source_identifiers: dict[str, str]


def normalize_content_idea_text(value: object | None) -> str:
    """Normalize text for duplicate comparison."""
    text = str(value or "").casefold()
    text = text.translate(_PUNCT_TRANSLATION)
    return re.sub(r"\s+", " ", text).strip()


def content_idea_similarity(first: dict, second: dict) -> float:
    """Return lexical similarity across normalized note/topic text."""
    first_text = _comparison_text(first)
    second_text = _comparison_text(second)
    if not first_text or not second_text:
        return 0.0
    return SequenceMatcher(None, first_text, second_text).ratio()


def find_duplicate_clusters(
    db,
    *,
    min_similarity: float = 0.86,
    topic: str | None = None,
    include_low_priority: bool = False,
) -> list[ContentIdeaDuplicateCluster]:
    """Find duplicate clusters among open content ideas without modifying state."""
    if min_similarity < 0 or min_similarity > 1:
        raise ValueError("min_similarity must be between 0 and 1")

    ideas = _load_open_content_ideas(db, topic=topic, include_low_priority=include_low_priority)
    if len(ideas) < 2:
        return []

    adjacency: dict[int, set[int]] = {int(idea["id"]): set() for idea in ideas}
    pair_reasons: dict[frozenset[int], tuple[set[str], float, dict[str, str]]] = {}

    for index, first in enumerate(ideas):
        for second in ideas[index + 1 :]:
            reasons: set[str] = set()
            shared_ids = _shared_source_identifiers(first, second)
            similarity = content_idea_similarity(first, second)
            if similarity >= min_similarity:
                reasons.add("lexical_similarity")
            for key in shared_ids:
                reasons.add(f"source_metadata.{key}")
            if not reasons:
                continue

            first_id = int(first["id"])
            second_id = int(second["id"])
            adjacency[first_id].add(second_id)
            adjacency[second_id].add(first_id)
            pair_reasons[frozenset((first_id, second_id))] = (reasons, similarity, shared_ids)

    clusters: list[ContentIdeaDuplicateCluster] = []
    seen: set[int] = set()
    idea_by_id = {int(idea["id"]): idea for idea in ideas}
    for idea_id in sorted(adjacency):
        if idea_id in seen or not adjacency[idea_id]:
            continue
        component = _connected_component(idea_id, adjacency)
        seen.update(component)
        clusters.append(_build_cluster(component, idea_by_id, pair_reasons))

    return sorted(clusters, key=lambda cluster: (_primary_sort_key(idea_by_id[cluster.primary_idea_id]), cluster.idea_ids))


def clusters_to_dict(clusters: list[ContentIdeaDuplicateCluster]) -> dict[str, Any]:
    """Convert clusters to a JSON-serializable report payload."""
    return {
        "cluster_count": len(clusters),
        "clusters": [
            {
                "primary_idea_id": cluster.primary_idea_id,
                "idea_ids": list(cluster.idea_ids),
                "reasons": list(cluster.reasons),
                "max_similarity": round(cluster.max_similarity, 4),
                "shared_source_identifiers": cluster.shared_source_identifiers,
                "members": [
                    {
                        "id": member.id,
                        "note": member.note,
                        "topic": member.topic,
                        "priority": member.priority,
                        "source": member.source,
                        "created_at": member.created_at,
                        "matching_reasons": list(member.matching_reasons),
                    }
                    for member in cluster.members
                ],
            }
            for cluster in clusters
        ],
    }


def _load_open_content_ideas(
    db,
    *,
    topic: str | None,
    include_low_priority: bool,
) -> list[dict]:
    filters = ["status = 'open'"]
    params: list[object] = []
    if not include_low_priority:
        filters.append("COALESCE(priority, 'normal') != 'low'")
    normalized_topic = normalize_content_idea_text(topic)
    if normalized_topic:
        filters.append("topic IS NOT NULL")

    cursor = db.conn.execute(
        f"""SELECT *
            FROM content_ideas
            WHERE {' AND '.join(filters)}
            ORDER BY created_at ASC, id ASC""",
        params,
    )
    ideas = [dict(row) for row in cursor.fetchall()]
    if normalized_topic:
        ideas = [
            idea
            for idea in ideas
            if normalize_content_idea_text(idea.get("topic")) == normalized_topic
        ]
    return ideas


def _comparison_text(idea: dict) -> str:
    return " ".join(
        part
        for part in (
            normalize_content_idea_text(idea.get("topic")),
            normalize_content_idea_text(idea.get("note")),
        )
        if part
    )


def _connected_component(start: int, adjacency: dict[int, set[int]]) -> set[int]:
    component: set[int] = set()
    stack = [start]
    while stack:
        idea_id = stack.pop()
        if idea_id in component:
            continue
        component.add(idea_id)
        stack.extend(sorted(adjacency[idea_id] - component))
    return component


def _build_cluster(
    component: set[int],
    idea_by_id: dict[int, dict],
    pair_reasons: dict[frozenset[int], tuple[set[str], float, dict[str, str]]],
) -> ContentIdeaDuplicateCluster:
    ideas = [idea_by_id[idea_id] for idea_id in sorted(component)]
    primary = min(ideas, key=_primary_sort_key)
    reasons: set[str] = set()
    shared_source_identifiers: dict[str, str] = {}
    max_similarity = 0.0
    member_reasons: dict[int, set[str]] = {int(idea["id"]): set() for idea in ideas}

    for first_id in component:
        for second_id in component:
            if first_id >= second_id:
                continue
            pair = pair_reasons.get(frozenset((first_id, second_id)))
            if pair is None:
                continue
            pair_reason_set, similarity, shared_ids = pair
            reasons.update(pair_reason_set)
            member_reasons[first_id].update(pair_reason_set)
            member_reasons[second_id].update(pair_reason_set)
            max_similarity = max(max_similarity, similarity)
            shared_source_identifiers.update(shared_ids)

    members = tuple(
        ContentIdeaDuplicateMember(
            id=int(idea["id"]),
            note=str(idea.get("note") or ""),
            topic=idea.get("topic"),
            priority=idea.get("priority") or "normal",
            source=idea.get("source"),
            created_at=idea.get("created_at"),
            matching_reasons=tuple(sorted(member_reasons[int(idea["id"])])),
        )
        for idea in sorted(ideas, key=_primary_sort_key)
    )
    return ContentIdeaDuplicateCluster(
        primary_idea_id=int(primary["id"]),
        idea_ids=tuple(sorted(component)),
        members=members,
        reasons=tuple(sorted(reasons)),
        max_similarity=max_similarity,
        shared_source_identifiers=dict(sorted(shared_source_identifiers.items())),
    )


def _primary_sort_key(idea: dict) -> tuple[int, str, int]:
    return (
        _PRIORITY_RANK.get(idea.get("priority") or "normal", 3),
        str(idea.get("created_at") or ""),
        int(idea["id"]),
    )


def _shared_source_identifiers(first: dict, second: dict) -> dict[str, str]:
    first_ids = _source_metadata_ids(first.get("source_metadata"))
    second_ids = _source_metadata_ids(second.get("source_metadata"))
    return {
        key: value
        for key, value in first_ids.items()
        if second_ids.get(key) == value
    }


def _source_metadata_ids(source_metadata: dict | str | None) -> dict[str, str]:
    metadata = _decode_metadata(source_metadata)
    if not isinstance(metadata, dict):
        return {}

    ids: dict[str, str] = {}
    for key, value in metadata.items():
        if value in (None, ""):
            continue
        normalized_key = str(key).strip().lower()
        is_identity_key = (
            normalized_key in CONTENT_IDEA_METADATA_ID_KEYS
            or (
                normalized_key.endswith("_id")
                and normalized_key not in CONTENT_IDEA_METADATA_ID_EXCLUSIONS
            )
        )
        if is_identity_key:
            ids[normalized_key] = _metadata_value(value)
    return ids


def _decode_metadata(source_metadata: dict | str | None) -> dict | None:
    if isinstance(source_metadata, dict):
        return source_metadata
    if not source_metadata:
        return None
    try:
        decoded = json.loads(source_metadata)
    except (TypeError, ValueError):
        return None
    return decoded if isinstance(decoded, dict) else None


def _metadata_value(value: object) -> str:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return str(value).strip().lower()
