"""Dry-run diffing for knowledge ingestion candidates."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
import re
from typing import Any, Iterable

from .embeddings import cosine_similarity, deserialize_embedding
from .store import KnowledgeItem, KnowledgeStore


@dataclass(frozen=True)
class CandidateKnowledgeItem:
    source_type: str
    source_id: str
    content: str
    source_url: str | None = None
    author: str | None = None
    insight: str | None = None
    license: str = "attribution_required"
    attribution_required: bool | None = None
    approved: bool = True
    published_at: str | None = None


@dataclass(frozen=True)
class CandidateIssue:
    input_index: int
    reason: str
    candidate: dict[str, Any]


@dataclass(frozen=True)
class NewItem:
    input_index: int
    source_type: str
    source_id: str
    content_preview: str


@dataclass(frozen=True)
class ExistingItem:
    input_index: int
    knowledge_id: int | None
    source_type: str
    source_id: str
    status: str = "unchanged"


@dataclass(frozen=True)
class ChangedItem:
    input_index: int
    knowledge_id: int | None
    source_type: str
    source_id: str
    changed_fields: list[str]
    existing_content_preview: str
    candidate_content_preview: str


@dataclass(frozen=True)
class DuplicateCandidate:
    input_index: int
    source_type: str
    source_id: str
    duplicate_of_id: int | None
    duplicate_of_source_type: str
    duplicate_of_source_id: str
    similarity: float
    match_type: str


@dataclass(frozen=True)
class KnowledgeIngestDiff:
    new_items: list[NewItem]
    existing_items: list[ExistingItem]
    changed_items: list[ChangedItem]
    rejected_items: list[CandidateIssue]
    duplicate_candidates: list[DuplicateCandidate]

    def to_dict(self) -> dict[str, Any]:
        return {
            "changed_items": [asdict(item) for item in self.changed_items],
            "duplicate_candidates": [
                asdict(item) for item in self.duplicate_candidates
            ],
            "existing_items": [asdict(item) for item in self.existing_items],
            "new_items": [asdict(item) for item in self.new_items],
            "rejected_items": [asdict(item) for item in self.rejected_items],
            "summary": {
                "changed": len(self.changed_items),
                "duplicates": len(self.duplicate_candidates),
                "new": len(self.new_items),
                "rejected": len(self.rejected_items),
                "unchanged": len(self.existing_items),
            },
        }


_WHITESPACE_RE = re.compile(r"\s+")


def normalize_content(content: str) -> str:
    """Canonicalize content for stable dry-run comparisons."""
    return _WHITESPACE_RE.sub(" ", content).strip().casefold()


def build_candidate(data: dict[str, Any], input_index: int) -> CandidateKnowledgeItem:
    """Validate and normalize a raw JSON candidate."""
    required = ("source_type", "source_id", "content")
    missing = [
        field
        for field in required
        if not isinstance(data.get(field), str) or not data.get(field, "").strip()
    ]
    if missing:
        raise ValueError(f"missing required field(s): {', '.join(missing)}")

    attribution_required = data.get("attribution_required")
    if attribution_required is not None and not isinstance(attribution_required, bool):
        raise ValueError("attribution_required must be a boolean when provided")

    approved = data.get("approved", True)
    if not isinstance(approved, bool):
        raise ValueError("approved must be a boolean when provided")

    return CandidateKnowledgeItem(
        source_type=data["source_type"].strip(),
        source_id=data["source_id"].strip(),
        content=data["content"].strip(),
        source_url=_optional_str(data.get("source_url")),
        author=_optional_str(data.get("author")),
        insight=_optional_str(data.get("insight")),
        license=_optional_str(data.get("license")) or "attribution_required",
        attribution_required=attribution_required,
        approved=approved,
        published_at=_optional_str(data.get("published_at")),
    )


def generate_ingest_diff(
    store: KnowledgeStore,
    candidates: Iterable[dict[str, Any] | CandidateKnowledgeItem],
    *,
    duplicate_similarity_threshold: float = 0.92,
    preview_chars: int = 120,
) -> KnowledgeIngestDiff:
    """Classify ingestion candidates without mutating the knowledge store."""
    new_items: list[NewItem] = []
    existing_items: list[ExistingItem] = []
    changed_items: list[ChangedItem] = []
    rejected_items: list[CandidateIssue] = []
    duplicate_candidates: list[DuplicateCandidate] = []

    existing_rows = _load_existing_rows(store)

    for input_index, raw_candidate in enumerate(candidates):
        raw_dict = (
            _candidate_to_dict(raw_candidate)
            if isinstance(raw_candidate, CandidateKnowledgeItem)
            else dict(raw_candidate)
        )
        try:
            candidate = (
                raw_candidate
                if isinstance(raw_candidate, CandidateKnowledgeItem)
                else build_candidate(raw_dict, input_index)
            )
        except ValueError as exc:
            rejected_items.append(CandidateIssue(input_index, str(exc), raw_dict))
            continue

        existing = store.get_by_source(candidate.source_type, candidate.source_id)
        if existing:
            if normalize_content(existing.content) == normalize_content(candidate.content):
                existing_items.append(
                    ExistingItem(
                        input_index=input_index,
                        knowledge_id=existing.id,
                        source_type=candidate.source_type,
                        source_id=candidate.source_id,
                    )
                )
            else:
                changed_items.append(
                    ChangedItem(
                        input_index=input_index,
                        knowledge_id=existing.id,
                        source_type=candidate.source_type,
                        source_id=candidate.source_id,
                        changed_fields=["content"],
                        existing_content_preview=_preview(
                            existing.content, preview_chars
                        ),
                        candidate_content_preview=_preview(
                            candidate.content, preview_chars
                        ),
                    )
                )
            continue

        duplicate = _find_duplicate(
            store,
            candidate,
            existing_rows,
            duplicate_similarity_threshold,
        )
        if duplicate:
            duplicate_candidates.append(
                DuplicateCandidate(
                    input_index=input_index,
                    source_type=candidate.source_type,
                    source_id=candidate.source_id,
                    duplicate_of_id=duplicate["id"],
                    duplicate_of_source_type=duplicate["source_type"],
                    duplicate_of_source_id=duplicate["source_id"],
                    similarity=round(duplicate["similarity"], 6),
                    match_type=duplicate["match_type"],
                )
            )
            continue

        new_items.append(
            NewItem(
                input_index=input_index,
                source_type=candidate.source_type,
                source_id=candidate.source_id,
                content_preview=_preview(candidate.content, preview_chars),
            )
        )

    return KnowledgeIngestDiff(
        new_items=new_items,
        existing_items=existing_items,
        changed_items=changed_items,
        rejected_items=rejected_items,
        duplicate_candidates=duplicate_candidates,
    )


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("optional text fields must be strings when provided")
    stripped = value.strip()
    return stripped or None


def _candidate_to_dict(candidate: CandidateKnowledgeItem) -> dict[str, Any]:
    return asdict(candidate)


def _preview(content: str, max_chars: int) -> str:
    normalized = _WHITESPACE_RE.sub(" ", content).strip()
    if len(normalized) <= max_chars:
        return normalized
    return normalized[: max_chars - 3].rstrip() + "..."


def _load_existing_rows(store: KnowledgeStore) -> list[dict[str, Any]]:
    rows = store.conn.execute(
        """SELECT id, source_type, source_id, content, embedding
           FROM knowledge
           ORDER BY id ASC"""
    ).fetchall()
    return [
        {
            "id": row["id"],
            "source_type": row["source_type"],
            "source_id": row["source_id"],
            "content": row["content"],
            "embedding": row["embedding"],
            "normalized_content": normalize_content(row["content"] or ""),
        }
        for row in rows
    ]


def _find_duplicate(
    store: KnowledgeStore,
    candidate: CandidateKnowledgeItem,
    rows: list[dict[str, Any]],
    threshold: float,
) -> dict[str, Any] | None:
    candidate_normalized = normalize_content(candidate.content)
    best: dict[str, Any] | None = None

    for row in rows:
        if (
            row["source_type"] == candidate.source_type
            and row["source_id"] == candidate.source_id
        ):
            continue
        if row["normalized_content"] == candidate_normalized:
            return {**row, "similarity": 1.0, "match_type": "normalized_content"}

        lexical_similarity = SequenceMatcher(
            None, candidate_normalized, row["normalized_content"]
        ).ratio()
        if lexical_similarity >= threshold:
            best = _better_duplicate(
                best,
                {**row, "similarity": lexical_similarity, "match_type": "lexical"},
            )

    embedding_match = _find_embedding_duplicate(store, candidate, rows, threshold)
    if embedding_match:
        best = _better_duplicate(best, embedding_match)

    return best


def _find_embedding_duplicate(
    store: KnowledgeStore,
    candidate: CandidateKnowledgeItem,
    rows: list[dict[str, Any]],
    threshold: float,
) -> dict[str, Any] | None:
    rows_with_embeddings = [row for row in rows if row["embedding"]]
    if not rows_with_embeddings:
        return None

    candidate_embedding = store.embedder.embed(candidate.insight or candidate.content)
    best: dict[str, Any] | None = None
    for row in rows_with_embeddings:
        existing_embedding = deserialize_embedding(row["embedding"])
        similarity = cosine_similarity(candidate_embedding, existing_embedding)
        if similarity >= threshold:
            best = _better_duplicate(
                best,
                {**row, "similarity": similarity, "match_type": "embedding"},
            )
    return best


def _better_duplicate(
    current: dict[str, Any] | None, candidate: dict[str, Any]
) -> dict[str, Any]:
    if current is None:
        return candidate
    if candidate["similarity"] > current["similarity"]:
        return candidate
    if candidate["similarity"] == current["similarity"] and candidate["id"] < current["id"]:
        return candidate
    return current
