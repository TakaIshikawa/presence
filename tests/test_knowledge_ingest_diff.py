"""Tests for dry-run knowledge ingestion diffs."""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from knowledge.embeddings import EmbeddingProvider
from knowledge.ingest_diff import generate_ingest_diff
from knowledge.store import KnowledgeItem, KnowledgeStore

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import knowledge_ingest_diff  # noqa: E402


class StableEmbedder(EmbeddingProvider):
    def embed(self, text: str) -> list[float]:
        lowered = text.casefold()
        if "duplicate" in lowered:
            return [1.0, 0.0]
        if "same" in lowered:
            return [0.0, 1.0]
        if "changed" in lowered:
            return [0.0, -1.0]
        if "new" in lowered or "brand" in lowered:
            return [-1.0, 0.0]
        return [1 / math.sqrt(2), 1 / math.sqrt(2)]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [self.embed(text) for text in texts]


def _store(db) -> KnowledgeStore:
    return KnowledgeStore(db.conn, StableEmbedder())


def _item(source_id: str, content: str, source_type: str = "curated_x") -> KnowledgeItem:
    return KnowledgeItem(
        id=None,
        source_type=source_type,
        source_id=source_id,
        source_url=f"https://example.com/{source_id}",
        author="tester",
        content=content,
        insight=None,
        embedding=None,
        attribution_required=True,
        approved=True,
        created_at=None,
    )


def test_diff_classifies_new_unchanged_changed_duplicate_and_rejected(db):
    store = _store(db)
    unchanged_id = store.add_item(_item("same", "Same content with spacing"))
    changed_id = store.add_item(_item("changed", "Original changed content"))
    duplicate_id = store.add_item(_item("existing-dup", "Duplicate content about agents"))
    before_count = db.conn.execute("SELECT COUNT(*) FROM knowledge").fetchone()[0]

    diff = generate_ingest_diff(
        store,
        [
            {
                "source_type": "curated_x",
                "source_id": "new",
                "content": "Brand new content",
            },
            {
                "source_type": "curated_x",
                "source_id": "same",
                "content": "  same   content with spacing ",
            },
            {
                "source_type": "curated_x",
                "source_id": "changed",
                "content": "Updated changed content",
            },
            {
                "source_type": "curated_article",
                "source_id": "near-dup",
                "content": "Duplicate content about agent",
            },
            {"source_type": "curated_x", "source_id": "", "content": "bad"},
        ],
        duplicate_similarity_threshold=0.90,
    )

    assert [item.source_id for item in diff.new_items] == ["new"]
    assert [(item.knowledge_id, item.source_id) for item in diff.existing_items] == [
        (unchanged_id, "same")
    ]
    assert [(item.knowledge_id, item.changed_fields) for item in diff.changed_items] == [
        (changed_id, ["content"])
    ]
    assert len(diff.duplicate_candidates) == 1
    duplicate = diff.duplicate_candidates[0]
    assert duplicate.duplicate_of_id == duplicate_id
    assert duplicate.duplicate_of_source_id == "existing-dup"
    assert duplicate.match_type in {"lexical", "embedding"}
    assert diff.rejected_items[0].reason == "missing required field(s): source_id"

    after_count = db.conn.execute("SELECT COUNT(*) FROM knowledge").fetchone()[0]
    assert after_count == before_count
    assert store.get_by_source("curated_x", "new") is None
    assert store.get_by_source("curated_x", "changed").content == "Original changed content"


def test_near_duplicate_is_separate_from_exact_source_match(db):
    store = _store(db)
    store.add_item(_item("source-1", "Duplicate content about agents"))

    diff = generate_ingest_diff(
        store,
        [
            {
                "source_type": "curated_x",
                "source_id": "source-1",
                "content": "Duplicate content about agents",
            },
            {
                "source_type": "curated_x",
                "source_id": "source-2",
                "content": "Duplicate content about agent",
            },
        ],
        duplicate_similarity_threshold=0.90,
    )

    assert [item.source_id for item in diff.existing_items] == ["source-1"]
    assert [item.source_id for item in diff.duplicate_candidates] == ["source-2"]


def test_cli_reads_jsonl_and_emits_stable_json(db, tmp_path, capsys):
    store_db = db
    store = _store(store_db)
    store.add_item(_item("same", "Same content"))
    candidate_file = tmp_path / "candidates.jsonl"
    candidate_file.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "source_type": "curated_x",
                        "source_id": "same",
                        "content": "Same content",
                    }
                ),
                json.dumps(
                    {
                        "source_type": "curated_x",
                        "source_id": "new",
                        "content": "New content",
                    }
                ),
            ]
        )
    )

    config = SimpleNamespace(
        embeddings=SimpleNamespace(provider="stable", api_key="test", model="stable")
    )

    class Context:
        def __enter__(self):
            return config, store_db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("knowledge_ingest_diff.script_context", return_value=Context()), patch(
        "knowledge_ingest_diff.get_embedding_provider", return_value=StableEmbedder()
    ):
        result = knowledge_ingest_diff.main([str(candidate_file), "--json"])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert list(payload.keys()) == [
        "changed_items",
        "duplicate_candidates",
        "existing_items",
        "new_items",
        "rejected_items",
        "summary",
    ]
    assert payload["summary"] == {
        "changed": 0,
        "duplicates": 0,
        "new": 1,
        "rejected": 0,
        "unchanged": 1,
    }
    assert payload["existing_items"][0]["source_id"] == "same"
    assert payload["new_items"][0]["source_id"] == "new"
