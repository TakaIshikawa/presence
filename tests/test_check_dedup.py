"""Tests for scripts/check_dedup.py."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from check_dedup import build_report, extract_opening, main
from knowledge.embeddings import serialize_embedding


class FakeEmbedder:
    def __init__(self, embedding):
        self.embedding = embedding
        self.calls = []

    def embed(self, text):
        self.calls.append(text)
        return self.embedding


def _published(db, content, content_type="x_post", embedding=None):
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.mark_published(content_id, f"http://x.com/{content_id}", f"tweet-{content_id}")
    if embedding is not None:
        db.set_content_embedding(content_id, serialize_embedding(embedding))
    return content_id


def test_extract_opening_matches_pipeline_clause_rules():
    assert extract_opening("TWEET 1:\nDebugging is context: details follow") == "debugging is context"
    assert extract_opening("Error handling matters. The rest explains why") == "error handling matters"


def test_report_flags_opening_clause_similarity(db):
    matched_id = _published(db, "Same opening everywhere: this is the older post.")

    report = build_report(
        "Same opening everywhere: this is the candidate.",
        db,
        semantic_enabled=False,
    )

    assert report.rejected is True
    assert report.opening_clause_similarity.rejected is True
    assert report.opening_clause_similarity.matched_content_id == matched_id
    assert report.stale_patterns.rejected is False
    assert report.semantic_similarity.enabled is False


def test_report_flags_stale_patterns_without_database_matches(db):
    report = build_report(
        "Unpopular opinion: unit tests are not enough",
        db,
        semantic_enabled=False,
    )

    assert report.rejected is True
    assert report.stale_patterns.rejected is True
    assert report.stale_patterns.matches


def test_report_flags_semantic_similarity_when_enabled(db):
    matched_id = _published(
        db,
        "Built retry logic around a queue worker.",
        embedding=[1.0, 0.0, 0.0],
    )
    embedder = FakeEmbedder([1.0, 0.0, 0.0])

    report = build_report(
        "Added resilient retry handling to the worker.",
        db,
        embedder=embedder,
        semantic_enabled=True,
        semantic_threshold=0.9,
    )

    assert report.rejected is True
    assert report.semantic_similarity.rejected is True
    assert report.semantic_similarity.matched_content_id == matched_id
    assert report.semantic_similarity.max_similarity == pytest.approx(1.0)
    assert embedder.calls == ["Added resilient retry handling to the worker."]


def test_semantic_disabled_does_not_call_embedder(db):
    _published(db, "Built retry logic around a queue worker.", embedding=[1.0, 0.0])
    embedder = FakeEmbedder([1.0, 0.0])

    report = build_report(
        "A clean candidate with different framing",
        db,
        embedder=embedder,
        semantic_enabled=False,
    )

    assert report.semantic_similarity.enabled is False
    assert embedder.calls == []


def test_cli_json_reports_layers_with_semantic_disabled(file_db, capsys):
    _published(file_db, "Shipping taught me to make rollback boring.")

    exit_code = main([
        "--db",
        str(file_db.db_path),
        "--json",
        "Everyone says deployment should be exciting",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["rejected"] is True
    assert payload["stale_patterns"]["rejected"] is True
    assert payload["semantic_similarity"]["enabled"] is False
