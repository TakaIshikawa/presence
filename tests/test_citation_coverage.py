"""Tests for citation_coverage.py."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from citation_coverage import build_coverage_payload, format_text_report, main
from knowledge.citation_coverage import CitationCoverageScorer


def insert_knowledge(
    db,
    *,
    source_type: str = "curated_article",
    source_id: str = "article-coverage",
    source_url: str | None = "https://example.test/source",
    content: str = "Postgres added JSONB indexing for application queries.",
    insight: str | None = None,
) -> int:
    return db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight, license, attribution_required, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)""",
        (
            source_type,
            source_id,
            source_url,
            "Grace",
            content,
            insight,
            "open",
            0,
        ),
    ).lastrowid


def test_scorer_marks_claim_covered_by_curated_source(db):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Postgres added JSONB indexing for application queries.",
        eval_score=8.0,
        eval_feedback="grounded",
    )
    knowledge_id = insert_knowledge(db)
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.93)])

    result = CitationCoverageScorer().score_provenance(db.get_content_provenance(content_id))

    assert result.status == "covered"
    assert result.score == 1.0
    assert result.claims[0].status == "covered"
    assert result.claims[0].evidence_types == ["curated_knowledge"]


def test_scorer_marks_claim_covered_by_provenance_evidence(db):
    db.insert_commit(
        "presence",
        "sha-coverage",
        "fix: change backoff and cut retry errors by 42% in the polling worker",
        "2026-04-22T12:00:00+00:00",
        "taka",
    )
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha-coverage"],
        source_messages=[],
        content="The backoff change cut retry errors by 42%.",
        eval_score=8.0,
        eval_feedback="grounded",
    )

    result = CitationCoverageScorer().score_provenance(db.get_content_provenance(content_id))

    assert result.status == "covered"
    assert result.claims[0].status == "covered"
    assert result.claims[0].evidence_types == ["provenance"]


def test_scorer_marks_partial_and_missing_claims(db):
    scorer = CitationCoverageScorer()

    thin = scorer.score_content(
        {"id": 1, "content_type": "x_post", "content": "The backoff change cut retry errors by 87%."},
        {
            "content": {"id": 1, "content_type": "x_post", "content": "The backoff change cut retry errors by 87%."},
            "source_commits": [
                {"commit_sha": "sha-thin", "commit_message": "fix: change backoff for retry errors"}
            ],
            "knowledge_links": [],
        },
    )
    missing = scorer.score_content(
        {"id": 2, "content_type": "x_post", "content": "Postgres removed JSONB indexing."},
        {"content": {"id": 2, "content_type": "x_post", "content": "Postgres removed JSONB indexing."}},
    )

    assert thin.status == "thin"
    assert thin.claims[0].reason == "only partial claim terms found in evidence"
    assert missing.status == "missing"
    assert missing.claims[0].reason == "no source evidence"


def test_build_payload_filters_below_min_score(db):
    covered_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Postgres added JSONB indexing for application queries.",
        eval_score=8.0,
        eval_feedback="grounded",
    )
    knowledge_id = insert_knowledge(db)
    db.insert_content_knowledge_links(covered_id, [(knowledge_id, 0.9)])
    missing_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Redis removed vector indexing.",
        eval_score=6.0,
        eval_feedback="unsupported",
    )

    payload = build_coverage_payload(db, days=30, min_score=0.75)

    assert [item["content_id"] for item in payload["items"]] == [missing_id]
    assert payload["items"][0]["below_min_score"] is True


def test_format_text_report_includes_claim_reasons(db):
    payload = {
        "content_id": None,
        "days": 30,
        "min_score": None,
        "count": 1,
        "items": [
            {
                "content_id": 7,
                "content_type": "x_post",
                "score": 0.5,
                "status": "thin",
                "content": "The backoff change cut retry errors by 87%.",
                "claim_count": 1,
                "covered_count": 0,
                "thin_count": 1,
                "missing_count": 0,
                "missing_traceable_link_count": 0,
                "below_min_score": False,
                "reasons": ["only partial claim terms found in evidence"],
                "claims": [
                    {
                        "status": "thin",
                        "kind": "metric",
                        "evidence_types": ["provenance"],
                        "text": "The backoff change cut retry errors by 87%.",
                        "reason": "only partial claim terms found in evidence",
                    }
                ],
            }
        ],
    }

    output = format_text_report(payload)

    assert "Citation Coverage Report" in output
    assert "status=thin" in output
    assert "only partial claim terms found in evidence" in output


def test_main_json_output(db, capsys):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Postgres added JSONB indexing for application queries.",
        eval_score=8.0,
        eval_feedback="grounded",
    )
    knowledge_id = insert_knowledge(db)
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.93)])

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("citation_coverage.script_context", fake_script_context):
        main(["--content-id", str(content_id), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["items"][0]["content_id"] == content_id
    assert payload["items"][0]["status"] == "covered"


def test_main_rejects_invalid_min_score():
    with pytest.raises(SystemExit, match="--min-score must be between 0 and 1"):
        main(["--min-score", "1.5"])

