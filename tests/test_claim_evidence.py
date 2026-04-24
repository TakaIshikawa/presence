"""Tests for claim evidence artifact exports."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from claim_evidence import main
from synthesis.claim_evidence import (
    format_claim_evidence_json,
    format_claim_evidence_markdown,
    list_claim_checked_content_ids,
    load_claim_evidence,
    load_claim_evidence_export,
)


def _insert_knowledge(db, *, insight: str, content: str = "source material") -> int:
    return db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            f"article-{insight[:8]}",
            "https://example.test/source",
            "Ada",
            content,
            insight,
        ),
    ).lastrowid


def seed_claim_evidence(db) -> tuple[int, int]:
    db.insert_commit(
        "presence",
        "sha-claim",
        "fix: change backoff and cut retry errors by 42% in the polling worker",
        "2026-04-22T12:00:00+00:00",
        "taka",
    )
    db.insert_claude_message(
        "session-claim",
        "msg-claim",
        "/repo",
        "2026-04-22T11:55:00+00:00",
        "Redis added vector indexing for search workloads.",
    )
    unsupported_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha-claim", "missing-sha"],
        source_messages=["msg-claim", "missing-msg"],
        content=(
            "The backoff change cut retry errors by 42%. "
            "Postgres removed JSONB indexing."
        ),
        eval_score=8.0,
        eval_feedback="clear",
        claim_check_summary={
            "supported_count": 1,
            "unsupported_count": 1,
            "annotation_text": "factual: Postgres removed JSONB indexing. (factual terms not found in sources)",
        },
    )
    knowledge_id = _insert_knowledge(
        db,
        insight="Redis added vector indexing for search workloads.",
    )
    db.insert_content_knowledge_links(unsupported_id, [(knowledge_id, 0.91), (9999, 0.5)])

    supported_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=["msg-claim"],
        content="Redis added vector indexing for search workloads.",
        eval_score=7.6,
        eval_feedback="specific",
        claim_check_summary={
            "supported_count": 1,
            "unsupported_count": 0,
            "annotation_text": None,
        },
    )
    db.insert_content_knowledge_links(supported_id, [(knowledge_id, 0.7)])
    return unsupported_id, supported_id


def test_load_claim_evidence_includes_summary_claims_sources_and_warnings(db):
    content_id, _supported_id = seed_claim_evidence(db)

    payload = load_claim_evidence(db, content_id)

    assert payload["content"]["id"] == content_id
    assert payload["claim_check"]["status"] == "unsupported"
    assert payload["claim_check"]["unsupported_count"] == 1
    assert payload["claims"][0]["supported"] is False
    assert payload["claims"][0]["text"] == "Postgres removed JSONB indexing."
    assert payload["claims"][1]["supported"] is True
    assert {reference["type"] for reference in payload["source_references"]} == {
        "commit",
        "message",
        "knowledge",
    }
    assert "Missing source commit row for missing-sha" in payload["warnings"]
    assert "Missing source message row for missing-msg" in payload["warnings"]
    assert "Missing knowledge row for 9999" in payload["warnings"]


def test_status_filtered_multi_export(db):
    unsupported_id, supported_id = seed_claim_evidence(db)

    assert list_claim_checked_content_ids(db, "unsupported") == [unsupported_id]
    assert list_claim_checked_content_ids(db, "supported") == [supported_id]

    payload = load_claim_evidence_export(db, status="unsupported")

    assert isinstance(payload, list)
    assert [item["content"]["id"] for item in payload] == [unsupported_id]


def test_single_content_status_filter_returns_empty_list_on_mismatch(db):
    unsupported_id, _supported_id = seed_claim_evidence(db)

    assert load_claim_evidence_export(
        db,
        content_id=unsupported_id,
        status="supported",
    ) == []


def test_format_json_claim_evidence(db):
    content_id, _supported_id = seed_claim_evidence(db)

    payload = json.loads(format_claim_evidence_json(load_claim_evidence(db, content_id)))

    assert payload["content"]["id"] == content_id
    assert payload["claims"][0]["supported"] is False
    assert payload["source_references"][0]["type"] == "commit"


def test_markdown_is_readable_and_lists_unsupported_claims_first(db):
    content_id, _supported_id = seed_claim_evidence(db)

    output = format_claim_evidence_markdown(load_claim_evidence(db, content_id))

    assert output.startswith(f"# Claim Evidence: Content #{content_id}")
    unsupported_index = output.index("## Unsupported Claims (1)")
    supported_index = output.index("## Supported Claims (1)")
    assert unsupported_index < supported_index
    assert "Postgres removed JSONB indexing." in output[unsupported_index:supported_index]
    assert "Missing source commit row for missing-sha" in output


def test_main_writes_markdown_export(db, tmp_path, capsys):
    content_id, _supported_id = seed_claim_evidence(db)
    output_path = tmp_path / "claim-evidence.md"

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("claim_evidence.script_context", fake_script_context):
        main(
            [
                "--content-id",
                str(content_id),
                "--format",
                "markdown",
                "--output",
                str(output_path),
            ]
        )

    output = output_path.read_text(encoding="utf-8")
    assert output.startswith(f"# Claim Evidence: Content #{content_id}")
    assert "## Source References" in output
    assert "Exported claim evidence" in capsys.readouterr().err


def test_main_missing_content_exits(db):
    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("claim_evidence.script_context", fake_script_context):
        with pytest.raises(SystemExit, match="Content ID 9999 not found"):
            main(["--content-id", "9999"])
