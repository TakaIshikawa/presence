"""Tests for content_provenance.py."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from content_provenance import (
    format_human_provenance,
    format_json_provenance,
    format_provenance_markdown,
    main,
)


def seed_content_provenance(db) -> int:
    db.insert_commit(
        "presence",
        "sha-prov",
        "feat: add content provenance",
        "2026-04-22T12:00:00+00:00",
        "taka",
    )
    db.insert_claude_message(
        "session-prov",
        "msg-prov",
        "/repo",
        "2026-04-22T11:55:00+00:00",
        "Create a provenance inspector",
    )
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha-prov"],
        source_messages=["msg-prov"],
        content="A concise generated post about provenance.",
        eval_score=8.6,
        eval_feedback="clear",
    )
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "article-prov",
            "https://example.test/prov",
            "Grace",
            "Source material",
            "Traceability lowers review cost",
        ),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.88)])
    db.upsert_content_variant(
        content_id,
        "bluesky",
        "post",
        "A Bluesky-specific provenance variant.",
        {"tone": "plain"},
    )
    db.upsert_publication_success(
        content_id,
        "bluesky",
        platform_post_id="at://did:plc:test/app.bsky.feed.post/1",
        platform_url="https://bsky.test/post/1",
        published_at="2026-04-22T13:00:00+00:00",
    )
    db.insert_engagement(content_id, "tweet-prov", 4, 1, 0, 0, 5.0)
    db.insert_bluesky_engagement(
        content_id,
        "at://did:plc:test/app.bsky.feed.post/1",
        7,
        2,
        1,
        0,
        10.0,
    )
    db.insert_pipeline_run(
        batch_id="batch-prov",
        content_type="x_post",
        candidates_generated=2,
        best_candidate_index=0,
        best_score_before_refine=8.1,
        final_score=8.6,
        published=True,
        content_id=content_id,
        outcome="published",
        filter_stats={"accepted": 1},
    )
    return content_id


def test_format_json_provenance(db):
    content_id = seed_content_provenance(db)
    payload = json.loads(format_json_provenance(db.get_content_provenance(content_id)))

    assert payload["content"]["id"] == content_id
    assert payload["source_commits"][0]["commit_sha"] == "sha-prov"
    assert payload["source_messages"][0]["message_uuid"] == "msg-prov"
    assert payload["knowledge_links"][0]["author"] == "Grace"
    assert payload["variants"][0]["metadata"] == {"tone": "plain"}
    assert {snapshot["platform"] for snapshot in payload["engagement_snapshots"]} == {
        "x",
        "bluesky",
    }
    assert payload["pipeline_runs"][0]["filter_stats"] == {"accepted": 1}


def test_format_human_provenance_is_concise(db):
    content_id = seed_content_provenance(db)
    output = format_human_provenance(db.get_content_provenance(content_id))

    assert f"Content #{content_id}" in output
    assert "Source commits (1)" in output
    assert "Claude messages (1)" in output
    assert "Knowledge links (1)" in output
    assert "Variants (1)" in output
    assert "Publications (1)" in output
    assert "Engagement snapshots (2)" in output
    assert "Pipeline runs (1)" in output
    assert "A concise generated post about provenance." in output


def test_format_markdown_provenance_includes_sections_and_missing_markers(db):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Bare provenance content.",
        eval_score=6.0,
        eval_feedback="plain",
    )

    output = format_provenance_markdown(db.get_content_provenance(content_id))

    assert output.startswith(f"# Content #{content_id} (x_post, unpublished)")
    assert "## Source commits (0)" in output
    assert "## Claude messages (0)" in output
    assert "## GitHub activity (0)" in output
    assert "## Knowledge links (0)" in output
    assert "## Variants (0)" in output
    assert "## Publications (0)" in output
    assert "## Engagement snapshots (0)" in output
    assert "## Pipeline runs (0)" in output
    assert output.count("- none") >= 8


def test_main_json_output(db, capsys):
    content_id = seed_content_provenance(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("content_provenance.script_context", fake_script_context):
        main([str(content_id), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["content"]["id"] == content_id
    assert payload["publications"][0]["platform"] == "bluesky"


def test_main_writes_markdown_bundle(db, tmp_path, capsys):
    content_id = seed_content_provenance(db)
    output_path = tmp_path / "provenance.md"

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("content_provenance.script_context", fake_script_context):
        main([str(content_id), "--markdown", "--output", str(output_path)])

    output = output_path.read_text(encoding="utf-8")
    assert output.startswith(f"# Content #{content_id}")
    assert "## Source commits (1)" in output
    assert "## Pipeline runs (1)" in output
    assert "Exported provenance bundle" in capsys.readouterr().err


def test_main_missing_content_exits(db):
    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("content_provenance.script_context", fake_script_context):
        with pytest.raises(SystemExit, match="Content ID 9999 not found"):
            main(["9999"])
