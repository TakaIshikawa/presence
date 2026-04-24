"""Tests for deterministic knowledge contradiction scans."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge_contradictions import build_scan_payload, format_json_scan, main
from synthesis.knowledge_contradictions import scan_content_id


def _seed_content(db, content: str, knowledge_content: str | None = None, insight: str | None = None) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="clear",
    )
    if knowledge_content is not None:
        knowledge_id = db.conn.execute(
            """INSERT INTO knowledge
               (source_type, source_id, source_url, author, content, insight, approved)
               VALUES (?, ?, ?, ?, ?, ?, 1)""",
            (
                "curated_article",
                f"knowledge-{content_id}",
                "https://example.test/source",
                "Ada",
                knowledge_content,
                insight,
            ),
        ).lastrowid
        db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])
    return content_id


def test_numeric_mismatch_is_flagged(db):
    content_id = _seed_content(
        db,
        "The retry worker reduced errors by 42%.",
        "The retry worker reduced errors by 24%.",
    )

    warnings = scan_content_id(db, content_id)

    assert len(warnings) == 1
    assert warnings[0].kind == "numeric"
    assert warnings[0].claim_value == "42%"
    assert warnings[0].evidence_value == "24%"
    assert warnings[0].knowledge_id


def test_date_mismatch_is_flagged(db):
    content_id = _seed_content(
        db,
        "The preview API launched on April 24, 2026.",
        "The preview API launched on April 22, 2026.",
    )

    warnings = scan_content_id(db, content_id)

    assert len(warnings) == 1
    assert warnings[0].kind == "date"
    assert warnings[0].claim_value == "2026-04-24"
    assert warnings[0].evidence_value == "2026-04-22"


def test_no_linked_knowledge_returns_no_warnings(db):
    content_id = _seed_content(db, "The retry worker reduced errors by 42%.")

    assert scan_content_id(db, content_id) == []


def test_clean_content_returns_no_warnings(db):
    content_id = _seed_content(
        db,
        "The retry worker reduced errors by 42%. The preview API launched on April 24, 2026.",
        "The retry worker reduced errors by 42%. The preview API launched on April 24, 2026.",
        insight="React 19.2 is the supported named version.",
    )

    assert scan_content_id(db, content_id) == []


def test_named_version_mismatch_is_flagged(db):
    content_id = _seed_content(
        db,
        "React 19.3 is the supported version for this workflow.",
        "React 19.2 is the supported version for this workflow.",
    )

    warnings = scan_content_id(db, content_id)

    assert len(warnings) == 1
    assert warnings[0].kind == "version"
    assert warnings[0].claim_value == "react 19.3"
    assert warnings[0].evidence_value == "react 19.2"


def test_unrelated_numbers_are_not_flagged(db):
    content_id = _seed_content(
        db,
        "The retry worker reduced errors by 42%.",
        "The benchmark suite runs 24 tests.",
    )

    assert scan_content_id(db, content_id) == []


def test_json_output_is_stable(db):
    content_id = _seed_content(
        db,
        "The retry worker reduced errors by 42%.",
        "The retry worker reduced errors by 24%.",
    )
    payload = build_scan_payload(db, content_id=content_id, recent_days=7)

    output = format_json_scan(payload)
    parsed = json.loads(output)

    assert list(parsed) == ["content_id", "recent_days", "warning_count", "warnings"]
    assert parsed["warnings"][0]["claim_value"] == "42%"
    assert parsed["warnings"][0]["source_url"] == "https://example.test/source"


def test_main_scans_recent_unpublished_json(db, capsys):
    content_id = _seed_content(
        db,
        "The retry worker reduced errors by 42%.",
        "The retry worker reduced errors by 24%.",
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("knowledge_contradictions.script_context", fake_script_context):
        main(["--recent-days", "30", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["warning_count"] == 1
    assert payload["rows"][0]["content_id"] == content_id
