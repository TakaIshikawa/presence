"""Tests for the low-resonance rewrite seeding CLI."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from seed_rewrite_ideas import format_results_table, main, seed_rewrite_ideas


def _add_candidate(db) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="A generic rewrite candidate about test fixtures.",
        eval_score=7.0,
        eval_feedback="good",
    )
    db.conn.execute(
        """UPDATE generated_content
           SET published = 1,
               published_at = '2026-04-24T10:00:00+00:00',
               auto_quality = 'low_resonance'
           WHERE id = ?""",
        (content_id,),
    )
    db.conn.commit()
    db.insert_content_topics(content_id, [("testing", "", 1.0)])
    db.insert_engagement(content_id, f"tweet-{content_id}", 0, 0, 0, 0, 0.0)
    db.insert_prediction(
        content_id=content_id,
        predicted_score=7.0,
        hook_strength=6.0,
        specificity=3.0,
        emotional_resonance=5.0,
        novelty=7.0,
        actionability=4.0,
    )
    return content_id


def test_seed_rewrite_ideas_dry_run_does_not_write(db):
    content_id = _add_candidate(db)

    results = seed_rewrite_ideas(
        db,
        days=7,
        limit=10,
        min_score_gap=2.0,
        dry_run=True,
        priority="high",
    )

    assert len(results) == 1
    assert results[0].source_content_id == content_id
    assert results[0].reason == "dry run"
    assert db.get_content_ideas(status="open") == []


def test_format_results_table_shows_rewrite_candidates(db):
    assert "no eligible rewrite ideas" in format_results_table([])


def test_main_prints_table(db, capsys):
    _add_candidate(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("seed_rewrite_ideas.script_context", fake_script_context):
        main(["--days", "7", "--min-score-gap", "2", "--dry-run"])

    output = capsys.readouterr().out
    assert "skipped" in output
    assert "dry run" in output
    assert "testing" in output
    assert db.get_content_ideas(status="open") == []


def test_main_prints_json(db, capsys):
    content_id = _add_candidate(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("seed_rewrite_ideas.script_context", fake_script_context):
        main(["--days", "7", "--min-score-gap", "2", "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["status"] == "skipped"
    assert payload[0]["source_content_id"] == content_id
    assert payload[0]["candidate"]["source_metadata"]["source_content_id"] == content_id


def test_format_results_table_includes_created_result(db):
    class Candidate:
        def to_dict(self):
            return {}

    class Result:
        status = "created"
        idea_id = 42
        source_content_id = 7
        topic = "testing"
        reason = "created"
        note = "Rewrite low-resonance testing with clearer stakes."
        candidate = Candidate()

    output = format_results_table([Result()])

    assert "created" in output
    assert "testing" in output
    assert "Rewrite low-resonance" in output
