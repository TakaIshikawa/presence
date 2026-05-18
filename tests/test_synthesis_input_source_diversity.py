"""Tests for synthesis input source diversity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.synthesis_input_source_diversity import (
    build_synthesis_input_source_diversity_report,
    build_synthesis_input_source_diversity_report_from_db,
    format_synthesis_input_source_diversity_json,
    format_synthesis_input_source_diversity_text,
)


NOW = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "synthesis_input_source_diversity.py"
spec = importlib.util.spec_from_file_location("synthesis_input_source_diversity_script", SCRIPT_PATH)
synthesis_input_source_diversity_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(synthesis_input_source_diversity_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_balanced_inputs_are_not_flagged():
    report = build_synthesis_input_source_diversity_report(
        [
            {"content_id": 1, "source_type": "commit"},
            {"content_id": 1, "source_type": "commit"},
            {"content_id": 1, "source_type": "claude_session"},
            {"content_id": 1, "source_type": "curated_article"},
        ],
        dominance_threshold=0.5,
        now=NOW,
    )

    assert report.findings == ()
    assert report.total_groups == 1
    assert report.total_source_count == 4


def test_single_source_dominance_is_flagged_with_counts_and_reason():
    report = build_synthesis_input_source_diversity_report(
        [
            {"content_id": "42", "run_id": "run-a", "source_type": "source_commits"},
            {"content_id": "42", "run_id": "run-a", "source_type": "github_commit"},
            {"content_id": "42", "run_id": "run-a", "source_type": "commit"},
            {"content_id": "42", "run_id": "run-a", "source_type": "reply_context"},
        ],
        dominance_threshold=0.7,
        now=NOW,
    )
    finding = report.findings[0]

    assert finding.content_id == "42"
    assert finding.run_id == "run-a"
    assert finding.total_source_count == 4
    assert finding.source_type_counts == {"commit": 3, "reply_context": 1}
    assert finding.dominant_source_type == "commit"
    assert finding.dominant_share == 0.75
    assert "above threshold" in finding.reason


def test_missing_source_types_are_counted_as_unknown():
    report = build_synthesis_input_source_diversity_report(
        [
            {"content_id": 7, "source_type": ""},
            {"content_id": 7},
            {"content_id": 7, "source_type": "curated_newsletter"},
        ],
        dominance_threshold=0.6,
        now=NOW,
    )
    finding = report.findings[0]

    assert finding.dominant_source_type == "unknown"
    assert finding.source_type_counts == {"curated_knowledge": 1, "unknown": 2}
    assert finding.dominant_share == 0.6667


def test_threshold_must_be_exceeded_and_is_configurable():
    rows = [
        {"content_id": 1, "source_type": "reply_queue"},
        {"content_id": 1, "source_type": "reply_context"},
        {"content_id": 1, "source_type": "commit"},
    ]

    equal_threshold = build_synthesis_input_source_diversity_report(rows, dominance_threshold=2 / 3, now=NOW)
    lower_threshold = build_synthesis_input_source_diversity_report(rows, dominance_threshold=0.65, now=NOW)

    assert equal_threshold.findings == ()
    assert lower_threshold.findings[0].dominant_source_type == "reply_context"


def test_empty_input_returns_empty_report_and_json_shape():
    report = build_synthesis_input_source_diversity_report([], now=NOW)
    payload = json.loads(format_synthesis_input_source_diversity_json(report))

    assert payload["artifact_type"] == "synthesis_input_source_diversity"
    assert payload["empty_state"]["is_empty"] is True
    assert payload["totals"]["group_count"] == 0
    assert payload["findings"] == []


def test_db_loader_includes_generated_content_and_knowledge_links(db):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha-1", "sha-2", "sha-3"],
        source_messages=["msg-1"],
        source_activity_ids=[],
        content="Dominated by commits.",
        eval_score=8.0,
        eval_feedback="ok",
    )
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge (source_type, source_id, content, approved)
           VALUES ('curated_article', 'article-1', 'Useful source', 1)"""
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    report = build_synthesis_input_source_diversity_report_from_db(db, dominance_threshold=0.55, now=NOW)
    finding = report.findings[0]

    assert finding.content_id == str(content_id)
    assert finding.source_type_counts == {"claude_session": 1, "commit": 3, "curated_knowledge": 1}
    assert finding.dominant_source_type == "commit"


def test_cli_outputs_json_and_table(db, monkeypatch, capsys):
    db.insert_generated_content(
        content_type="blog_post",
        source_commits=["sha-1", "sha-2", "sha-3"],
        source_messages=[],
        source_activity_ids=["presence#1:issue"],
        content="CLI source mix.",
        eval_score=8.0,
        eval_feedback="ok",
    )
    monkeypatch.setattr(synthesis_input_source_diversity_script, "script_context", lambda: _script_context(db))

    assert synthesis_input_source_diversity_script.main(["--dominance-threshold", "0.7", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["findings"][0]["dominant_source_type"] == "commit"
    assert payload["dominance_threshold"] == 0.7

    assert synthesis_input_source_diversity_script.main(["--dominance-threshold", "0.7", "--table"]) == 0
    text = capsys.readouterr().out
    assert "Synthesis Input Source Diversity" in text
    assert "commit" in text
