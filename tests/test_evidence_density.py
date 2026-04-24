"""Tests for deterministic evidence-density scoring."""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from evidence_density import main  # noqa: E402
from synthesis.evidence_density import score_evidence_density  # noqa: E402


def test_specific_draft_scores_higher_than_generic_observation():
    generic = score_evidence_density(
        "Everyone should make systems better because best practices always matter."
    )
    specific = score_evidence_density(
        "Commit abc123def456 moved `retry_queue` into the worker and cut 14 errors "
        "across 3 runs after the SQLite migration."
    )

    assert specific.score > generic.score
    assert specific.status == "grounded"
    assert generic.status == "thin"
    assert {signal.name for signal in specific.positive_signals} >= {
        "numeric_specifics",
        "source_references",
        "quoted_technical_nouns",
        "implementation_terms",
    }
    assert {signal.name for signal in generic.negative_signals} >= {
        "vague_filler_phrases",
        "unsupported_absolute_claims",
    }


def test_report_serializes_to_json_shape():
    report = score_evidence_density(
        "PR #42 added the `schema.sql` index and reduced queue latency by 120 ms.",
        content_id=9,
    )

    payload = json.loads(json.dumps(asdict(report)))

    assert payload["content_id"] == 9
    assert payload["score"] >= 70
    assert payload["status"] == "grounded"
    assert payload["positive_signals"][0]["name"]
    assert isinstance(payload["recommendations"], list)


def test_cli_text_input_does_not_open_database(capsys):
    with patch("evidence_density.Database") as database_cls:
        exit_code = main([
            "--text",
            "The parser kept 7 files stable after `content_id` serialization changed.",
        ])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Evidence density:" in captured.out
    assert "Positive signals:" in captured.out
    database_cls.assert_not_called()


def test_cli_json_content_id_lookup(file_db, capsys):
    content_id = file_db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123def456"],
        source_messages=["msg-123456"],
        source_activity_ids=["presence#77:pr"],
        content="The worker retry path now logs 5 failed requests from `queue_adapter`.",
        eval_score=8.0,
        eval_feedback="ok",
    )

    exit_code = main([
        "--db",
        str(file_db.db_path),
        "--content-id",
        str(content_id),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["content_id"] == content_id
    assert payload["score"] >= 70
    source_signal = next(
        signal for signal in payload["positive_signals"] if signal["name"] == "source_references"
    )
    assert source_signal["count"] == 3


def test_cli_threshold_failure_exits_nonzero(capsys):
    exit_code = main([
        "--text",
        "Everyone should do better things because it is always the best way.",
        "--min-score",
        "60",
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["score"] < 60
    assert payload["negative_signals"]
