"""Tests for deterministic synthesis source bundle ranking."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rank_source_bundles import main  # noqa: E402
from synthesis.source_bundle_ranker import (  # noqa: E402
    build_source_bundle_rank_report,
    format_source_bundle_rank_text,
    rank_source_bundles,
)


NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def test_ranker_orders_fresh_specific_multi_source_bundle_first():
    bundles = rank_source_bundles(
        commits=[
            {
                "repo_name": "presence",
                "commit_sha": "abc1234",
                "commit_message": "Add source bundle ranker for synthesis evidence scoring",
                "timestamp": "2026-04-25T09:00:00+00:00",
            },
            {
                "repo_name": "presence",
                "commit_sha": "old1111",
                "commit_message": "Update docs",
                "timestamp": "2026-03-01T09:00:00+00:00",
            },
        ],
        messages=[
            {
                "message_uuid": "msg-1",
                "prompt_text": "Create a deterministic source bundle ranker that scores freshness, evidence density, and duplicate source text.",
                "timestamp": "2026-04-25T10:00:00+00:00",
            }
        ],
        github_activity=[
            {
                "repo_name": "presence",
                "activity_type": "issue",
                "number": 7,
                "activity_id": "presence#7:issue",
                "title": "Rank source bundles before synthesis",
                "body": "Operators need a preflight signal for fresh multi-source evidence.",
                "updated_at": "2026-04-25T11:00:00+00:00",
            }
        ],
        now=NOW,
        limit=None,
    )

    assert bundles[0]["source_count"] == 3
    assert set(bundles[0]["source_types"]) == {"commit", "github_activity", "message"}
    assert "multi-source corroboration" in bundles[0]["rationale"]
    assert "specific evidence-dense text" in bundles[0]["rationale"]
    assert bundles[0]["score"] > bundles[-1]["score"]
    assert bundles[-1]["source_ids"] == {"commit": ["old1111"]}


def test_near_duplicate_source_texts_are_grouped_and_penalized():
    bundles = rank_source_bundles(
        commits=[
            {
                "commit_sha": "aaa1111",
                "commit_message": "Fix retry scheduler race condition",
                "timestamp": "2026-04-25T09:00:00+00:00",
            },
            {
                "commit_sha": "bbb2222",
                "commit_message": "Fix retry scheduler race condition",
                "timestamp": "2026-04-25T09:05:00+00:00",
            },
        ],
        now=NOW,
        limit=None,
    )

    assert len(bundles) == 1
    assert bundles[0]["source_ids"] == {"commit": ["bbb2222", "aaa1111"]}
    assert bundles[0]["dedup_penalties"][0]["penalty"] == 10
    assert "near-duplicate" in bundles[0]["dedup_penalties"][0]["reason"]
    assert "near-duplicate evidence reduced score" in bundles[0]["rationale"]


def test_report_reads_recent_sources_from_database(db):
    db.insert_commit(
        repo_name="presence",
        commit_sha="abc1234",
        commit_message="Add source bundle ranker for synthesis inputs",
        timestamp="2026-04-25T09:00:00+00:00",
        author="taka",
    )
    db.insert_claude_message(
        session_id="session-1",
        message_uuid="msg-1",
        project_path="/tmp/presence",
        timestamp="2026-04-25T10:00:00+00:00",
        prompt_text="Rank source bundles using freshness and evidence density signals.",
    )
    db.upsert_github_activity(
        repo_name="presence",
        activity_type="issue",
        number=7,
        title="Rank source bundles before synthesis",
        body="Expose source IDs and rationale for operators.",
        state="open",
        author="taka",
        url="https://github.com/taka/presence/issues/7",
        updated_at="2026-04-25T11:00:00+00:00",
        created_at="2026-04-25T08:00:00+00:00",
        labels=["enhancement"],
        metadata={},
    )

    report = build_source_bundle_rank_report(db, days=7, limit=5, now=NOW)

    assert report["artifact_type"] == "source_bundle_rank"
    assert report["counts"] == {
        "commits": 1,
        "messages": 1,
        "github_activity": 1,
        "bundles": 1,
    }
    assert report["bundles"][0]["source_ids"]["github_activity"] == ["presence#7:issue"]


def test_text_output_includes_scores_ids_and_rationale():
    report = {
        "counts": {"commits": 1, "messages": 0, "github_activity": 0, "bundles": 1},
        "bundles": [
            {
                "score": 76,
                "source_count": 1,
                "source_types": ["commit"],
                "title": "Add ranker",
                "freshness_signals": {"newest_at": "2026-04-25T09:00:00+00:00"},
                "evidence_density_signals": {
                    "unique_token_count": 4,
                    "specific_term_count": 1,
                    "average_word_count": 3,
                },
                "source_ids": {"commit": ["abc1234"]},
                "rationale": ["fresh source activity"],
                "dedup_penalties": [],
            }
        ],
    }

    output = format_source_bundle_rank_text(report)

    assert "Source Bundle Rank" in output
    assert "score=76" in output
    assert "commit=abc1234" in output
    assert "fresh source activity" in output


def test_invalid_filters_are_rejected(db):
    with pytest.raises(ValueError, match="days"):
        build_source_bundle_rank_report(db, days=0, now=NOW)
    with pytest.raises(ValueError, match="limit"):
        build_source_bundle_rank_report(db, limit=0, now=NOW)
    with pytest.raises(ValueError, match="limit"):
        rank_source_bundles(limit=0)


def test_cli_json_output(db, capsys):
    db.insert_commit(
        repo_name="presence",
        commit_sha="abc1234",
        commit_message="Add source bundle ranker for synthesis inputs",
        timestamp=datetime.now(timezone.utc).isoformat(),
        author="taka",
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("rank_source_bundles.script_context", fake_script_context):
        assert main(["--format", "json", "--days", "7", "--limit", "3"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "source_bundle_rank"
    assert payload["filters"] == {"days": 7, "limit": 3}
    assert payload["bundles"][0]["source_ids"] == {"commit": ["abc1234"]}

