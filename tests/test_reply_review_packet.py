"""Tests for reply review packet export."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from export_reply_packets import list_pending_reply_drafts, main
from output.reply_review_packet import (
    build_reply_review_packet,
    packet_filename,
    write_reply_review_packets,
)


def _insert_reply(db, tweet_id: str = "tw-1", **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="How would you handle this?",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="I would start by making the failure mode observable.",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _mock_script_context(db):
    @contextmanager
    def _ctx():
        yield (SimpleNamespace(), db)

    return _ctx


def test_packet_includes_review_fields_when_available(db):
    relationship = {
        "stage_name": "Active",
        "engagement_stage": 3,
        "tier_name": "Key Network",
        "dunbar_tier": 2,
    }
    metadata = {
        "dedup": {"status": "passed", "lookback_hours": 72},
        "recommended_action": {"action": "revise", "rationale": "Needs specificity"},
    }
    reply_id = _insert_reply(
        db,
        platform="bluesky",
        relationship_context=json.dumps(relationship),
        quality_score=4.5,
        quality_flags=json.dumps(["sycophantic", "generic"]),
        platform_metadata=json.dumps(metadata),
        intent="question",
        priority="high",
    )
    reply = db.get_pending_replies()[0]

    packet = build_reply_review_packet(reply)

    assert packet["draft_id"] == reply_id
    assert packet["inbound"]["text"] == "How would you handle this?"
    assert packet["inbound"]["author"] == {"handle": "alice", "id": "user-a"}
    assert packet["relationship_context"] == relationship
    assert packet["draft"]["text"] == "I would start by making the failure mode observable."
    assert packet["evaluator"]["quality_score"] == 4.5
    assert packet["evaluator"]["sycophancy_flags"] == ["sycophantic"]
    assert packet["evaluator"]["generic_flags"] == ["generic"]
    assert packet["dedup"] == {"status": "passed", "lookback_hours": 72}
    assert packet["recommended_action"]["action"] == "revise"


def test_missing_optional_relationship_context_does_not_prevent_packet(db):
    _insert_reply(db, relationship_context=None, quality_score=None, quality_flags=None)

    packet = build_reply_review_packet(db.get_pending_replies()[0])

    assert packet["relationship_context"] is None
    assert packet["evaluator"]["quality_flags"] == []
    assert packet["dedup"]["status"] == "passed"
    assert packet["recommended_action"]["action"] == "review"


def test_single_draft_can_be_exported_by_id(db):
    first_id = _insert_reply(db, "tw-1", platform="x")
    second_id = _insert_reply(db, "tw-2", platform="bluesky")

    rows = list_pending_reply_drafts(db, draft_id=second_id)

    assert [row["id"] for row in rows] == [second_id]
    assert first_id != second_id


def test_output_dir_writes_deterministic_platform_and_draft_filename(db, tmp_path):
    reply_id = _insert_reply(db, platform="bluesky")
    packet = build_reply_review_packet(db.get_pending_replies()[0])

    paths = write_reply_review_packets([packet], tmp_path)

    assert packet_filename(packet) == f"bluesky-draft-{reply_id}.json"
    assert paths == [tmp_path / f"bluesky-draft-{reply_id}.json"]
    assert json.loads(paths[0].read_text())["draft_id"] == reply_id


def test_cli_json_exports_single_packet(db, capsys):
    reply_id = _insert_reply(db, platform="x")

    with patch("export_reply_packets.script_context", _mock_script_context(db)):
        assert main(["--draft-id", str(reply_id), "--json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert len(payload) == 1
    assert payload[0]["draft_id"] == reply_id
