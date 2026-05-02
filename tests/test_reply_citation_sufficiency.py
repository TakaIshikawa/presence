"""Tests for reply citation sufficiency reporting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from engagement.reply_citation_sufficiency import (  # noqa: E402
    build_reply_citation_sufficiency_report,
    format_reply_citation_sufficiency_json,
    format_reply_citation_sufficiency_text,
)
from reply_citation_sufficiency import main  # noqa: E402


def _mock_script_context(db):
    @contextmanager
    def _ctx():
        yield (SimpleNamespace(), db)

    return _ctx


def _insert_reply(db, tweet_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="How do you think about release reliability?",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Reliability work needs evidence.",
        draft_text="Reliability improves when teams always pin incidents to a clear metric.",
        intent="question",
        status="pending",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (detected_at, reply_id),
    )
    db.conn.commit()


def _link(db, reply_id: int, knowledge_id: int, relevance_score: float = 0.9) -> None:
    db.conn.execute(
        """INSERT INTO knowledge (id, source_type, source_id, content)
           VALUES (?, 'own_post', ?, 'Prior knowledge')
           ON CONFLICT(id) DO NOTHING""",
        (knowledge_id, f"k-{knowledge_id}"),
    )
    db.conn.execute(
        """INSERT INTO reply_knowledge_links
           (reply_queue_id, knowledge_id, relevance_score)
           VALUES (?, ?, ?)""",
        (reply_id, knowledge_id, relevance_score),
    )
    db.conn.commit()


def test_assertive_queued_reply_without_links_is_missing_evidence(db):
    reply_id = _insert_reply(
        db,
        "missing",
        draft_text=(
            "The best release workflow always uses incident metrics. "
            "That proves which reliability fix should ship next."
        ),
    )
    _set_detected_at(db, reply_id, "2026-04-23 01:00:00")

    report = build_reply_citation_sufficiency_report(db, status="pending")

    assert report["totals"]["missing_evidence"] == 1
    item = report["findings"][0]
    assert item["id"] == reply_id
    assert item["citation_status"] == "missing_evidence"
    assert item["knowledge_link_count"] == 0
    assert item["suggested_actions"] == [
        "add_knowledge_link",
        "route_for_manual_review",
        "soften_claim",
    ]


def test_reply_with_adequate_linked_knowledge_is_sufficient(db):
    reply_id = _insert_reply(
        db,
        "sufficient",
        draft_text="Reliability reviews should connect production incidents to tests.",
    )
    _set_detected_at(db, reply_id, "2026-04-23 01:00:00")
    _link(db, reply_id, 1, 0.91)
    _link(db, reply_id, 2, 0.82)

    report = build_reply_citation_sufficiency_report(db, status="pending")

    assert report["totals"]["sufficient"] == 1
    item = report["findings"][0]
    assert item["citation_status"] == "sufficient"
    assert item["strong_knowledge_link_count"] == 2
    assert item["average_relevance_score"] == 0.865
    assert item["suggested_actions"] == ["ready_for_review"]


def test_high_claim_reply_with_one_link_is_thin_evidence(db):
    reply_id = _insert_reply(
        db,
        "thin",
        draft_text=(
            "The only reliable SDK workflow is to cache every build metric. "
            "It always shows which production release will regress."
        ),
    )
    _set_detected_at(db, reply_id, "2026-04-23 01:00:00")
    _link(db, reply_id, 1, 0.93)

    report = build_reply_citation_sufficiency_report(db, status="pending")

    assert report["totals"]["thin_evidence"] == 1
    item = report["findings"][0]
    assert item["citation_status"] == "thin_evidence"
    assert item["knowledge_link_count"] == 1
    assert item["suggested_actions"] == [
        "add_knowledge_link",
        "route_for_manual_review",
        "soften_claim",
    ]


def test_status_filter_limit_and_output_are_deterministic(db):
    first = _insert_reply(db, "first", inbound_author_handle="bob")
    second = _insert_reply(db, "second", status="approved")
    _set_detected_at(db, first, "2026-04-23 01:00:00")
    _set_detected_at(db, second, "2026-04-23 00:00:00")

    report = build_reply_citation_sufficiency_report(db, status="pending", limit=1)

    assert [item["id"] for item in report["findings"]] == [first]
    payload = json.loads(format_reply_citation_sufficiency_json(report))
    assert payload["filters"] == {"limit": 1, "status": ["pending"]}
    assert format_reply_citation_sufficiency_text(report) == "\n".join(
        [
            "Reply Citation Sufficiency Report",
            "Status: pending",
            "Limit: 1",
            "Rows: scanned=1 sufficient=0 thin_evidence=0 missing_evidence=1",
            "",
            "Findings:",
            "#1 missing_evidence links=0 strong=0 score=2 @bob",
            "  reasons: assertive_language, knowledge_backed_topic",
            "  actions: add_knowledge_link, route_for_manual_review, soften_claim",
            "  draft: Reliability improves when teams always pin incidents to a clear metric.",
        ]
    )


def test_cli_can_emit_text_and_json_reports(db, capsys):
    reply_id = _insert_reply(db, "cli")
    _set_detected_at(db, reply_id, "2026-04-23 01:00:00")

    with patch("reply_citation_sufficiency.script_context", _mock_script_context(db)):
        text_result = main(["--status", "pending", "--limit", "5", "--format", "text"])
    assert text_result == 0
    assert "Reply Citation Sufficiency Report" in capsys.readouterr().out

    with patch("reply_citation_sufficiency.script_context", _mock_script_context(db)):
        json_result = main(["--status", "pending", "--limit", "5", "--format", "json"])
    assert json_result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["findings"][0]["id"] == reply_id
