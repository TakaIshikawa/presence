"""Tests for reply draft unsupported factual-claim auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_unsupported_claims import (
    build_reply_unsupported_claims_report,
    format_reply_unsupported_claims_json,
    format_reply_unsupported_claims_text,
    inspect_reply_unsupported_claims,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_unsupported_claims.py"
spec = importlib.util.spec_from_file_location("reply_unsupported_claims_script", SCRIPT_PATH)
reply_unsupported_claims_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_unsupported_claims_script)

NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_reply(db, tweet_id: str, draft_text: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="How should we think about release reliability?",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Reliability work needs evidence.",
        draft_text=draft_text,
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


def _link_knowledge(db, reply_id: int) -> None:
    db.conn.execute(
        """INSERT INTO knowledge (id, source_type, source_id, content)
           VALUES (1, 'own_post', 'source-1', 'Reliability source')""",
    )
    db.conn.execute(
        """INSERT INTO reply_knowledge_links
           (reply_queue_id, knowledge_id, relevance_score)
           VALUES (?, 1, 0.9)""",
        (reply_id,),
    )
    db.conn.commit()


def test_unsupported_factual_claims_are_flagged_with_snippets_and_severity(db):
    reply_id = _insert_reply(
        db,
        "unsupported",
        (
            "The only reliable release workflow is a daily incident metric. "
            "It reduces regressions by 30% in production."
        ),
        inbound_author_handle="zoe",
    )
    _set_detected_at(db, reply_id, "2026-05-03 10:00:00")

    report = build_reply_unsupported_claims_report(db, now=NOW)
    payload = json.loads(format_reply_unsupported_claims_json(report))

    assert report.ok is False
    assert report.audited_count == 1
    assert report.finding_count == 2
    assert report.by_severity == {"high": 2}
    assert payload["artifact_type"] == "reply_unsupported_claims"
    assert list(payload) == sorted(payload)

    item = report.items[0]
    assert item.id == reply_id
    assert item.reply_id == "unsupported"
    assert item.author == "zoe"
    assert item.severity == "high"
    assert item.evidence_status == "none"
    assert item.reason == "unsupported_numeric_claim"
    assert item.claim_snippets == (
        "The only reliable release workflow is a daily incident metric.",
        "It reduces regressions by 30% in production.",
    )
    assert item.findings[0].to_dict() == {
        "reply_id": reply_id,
        "severity": "high",
        "claim_snippet": "The only reliable release workflow is a daily incident metric.",
        "reason": "unsupported_numeric_claim",
        "evidence_status": "none",
    }


def test_supported_claims_with_source_quoted_relationship_or_knowledge_evidence_are_not_flagged(db):
    linked = _insert_reply(
        db,
        "linked",
        "The benchmark is 30% faster in production: https://example.com/benchmarks.",
    )
    quoted = _insert_reply(
        db,
        "quoted",
        "That deployment path is slower because the queue blocks on release tests.",
        platform_metadata=json.dumps({"quoted_text": "The queue blocks on release tests."}),
    )
    relationship = _insert_reply(
        db,
        "relationship",
        "Your team always pins rollout decisions to customer incidents.",
        relationship_context=json.dumps({"relationship_notes": "They shared this rollout habit last week."}),
    )
    knowledge = _insert_reply(
        db,
        "knowledge",
        "Production latency depends on the cache architecture.",
    )
    for reply_id in (linked, quoted, relationship, knowledge):
        _set_detected_at(db, reply_id, "2026-05-03 09:00:00")
    _link_knowledge(db, knowledge)

    report = build_reply_unsupported_claims_report(db, now=NOW)

    assert report.ok is True
    assert report.finding_count == 0
    assert report.items == ()


def test_harmless_opinion_only_replies_are_not_flagged(db):
    _insert_reply(
        db,
        "opinion",
        "I think a smaller rollout might be a good first step. Thanks for the thoughtful thread.",
    )

    report = build_reply_unsupported_claims_report(db, now=NOW)

    assert report.ok is True
    assert report.finding_count == 0


def test_inspector_suppresses_nearby_evidence_markers():
    item = inspect_reply_unsupported_claims(
        {
            "id": 7,
            "inbound_tweet_id": "nearby",
            "draft_text": "According to the docs, this release requires the new cache workflow.",
        }
    )

    assert item is None


def test_status_days_limit_text_and_json_are_deterministic(db):
    old = _insert_reply(db, "old", "The release is 2x faster in production.", status="pending")
    approved = _insert_reply(db, "approved", "The release is 3x faster in production.", status="approved")
    pending = _insert_reply(db, "pending", "The release is 4x faster in production.", status="pending")
    _set_detected_at(db, old, "2026-04-01 10:00:00")
    _set_detected_at(db, approved, "2026-05-03 11:00:00")
    _set_detected_at(db, pending, "2026-05-03 10:00:00")

    pending_report = build_reply_unsupported_claims_report(db, days=7, status="pending", now=NOW)
    approved_report = build_reply_unsupported_claims_report(db, days=7, status="approved", now=NOW)
    all_report = build_reply_unsupported_claims_report(db, days=40, status="all", limit=2, now=NOW)
    text = format_reply_unsupported_claims_text(pending_report)

    assert [item.reply_id for item in pending_report.items] == ["pending"]
    assert [item.reply_id for item in approved_report.items] == ["approved"]
    assert [item.reply_id for item in all_report.items] == ["approved", "pending"]
    assert "Reply Unsupported Claims Audit" in text
    assert "claim: The release is 4x faster in production." in text


def test_cli_emits_json_and_exits_one_when_findings_exist(db, monkeypatch, capsys):
    _insert_reply(db, "cli", "The workflow always reduces release incidents by 20%.")
    monkeypatch.setattr(
        reply_unsupported_claims_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = reply_unsupported_claims_script.main(
        ["--format", "json", "--status", "pending", "--days", "7", "--limit", "5"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["filters"] == {"days": 7, "limit": 5, "status": "pending"}
    assert payload["finding_count"] == 1
    assert payload["items"][0]["claim_snippets"] == [
        "The workflow always reduces release incidents by 20%."
    ]


def test_cli_exits_zero_when_no_findings(db, monkeypatch, capsys):
    _insert_reply(db, "safe", "I think that approach could work well for this thread.")
    monkeypatch.setattr(
        reply_unsupported_claims_script,
        "script_context",
        lambda: _script_context(db),
    )

    assert reply_unsupported_claims_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["finding_count"] == 0
