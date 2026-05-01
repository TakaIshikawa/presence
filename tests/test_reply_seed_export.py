"""Tests for exporting reply knowledge seed candidates."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from knowledge.reply_seed_export import (
    build_reply_knowledge_seed_export,
    format_reply_knowledge_seed_export_json,
    format_reply_knowledge_seed_export_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "export_reply_knowledge_seeds.py"
)
spec = importlib.util.spec_from_file_location("export_reply_knowledge_seeds", SCRIPT_PATH)
export_reply_knowledge_seeds = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_reply_knowledge_seeds)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _reply(
    db,
    *,
    inbound_id: str,
    handle: str = "alice",
    inbound_text: str = "How do you keep quality review lightweight?",
    draft_text: str = "Use a small checklist that is visible at the point of decision.",
    quality_score: float = 8.0,
    quality_flags: list[str] | None = None,
    status: str = "posted",
    detected_at: str = "2026-04-25T10:00:00+00:00",
    reviewed_at: str | None = "2026-04-25T11:00:00+00:00",
    posted_at: str | None = "2026-04-25T12:00:00+00:00",
) -> int:
    reply_id = db.insert_reply_draft(
        inbound_tweet_id=inbound_id,
        inbound_author_handle=handle,
        inbound_author_id=f"{handle}-id",
        inbound_text=inbound_text,
        our_tweet_id=f"our-{inbound_id}",
        our_content_id=None,
        our_post_text="Reviewable systems are easier to improve.",
        draft_text=draft_text,
        relationship_context=json.dumps({"stage": "known"}),
        quality_score=quality_score,
        quality_flags=json.dumps(quality_flags or []),
        platform="bluesky",
        inbound_url=f"https://bsky.app/profile/{handle}/post/{inbound_id}",
        inbound_cid=f"cid-{inbound_id}",
        our_platform_id=f"at://did:example/app.bsky.feed.post/{inbound_id}",
        platform_metadata=json.dumps({"thread_root": "root"}),
        intent="question",
        priority="high",
        status=status,
    )
    db.conn.execute(
        """UPDATE reply_queue
           SET detected_at = ?, reviewed_at = ?, posted_at = ?,
               posted_platform_id = ?, posted_tweet_id = ?
           WHERE id = ?""",
        (
            detected_at,
            reviewed_at,
            posted_at,
            f"posted-platform-{inbound_id}" if posted_at else None,
            f"posted-{inbound_id}" if posted_at else None,
            reply_id,
        ),
    )
    db.conn.commit()
    return reply_id


def _knowledge(db) -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge (source_type, source_id, author, content, approved)
           VALUES ('curated_article', 'quality-review', 'Ada', 'Keep review visible.', 1)"""
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_export_selects_posted_or_approved_replies_above_threshold(db):
    posted_id = _reply(db, inbound_id="posted", status="posted", quality_score=8.5)
    approved_id = _reply(
        db,
        inbound_id="approved",
        status="approved",
        quality_score=7.5,
        posted_at=None,
    )
    _reply(db, inbound_id="pending", status="pending", quality_score=9.5)
    _reply(db, inbound_id="low", status="posted", quality_score=6.9)
    _reply(
        db,
        inbound_id="old",
        status="posted",
        quality_score=9.0,
        detected_at="2026-01-01T10:00:00+00:00",
        reviewed_at="2026-01-01T11:00:00+00:00",
        posted_at="2026-01-01T12:00:00+00:00",
    )

    export = build_reply_knowledge_seed_export(db, days=30, min_quality=7.0, now=NOW)

    assert [seed.source_reply_id for seed in export.seeds] == [posted_id, approved_id]
    assert export.summary["seed_count"] == 2
    assert export.summary["excluded_by_outcome"] == 1
    seed = export.seeds[0]
    assert seed.source_type == "reply_queue"
    assert seed.source_id == f"reply_queue:{posted_id}"
    assert seed.author_handle == "alice"
    assert seed.inbound_context["text"] == "How do you keep quality review lightweight?"
    assert seed.inbound_context["platform"] == "bluesky"
    assert seed.draft_text.startswith("Use a small checklist")
    assert seed.metadata["relationship_context"] == {"stage": "known"}
    assert seed.metadata["platform_metadata"] == {"thread_root": "root"}
    assert seed.metadata["posted_platform_id"] == "posted-platform-posted"


def test_export_uses_review_events_to_qualify_rows_and_includes_link_metadata(db):
    reply_id = _reply(
        db,
        inbound_id="event-approved",
        status="pending",
        quality_score=8.2,
        reviewed_at="2026-04-26T10:00:00+00:00",
        posted_at=None,
    )
    db.conn.execute(
        """INSERT INTO reply_review_events
           (reply_queue_id, event_type, actor, old_status, new_status, notes, created_at)
           VALUES (?, 'approved', 'operator', 'pending', 'approved', 'useful explanation', ?)""",
        (reply_id, "2026-04-26T10:00:00+00:00"),
    )
    knowledge_id = _knowledge(db)
    db.insert_reply_knowledge_links(reply_id, [(knowledge_id, 0.91)])

    export = build_reply_knowledge_seed_export(db, days=30, min_quality=7.0, now=NOW)

    assert [seed.source_reply_id for seed in export.seeds] == [reply_id]
    seed = export.seeds[0]
    assert seed.metadata["linked_knowledge_ids"] == [knowledge_id]
    assert seed.metadata["reply_knowledge_links"][0]["relevance_score"] == 0.91
    assert seed.metadata["review_events"][0]["event_type"] == "approved"
    assert seed.metadata["review_events"][0]["notes"] == "useful explanation"


def test_export_excludes_low_quality_flags(db):
    _reply(db, inbound_id="clean", status="posted", quality_score=8.0)
    _reply(db, inbound_id="generic", status="posted", quality_score=9.0, quality_flags=["generic"])
    _reply(db, inbound_id="spam", status="approved", quality_score=9.0, quality_flags=["spam"])
    _reply(
        db,
        inbound_id="sycophantic",
        status="posted",
        quality_score=9.0,
        quality_flags=["substantive", "sycophantic"],
    )

    export = build_reply_knowledge_seed_export(db, days=30, min_quality=7.0, now=NOW)

    assert [seed.inbound_context["inbound_tweet_id"] for seed in export.seeds] == ["clean"]
    assert export.summary["excluded_by_quality_flag"] == 3


def test_json_text_and_cli_outputs_are_deterministic(db, capsys):
    reply_id = _reply(db, inbound_id="cli", status="posted", quality_score=8.0)

    export = build_reply_knowledge_seed_export(db, days=30, min_quality=7.5, now=NOW)
    assert format_reply_knowledge_seed_export_json(export) == (
        format_reply_knowledge_seed_export_json(export)
    )
    payload = json.loads(format_reply_knowledge_seed_export_json(export))
    assert payload["filters"]["min_quality"] == 7.5
    assert payload["seeds"][0]["source_reply_id"] == reply_id
    text = format_reply_knowledge_seed_export_text(export)
    assert "Reply Knowledge Seeds" in text
    assert f"reply_queue:{reply_id}" in text

    with patch.object(
        export_reply_knowledge_seeds,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        export_reply_knowledge_seeds,
        "build_reply_knowledge_seed_export",
        wraps=lambda db, **kwargs: build_reply_knowledge_seed_export(db, now=NOW, **kwargs),
    ):
        assert export_reply_knowledge_seeds.main(["--days", "30", "--min-quality", "7.5", "--json"]) == 0
        cli_payload = json.loads(capsys.readouterr().out)
        assert export_reply_knowledge_seeds.main(["--days", "30", "--min-quality", "7.5"]) == 0

    assert cli_payload["seeds"][0]["source_reply_id"] == reply_id
    assert "Reply Knowledge Seeds" in capsys.readouterr().out


def test_invalid_filters_return_cli_errors(capsys):
    assert export_reply_knowledge_seeds.main(["--days", "0"]) == 1
    assert "days must be positive" in capsys.readouterr().err

    assert export_reply_knowledge_seeds.main(["--min-quality", "11"]) == 1
    assert "min-quality must be between 0 and 10" in capsys.readouterr().err


def test_missing_reply_queue_table_returns_empty_export():
    conn = sqlite3.connect(":memory:")
    try:
        export = build_reply_knowledge_seed_export(conn, now=NOW)
    finally:
        conn.close()

    assert export.seeds == ()
    assert "reply_queue" in export.missing_tables
    assert export.summary["seed_count"] == 0
