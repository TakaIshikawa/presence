"""Tests for proactive action outcome export."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from engagement.proactive_action_outcomes import (
    build_proactive_action_outcome_report,
    format_proactive_action_outcomes_json,
    format_proactive_action_outcomes_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "export_proactive_action_outcomes.py"
)
spec = importlib.util.spec_from_file_location("export_proactive_action_outcomes_script", SCRIPT_PATH)
export_proactive_action_outcomes_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_proactive_action_outcomes_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_action(db, **kwargs) -> int:
    defaults = dict(
        action_type="reply",
        target_tweet_id="target-1",
        target_tweet_text="Useful thought",
        target_author_handle="alice",
        target_author_id="author-a",
        discovery_source="search",
        relevance_score=0.8,
        draft_text="Good point.",
        platform_metadata=json.dumps(
            {"platform": "x", "target_url": "https://x.com/alice/status/target-1"},
            sort_keys=True,
        ),
    )
    defaults.update(kwargs)
    return db.insert_proactive_action(**defaults)


def _set_action_state(
    db,
    action_id: int,
    *,
    status: str,
    created_at: str,
    reviewed_at: str | None = None,
    posted_at: str | None = None,
    posted_tweet_id: str | None = None,
) -> None:
    db.conn.execute(
        """UPDATE proactive_actions
           SET status = ?, created_at = ?, reviewed_at = ?, posted_at = ?, posted_tweet_id = ?
           WHERE id = ?""",
        (status, created_at, reviewed_at, posted_at, posted_tweet_id, action_id),
    )
    db.conn.commit()


def _insert_linked_reply(db, inbound_id: str, *, our_tweet_id: str, status: str = "pending") -> int:
    reply_id = db.insert_reply_draft(
        inbound_tweet_id=inbound_id,
        inbound_author_handle="bob",
        inbound_author_id="author-b",
        inbound_text="Following up",
        our_tweet_id=our_tweet_id,
        our_content_id=123,
        our_post_text="Original proactive reply",
        draft_text="Thanks for following up.",
        status=status,
        platform="x",
        inbound_url=f"https://x.com/bob/status/{inbound_id}",
    )
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        ("2026-05-02T11:00:00+00:00", reply_id),
    )
    db.conn.commit()
    return reply_id


def test_exports_completed_pending_expired_and_unresolved_actions(db):
    completed = _insert_action(db, target_tweet_id="target-completed")
    _set_action_state(
        db,
        completed,
        status="posted",
        created_at="2026-05-01T10:00:00+00:00",
        reviewed_at="2026-05-01T11:00:00+00:00",
        posted_at="2026-05-01T11:05:00+00:00",
        posted_tweet_id="posted-1",
    )
    reply_id = _insert_linked_reply(db, "reply-1", our_tweet_id="posted-1", status="posted")

    pending = _insert_action(db, target_tweet_id="target-pending", action_type="like")
    _set_action_state(db, pending, status="pending", created_at="2026-05-02T08:00:00+00:00")

    expired = _insert_action(db, target_tweet_id="target-expired", action_type="quote_tweet")
    _set_action_state(
        db,
        expired,
        status="dismissed",
        created_at="2026-04-30T08:00:00+00:00",
        reviewed_at="2026-05-01T08:00:00+00:00",
    )

    unresolved = _insert_action(db, target_tweet_id="target-unresolved")
    _set_action_state(
        db,
        unresolved,
        status="approved",
        created_at="2026-05-02T09:00:00+00:00",
        reviewed_at="2026-05-02T10:00:00+00:00",
    )

    report = build_proactive_action_outcome_report(db, now=NOW, days=7, limit=10)
    payload = json.loads(format_proactive_action_outcomes_json(report))
    by_id = {item["id"]: item for item in payload["actions"]}

    assert by_id[completed]["status"] == "completed"
    assert by_id[completed]["publication_resulted"] is True
    assert by_id[completed]["reply_draft_resulted"] is True
    assert by_id[completed]["linked_reply_counts"] == {"posted": 1, "total": 1}
    assert by_id[completed]["linked_replies"][0]["id"] == reply_id
    assert by_id[pending]["status"] == "pending"
    assert by_id[expired]["status"] == "expired"
    assert by_id[unresolved]["status"] == "unresolved"
    assert payload["summary"]["by_action_type_status"] == [
        {"action_type": "like", "count": 1, "status": "pending"},
        {"action_type": "quote_tweet", "count": 1, "status": "expired"},
        {"action_type": "reply", "count": 1, "status": "completed"},
        {"action_type": "reply", "count": 1, "status": "unresolved"},
    ]
    assert "recommended_next_step" in by_id[unresolved]


def test_limit_and_status_filtering_are_deterministic(db):
    old = _insert_action(db, target_tweet_id="old")
    _set_action_state(db, old, status="posted", created_at="2026-04-01T00:00:00+00:00")
    first = _insert_action(db, target_tweet_id="first")
    _set_action_state(db, first, status="posted", created_at="2026-05-01T00:00:00+00:00")
    second = _insert_action(db, target_tweet_id="second")
    _set_action_state(db, second, status="pending", created_at="2026-05-02T00:00:00+00:00")

    report = build_proactive_action_outcome_report(
        db,
        now=NOW,
        days=7,
        statuses=("completed",),
        limit=2,
    )
    payload = json.loads(format_proactive_action_outcomes_json(report))

    assert [item["id"] for item in payload["actions"]] == [first]
    assert payload["filters"] == {"days": 7, "limit": 2, "status": ["completed"]}
    assert payload["summary"]["by_status"] == {"completed": 1}
    assert old not in [item["id"] for item in payload["actions"]]


def test_text_formatter_and_missing_schema_reports_are_stable(db):
    action_id = _insert_action(db, target_tweet_id="text")
    _set_action_state(db, action_id, status="approved", created_at="2026-05-02T09:00:00+00:00")

    report = build_proactive_action_outcome_report(db, now=NOW)
    text = format_proactive_action_outcomes_text(report)

    assert "Proactive Action Outcomes" in text
    assert f"#{action_id} reply @alice" in text
    assert "status=unresolved" in text

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing = build_proactive_action_outcome_report(conn, now=NOW)
    assert missing.total_actions == 0
    assert missing.missing_tables == ("proactive_actions",)


def test_cli_supports_text_json_status_limit_and_validation(db, monkeypatch, capsys):
    action_id = _insert_action(db, target_tweet_id="cli")
    _set_action_state(
        db,
        action_id,
        status="posted",
        created_at="2026-05-02T09:00:00+00:00",
        posted_at="2026-05-02T10:00:00+00:00",
        posted_tweet_id="posted-cli",
    )
    monkeypatch.setattr(
        export_proactive_action_outcomes_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = export_proactive_action_outcomes_script.main(
        ["--format", "json", "--status", "completed", "--days", "3", "--limit", "1"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"] == {"days": 3, "limit": 1, "status": ["completed"]}
    assert payload["actions"][0]["id"] == action_id

    text_exit = export_proactive_action_outcomes_script.main(["--format", "text"])
    text = capsys.readouterr().out
    assert text_exit == 0
    assert "Proactive Action Outcomes" in text

    invalid = export_proactive_action_outcomes_script.main(["--days", "0"])
    captured = capsys.readouterr()
    assert invalid == 2
    assert "value must be positive" in captured.err
