from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_review_priority import (
    build_reply_review_priority_report,
    format_reply_review_priority_json,
    format_reply_review_priority_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_review_priority.py"
spec = importlib.util.spec_from_file_location("reply_review_priority_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _reply(db, *, text: str, hours_ago: int, quality_score: float = 8, flags=None, relationship=None, status="pending") -> int:
    cursor = db.conn.execute(
        """INSERT INTO reply_queue
           (inbound_tweet_id, inbound_text, our_tweet_id, draft_text, intent,
            relationship_context, quality_score, quality_flags, status, detected_at)
           VALUES (?, ?, 'ours', 'draft reply', ?, ?, ?, ?, ?, ?)""",
        (
            f"in-{text}-{hours_ago}-{quality_score}",
            text,
            "question" if "?" in text else "other",
            json.dumps(relationship) if relationship is not None else None,
            quality_score,
            json.dumps(flags or []),
            status,
            (NOW - timedelta(hours=hours_ago)).isoformat(),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_ranks_direct_questions_and_older_mentions_higher(db):
    old_question = _reply(db, text="Can you explain this?", hours_ago=30, relationship={"is_known": True})
    new_statement = _reply(db, text="Nice post", hours_ago=1)

    report = build_reply_review_priority_report(db, now=NOW)

    assert [item["reply_queue_id"] for item in report["items"]] == [old_question, new_statement]
    assert report["items"][0]["direct_question"] is True
    assert any(signal.startswith("direct_question") for signal in report["items"][0]["signals"])


def test_low_quality_filtered_by_default_and_included_on_flag(db):
    good = _reply(db, text="What changed?", hours_ago=4, quality_score=8)
    bad = _reply(db, text="What changed exactly?", hours_ago=50, quality_score=3, flags=["generic", "sycophantic"])

    default = build_reply_review_priority_report(db, now=NOW)
    included = build_reply_review_priority_report(db, include_low_quality=True, now=NOW)

    assert [item["reply_queue_id"] for item in default["items"]] == [good]
    assert default["totals"]["low_quality_excluded"] == 1
    assert bad in [item["reply_queue_id"] for item in included["items"]]
    bad_item = next(item for item in included["items"] if item["reply_queue_id"] == bad)
    assert bad_item["low_quality"] is True
    assert any(signal.startswith("quality_flags") for signal in bad_item["signals"])


def test_duplicate_intent_signal_and_formatters_cli(db, monkeypatch, capsys):
    first = _reply(db, text="Can you explain pricing?", hours_ago=6)
    _reply(db, text="Can you explain pricing?", hours_ago=5)
    report = build_reply_review_priority_report(db, now=NOW)
    payload = json.loads(format_reply_review_priority_json(report))
    text = format_reply_review_priority_text(report)

    assert list(payload) == sorted(payload)
    assert any("duplicate_intent" in signal for signal in report["items"][0]["signals"])
    assert f"reply_id={first}" in text

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_reply_review_priority_report",
        lambda db, **kwargs: build_reply_review_priority_report(db, now=NOW, **kwargs),
    )
    assert script.main(["--days", "14", "--limit", "1", "--include-low-quality", "--format", "json"]) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["include_low_quality"] is True
    assert cli_payload["filters"]["limit"] == 1
