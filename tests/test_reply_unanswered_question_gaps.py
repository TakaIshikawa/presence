from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.reply_unanswered_question_gaps import build_reply_unanswered_question_gaps_report_from_db, format_reply_unanswered_question_gaps_json, format_reply_unanswered_question_gaps_text

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "reply_unanswered_question_gaps.py"
spec = importlib.util.spec_from_file_location("reply_unanswered_question_gaps_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)

NOW = datetime(2026, 5, 25, tzinfo=timezone.utc)


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE inbound_mentions (id TEXT, author_handle TEXT, text TEXT, received_at TEXT, platform TEXT);
           CREATE TABLE reply_queue (id INTEGER, inbound_tweet_id TEXT, status TEXT, queued_at TEXT);"""
    )
    conn.executemany(
        "INSERT INTO inbound_mentions VALUES (?,?,?,?,?)",
        [
            ("m1", "alice", "Can you explain the pricing", "2026-05-23T00:00:00+00:00", "bluesky"),
            ("m2", "bob", "What happened here?", "2026-05-23T00:00:00+00:00", "mastodon"),
            ("m3", "cara", "nice post", "2026-05-22T00:00:00+00:00", "bluesky"),
            ("m4", "dee", "why now", "2026-05-24T23:00:00+00:00", "bluesky"),
        ],
    )
    conn.execute("INSERT INTO reply_queue VALUES (1, 'm2', 'approved', '2026-05-24T00:00:00+00:00')")
    conn.commit()
    return conn


def test_report_detects_unanswered_question_and_excludes_answered():
    report = build_reply_unanswered_question_gaps_report_from_db(_db(), min_age_hours=24, now=NOW)
    assert [item["mention_id"] for item in report["findings"]] == ["m1"]
    assert report["findings"][0]["author"] == "alice"
    assert report["findings"][0]["question_signal"] == "interrogative:can"
    assert "m2" not in {item["mention_id"] for item in report["findings"]}


def test_platform_filter_formatters_and_cli(tmp_path, capsys):
    report = build_reply_unanswered_question_gaps_report_from_db(_db(), min_age_hours=1, platform="mastodon", now=NOW)
    assert not report["findings"]
    assert json.loads(format_reply_unanswered_question_gaps_json(report))["artifact_type"] == "reply_unanswered_question_gaps"
    assert "Reply Unanswered" in format_reply_unanswered_question_gaps_text(report)
    db_path = tmp_path / "db.sqlite"
    out = sqlite3.connect(db_path)
    _db().backup(out)
    out.close()
    assert script.main(["--db", str(db_path), "--format", "text", "--min-age-hours", "24"]) == 0
    assert "m1" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--min-age-hours", "0"]) == 2
