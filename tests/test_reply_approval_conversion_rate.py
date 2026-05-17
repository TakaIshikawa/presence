"""Tests for reply approval conversion rate."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_approval_conversion_rate import build_reply_approval_conversion_rate_report_from_db


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_approval_conversion_rate.py"
spec = importlib.util.spec_from_file_location("reply_approval_conversion_rate_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_computes_grouped_conversion_rates(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE reply_drafts (id INTEGER, platform TEXT, author_id TEXT, draft_reason TEXT, review_status TEXT, published INTEGER, published_at TEXT, metadata TEXT)"
    )
    conn.executemany(
        "INSERT INTO reply_drafts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, "bluesky", "a1", "question", "approved", 1, NOW.isoformat(), "{}"),
            (2, "bluesky", "a1", "question", "rejected", 0, None, "{}"),
            (3, "mastodon", "a2", "followup", "revised", 0, None, "{}"),
        ],
    )
    db = SimpleNamespace(conn=conn)

    report = build_reply_approval_conversion_rate_report_from_db(db, now=NOW)

    assert report["summary"]["drafted"] == 3
    assert report["summary"]["approved"] == 1
    assert report["summary"]["rejected"] == 1
    assert report["summary"]["revised"] == 1
    assert report["summary"]["published"] == 1
    assert report["findings"][0]["approval_rate"] == 0.5

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_reply_approval_conversion_rate_report_from_db",
        lambda db, **kwargs: build_reply_approval_conversion_rate_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "reply_approval_conversion_rate"
    assert script.main(["--table"]) == 0
    assert "platform=bluesky" in capsys.readouterr().out
