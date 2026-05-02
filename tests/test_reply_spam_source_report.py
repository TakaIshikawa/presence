"""Tests for repeated reply spam source reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.reply_spam_source_report import (
    build_reply_spam_source_report,
    fingerprint_inbound_text,
    format_reply_spam_source_report_json,
    format_reply_spam_source_report_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_spam_source_report.py"
spec = importlib.util.spec_from_file_location("reply_spam_source_report_script", SCRIPT_PATH)
reply_spam_source_report_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_spam_source_report_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _reply_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            inbound_author_handle TEXT,
            inbound_text TEXT,
            intent TEXT,
            quality_flags TEXT,
            quality_score REAL,
            status TEXT,
            detected_at TEXT
        )"""
    )
    return conn


def _insert_reply(
    conn: sqlite3.Connection,
    *,
    reply_id: int,
    platform: str,
    handle: str,
    text: str,
    intent: str = "other",
    flags: list[str] | None = None,
    score: float | None = None,
    detected_at: str = "2026-05-01T10:00:00+00:00",
    status: str = "pending",
) -> None:
    conn.execute(
        """INSERT INTO reply_queue
           (id, platform, inbound_author_handle, inbound_text, intent,
            quality_flags, quality_score, status, detected_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            reply_id,
            platform,
            handle,
            text,
            intent,
            json.dumps(flags) if flags is not None else None,
            score,
            status,
            detected_at,
        ),
    )
    conn.commit()


def test_groups_sources_scores_and_sorts_by_spam_load():
    conn = _reply_db()
    _insert_reply(
        conn,
        reply_id=1,
        platform="x",
        handle="@SpamBot",
        text="DM me for a crypto giveaway https://spam.example/a",
        intent="spam",
        flags=["no_response", "low_value"],
        score=2.0,
        detected_at="2026-05-01T09:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=2,
        platform="x",
        handle="spambot",
        text="DM me for a crypto giveaway https://spam.example/b",
        intent="spam",
        flags=["generic"],
        score=3.0,
        detected_at="2026-05-01T09:05:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=3,
        platform="bluesky",
        handle="noisy",
        text="Check this out https://one.example https://two.example",
        flags=["sycophantic"],
        score=4.0,
        detected_at="2026-05-01T10:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=4,
        platform="bluesky",
        handle="noisy",
        text="Interesting post",
        flags=[],
        score=8.0,
        detected_at="2026-05-01T10:05:00+00:00",
    )

    report = build_reply_spam_source_report(conn, days=7, min_count=2, now=NOW)
    payload = json.loads(format_reply_spam_source_report_json(report))
    text = format_reply_spam_source_report_text(report)

    assert payload["artifact_type"] == "reply_spam_source_report"
    assert payload["totals"]["rows_scanned"] == 4
    assert [finding["inbound_author_handle"] for finding in payload["findings"]] == [
        "spambot",
        "noisy",
    ]
    first = payload["findings"][0]
    assert first["platform"] == "x"
    assert first["counts"]["total"] == 2
    assert first["counts"]["spam_intent_count"] == 2
    assert first["counts"]["suspicious_phrase_hit_count"] == 6
    assert first["counts"]["url_heavy_mention_count"] == 2
    assert first["counts"]["duplicate_fingerprint_count"] == 1
    assert first["counts"]["duplicate_mention_count"] == 2
    assert first["counts"]["low_quality_draft_flag_count"] == 3
    assert first["counts"]["no_response_quality_flag_count"] == 2
    assert first["recommended_action"] == "consider_source_mute_or_filter"
    assert "x/@spambot" in text


def test_min_count_and_window_filtering_are_applied():
    rows = [
        {
            "id": 1,
            "platform": "x",
            "inbound_author_handle": "solo",
            "inbound_text": "crypto giveaway",
            "intent": "spam",
            "detected_at": "2026-05-01T09:00:00+00:00",
        },
        {
            "id": 2,
            "platform": "x",
            "inbound_author_handle": "old",
            "inbound_text": "crypto giveaway",
            "intent": "spam",
            "detected_at": "2026-03-01T09:00:00+00:00",
        },
        {
            "id": 3,
            "platform": "x",
            "inbound_author_handle": "old",
            "inbound_text": "crypto giveaway",
            "intent": "spam",
            "detected_at": "2026-03-01T10:00:00+00:00",
        },
        {
            "id": 4,
            "platform": "x",
            "inbound_author_handle": "repeat",
            "inbound_text": "follow back",
            "intent": "spam",
            "detected_at": "2026-05-01T09:00:00+00:00",
        },
        {
            "id": 5,
            "platform": "x",
            "inbound_author_handle": "repeat",
            "inbound_text": "follow back",
            "intent": "spam",
            "detected_at": "2026-05-01T10:00:00+00:00",
        },
    ]

    report = build_reply_spam_source_report(rows, days=7, min_count=2, now=NOW)

    assert report.totals["rows_scanned"] == 3
    assert [finding.inbound_author_handle for finding in report.findings] == ["repeat"]
    assert report.findings[0].counts["duplicate_mention_count"] == 2


def test_missing_table_returns_empty_schema_gap():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_reply_spam_source_report(conn, now=NOW)

    assert report.findings == ()
    assert report.missing_tables == ("reply_queue",)
    assert report.totals["rows_scanned"] == 0
    assert "Missing tables: reply_queue" in format_reply_spam_source_report_text(report)


def test_fingerprint_collapses_urls_for_duplicate_detection():
    assert fingerprint_inbound_text("Check this https://a.example") == fingerprint_inbound_text(
        "check this https://b.example"
    )


def test_cli_validates_args_and_emits_json(monkeypatch, capsys):
    db = _reply_db()
    _insert_reply(
        db,
        reply_id=1,
        platform="x",
        handle="cli",
        text="follow back",
        intent="spam",
    )
    _insert_reply(
        db,
        reply_id=2,
        platform="x",
        handle="cli",
        text="follow back",
        intent="spam",
    )
    monkeypatch.setattr(
        reply_spam_source_report_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        reply_spam_source_report_script,
        "build_reply_spam_source_report",
        lambda db, **kwargs: build_reply_spam_source_report(db, now=NOW, **kwargs),
    )

    assert reply_spam_source_report_script.main(["--min-count", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    assert reply_spam_source_report_script.main(["--days", "7", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert list(payload) == sorted(payload)
    assert payload["filters"]["days"] == 7
    assert payload["findings"][0]["inbound_author_handle"] == "cli"
