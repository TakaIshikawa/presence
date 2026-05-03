"""Tests for reply draft source freshness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_source_freshness import (
    WARNING_MALFORMED_CONTEXT,
    WARNING_MISSING_CONTEXT,
    WARNING_STALE_CONTEXT,
    build_reply_source_freshness_report,
    format_reply_source_freshness_json,
    format_reply_source_freshness_text,
)


NOW = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_source_freshness.py"
spec = importlib.util.spec_from_file_location("reply_source_freshness_script", SCRIPT_PATH)
reply_source_freshness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_source_freshness_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


@contextmanager
def _memory_db():
    """Create an in-memory SQLite database with required schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inbound_tweet_id TEXT,
            inbound_author_handle TEXT,
            inbound_author_id TEXT,
            inbound_text TEXT,
            our_tweet_id TEXT,
            our_post_text TEXT,
            draft_text TEXT,
            status TEXT DEFAULT 'pending',
            platform TEXT DEFAULT 'x',
            detected_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL,
            content TEXT NOT NULL,
            published_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE reply_knowledge_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reply_queue_id INTEGER REFERENCES reply_queue(id),
            knowledge_id INTEGER REFERENCES knowledge(id),
            relevance_score REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    yield conn
    conn.close()


def _insert_reply(
    conn: sqlite3.Connection,
    inbound_id: str,
    **kwargs,
) -> int:
    defaults = dict(
        inbound_tweet_id=inbound_id,
        inbound_author_handle="alice",
        inbound_text="Nice post",
        draft_text="Thanks",
        status="pending",
        platform="x",
    )
    defaults.update(kwargs)
    columns = ", ".join(defaults.keys())
    placeholders = ", ".join("?" for _ in defaults)
    cursor = conn.execute(
        f"INSERT INTO reply_queue ({columns}) VALUES ({placeholders})",
        tuple(defaults.values()),
    )
    conn.commit()
    return cursor.lastrowid


def _insert_knowledge(
    conn: sqlite3.Connection,
    content: str,
    **kwargs,
) -> int:
    defaults = dict(
        source_type="curated_article",
        content=content,
    )
    defaults.update(kwargs)
    columns = ", ".join(defaults.keys())
    placeholders = ", ".join("?" for _ in defaults)
    cursor = conn.execute(
        f"INSERT INTO knowledge ({columns}) VALUES ({placeholders})",
        tuple(defaults.values()),
    )
    conn.commit()
    return cursor.lastrowid


def _link_knowledge(
    conn: sqlite3.Connection,
    reply_id: int,
    knowledge_id: int,
    relevance_score: float = 0.9,
) -> None:
    conn.execute(
        "INSERT INTO reply_knowledge_links (reply_queue_id, knowledge_id, relevance_score) "
        "VALUES (?, ?, ?)",
        (reply_id, knowledge_id, relevance_score),
    )
    conn.commit()


def test_fresh_context_has_no_warnings():
    with _memory_db() as conn:
        # Create draft from 3 days ago
        draft_time = NOW - timedelta(days=3)
        # Create fresh knowledge (2 days before the draft)
        k1 = _insert_knowledge(
            conn,
            "Recent insight",
            published_at=(draft_time - timedelta(days=2)).isoformat(),
        )
        r1 = _insert_reply(
            conn,
            "mention-1",
            detected_at=draft_time.isoformat(),
        )
        _link_knowledge(conn, r1, k1)

        report = build_reply_source_freshness_report(conn, days=7, stale_days=30, now=NOW)

        assert report.scanned_count == 1
        assert report.fresh_count == 1
        assert report.stale_count == 0
        assert report.missing_count == 0
        assert report.malformed_count == 0
        assert report.ok is True

        finding = report.findings[0]
        assert finding.draft_id == r1
        assert finding.context_item_count == 1
        assert finding.newest_context_age_days == 2.0  # 2 days old at draft time
        assert finding.oldest_context_age_days == 2.0
        assert finding.warnings == ()


def test_stale_context_triggers_warning():
    with _memory_db() as conn:
        # Create stale knowledge (40 days old)
        k1 = _insert_knowledge(
            conn,
            "Old insight",
            published_at=(NOW - timedelta(days=40)).isoformat(),
        )
        r1 = _insert_reply(conn, "mention-2", detected_at=NOW.isoformat())
        _link_knowledge(conn, r1, k1)

        report = build_reply_source_freshness_report(conn, days=7, stale_days=30, now=NOW)

        assert report.stale_count == 1
        assert report.fresh_count == 0
        assert report.ok is False

        finding = report.findings[0]
        assert WARNING_STALE_CONTEXT in finding.warnings
        assert finding.oldest_context_age_days == 40.0


def test_multiple_context_items_tracks_newest_and_oldest():
    with _memory_db() as conn:
        k1 = _insert_knowledge(
            conn,
            "Fresh",
            published_at=(NOW - timedelta(days=5)).isoformat(),
        )
        k2 = _insert_knowledge(
            conn,
            "Medium",
            published_at=(NOW - timedelta(days=15)).isoformat(),
        )
        k3 = _insert_knowledge(
            conn,
            "Old",
            published_at=(NOW - timedelta(days=35)).isoformat(),
        )
        r1 = _insert_reply(conn, "mention-3", detected_at=NOW.isoformat())
        _link_knowledge(conn, r1, k1)
        _link_knowledge(conn, r1, k2)
        _link_knowledge(conn, r1, k3)

        report = build_reply_source_freshness_report(conn, days=7, stale_days=30, now=NOW)

        finding = report.findings[0]
        assert finding.context_item_count == 3
        assert finding.newest_context_age_days == 5.0
        assert finding.oldest_context_age_days == 35.0
        assert WARNING_STALE_CONTEXT in finding.warnings


def test_missing_context_when_no_knowledge_links():
    with _memory_db() as conn:
        r1 = _insert_reply(conn, "mention-4")

        report = build_reply_source_freshness_report(conn, days=7, stale_days=30, now=NOW)

        assert report.missing_count == 1
        assert report.fresh_count == 0

        finding = report.findings[0]
        assert finding.context_item_count == 0
        assert WARNING_MISSING_CONTEXT in finding.warnings


def test_malformed_context_when_timestamp_unparseable():
    with _memory_db() as conn:
        k1 = _insert_knowledge(
            conn,
            "Bad timestamp",
            published_at="not-a-date",
        )
        r1 = _insert_reply(conn, "mention-5", detected_at=NOW.isoformat())
        _link_knowledge(conn, r1, k1)

        report = build_reply_source_freshness_report(conn, days=7, stale_days=30, now=NOW)

        assert report.malformed_count == 1

        finding = report.findings[0]
        assert WARNING_MALFORMED_CONTEXT in finding.warnings
        assert finding.context_item_count == 1
        assert finding.newest_context_age_days is None
        assert finding.oldest_context_age_days is None


def test_mixed_warnings_when_some_malformed_and_some_stale():
    with _memory_db() as conn:
        k1 = _insert_knowledge(
            conn,
            "Malformed",
            published_at="invalid",
        )
        k2 = _insert_knowledge(
            conn,
            "Stale",
            published_at=(NOW - timedelta(days=40)).isoformat(),
        )
        r1 = _insert_reply(conn, "mention-6", detected_at=NOW.isoformat())
        _link_knowledge(conn, r1, k1)
        _link_knowledge(conn, r1, k2)

        report = build_reply_source_freshness_report(conn, days=7, stale_days=30, now=NOW)

        finding = report.findings[0]
        assert WARNING_MALFORMED_CONTEXT in finding.warnings
        assert WARNING_STALE_CONTEXT in finding.warnings
        assert finding.oldest_context_age_days == 40.0


def test_missing_reply_queue_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_reply_source_freshness_report(conn, days=7, stale_days=30, now=NOW)

    assert report.ok is True
    assert report.scanned_count == 0
    assert report.missing_tables == ("reply_queue",)


def test_missing_required_columns_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE reply_queue (status TEXT)")
    conn.commit()

    report = build_reply_source_freshness_report(conn, days=7, stale_days=30, now=NOW)

    assert report.ok is True
    assert report.scanned_count == 0
    assert "reply_queue" in report.missing_columns


def test_missing_knowledge_tables_treats_all_as_missing_context():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            inbound_tweet_id TEXT,
            detected_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("INSERT INTO reply_queue (inbound_tweet_id) VALUES ('mention-7')")
    conn.commit()

    report = build_reply_source_freshness_report(conn, days=7, stale_days=30, now=NOW)

    assert report.missing_count == 1
    finding = report.findings[0]
    assert WARNING_MISSING_CONTEXT in finding.warnings


def test_filters_by_status():
    with _memory_db() as conn:
        r1 = _insert_reply(conn, "pending-1", status="pending")
        r2 = _insert_reply(conn, "reviewed-1", status="reviewed")

        report = build_reply_source_freshness_report(
            conn, days=7, stale_days=30, status=("pending",), now=NOW
        )

        assert report.scanned_count == 1
        assert report.findings[0].draft_id == r1


def test_filters_by_platform():
    with _memory_db() as conn:
        r1 = _insert_reply(conn, "x-1", platform="x")
        r2 = _insert_reply(conn, "bluesky-1", platform="bluesky")

        report = build_reply_source_freshness_report(
            conn, days=7, stale_days=30, platform=("x",), now=NOW
        )

        assert report.scanned_count == 1
        assert report.findings[0].draft_id == r1


def test_filters_by_days_lookback():
    with _memory_db() as conn:
        r1 = _insert_reply(
            conn, "recent", detected_at=(NOW - timedelta(days=3)).isoformat()
        )
        r2 = _insert_reply(
            conn, "old", detected_at=(NOW - timedelta(days=10)).isoformat()
        )

        report = build_reply_source_freshness_report(conn, days=7, stale_days=30, now=NOW)

        assert report.scanned_count == 1
        assert report.findings[0].draft_id == r1


def test_json_formatter_produces_deterministic_output():
    with _memory_db() as conn:
        r1 = _insert_reply(conn, "mention-8")

        report = build_reply_source_freshness_report(conn, days=7, stale_days=30, now=NOW)
        output = format_reply_source_freshness_json(report)

        parsed = json.loads(output)
        assert parsed["artifact_type"] == "reply_source_freshness"
        assert "generated_at" in parsed
        assert "filters" in parsed
        assert "findings" in parsed


def test_text_formatter_produces_readable_output():
    with _memory_db() as conn:
        k1 = _insert_knowledge(
            conn,
            "Stale",
            published_at=(NOW - timedelta(days=40)).isoformat(),
        )
        r1 = _insert_reply(conn, "mention-9", detected_at=NOW.isoformat())
        _link_knowledge(conn, r1, k1)

        report = build_reply_source_freshness_report(conn, days=7, stale_days=30, now=NOW)
        output = format_reply_source_freshness_text(report)

        assert "Reply Source Freshness Report" in output
        assert "Stale: 1" in output
        assert "stale_context" in output


def test_script_json_output_format(monkeypatch):
    with _memory_db() as conn:
        _insert_reply(conn, "mention-10")

        monkeypatch.setattr(
            reply_source_freshness_script,
            "script_context",
            lambda: _script_context(conn),
        )

        exit_code = reply_source_freshness_script.main(["--format", "json"])
        assert exit_code == 0


def test_script_text_output_format(monkeypatch):
    with _memory_db() as conn:
        _insert_reply(conn, "mention-11")

        monkeypatch.setattr(
            reply_source_freshness_script,
            "script_context",
            lambda: _script_context(conn),
        )

        exit_code = reply_source_freshness_script.main(["--format", "text"])
        assert exit_code == 0


def test_script_accepts_days_and_stale_days_arguments(monkeypatch):
    with _memory_db() as conn:
        _insert_reply(conn, "mention-12")

        monkeypatch.setattr(
            reply_source_freshness_script,
            "script_context",
            lambda: _script_context(conn),
        )

        exit_code = reply_source_freshness_script.main(
            ["--days", "14", "--stale-days", "60", "--format", "json"]
        )
        assert exit_code == 0


def test_script_accepts_status_and_platform_filters(monkeypatch):
    with _memory_db() as conn:
        _insert_reply(conn, "mention-13", status="pending", platform="x")

        monkeypatch.setattr(
            reply_source_freshness_script,
            "script_context",
            lambda: _script_context(conn),
        )

        exit_code = reply_source_freshness_script.main(
            ["--status", "pending", "--platform", "x", "--format", "json"]
        )
        assert exit_code == 0


def test_sorting_prioritizes_malformed_then_missing_then_stale():
    with _memory_db() as conn:
        # Stale
        k1 = _insert_knowledge(
            conn,
            "Stale",
            published_at=(NOW - timedelta(days=40)).isoformat(),
        )
        r1 = _insert_reply(conn, "stale", detected_at=NOW.isoformat())
        _link_knowledge(conn, r1, k1)

        # Missing
        r2 = _insert_reply(conn, "missing", detected_at=NOW.isoformat())

        # Malformed
        k2 = _insert_knowledge(conn, "Bad", published_at="invalid")
        r3 = _insert_reply(conn, "malformed", detected_at=NOW.isoformat())
        _link_knowledge(conn, r3, k2)

        # Fresh
        k3 = _insert_knowledge(
            conn,
            "Fresh",
            published_at=(NOW - timedelta(days=5)).isoformat(),
        )
        r4 = _insert_reply(conn, "fresh", detected_at=NOW.isoformat())
        _link_knowledge(conn, r4, k3)

        report = build_reply_source_freshness_report(conn, days=7, stale_days=30, now=NOW)

        assert len(report.findings) == 4
        assert report.findings[0].mention_id == "malformed"
        assert report.findings[1].mention_id == "missing"
        assert report.findings[2].mention_id == "stale"
        assert report.findings[3].mention_id == "fresh"


def test_uses_created_at_fallback_when_published_at_missing():
    with _memory_db() as conn:
        k1 = _insert_knowledge(
            conn,
            "No published_at",
            created_at=(NOW - timedelta(days=10)).isoformat(),
        )
        r1 = _insert_reply(conn, "mention-14", detected_at=NOW.isoformat())
        _link_knowledge(conn, r1, k1)

        report = build_reply_source_freshness_report(conn, days=7, stale_days=30, now=NOW)

        finding = report.findings[0]
        assert finding.context_item_count == 1
        assert finding.newest_context_age_days == 10.0
        assert finding.warnings == ()


def test_negative_days_raises_value_error():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    try:
        build_reply_source_freshness_report(conn, days=-1, stale_days=30, now=NOW)
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "days must be positive" in str(exc)


def test_negative_stale_days_raises_value_error():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    try:
        build_reply_source_freshness_report(conn, days=7, stale_days=-1, now=NOW)
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "stale_days must be positive" in str(exc)
