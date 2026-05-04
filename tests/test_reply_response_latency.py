"""Tests for reply draft response latency reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from engagement.reply_response_latency import (
    build_reply_response_latency_report,
    format_reply_response_latency_json,
    format_reply_response_latency_text,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_response_latency.py"
spec = importlib.util.spec_from_file_location("reply_response_latency_script", SCRIPT_PATH)
reply_response_latency_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_response_latency_script)


def _reply_db(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            inbound_tweet_id TEXT,
            inbound_author_handle TEXT,
            inbound_text TEXT,
            draft_text TEXT,
            status TEXT,
            detected_at TEXT,
            draft_created_at TEXT
        )"""
    )
    return conn


def _insert_reply(
    conn: sqlite3.Connection,
    *,
    reply_id: int,
    detected_at: str,
    draft_created_at: str | None = None,
    draft_text: str | None = "Thanks for asking.",
    handle: str = "alice",
    platform: str = "x",
    status: str = "pending",
) -> None:
    conn.execute(
        """INSERT INTO reply_queue
           (id, platform, inbound_tweet_id, inbound_author_handle, inbound_text,
            draft_text, status, detected_at, draft_created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            reply_id,
            platform,
            f"mention-{reply_id}",
            handle,
            "Can you explain this?",
            draft_text,
            status,
            detected_at,
            draft_created_at,
        ),
    )
    conn.commit()


def test_report_emits_on_time_draft_latency_and_summary_buckets():
    conn = _reply_db()
    _insert_reply(
        conn,
        reply_id=1,
        detected_at="2026-05-03T09:00:00+00:00",
        draft_created_at="2026-05-03T09:30:00+00:00",
    )

    report = build_reply_response_latency_report(
        conn,
        days=1,
        threshold_minutes=60,
        now=NOW,
    )
    payload = json.loads(format_reply_response_latency_json(report))
    text = format_reply_response_latency_text(report)

    assert payload["artifact_type"] == "reply_response_latency"
    assert list(payload) == sorted(payload)
    assert payload["rows"][0]["latency_minutes"] == 30.0
    assert payload["rows"][0]["latency_bucket"] == "16-60m"
    assert payload["rows"][0]["flagged"] is False
    assert payload["summary"]["median"] == 30.0
    assert payload["latency_buckets"][1]["count"] == 1
    assert "No missing or delayed reply drafts." in text


def test_delayed_draft_is_flagged_after_threshold():
    conn = _reply_db()
    _insert_reply(
        conn,
        reply_id=2,
        handle="slow",
        detected_at="2026-05-03T08:00:00+00:00",
        draft_created_at="2026-05-03T10:30:00+00:00",
    )

    report = build_reply_response_latency_report(
        conn,
        days=1,
        threshold_minutes=60,
        now=NOW,
    )

    row = report["rows"][0]
    assert row["latency_minutes"] == 150.0
    assert row["latency_bucket"] == "61-240m"
    assert row["delayed"] is True
    assert row["flag_reason"] == "delayed_draft"
    assert report["totals"]["delayed_count"] == 1
    assert report["flagged_mentions"][0]["mention_id"] == 2


def test_missing_draft_mentions_are_included_and_flagged():
    conn = _reply_db()
    _insert_reply(
        conn,
        reply_id=3,
        handle="missing",
        detected_at="2026-05-03T07:00:00+00:00",
        draft_text="",
    )

    report = build_reply_response_latency_report(conn, days=1, now=NOW)

    row = report["rows"][0]
    assert row["draft_created_at"] is None
    assert row["latency_minutes"] is None
    assert row["latency_bucket"] == "missing_draft"
    assert row["missing_draft"] is True
    assert row["flag_reason"] == "missing_draft"
    assert report["totals"]["missing_draft_count"] == 1
    assert report["latency_buckets"][-1]["count"] == 1


def test_threshold_boundary_is_not_delayed_until_latency_exceeds_threshold():
    conn = _reply_db()
    _insert_reply(
        conn,
        reply_id=4,
        detected_at="2026-05-03T08:00:00+00:00",
        draft_created_at="2026-05-03T09:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=5,
        detected_at="2026-05-03T08:00:00+00:00",
        draft_created_at="2026-05-03T09:01:00+00:00",
    )

    report = build_reply_response_latency_report(
        conn,
        days=1,
        threshold_minutes=60,
        now=NOW,
    )
    rows = {row["mention_id"]: row for row in report["rows"]}

    assert rows[4]["latency_minutes"] == 60.0
    assert rows[4]["flagged"] is False
    assert rows[5]["latency_minutes"] == 61.0
    assert rows[5]["flag_reason"] == "delayed_draft"
    assert report["totals"]["flagged_count"] == 1


def test_missing_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_reply_response_latency_report(conn, days=1, now=NOW)

    assert report["missing_tables"] == ["reply_queue"]
    assert report["rows"] == []
    assert report["totals"]["mention_count"] == 0


def test_cli_outputs_json_and_validates_arguments(capsys, tmp_path):
    db_path = tmp_path / "reply.db"
    conn = _reply_db(db_path)
    _insert_reply(
        conn,
        reply_id=6,
        detected_at="2026-05-03T09:00:00+00:00",
        draft_created_at="2026-05-03T09:20:00+00:00",
    )
    conn.close()

    assert (
        reply_response_latency_script.main(
            [
                "--db",
                str(db_path),
                "--days",
                "2",
                "--threshold-minutes",
                "30",
                "--format",
                "json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["filters"]["threshold_minutes"] == 30
    assert payload["rows"][0]["mention_id"] == 6
    assert reply_response_latency_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_aggregation_functions_calculate_percentiles_correctly():
    conn = _reply_db()
    # Create mentions with varied latencies for testing percentile calculations
    latencies = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    for i, latency in enumerate(latencies):
        detected_at = NOW - timedelta(hours=2)
        draft_created_at = detected_at + timedelta(minutes=latency)
        _insert_reply(
            conn,
            reply_id=i + 1,
            detected_at=detected_at.isoformat(),
            draft_created_at=draft_created_at.isoformat(),
        )

    report = build_reply_response_latency_report(conn, days=1, now=NOW)

    assert report["summary"]["count"] == 10
    assert report["summary"]["minimum"] == 10.0
    assert report["summary"]["median"] == 55.0
    assert report["summary"]["p90"] == 91.0
    assert report["summary"]["maximum"] == 100.0
    assert report["summary"]["average"] == 55.0


def test_same_timestamp_handling():
    """Test handling of replies with identical timestamps."""
    conn = _reply_db()
    same_time = "2026-05-03T10:00:00+00:00"
    _insert_reply(
        conn,
        reply_id=1,
        detected_at=same_time,
        draft_created_at=same_time,
    )
    _insert_reply(
        conn,
        reply_id=2,
        detected_at=same_time,
        draft_created_at=same_time,
    )

    report = build_reply_response_latency_report(conn, days=1, now=NOW)

    assert len(report["rows"]) == 2
    for row in report["rows"]:
        assert row["latency_minutes"] == 0.0
        assert row["latency_bucket"] == "0-15m"


def test_out_of_order_replies_negative_latency():
    """Test that out-of-order replies (draft before detection) are handled."""
    conn = _reply_db()
    detected_at = NOW - timedelta(hours=1)
    # Draft created before detection (invalid scenario)
    draft_created_at = detected_at - timedelta(minutes=30)

    _insert_reply(
        conn,
        reply_id=1,
        detected_at=detected_at.isoformat(),
        draft_created_at=draft_created_at.isoformat(),
    )

    report = build_reply_response_latency_report(conn, days=1, now=NOW)

    row = report["rows"][0]
    # Negative latency should be treated as None
    assert row["latency_minutes"] is None
    assert row["latency_bucket"] == "missing_draft"


def test_multiple_platforms_tracking():
    """Test tracking replies across multiple platforms."""
    conn = _reply_db()
    _insert_reply(
        conn,
        reply_id=1,
        platform="x",
        detected_at="2026-05-03T09:00:00+00:00",
        draft_created_at="2026-05-03T09:30:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=2,
        platform="linkedin",
        detected_at="2026-05-03T09:00:00+00:00",
        draft_created_at="2026-05-03T09:45:00+00:00",
    )

    report = build_reply_response_latency_report(conn, days=1, now=NOW)

    platforms = {row["platform"] for row in report["rows"]}
    assert platforms == {"x", "linkedin"}
    assert len(report["rows"]) == 2


def test_text_format_output():
    """Test text format output is properly formatted."""
    conn = _reply_db()
    _insert_reply(
        conn,
        reply_id=1,
        detected_at="2026-05-03T09:00:00+00:00",
        draft_created_at="2026-05-03T09:30:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=2,
        detected_at="2026-05-03T08:00:00+00:00",
        draft_created_at="2026-05-03T10:30:00+00:00",
    )

    report = build_reply_response_latency_report(
        conn,
        days=1,
        threshold_minutes=60,
        now=NOW,
    )
    text = format_reply_response_latency_text(report)

    assert "Reply Response Latency Report" in text
    assert "Generated:" in text
    assert "Lookback:" in text
    assert "Delayed threshold: >60m" in text
    assert "Totals:" in text
    assert "Latency minutes:" in text
    assert "Buckets:" in text
    assert "Flagged mentions:" in text
    assert "reply_queue:2" in text


def test_invalid_input_days_validation():
    """Test that invalid days parameter raises ValueError."""
    conn = _reply_db()

    try:
        build_reply_response_latency_report(conn, days=0, now=NOW)
        assert False, "Should have raised ValueError"
    except ValueError as exc:
        assert "days must be positive" in str(exc)

    try:
        build_reply_response_latency_report(conn, days=-5, now=NOW)
        assert False, "Should have raised ValueError"
    except ValueError as exc:
        assert "days must be positive" in str(exc)


def test_invalid_input_threshold_validation():
    """Test that invalid threshold_minutes parameter raises ValueError."""
    conn = _reply_db()

    try:
        build_reply_response_latency_report(conn, threshold_minutes=0, now=NOW)
        assert False, "Should have raised ValueError"
    except ValueError as exc:
        assert "threshold_minutes must be positive" in str(exc)


def test_invalid_input_limit_validation():
    """Test that invalid limit parameter raises ValueError."""
    conn = _reply_db()

    try:
        build_reply_response_latency_report(conn, limit=0, now=NOW)
        assert False, "Should have raised ValueError"
    except ValueError as exc:
        assert "limit must be positive" in str(exc)


def test_empty_draft_text_treated_as_missing():
    """Test that empty/whitespace draft_text is treated as missing draft."""
    conn = _reply_db()
    _insert_reply(
        conn,
        reply_id=1,
        detected_at="2026-05-03T09:00:00+00:00",
        draft_text="   ",  # Whitespace only
    )

    report = build_reply_response_latency_report(conn, days=1, now=NOW)

    row = report["rows"][0]
    assert row["missing_draft"] is True
    assert row["flag_reason"] == "missing_draft"
    assert row["latency_bucket"] == "missing_draft"


def test_multiple_authors_tracking():
    """Test tracking replies from multiple authors."""
    conn = _reply_db()
    _insert_reply(
        conn,
        reply_id=1,
        handle="alice",
        detected_at="2026-05-03T09:00:00+00:00",
        draft_created_at="2026-05-03T09:30:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=2,
        handle="bob",
        detected_at="2026-05-03T09:00:00+00:00",
        draft_created_at="2026-05-03T09:15:00+00:00",
    )

    report = build_reply_response_latency_report(conn, days=1, now=NOW)

    handles = {row["inbound_author_handle"] for row in report["rows"]}
    assert handles == {"alice", "bob"}


def test_latency_bucket_boundaries():
    """Test latency bucket assignments at boundaries."""
    conn = _reply_db()
    base_time = NOW - timedelta(hours=2)

    test_cases = [
        (0, "0-15m"),
        (15, "0-15m"),
        (15.1, "16-60m"),
        (60, "16-60m"),
        (60.1, "61-240m"),
        (240, "61-240m"),
        (240.1, ">240m"),
        (500, ">240m"),
    ]

    for i, (latency_minutes, expected_bucket) in enumerate(test_cases):
        draft_time = base_time + timedelta(minutes=latency_minutes)
        _insert_reply(
            conn,
            reply_id=i + 1,
            detected_at=base_time.isoformat(),
            draft_created_at=draft_time.isoformat(),
        )

    report = build_reply_response_latency_report(conn, days=1, now=NOW)

    for i, (_, expected_bucket) in enumerate(test_cases):
        row = report["rows"][i]
        assert (
            row["latency_bucket"] == expected_bucket
        ), f"Row {i}: expected {expected_bucket}, got {row['latency_bucket']}"


def test_cli_text_format_output(capsys, tmp_path):
    """Test CLI with text format output."""
    db_path = tmp_path / "reply.db"
    conn = _reply_db(db_path)
    _insert_reply(
        conn,
        reply_id=1,
        detected_at="2026-05-03T09:00:00+00:00",
        draft_created_at="2026-05-03T09:30:00+00:00",
    )
    conn.close()

    assert (
        reply_response_latency_script.main(
            [
                "--db",
                str(db_path),
                "--format",
                "text",
            ]
        )
        == 0
    )

    output = capsys.readouterr().out
    assert "Reply Response Latency Report" in output
    assert "Generated:" in output


def test_status_field_preserved():
    """Test that status field is correctly preserved in report."""
    conn = _reply_db()
    _insert_reply(
        conn,
        reply_id=1,
        detected_at="2026-05-03T09:00:00+00:00",
        draft_created_at="2026-05-03T09:30:00+00:00",
        status="posted",
    )
    _insert_reply(
        conn,
        reply_id=2,
        detected_at="2026-05-03T09:00:00+00:00",
        draft_created_at="2026-05-03T09:30:00+00:00",
        status="pending",
    )

    report = build_reply_response_latency_report(conn, days=1, now=NOW)

    statuses = {row["status"] for row in report["rows"]}
    assert statuses == {"posted", "pending"}
