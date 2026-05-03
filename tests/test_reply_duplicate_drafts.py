"""Tests for duplicate pending reply draft reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from engagement.reply_duplicate_drafts import (
    build_reply_duplicate_drafts_report,
    format_reply_duplicate_drafts_json,
    format_reply_duplicate_drafts_text,
    normalize_reply_draft_signature,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_duplicate_drafts.py"
spec = importlib.util.spec_from_file_location("reply_duplicate_drafts_script", SCRIPT_PATH)
reply_duplicate_drafts_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_duplicate_drafts_script)


def _reply_db(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            inbound_author_handle TEXT,
            draft_text TEXT,
            status TEXT,
            detected_at TEXT
        )"""
    )
    return conn


def _insert_reply(
    conn: sqlite3.Connection,
    *,
    reply_id: int,
    draft_text: str | None,
    handle: str = "alice",
    status: str = "pending",
    detected_at: str = "2026-05-03T10:00:00+00:00",
) -> None:
    conn.execute(
        """INSERT INTO reply_queue
           (id, inbound_author_handle, draft_text, status, detected_at)
           VALUES (?, ?, ?, ?, ?)""",
        (reply_id, handle, draft_text, status, detected_at),
    )
    conn.commit()


def test_groups_equivalent_drafts_after_text_normalization_and_sorts_groups():
    conn = _reply_db()
    _insert_reply(
        conn,
        reply_id=1,
        handle="@Casey",
        draft_text="Thanks @Casey! Here's the write-up: https://example.com/a",
        detected_at="2026-05-03T08:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=2,
        handle="casey",
        draft_text="thanks here's the write up",
        detected_at="2026-05-03T10:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=3,
        handle="casey",
        draft_text="THANKS, here's   the write-up!!! https://example.com/b",
        detected_at="2026-05-03T11:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=4,
        handle="devon",
        draft_text="I would split the migration into batches.",
        detected_at="2026-05-03T09:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=5,
        handle="devon",
        draft_text="i would split the migration into batches",
        detected_at="2026-05-03T11:30:00+00:00",
    )

    report = build_reply_duplicate_drafts_report(conn, days=1, threshold=2, now=NOW)
    payload = json.loads(format_reply_duplicate_drafts_json(report))
    text = format_reply_duplicate_drafts_text(report)

    assert payload["artifact_type"] == "reply_duplicate_drafts"
    assert list(payload) == sorted(payload)
    assert payload["totals"] == {
        "drafted_rows": 5,
        "duplicate_draft_count": 5,
        "duplicate_groups": 2,
        "rows_scanned": 5,
    }
    assert [group["duplicate_count"] for group in payload["groups"]] == [3, 2]
    assert payload["groups"][0]["normalized_signature"] == "thanks heres the write up"
    assert payload["groups"][0]["newest_detected_at"] == "2026-05-03T11:00:00+00:00"
    assert payload["groups"][0]["reply_ids"] == [1, 2, 3]
    assert payload["groups"][0]["examples"][0]["reply_id"] == 3
    assert payload["groups"][0]["examples"][0]["handle"] == "casey"
    assert "Reply Duplicate Drafts Report" in text
    assert "count=3" in text


def test_threshold_limit_status_window_and_empty_drafts_are_applied():
    conn = _reply_db()
    _insert_reply(
        conn,
        reply_id=1,
        draft_text="Repeat me",
        detected_at="2026-05-03T08:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=2,
        draft_text="repeat me!",
        detected_at="2026-05-03T09:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=3,
        draft_text="small pair",
        detected_at="2026-05-03T10:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=4,
        draft_text="small pair",
        detected_at="2026-05-03T11:00:00+00:00",
    )
    _insert_reply(conn, reply_id=5, draft_text="repeat me", status="posted")
    _insert_reply(
        conn,
        reply_id=6,
        draft_text="repeat me",
        detected_at="2026-04-01T08:00:00+00:00",
    )
    _insert_reply(conn, reply_id=7, draft_text="")
    _insert_reply(conn, reply_id=8, draft_text=None)

    report = build_reply_duplicate_drafts_report(
        conn,
        days=1,
        threshold=2,
        limit=1,
        now=NOW,
    )

    assert report.totals["rows_scanned"] == 6
    assert report.totals["drafted_rows"] == 4
    assert report.totals["duplicate_groups"] == 1
    assert report.groups[0].normalized_signature == "small pair"
    assert report.groups[0].newest_detected_at == "2026-05-03T11:00:00+00:00"


def test_groups_below_threshold_are_omitted():
    rows = [
        {"id": 1, "draft_text": "same copy", "status": "pending", "detected_at": "2026-05-03"},
        {"id": 2, "draft_text": "same copy!", "status": "pending", "detected_at": "2026-05-03"},
    ]

    report = build_reply_duplicate_drafts_report(rows, threshold=3, now=NOW)

    assert report.groups == ()
    assert report.totals["duplicate_draft_count"] == 0
    assert "No duplicate pending reply drafts matched." in format_reply_duplicate_drafts_text(
        report
    )


def test_missing_table_and_missing_columns_are_reported_gracefully():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing_table = build_reply_duplicate_drafts_report(conn, now=NOW)

    conn.execute("CREATE TABLE reply_queue (id INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO reply_queue (id) VALUES (1)")
    conn.commit()
    missing_columns = build_reply_duplicate_drafts_report(conn, now=NOW)

    assert missing_table.missing_tables == ("reply_queue",)
    assert missing_table.totals["rows_scanned"] == 0
    assert missing_columns.missing_columns == {
        "reply_queue": ("draft_text", "detected_at", "inbound_author_handle", "status")
    }
    assert missing_columns.groups == ()


def test_normalizer_removes_case_whitespace_urls_mentions_and_punctuation():
    assert normalize_reply_draft_signature(
        " Thanks @Ada!!! Read https://example.com/a, then retry. "
    ) == normalize_reply_draft_signature("thanks read then retry")


def test_cli_runs_against_explicit_db_and_validates_arguments(capsys, tmp_path):
    db_path = tmp_path / "reply.db"
    conn = _reply_db(db_path)
    _insert_reply(conn, reply_id=1, draft_text="Same draft", handle="cli")
    _insert_reply(conn, reply_id=2, draft_text="same draft!", handle="cli")
    conn.close()

    assert (
        reply_duplicate_drafts_script.main(
            [
                "--db",
                str(db_path),
                "--days",
                "7",
                "--threshold",
                "2",
                "--format",
                "json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["filters"]["threshold"] == 2
    assert payload["groups"][0]["reply_ids"] == [1, 2]
    assert reply_duplicate_drafts_script.main(["--threshold", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
