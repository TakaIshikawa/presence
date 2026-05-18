"""Tests for content claim evidence aging report."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.content_claim_evidence_aging import (
    build_content_claim_evidence_aging_report,
    format_content_claim_evidence_aging_json,
    format_content_claim_evidence_aging_text,
)


NOW = datetime(2026, 5, 16, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_claim_evidence_aging.py"
spec = importlib.util.spec_from_file_location("content_claim_evidence_aging_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _content(db, text: str, *, published: int = 0) -> int:
    content_id = db.insert_generated_content(
        content_type="blog_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = ?, published_at = ? WHERE id = ?",
        (published, NOW.isoformat() if published else None, content_id),
    )
    db.conn.commit()
    return int(content_id)


def _claim_check(db, content_id: int, *, updated_at: datetime, annotation: str = "claim summary") -> None:
    db.save_claim_check_summary(
        content_id,
        supported_count=2,
        unsupported_count=0,
        annotation_text=annotation,
    )
    db.conn.execute(
        "UPDATE content_claim_checks SET created_at = ?, updated_at = ? WHERE content_id = ?",
        (updated_at.isoformat(), updated_at.isoformat(), content_id),
    )
    db.conn.commit()


def test_flags_old_claim_checks_and_prioritizes_queued_unpublished_content(db):
    published = _content(db, "Published stale claim", published=1)
    queued = _content(db, "Queued stale claim")
    fresh = _content(db, "Fresh claim")
    unpublished = _content(db, "Unpublished older claim")
    db.conn.execute(
        "INSERT INTO publish_queue (content_id, scheduled_at, status) VALUES (?, ?, 'queued')",
        (queued, (NOW + timedelta(days=1)).isoformat()),
    )
    _claim_check(db, published, updated_at=NOW - timedelta(days=50), annotation="published claim")
    _claim_check(db, queued, updated_at=NOW - timedelta(days=31), annotation="queued claim")
    _claim_check(db, fresh, updated_at=NOW - timedelta(days=5), annotation="fresh claim")
    _claim_check(db, unpublished, updated_at=NOW - timedelta(days=45), annotation="unpublished claim")

    report = build_content_claim_evidence_aging_report(db, stale_days=30, now=NOW)

    assert [item["content_id"] for item in report["issues"]] == [queued, unpublished, published]
    first = report["issues"][0]
    assert first["claim_check_id"] == str(queued)
    assert first["content_type"] == "blog_post"
    assert first["claim"] == "queued claim"
    assert first["checked_at"] == (NOW - timedelta(days=31)).isoformat()
    assert first["age_days"] == 31
    assert first["content_status"] == "queued"
    assert first["severity"] == "medium"
    assert first["recommendation"] == "recheck claim evidence before publication"
    assert fresh not in {item["content_id"] for item in report["issues"]}
    assert report["totals"]["by_content_status"] == {
        "published": 1,
        "queued": 1,
        "unpublished": 1,
    }


def test_missing_checked_timestamp_is_high_severity_hold():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content_type TEXT, content TEXT)")
    conn.execute(
        "CREATE TABLE content_claim_checks (content_id INTEGER PRIMARY KEY, annotation_text TEXT)"
    )
    conn.execute("INSERT INTO generated_content (id, content_type, content) VALUES (1, 'x_post', 'Claim')")
    conn.execute("INSERT INTO content_claim_checks (content_id, annotation_text) VALUES (1, 'Claim')")

    report = build_content_claim_evidence_aging_report(conn, now=NOW)

    assert report["issues"][0]["checked_at"] is None
    assert report["issues"][0]["age_days"] == 999999
    assert report["issues"][0]["severity"] == "high"
    assert report["issues"][0]["recommendation"] == "hold publication and recheck claim evidence"


def test_missing_tables_degrade_with_warnings():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY)")

    report = build_content_claim_evidence_aging_report(conn, now=NOW)

    assert report["issues"] == []
    assert report["missing_tables"] == ["content_claim_checks"]
    assert "content_claim_checks" in format_content_claim_evidence_aging_text(report)


def test_formatters_and_script_json(file_db, capsys):
    content_id = _content(file_db, "CLI stale claim")
    _claim_check(file_db, content_id, updated_at=NOW - timedelta(days=40))

    report = build_content_claim_evidence_aging_report(file_db, stale_days=30, now=NOW)
    payload = json.loads(format_content_claim_evidence_aging_json(report))

    assert payload["artifact_type"] == "content_claim_evidence_aging"
    assert payload["issues"][0]["content_id"] == content_id

    status = script.main(["--db", str(file_db.db_path), "--stale-days", "30", "--format", "json"])
    output = capsys.readouterr().out
    assert status == 0
    assert json.loads(output)["artifact_type"] == "content_claim_evidence_aging"
