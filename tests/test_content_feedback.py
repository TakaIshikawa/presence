"""Tests for durable generated-content feedback storage."""

from __future__ import annotations

import sqlite3

from storage.db import Database


def _content(db, content: str = "Generated post", content_type: str = "x_post") -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=7.0,
        eval_feedback="",
    )


def test_add_content_feedback_stores_normalized_tags(db):
    content_id = _content(db)

    feedback_id = db.add_content_feedback(
        content_id,
        "reject",
        "Too broad.",
        tags=["Too-Generic", "unsupported_claim", "too generic"],
    )

    row = db.conn.execute("SELECT tags FROM content_feedback WHERE id = ?", (feedback_id,)).fetchone()
    assert row["tags"] == '["too_generic", "unsupported_claim"]'

    feedback = db.get_recent_content_feedback()
    assert feedback[0]["tags"] == ["too_generic", "unsupported_claim"]


def test_get_recent_content_feedback_filters_by_any_tag(db):
    first = _content(db, "First")
    second = _content(db, "Second")
    third = _content(db, "Third")
    db.add_content_feedback(first, "reject", "Too broad.", tags=["too_generic"])
    db.add_content_feedback(second, "revise", "Audience mismatch.", tags=["wrong_audience"])
    db.add_content_feedback(third, "prefer", "Good hook.", tags=["good_hook"])

    feedback = db.get_recent_content_feedback(tags=["wrong_audience", "too_generic"])

    assert {item["content_id"] for item in feedback} == {first, second}
    assert all(item["tags"] for item in feedback)


def test_legacy_content_feedback_rows_without_tags_return_empty_list(db):
    content_id = _content(db)
    db.conn.execute(
        """INSERT INTO content_feedback
           (content_id, feedback_type, notes, replacement_text)
           VALUES (?, ?, ?, ?)""",
        (content_id, "reject", "Legacy note.", None),
    )
    db.conn.commit()

    feedback = db.get_recent_content_feedback()

    assert feedback[0]["tags"] == []


def test_add_content_feedback_rejects_invalid_tags(db):
    content_id = _content(db)

    try:
        db.add_content_feedback(content_id, "reject", tags=["bad/tag"])
    except ValueError as exc:
        assert "tags" in str(exc)
    else:
        raise AssertionError("invalid tag was accepted")


def test_init_schema_migrates_existing_content_feedback_tags_column(tmp_path, schema_path):
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_type TEXT NOT NULL,
            source_commits TEXT,
            source_messages TEXT,
            content TEXT NOT NULL,
            eval_score REAL,
            eval_feedback TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE content_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_id INTEGER NOT NULL REFERENCES generated_content(id),
            feedback_type TEXT NOT NULL,
            notes TEXT,
            replacement_text TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.close()

    database = Database(str(db_path))
    database.connect()
    database.init_schema(schema_path)
    columns = {row[1] for row in database.conn.execute("PRAGMA table_info(content_feedback)")}
    database.close()

    assert "tags" in columns
