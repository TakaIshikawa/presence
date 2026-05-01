"""Tests for LinkedIn comment imports into reply_queue."""

import importlib.util
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ingestion.linkedin_comments import (
    import_linkedin_comments,
    parse_linkedin_comments_csv,
    parse_linkedin_comments_json,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "import_linkedin_comments.py"
spec = importlib.util.spec_from_file_location("import_linkedin_comments_script", SCRIPT_PATH)
import_linkedin_comments_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(import_linkedin_comments_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _linkedin_content(db):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Original LinkedIn post",
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.upsert_publication_success(
        content_id,
        "linkedin",
        platform_post_id="urn:li:activity:123",
        platform_url="https://www.linkedin.com/feed/update/urn:li:activity:123",
        published_at="2026-04-20T10:00:00+00:00",
    )
    return content_id


def test_parses_csv_comment_export(tmp_path):
    path = tmp_path / "comments.csv"
    path.write_text(
        "post_url,comment_id,author,author_profile_url,body,created_at,like_count\n"
        "https://www.linkedin.com/feed/update/urn:li:activity:123?utm_source=x,c-1,Alice,"
        "https://www.linkedin.com/in/alice,Great post,2026-04-21T09:30:00Z,\"1,234\"\n",
        encoding="utf-8",
    )

    rows = parse_linkedin_comments_csv(path)

    assert len(rows) == 1
    assert rows[0].post_id == "123"
    assert rows[0].comment_id == "c-1"
    assert rows[0].author == "Alice"
    assert rows[0].body == "Great post"
    assert rows[0].created_at.isoformat() == "2026-04-21T09:30:00+00:00"
    assert rows[0].like_count == 1234


def test_parses_json_comment_export(tmp_path):
    path = tmp_path / "comments.json"
    path.write_text(
        json.dumps(
            {
                "comments": [
                    {
                        "post_id": "urn:li:activity:123",
                        "comment_id": "c-2",
                        "author": "Bob",
                        "body": "Can you share more detail?",
                        "created_at": "2026-04-21T10:00:00+00:00",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    rows = parse_linkedin_comments_json(path)

    assert len(rows) == 1
    assert rows[0].post_id == "urn:li:activity:123"
    assert rows[0].comment_id == "c-2"
    assert rows[0].body == "Can you share more detail?"


def test_import_matches_content_and_queues_reply(db, tmp_path):
    content_id = _linkedin_content(db)
    path = tmp_path / "comments.json"
    path.write_text(
        json.dumps(
            [
                {
                    "post_url": "https://www.linkedin.com/feed/update/urn:li:activity:123?utm_campaign=test",
                    "comment_id": "comment-123",
                    "author": "Alice",
                    "author_profile_url": "https://www.linkedin.com/in/alice",
                    "body": "This is actionable",
                    "created_at": "2026-04-21T09:30:00Z",
                    "like_count": 2,
                }
            ]
        ),
        encoding="utf-8",
    )

    result = import_linkedin_comments(db, path, format="json")

    assert result.insert_count == 1
    row = db.conn.execute("SELECT * FROM reply_queue").fetchone()
    assert row["platform"] == "linkedin"
    assert row["inbound_tweet_id"] == "comment-123"
    assert row["inbound_author_handle"] == "Alice"
    assert row["inbound_author_id"] == "https://www.linkedin.com/in/alice"
    assert row["inbound_text"] == "This is actionable"
    assert row["our_tweet_id"] == "123"
    assert row["our_platform_id"] == "123"
    assert row["our_content_id"] == content_id
    assert row["our_post_text"] == "Original LinkedIn post"
    metadata = json.loads(row["platform_metadata"])
    assert metadata["source"] == "manual_linkedin_comment_import"
    assert metadata["comment_id"] == "comment-123"
    assert metadata["like_count"] == 2
    assert metadata["matched_content_id"] == content_id


def test_deduplicates_comment_ids_deterministically(db, tmp_path):
    _linkedin_content(db)
    path = tmp_path / "comments.json"
    path.write_text(
        json.dumps(
            [
                {
                    "post_id": "123",
                    "comment_id": "dupe",
                    "author": "Alice",
                    "body": "First",
                },
                {
                    "post_id": "123",
                    "comment_id": "dupe",
                    "author": "Alice",
                    "body": "Second",
                },
            ]
        ),
        encoding="utf-8",
    )

    first = import_linkedin_comments(db, path, format="json")
    second = import_linkedin_comments(db, path, format="json")

    assert first.insert_count == 1
    assert first.skipped[0]["reason"] == "already_processed"
    assert second.insert_count == 0
    assert [item["reason"] for item in second.skipped] == ["already_processed", "already_processed"]
    count = db.conn.execute("SELECT COUNT(*) FROM reply_queue").fetchone()[0]
    assert count == 1
    row = db.conn.execute("SELECT inbound_text FROM reply_queue").fetchone()
    assert row["inbound_text"] == "First"


def test_dry_run_reports_without_mutating_database(db, tmp_path):
    _linkedin_content(db)
    path = tmp_path / "comments.csv"
    path.write_text(
        "post_id,comment_id,author,body\n"
        "123,c-1,Alice,Would queue\n",
        encoding="utf-8",
    )

    result = import_linkedin_comments(db, path, format="csv", dry_run=True)

    assert result.dry_run is True
    assert result.insert_count == 1
    count = db.conn.execute("SELECT COUNT(*) FROM reply_queue").fetchone()[0]
    assert count == 0


def test_script_dry_run_reports_summary(db, tmp_path, capsys):
    _linkedin_content(db)
    path = tmp_path / "comments.csv"
    path.write_text(
        "post_id,comment_id,author,body\n"
        "123,c-1,Alice,Would queue\n",
        encoding="utf-8",
    )

    with patch.object(
        import_linkedin_comments_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = import_linkedin_comments_script.main(
            ["--input", str(path), "--format", "csv", "--dry-run", "--limit", "1"]
        )

    assert exit_code == 0
    assert "Would queue 1 LinkedIn comment." in capsys.readouterr().out
    count = db.conn.execute("SELECT COUNT(*) FROM reply_queue").fetchone()[0]
    assert count == 0
