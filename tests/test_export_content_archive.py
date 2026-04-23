"""Tests for export_content_archive.py."""

from __future__ import annotations

import io
import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from export_content_archive import iter_archive_records, main, write_jsonl


BASE_TIME = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _set_content_created_at(db, content_id: int, created_at: datetime) -> None:
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at.isoformat(), content_id),
    )
    db.conn.commit()


def seed_archive_records(db) -> dict[str, int]:
    large_body = "First line\n" + ("A large generated content body. " * 700)
    first_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha-archive"],
        source_messages=["msg-archive"],
        content=large_body,
        eval_score=8.4,
        eval_feedback=json.dumps({"summary": "clear", "flags": ["large-body"]}),
        content_format="micro_story",
    )
    _set_content_created_at(db, first_id, BASE_TIME - timedelta(days=1))
    db.conn.execute(
        "UPDATE generated_content SET content_embedding = ? WHERE id = ?",
        (b"\x00\x01archive", first_id),
    )
    db.conn.commit()

    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "article-archive",
            "https://example.test/archive",
            "Avery",
            "Knowledge source material",
            "Archive exports need durable lineage",
        ),
    ).lastrowid
    db.conn.commit()
    db.insert_content_knowledge_links(first_id, [(knowledge_id, 0.91)])
    db.upsert_content_variant(
        first_id,
        "x",
        "post",
        "X-specific archive variant",
        {"audience": "builders", "safe": True},
    )
    db.upsert_publication_success(
        first_id,
        "x",
        platform_post_id="tweet-archive",
        platform_url="https://x.test/status/tweet-archive",
        published_at=(BASE_TIME - timedelta(hours=12)).isoformat(),
    )
    db.insert_engagement(first_id, "tweet-archive", 11, 3, 2, 1, 17.0)
    db.insert_content_topics(first_id, [("testing", "archive contracts", 0.93)])
    campaign_id = db.create_campaign(
        name="Archive Campaign",
        goal="Keep external analysis simple",
        start_date="2026-04-01",
        end_date="2026-04-30",
        daily_limit=2,
        weekly_limit=5,
        status="active",
    )
    planned_id = db.insert_planned_topic(
        topic="testing",
        angle="backup analysis",
        target_date="2026-04-23",
        campaign_id=campaign_id,
    )
    db.mark_planned_topic_generated(planned_id, first_id)

    second_id = db.insert_generated_content(
        content_type="blog_post",
        source_commits=[],
        source_messages=[],
        content="Blog archive body",
        eval_score=7.0,
        eval_feedback="solid",
    )
    _set_content_created_at(db, second_id, BASE_TIME - timedelta(days=2))
    db.upsert_publication_success(
        second_id,
        "bluesky",
        platform_post_id="at://did:plc:test/app.bsky.feed.post/archive",
        platform_url="https://bsky.test/profile/test/post/archive",
        published_at=(BASE_TIME - timedelta(days=1)).isoformat(),
    )
    db.insert_bluesky_engagement(
        second_id,
        "at://did:plc:test/app.bsky.feed.post/archive",
        5,
        1,
        2,
        0,
        8.0,
    )

    old_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Old archive body",
        eval_score=6.0,
        eval_feedback="old",
    )
    _set_content_created_at(db, old_id, BASE_TIME - timedelta(days=40))
    return {"x": first_id, "bluesky": second_id, "old": old_id}


def test_archive_jsonl_has_one_object_per_content_item(db):
    ids = seed_archive_records(db)
    records = iter_archive_records(db, days=7, now=BASE_TIME)

    output = io.StringIO()
    count = write_jsonl(records, output)
    lines = output.getvalue().splitlines()
    payloads = [json.loads(line) for line in lines]

    assert count == 2
    assert len(lines) == 2
    assert [payload["content"]["id"] for payload in payloads] == [ids["x"], ids["bluesky"]]

    x_record = payloads[0]
    assert "First line\\nA large generated content body." in lines[0]
    assert x_record["provenance"]["content"]["id"] == ids["x"]
    assert x_record["content"]["source_commits"] == ["sha-archive"]
    assert x_record["content"]["content_embedding"]["encoding"] == "base64"
    assert x_record["knowledge_links"][0]["id"] > 0
    assert x_record["publications"][0]["platform"] == "x"
    assert x_record["engagement_snapshots"][0]["platform"] == "x"
    assert x_record["topics"][0]["topic"] == "testing"
    assert x_record["planned_topics"][0]["campaign"]["name"] == "Archive Campaign"


def test_archive_filters_by_content_type_and_platform(db):
    ids = seed_archive_records(db)

    blog_records = iter_archive_records(db, days=7, content_type="blog_post", now=BASE_TIME)
    bluesky_records = iter_archive_records(db, days=7, platform="bluesky", now=BASE_TIME)

    assert [record["content"]["id"] for record in blog_records] == [ids["bluesky"]]
    assert [record["content"]["id"] for record in bluesky_records] == [ids["bluesky"]]
    assert bluesky_records[0]["publications"][0]["platform"] == "bluesky"
    assert bluesky_records[0]["engagement_snapshots"][0]["platform"] == "bluesky"


def test_main_writes_output_file(db, tmp_path, capsys):
    seed_archive_records(db)
    output_path = tmp_path / "archive.jsonl"

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("export_content_archive.script_context", fake_script_context):
        main(["--days", "7", "--output", str(output_path)])

    payloads = [json.loads(line) for line in output_path.read_text().splitlines()]
    assert len(payloads) == 2
    assert "Exported 2 content archive records" in capsys.readouterr().err
