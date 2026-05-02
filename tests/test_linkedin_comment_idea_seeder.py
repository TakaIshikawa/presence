import importlib.util
import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.linkedin_comment_idea_seeder import (
    SOURCE_NAME,
    format_linkedin_comment_seed_json,
    format_linkedin_comment_seed_text,
    seed_linkedin_comment_ideas,
)


NOW = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "seed_linkedin_comment_ideas.py"
spec = importlib.util.spec_from_file_location("seed_linkedin_comment_ideas_script", SCRIPT_PATH)
seed_linkedin_comment_ideas_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(seed_linkedin_comment_ideas_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Original post about deterministic workflows",
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.upsert_publication_success(
        content_id,
        "linkedin",
        platform_post_id="urn:li:activity:123",
        platform_url="https://www.linkedin.com/feed/update/urn:li:activity:123",
        published_at="2026-04-27T10:00:00+00:00",
    )
    return content_id


def _linkedin_comment(
    db,
    *,
    comment_id: str,
    body: str = "This would make a useful follow-up post",
    author: str = "Alice",
    like_count: int = 3,
    content_id: int | None = None,
    post_id: str = "123",
    post_url: str = "https://www.linkedin.com/feed/update/urn:li:activity:123",
    created_at: str = "2026-04-29T09:00:00+00:00",
) -> int:
    metadata = {
        "source": "manual_linkedin_comment_import",
        "comment_id": comment_id,
        "author": author,
        "author_profile_url": f"https://www.linkedin.com/in/{author.lower()}",
        "body": body,
        "post_id": post_id,
        "post_url": post_url,
        "created_at": created_at,
        "like_count": like_count,
        "matched_content_id": content_id,
    }
    return db.insert_reply_draft(
        inbound_tweet_id=comment_id,
        inbound_author_handle=author,
        inbound_author_id=metadata["author_profile_url"],
        inbound_text=body,
        our_tweet_id=post_id,
        our_content_id=content_id,
        our_post_text="Original post",
        draft_text="",
        platform="linkedin",
        inbound_url=post_url,
        our_platform_id=post_id,
        platform_metadata=json.dumps(metadata, sort_keys=True),
        intent="other",
        priority="normal",
        status="pending",
    )


def test_seed_linkedin_comment_ideas_creates_grouped_content_idea(db):
    content_id = _content(db)
    _linkedin_comment(db, comment_id="c-1", author="Alice", like_count=4, content_id=content_id)
    _linkedin_comment(db, comment_id="c-2", author="Bob", like_count=6, content_id=content_id)

    results = seed_linkedin_comment_ideas(db, days=7, min_reactions=2, now=NOW)

    assert [result.status for result in results] == ["created"]
    assert results[0].comment_group_id == f"content:{content_id}"
    assert results[0].comment_ids == ["c-1", "c-2"]
    assert results[0].reaction_count == 10
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    assert ideas[0]["source"] == SOURCE_NAME
    metadata = json.loads(ideas[0]["source_metadata"])
    assert metadata["comment_group_id"] == f"content:{content_id}"
    assert metadata["comment_ids"] == ["c-1", "c-2"]
    assert metadata["authors"] == ["Alice", "Bob"]
    assert metadata["post_url"] == "https://www.linkedin.com/feed/update/urn:li:activity:123"
    assert "high-signal LinkedIn comments" in metadata["reason"]


def test_duplicate_runs_skip_open_idea_for_same_group(db):
    content_id = _content(db)
    _linkedin_comment(db, comment_id="c-1", like_count=5, content_id=content_id)

    first = seed_linkedin_comment_ideas(db, days=7, min_reactions=1, now=NOW)
    second = seed_linkedin_comment_ideas(db, days=7, min_reactions=1, now=NOW)

    assert first[0].status == "created"
    assert second[0].status == "skipped"
    assert second[0].reason == "open duplicate"
    assert second[0].idea_id == first[0].idea_id
    assert len(db.get_content_ideas(status="open")) == 1


def test_dry_run_and_limit_preview_without_writing(db):
    _linkedin_comment(db, comment_id="c-1", like_count=3, post_id="123", content_id=None)
    _linkedin_comment(
        db,
        comment_id="c-2",
        like_count=8,
        post_id="456",
        post_url="https://www.linkedin.com/feed/update/urn:li:activity:456",
        content_id=None,
    )

    results = seed_linkedin_comment_ideas(
        db,
        days=7,
        min_reactions=1,
        limit=1,
        dry_run=True,
        now=NOW,
    )

    assert len(results) == 1
    assert results[0].status == "proposed"
    assert results[0].comment_group_id == "post:456"
    assert db.get_content_ideas(status="open") == []


def test_ignores_old_and_low_reaction_comments(db):
    _linkedin_comment(db, comment_id="old", like_count=20, created_at="2026-04-01T09:00:00+00:00")
    _linkedin_comment(db, comment_id="quiet", like_count=0)

    results = seed_linkedin_comment_ideas(db, days=7, min_reactions=1, now=NOW)

    assert results == []
    assert db.get_content_ideas(status="open") == []


def test_json_and_text_output_include_counts_and_skip_reasons(db):
    content_id = _content(db)
    _linkedin_comment(db, comment_id="c-1", like_count=5, content_id=content_id)
    seed_linkedin_comment_ideas(db, days=7, min_reactions=1, now=NOW)
    results = seed_linkedin_comment_ideas(db, days=7, min_reactions=1, now=NOW)

    text = format_linkedin_comment_seed_text(results)
    payload = json.loads(format_linkedin_comment_seed_json(results))

    assert "created=0 proposed=0 skipped=1" in text
    assert "skip_reasons open duplicate=1" in text
    assert payload["summary"] == {
        "created": 0,
        "proposed": 0,
        "skipped": 1,
        "skip_reasons": {"open duplicate": 1},
    }


def test_cli_previews_json_without_writing(db, capsys):
    _linkedin_comment(db, comment_id="c-1", like_count=5)

    with patch.object(
        seed_linkedin_comment_ideas_script,
        "script_context",
        return_value=_script_context(db),
    ), patch.object(seed_linkedin_comment_ideas_script, "datetime") as datetime_mock:
        datetime_mock.now.return_value = NOW
        datetime_mock.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        exit_code = seed_linkedin_comment_ideas_script.main(
            ["--days", "7", "--min-reactions", "1", "--dry-run", "--format", "json"]
        )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["proposed"] == 1
    assert payload["results"][0]["status"] == "proposed"
    assert db.get_content_ideas(status="open") == []
