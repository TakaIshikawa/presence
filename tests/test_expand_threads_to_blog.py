import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from output.blog_writer import BlogWriter
from synthesis.thread_expander import ThreadExpansionResult
from expand_threads_to_blog import expand_candidates, select_candidates


def _insert_published_thread(db, content, engagement_score, source_commits=None, source_messages=None):
    content_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=source_commits or [],
        source_messages=source_messages or [],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
    )
    db.mark_published(content_id, f"https://x.com/test/{content_id}", tweet_id=str(content_id))
    db.insert_engagement(
        content_id=content_id,
        tweet_id=str(content_id),
        like_count=30,
        retweet_count=4,
        reply_count=3,
        quote_count=1,
        engagement_score=engagement_score,
    )
    return content_id


def test_select_candidates_returns_published_threads_above_threshold_with_context(db):
    db.insert_commit(
        repo_name="presence",
        commit_sha="abc123",
        commit_message="feat: add workflow expansion",
        timestamp="2026-04-20T10:00:00+00:00",
        author="taka",
    )
    db.insert_claude_message(
        session_id="sess-1",
        message_uuid="msg-1",
        project_path="/tmp/presence",
        timestamp="2026-04-20T10:05:00+00:00",
        prompt_text="Build the expansion workflow.",
    )
    selected_id = _insert_published_thread(
        db,
        "TWEET 1: A thread worth expanding",
        engagement_score=12.0,
        source_commits=["abc123"],
        source_messages=["msg-1"],
    )
    _insert_published_thread(db, "TWEET 1: Too quiet", engagement_score=4.0)
    post_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Not a thread",
        eval_score=8.0,
        eval_feedback="Good",
    )
    db.mark_published(post_id, "https://x.com/test/post", tweet_id="post")
    db.insert_engagement(post_id, "post", 30, 1, 1, 0, 20.0)

    candidates = select_candidates(db, min_engagement=10.0, limit=5)

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.content_id == selected_id
    assert candidate.engagement_score == 12.0
    assert candidate.commit_context[0].commit_message == "feat: add workflow expansion"
    assert candidate.message_context[0].prompt_text == "Build the expansion workflow."


def test_expand_candidates_writes_blog_draft_to_temp_static_site(db, tmp_path):
    source_id = _insert_published_thread(db, "TWEET 1: This resonated", engagement_score=15.0)
    candidate = select_candidates(db, min_engagement=10.0, limit=1)[0]
    expander = MagicMock()
    expander.expand.return_value = ThreadExpansionResult(
        source_id=source_id,
        content="TITLE: Expanded Thread\n\n## What Happened\n\nDraft body.",
        generation_prompt="prompt",
    )
    writer = BlogWriter(str(tmp_path))

    outcomes = expand_candidates(db, [candidate], expander, writer, dry_run=False)

    assert len(outcomes) == 1
    outcome = outcomes[0]
    assert outcome.success is True
    assert outcome.source_content_id == source_id
    assert outcome.generated_content_id is not None
    assert outcome.draft_path == str(tmp_path / "drafts" / "expanded-thread.md")
    draft = (tmp_path / "drafts" / "expanded-thread.md").read_text()
    assert "source_content_id: " + str(source_id) in draft
    assert "generated_content_id: " + str(outcome.generated_content_id) in draft
    row = db.conn.execute(
        "SELECT content_type, repurposed_from, content FROM generated_content WHERE id = ?",
        (outcome.generated_content_id,),
    ).fetchone()
    assert row["content_type"] == "blog_post"
    assert row["repurposed_from"] == source_id
    assert "TITLE: Expanded Thread" in row["content"]


def test_expand_candidates_dry_run_skips_db_and_static_site_writes(db, tmp_path):
    source_id = _insert_published_thread(db, "TWEET 1: Dry run this", engagement_score=15.0)
    candidate = select_candidates(db, min_engagement=10.0, limit=1)[0]
    expander = MagicMock()
    expander.expand.return_value = ThreadExpansionResult(
        source_id=source_id,
        content="TITLE: Dry Run Draft\n\nBody.",
        generation_prompt="prompt",
    )

    outcomes = expand_candidates(
        db,
        [candidate],
        expander,
        BlogWriter(str(tmp_path)),
        dry_run=True,
    )

    assert outcomes[0].success is True
    assert outcomes[0].generated_content_id is None
    assert outcomes[0].draft_path is None
    assert not (tmp_path / "drafts").exists()
    rows = db.conn.execute(
        "SELECT id FROM generated_content WHERE content_type = 'blog_post'"
    ).fetchall()
    assert rows == []
