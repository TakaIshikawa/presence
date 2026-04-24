"""Tests for publication preview alt-text guard output."""

import json

from output.preview import (
    build_publication_preview,
    format_preview,
    format_visual_post_artifact,
    preview_to_json,
    visual_post_artifact_filename,
    visual_post_artifact_to_json,
    write_visual_post_artifact,
)


def _insert_recent_accepted_posts(db):
    for text in (
        "I traced the queue worker timeout in worker.py and kept the retry path explicit.",
        "I kept the pipeline guard small with a fixture and the exact error from the log.",
        "Debugging the CLI is easier when the config failure names the file and branch.",
    ):
        recent_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content=text,
            eval_score=8.0,
            eval_feedback="Good",
        )
        db.mark_published(recent_id, f"https://x.example/{recent_id}", str(recent_id))


def test_preview_surfaces_failed_alt_text_guard(db):
    content_id = db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        content="Visual launch post",
        eval_score=8.0,
        eval_feedback="Good",
        image_path="/tmp/presence-images/visual.png",
        image_prompt="Launch metrics dashboard",
    )

    preview = build_publication_preview(db, content_id=content_id)

    assert preview["alt_text"]["status"] == "failed"
    assert preview["alt_text"]["required"] is True
    assert preview["alt_text"]["issues"][0]["code"] == "missing_alt_text"
    assert preview["platforms"]["x"]["alt_text"]["status"] == "failed"

    payload = json.loads(preview_to_json(preview))
    assert payload["alt_text"]["issues"][0]["code"] == "missing_alt_text"

    text = format_preview(preview)
    assert "Alt text guard: failed" in text
    assert "- missing_alt_text: Visual posts require alt text before publishing." in text


def test_visual_post_artifact_helpers_write_json_and_markdown(db, tmp_path):
    content_id = db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        content="Reviewable visual post",
        eval_score=8.0,
        eval_feedback="Good",
        image_path="/tmp/presence-images/visual.png",
        image_prompt="ANNOTATED | Launch | Reviewable visual post",
        image_alt_text="Annotated graphic titled Launch with reviewable visual post.",
    )
    preview = build_publication_preview(db, content_id=content_id)
    artifact = {
        "artifact_type": "visual_post_review",
        "generated_at": "2026-04-18T12:00:00+00:00",
        "run": {
            "outcome": "dry_run",
            "planned_topic_id": None,
        },
        "content": {
            "id": content_id,
            "content_type": preview["content"]["content_type"],
            "text": "Reviewable visual post",
            "image_path": "/tmp/presence-images/visual.png",
            "image_prompt": "ANNOTATED | Launch | Reviewable visual post",
            "image_alt_text": "Annotated graphic titled Launch with reviewable visual post.",
        },
        "image": {
            "path": "/tmp/presence-images/visual.png",
            "provider": "pillow",
            "style": "annotated",
            "prompt_used": "annotated: Launch",
            "alt_text": "Annotated graphic titled Launch with reviewable visual post.",
            "spec": "ANNOTATED | Launch | Reviewable visual post",
        },
        "preview": preview,
    }

    assert visual_post_artifact_filename(content_id) == f"visual-post-{content_id}.json"

    json_path = write_visual_post_artifact(
        artifact,
        tmp_path / visual_post_artifact_filename(content_id, artifact_format="json"),
        artifact_format="json",
    )
    markdown_path = write_visual_post_artifact(
        artifact,
        tmp_path / visual_post_artifact_filename(content_id, artifact_format="markdown"),
        artifact_format="markdown",
    )

    payload = json.loads(json_path.read_text())
    assert payload["artifact_type"] == "visual_post_review"
    assert payload["content"]["id"] == content_id
    assert payload["run"]["outcome"] == "dry_run"
    assert payload["preview"]["content"]["id"] == content_id

    markdown = markdown_path.read_text()
    assert markdown.startswith("# Visual Post Review")
    assert "## Final Text" in markdown
    assert "## Publication Preview" in markdown
    assert "Reviewable visual post" in markdown
    assert visual_post_artifact_to_json(artifact).startswith("{")
    assert format_visual_post_artifact(artifact).startswith("# Visual Post Review")


def test_preview_surfaces_passed_alt_text_guard(db):
    content_id = db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        content="Visual launch post",
        eval_score=8.0,
        eval_feedback="Good",
        image_path="/tmp/presence-images/visual.png",
        image_prompt="Launch metrics dashboard",
        image_alt_text="Launch metrics dashboard with trend annotations and labels.",
    )

    preview = build_publication_preview(db, content_id=content_id)

    assert preview["alt_text"]["status"] == "passed"
    assert preview["alt_text"]["issues"] == []
    assert "Alt text guard: passed" in format_preview(preview)


def test_preview_includes_compact_evidence_bundle(db):
    db.insert_commit(
        "presence",
        "sha-evidence",
        "feat: add preview evidence",
        "2026-04-22T12:00:00+00:00",
        "taka",
    )
    db.insert_claude_message(
        "session-evidence",
        "msg-evidence",
        "/repo",
        "2026-04-22T11:58:00+00:00",
        "Add evidence to preview output",
    )
    db.upsert_github_activity(
        repo_name="presence",
        activity_type="pull_request",
        number=42,
        title="Preview evidence bundle",
        state="merged",
        author="taka",
        url="https://github.com/taka/presence/pull/42",
        updated_at="2026-04-22T12:30:00+00:00",
        labels=["preview", "publishing"],
    )
    campaign_id = db.create_campaign(
        "Launch Campaign",
        goal="Make publish review evidence visible",
        start_date="2026-04-20",
        end_date="2026-04-30",
        daily_limit=2,
        weekly_limit=8,
        status="active",
    )
    planned_topic_id = db.insert_planned_topic(
        topic="publishing workflow",
        angle="review evidence at decision time",
        target_date="2026-04-24",
        source_material='{"source_activity_ids": ["presence#42:pull_request"]}',
        campaign_id=campaign_id,
    )
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha-evidence", "sha-missing"],
        source_messages=["msg-evidence"],
        source_activity_ids=["presence#42:pull_request"],
        content="Evidence-backed post",
        eval_score=8.0,
        eval_feedback="Good",
        claim_check_summary={
            "supported_count": 2,
            "unsupported_count": 1,
            "annotation_text": "launch metric needs citation",
        },
        persona_guard_summary={
            "checked": True,
            "passed": False,
            "status": "warning",
            "score": 0.67,
            "reasons": ["too generic"],
            "metrics": {"specificity": 0.4},
        },
    )
    db.mark_planned_topic_generated(planned_topic_id, content_id)
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight, license, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "article-evidence",
            "https://source.example/evidence",
            "Source Author",
            "Evidence source context",
            "Reviewers need provenance near publish controls.",
            "open",
            1,
        ),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.94)])

    preview = build_publication_preview(db, content_id=content_id)
    evidence = preview["evidence"]

    assert evidence["counts"] == {
        "source_commits": 2,
        "source_messages": 1,
        "github_activities": 1,
        "knowledge_links": 1,
        "has_planned_topic": True,
    }
    assert evidence["source_commits"][0]["commit_sha"] == "sha-evidence"
    assert evidence["source_commits"][0]["matched"] is True
    assert evidence["source_commits"][1]["commit_sha"] == "sha-missing"
    assert evidence["source_commits"][1]["matched"] is False
    assert evidence["source_messages"][0]["message_uuid"] == "msg-evidence"
    assert "prompt_text" not in evidence["source_messages"][0]
    assert evidence["github_activities"][0]["activity_id"] == "presence#42:pull_request"
    assert evidence["github_activities"][0]["labels"] == ["preview", "publishing"]
    assert evidence["knowledge_links"][0]["knowledge_id"] == knowledge_id
    assert evidence["knowledge_links"][0]["license"] == "open"
    assert evidence["claim_check"]["unsupported_count"] == 1
    assert evidence["persona_guard"]["reasons"] == ["too generic"]
    assert evidence["planned_topic"]["topic"] == "publishing workflow"
    assert evidence["planned_topic"]["campaign"]["name"] == "Launch Campaign"

    payload = json.loads(preview_to_json(preview))
    assert payload["evidence"]["planned_topic"]["campaign"]["daily_limit"] == 2
    assert payload["evidence"]["knowledge_links"][0]["knowledge_id"] == knowledge_id

    text = format_preview(preview)
    assert "Evidence: 2 commits, 1 session, 1 GitHub activity, 1 knowledge link" in text
    assert "- Commits: sha-evidence, sha-missing" in text
    assert "- Claude messages: msg-evidence" in text
    assert "- GitHub activities: presence#42:pull_request" in text
    assert f"- Knowledge: {knowledge_id}" in text
    assert (
        "- Planned topic: publishing workflow "
        "(review evidence at decision time; target 2026-04-24; campaign Launch Campaign)"
    ) in text


def test_preview_supports_legacy_content_without_evidence(db):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Legacy post",
        eval_score=8.0,
        eval_feedback="Good",
    )

    preview = build_publication_preview(db, content_id=content_id)

    assert preview["evidence"]["counts"] == {
        "source_commits": 0,
        "source_messages": 0,
        "github_activities": 0,
        "knowledge_links": 0,
        "has_planned_topic": False,
    }
    assert preview["evidence"]["planned_topic"] is None
    assert "Evidence:" not in format_preview(preview)


def test_preview_surfaces_restricted_knowledge_license_guard(db):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Source-backed post",
        eval_score=8.0,
        eval_feedback="Good",
    )
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "restricted-article",
            "https://source.example/restricted",
            "Source Author",
            "Restricted source context",
            "restricted",
            1,
        ),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    preview = build_publication_preview(db, content_id=content_id)

    assert preview["license_guard"]["status"] == "blocked"
    assert preview["license_guard"]["blocked"] is True
    assert preview["license_guard"]["restricted_sources"][0] == {
        "knowledge_id": knowledge_id,
        "source_url": "https://source.example/restricted",
        "license": "restricted",
    }
    assert preview["platforms"]["x"]["license_guard"]["status"] == "blocked"

    payload = json.loads(preview_to_json(preview))
    assert payload["license_guard"]["restricted_sources"][0]["knowledge_id"] == knowledge_id

    text = format_preview(preview)
    assert "License guard: blocked (1 restricted sources)" in text
    assert f"- knowledge {knowledge_id}: restricted https://source.example/restricted" in text


def test_preview_surfaces_attribution_required_guard(db):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Source-backed post without citation",
        eval_score=8.0,
        eval_feedback="Good",
    )
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "attribution-article",
            "https://source.example/attribution",
            "Source Author",
            "Attribution-required source context",
            "attribution_required",
            1,
        ),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    preview = build_publication_preview(db, content_id=content_id)

    assert preview["attribution_guard"]["status"] == "blocked"
    assert preview["attribution_guard"]["blocked"] is True
    assert preview["attribution_guard"]["missing_sources"][0] == {
        "knowledge_id": knowledge_id,
        "source_url": "https://source.example/attribution",
        "author": "Source Author",
        "license": "attribution_required",
    }
    assert preview["platforms"]["x"]["attribution_guard"]["status"] == "blocked"

    payload = json.loads(preview_to_json(preview))
    assert payload["attribution_guard"]["missing_sources"][0]["knowledge_id"] == knowledge_id

    text = format_preview(preview)
    assert "Attribution guard: blocked (1 missing citations, 1 attribution-required sources)" in text
    assert (
        f"- knowledge {knowledge_id}: attribution_required "
        "Source Author https://source.example/attribution"
    ) in text

def test_preview_includes_persona_drift_json_and_compact_warning(db):
    _insert_recent_accepted_posts(db)
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=(
            "Thrilled to announce a revolutionary framework to unlock scalable "
            "innovation and transform the future of high-performing teams."
        ),
        eval_score=8.0,
        eval_feedback="Good",
    )

    preview = build_publication_preview(db, content_id=content_id)

    assert preview["persona_drift"]["level"] == "high"
    assert preview["persona_drift"]["score"] >= 0.6
    assert "hype-heavy tone" in preview["persona_drift"]["reasons"]
    assert preview["platforms"]["x"]["persona_drift"]["level"] == "high"

    payload = json.loads(preview_to_json(preview))
    assert set(payload["persona_drift"]) >= {"score", "level", "reasons"}

    text = format_preview(preview)
    assert "Persona drift: high" in text
    assert "hype-heavy tone" in text


def test_preview_includes_saved_variants_grouped_by_platform(db):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Original copy for X",
        eval_score=8.0,
        eval_feedback="Good",
    )
    variant_id = db.upsert_content_variant(
        content_id,
        "bluesky",
        "post",
        "Saved Bluesky copy",
        {"adapter": "BlueskyPlatformAdapter"},
    )

    preview = build_publication_preview(db, content_id=content_id)

    assert preview["variants"]["bluesky"][0]["id"] == variant_id
    assert preview["platforms"]["bluesky"]["variants"][0]["content"] == "Saved Bluesky copy"
    assert preview["platforms"]["bluesky"]["posts"][0]["text"] == "Saved Bluesky copy"
    assert preview["platforms"]["bluesky"]["posts"][0]["source"] == "variant"

    text = format_preview(preview)
    assert "Saved variants:" in text
    assert f"- #{variant_id} post" in text
    assert "Saved Bluesky copy" in text


def test_preview_suppresses_low_persona_drift_warning(db):
    _insert_recent_accepted_posts(db)
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=(
            "I think the retry path in worker.py is clearer now because the test "
            "names the timeout and the log keeps the branch visible."
        ),
        eval_score=8.0,
        eval_feedback="Good",
    )

    preview = build_publication_preview(db, content_id=content_id)

    assert preview["persona_drift"]["level"] == "low"
    assert "Persona drift:" not in format_preview(preview)
