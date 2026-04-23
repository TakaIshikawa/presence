"""Tests for resonated post to blog seed repurposing."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

from synthesis.post_mortem_repurposer import (
    BLOG_SEED_VARIANT_PLATFORM,
    BLOG_SEED_VARIANT_TYPE,
    PostMortemRepurposer,
    artifact_to_json,
    write_artifact,
)

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))


def _published_post(db, *, content: str = "A tiny fix made the whole pipeline easier to reason about.", score: float = 14.0) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha1"],
        source_messages=["uuid1"],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
    )
    db.mark_published(content_id, "https://x.com/taka/status/1", tweet_id="1")
    db.insert_engagement(
        content_id=content_id,
        tweet_id="1",
        like_count=10,
        retweet_count=1,
        reply_count=0,
        quote_count=0,
        engagement_score=score,
    )
    return content_id


def _add_source_artifacts(db, content_id: int) -> None:
    db.insert_commit(
        "presence",
        "sha1",
        "fix: centralize pipeline state checks",
        "2026-04-22T10:00:00+00:00",
        "taka",
    )
    db.insert_claude_message(
        "session-1",
        "uuid1",
        "/repo/presence",
        "2026-04-22T09:58:00+00:00",
        "Please make the pipeline state easier to inspect.",
    )
    db.upsert_github_activity(
        repo_name="presence",
        activity_type="pull_request",
        number=12,
        title="Centralize pipeline state checks",
        state="merged",
        author="taka",
        url="https://github.com/taka/presence/pull/12",
        updated_at="2026-04-22T11:00:00+00:00",
    )
    db.conn.execute(
        "UPDATE generated_content SET source_activity_ids = ? WHERE id = ?",
        (json.dumps(["presence#12:pull_request"]), content_id),
    )
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "artifact-1",
            "https://example.com/artifact",
            "Ada",
            "Long source",
            "A shared status surface makes operational review easier.",
        ),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.88)])


def test_eligibility_thresholds_select_latest_resonated_posts(db):
    low_id = _published_post(db, content="Low signal", score=9.9)
    threshold_id = _published_post(db, content="Threshold signal", score=10.0)
    high_id = _published_post(db, content="High signal", score=22.0)

    repurposer = PostMortemRepurposer(db)
    candidates = repurposer.find_eligible_posts(min_engagement=10.0, limit=10)

    assert [candidate.content_id for candidate in candidates] == [high_id, threshold_id]
    assert low_id not in [candidate.content_id for candidate in candidates]


def test_artifact_format_includes_title_outline_source_links_and_risks(db, tmp_path):
    content_id = _published_post(db)
    _add_source_artifacts(db, content_id)
    db.save_claim_check_summary(
        content_id,
        supported_count=2,
        unsupported_count=0,
        annotation_text="All concrete claims were supported.",
    )

    repurposer = PostMortemRepurposer(db)
    artifact = repurposer.build_seed(repurposer.find_eligible_posts()[0])
    artifact_path = tmp_path / "seed.json"
    write_artifact(artifact, artifact_path)

    payload = json.loads(artifact_path.read_text())
    assert payload["artifact_type"] == "post_mortem_blog_seed"
    assert payload["source_content_id"] == content_id
    assert payload["title"].startswith("What Resonated:")
    assert len(payload["outline"]) >= 4
    assert "TITLE:" in payload["draft_seed"]
    assert {"type": "published_post", "url": "https://x.com/taka/status/1"} in payload["source_links"]
    assert {"type": "github_activity", "url": "https://github.com/taka/presence/pull/12"} in payload["source_links"]
    assert payload["source_artifacts"]["commits"][0]["sha"] == "sha1"
    assert payload["source_artifacts"]["messages"][0]["message_uuid"] == "uuid1"
    assert payload["risk_notes"] == []


def test_claim_check_summary_is_included_and_flags_unsupported_claims(db):
    content_id = _published_post(db)
    db.save_claim_check_summary(
        content_id,
        supported_count=1,
        unsupported_count=2,
        annotation_text="Unsupported metric needs review.",
    )

    repurposer = PostMortemRepurposer(db)
    artifact = repurposer.build_seed(repurposer.find_eligible_posts()[0])
    payload = json.loads(artifact_to_json(artifact))

    assert payload["claim_check"]["supported_count"] == 1
    assert payload["claim_check"]["unsupported_count"] == 2
    assert "Unsupported metric needs review." in payload["claim_check"]["annotation_text"]
    assert any("unsupported claims" in note for note in payload["risk_notes"])
    assert any("Resolve unsupported claims" in item for item in payload["outline"])


def test_duplicate_prevention_excludes_existing_blog_seed_variants(db):
    content_id = _published_post(db)
    repurposer = PostMortemRepurposer(db)
    candidate = repurposer.find_eligible_posts()[0]
    artifact = repurposer.build_seed(candidate)

    repurposer.record_seed_variant(artifact)

    assert repurposer.find_eligible_posts() == []
    variant = db.get_content_variant(
        content_id,
        BLOG_SEED_VARIANT_PLATFORM,
        BLOG_SEED_VARIANT_TYPE,
    )
    assert variant["metadata"]["artifact_type"] == "post_mortem_blog_seed"


def test_duplicate_prevention_excludes_existing_repurposed_blog_seed(db):
    content_id = _published_post(db)
    db.insert_repurposed_content(
        content_type="blog_seed",
        source_content_id=content_id,
        content="TITLE: Existing seed\n\nBody",
        eval_score=8.0,
        eval_feedback="Existing seed",
    )

    assert PostMortemRepurposer(db).find_eligible_posts() == []


def test_seed_records_carry_source_metadata_and_skip_duplicates(db):
    content_id = _published_post(db)
    repurposer = PostMortemRepurposer(db)
    artifact = repurposer.build_seed(repurposer.find_eligible_posts()[0])

    idea_result = repurposer.record_content_idea(artifact, topic="testing")
    idea = db.get_content_idea(idea_result.record_id)
    idea_metadata = json.loads(idea["source_metadata"])

    assert idea_result.created is True
    assert idea_metadata["source"] == "post_mortem_repurposer"
    assert idea_metadata["source_content_id"] == content_id
    assert idea_metadata["artifact_title"] == artifact.title
    assert idea_metadata["artifact_type"] == artifact.artifact_type
    duplicate_idea = repurposer.record_content_idea(artifact, topic="testing")
    assert duplicate_idea.record_id == idea_result.record_id
    assert not duplicate_idea.created
    assert not repurposer.find_eligible_posts()


def test_planned_topic_seed_carries_artifact_metadata_and_skips_duplicates(db):
    content_id = _published_post(db)
    repurposer = PostMortemRepurposer(db)
    artifact = repurposer.build_seed(repurposer.find_eligible_posts()[0])

    planned_result = repurposer.record_planned_topic(
        artifact,
        target_date="2026-05-01",
        topic="testing",
        angle="turn the post into a lesson",
    )
    planned = db.conn.execute(
        "SELECT topic, angle, target_date, source_material FROM planned_topics WHERE id = ?",
        (planned_result.record_id,),
    ).fetchone()
    planned_metadata = json.loads(planned["source_material"])

    assert planned_result.created is True
    assert planned["topic"] == "testing"
    assert planned["angle"] == "turn the post into a lesson"
    assert planned["target_date"] == "2026-05-01"
    assert planned_metadata["source"] == "post_mortem_repurposer"
    assert planned_metadata["source_content_id"] == content_id
    assert planned_metadata["artifact_title"] == artifact.title
    assert planned_metadata["planned_topic"]["target_date"] == "2026-05-01"
    assert (
        repurposer.record_planned_topic(
            artifact,
            target_date="2026-05-01",
            topic="testing",
            angle="turn the post into a lesson",
        ).record_id
        == planned_result.record_id
    )
    assert not repurposer.find_eligible_posts()


def test_repurpose_resonated_cli_writes_artifact_and_variant(db, tmp_path, capsys):
    content_id = _published_post(db)
    artifact_dir = tmp_path / "artifacts"

    import repurpose_resonated

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with (
        patch("repurpose_resonated.script_context", return_value=Context()),
        patch("repurpose_resonated.update_monitoring"),
    ):
        exit_code = repurpose_resonated.main(
            [
                "--artifact-dir",
                str(artifact_dir),
                "--format",
                "json",
            ]
        )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Blog seed artifact:" in captured.err
    artifacts = list(artifact_dir.glob("*.json"))
    assert len(artifacts) == 1
    payload = json.loads(artifacts[0].read_text())
    assert payload["source_content_id"] == content_id
    assert (
        db.get_content_variant(content_id, BLOG_SEED_VARIANT_PLATFORM, BLOG_SEED_VARIANT_TYPE)
        is not None
    )


def test_repurpose_resonated_cli_creates_content_idea(db, tmp_path, capsys):
    content_id = _published_post(db)
    artifact_dir = tmp_path / "artifacts"

    import repurpose_resonated

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with (
        patch("repurpose_resonated.script_context", return_value=Context()),
        patch("repurpose_resonated.update_monitoring"),
    ):
        exit_code = repurpose_resonated.main(
            [
                "--artifact-dir",
                str(artifact_dir),
                "--content-idea",
            ]
        )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload[0]["content_idea_id"] is not None
    assert payload[0]["content_idea_created"] is True
    idea = db.get_content_idea(payload[0]["content_idea_id"])
    assert idea["source"] == "post_mortem_repurposer"
    metadata = json.loads(idea["source_metadata"])
    assert metadata["source_content_id"] == content_id
    assert metadata["artifact_type"] == "post_mortem_blog_seed"
    assert Path(payload[0]["artifact_path"]).exists()


def test_repurpose_resonated_cli_creates_planned_topic(db, tmp_path, capsys):
    content_id = _published_post(db)
    artifact_dir = tmp_path / "artifacts"

    import repurpose_resonated

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with (
        patch("repurpose_resonated.script_context", return_value=Context()),
        patch("repurpose_resonated.update_monitoring"),
    ):
        exit_code = repurpose_resonated.main(
            [
                "--artifact-dir",
                str(artifact_dir),
                "--planned-topic",
                "--target-date",
                "2026-05-01",
                "--topic",
                "testing",
                "--angle",
                "turn the post into a lesson",
            ]
        )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload[0]["planned_topic_id"] is not None
    assert payload[0]["planned_topic_created"] is True
    planned = db.conn.execute(
        "SELECT topic, angle, target_date, source_material FROM planned_topics WHERE id = ?",
        (payload[0]["planned_topic_id"],),
    ).fetchone()
    assert planned["topic"] == "testing"
    assert planned["angle"] == "turn the post into a lesson"
    metadata = json.loads(planned["source_material"])
    assert metadata["source_content_id"] == content_id
    assert metadata["artifact_type"] == "post_mortem_blog_seed"
    assert metadata["planned_topic"]["target_date"] == "2026-05-01"


def test_repurpose_resonated_cli_dry_run_does_not_write_variant(db, capsys):
    content_id = _published_post(db)

    import repurpose_resonated

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("repurpose_resonated.script_context", return_value=Context()):
        exit_code = repurpose_resonated.main(["--dry-run"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload[0]["source_content_id"] == content_id
    assert payload[0]["artifact"]["claim_check"] is None
    assert db.get_content_variant(content_id, BLOG_SEED_VARIANT_PLATFORM, BLOG_SEED_VARIANT_TYPE) is None
