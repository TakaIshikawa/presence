"""Tests for synthesis feedback memory."""

from synthesis.feedback_memory import FeedbackMemory


def _content(db, content: str, content_type: str = "x_post") -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=5.0,
        eval_feedback="",
    )


def test_empty_feedback_returns_empty_constraints(db):
    memory = FeedbackMemory(db=db)

    assert memory.build_prompt_constraints("x_post") == ""


def test_reject_notes_become_avoid_constraints_without_draft_leakage(db):
    content_id = _content(
        db,
        "Today's breakthrough: I added auth retries and fixed the API handler.",
    )
    db.add_content_feedback(
        content_id,
        "reject",
        "Too changelog-like and too grandiose.",
    )

    constraints = FeedbackMemory(db=db).build_prompt_constraints("x_post")

    assert "Too changelog-like and too grandiose." in constraints
    assert "Today's breakthrough" not in constraints
    assert "Do not quote or imitate rejected drafts" in constraints


def test_revise_replacement_becomes_preference_not_full_replacement(db):
    content_id = _content(db, "Generic thought leadership about agents.")
    db.add_content_feedback(
        content_id,
        "revise",
        "Make it more concrete.",
        "I found the bug only after writing the failure case first.",
    )

    constraints = FeedbackMemory(db=db).build_prompt_constraints("x_post")

    assert "Make it more concrete." in constraints
    assert "Prefer concrete first-person builder observations" in constraints
    assert "I found the bug" not in constraints


def test_lookback_days_excludes_old_feedback(db):
    content_id = _content(db, "Old rejected draft")
    feedback_id = db.add_content_feedback(content_id, "reject", "Old note")
    db.conn.execute(
        "UPDATE content_feedback SET created_at = datetime('now', '-90 days') WHERE id = ?",
        (feedback_id,),
    )
    db.conn.commit()

    constraints = FeedbackMemory(db=db, lookback_days=30).build_prompt_constraints("x_post")

    assert constraints == ""


def test_classifies_obvious_patterns_when_notes_are_empty(db):
    content_id = _content(
        db,
        "Today's breakthrough: implemented queue retries and fixed stale jobs.",
    )
    db.add_content_feedback(content_id, "reject")

    constraints = FeedbackMemory(db=db).build_prompt_constraints("x_post")

    assert "Avoid stale announcement hooks" in constraints
    assert "Avoid changelog-style summaries" in constraints
    assert "Today's breakthrough" not in constraints


def test_falls_back_to_cross_type_feedback_when_type_has_no_rows(db):
    content_id = _content(db, "Rejected post", content_type="x_post")
    db.add_content_feedback(content_id, "reject", "Avoid vague hooks.")

    constraints = FeedbackMemory(db=db).build_prompt_constraints("blog_post")

    assert "Avoid vague hooks." in constraints
