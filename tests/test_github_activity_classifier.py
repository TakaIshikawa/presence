"""Tests for deterministic GitHub activity classification."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from classify_github_activity import main  # noqa: E402
from ingestion.github_activity_classifier import (  # noqa: E402
    CATEGORY_BUG_FIX,
    CATEGORY_DOCUMENTATION,
    CATEGORY_LOW_SIGNAL,
    CATEGORY_QUESTION,
    CATEGORY_RELEASE,
    CATEGORY_USER_FACING_FEATURE,
    build_github_activity_classification_report,
    classify_github_activity_row,
    format_github_activity_classification_text,
)


NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def _insert_activity(
    db,
    *,
    repo_name: str = "repo",
    activity_type: str = "issue",
    number: int | str = 1,
    title: str = "Issue",
    body: str = "",
    state: str = "open",
    updated_at: str = "2026-04-24T12:00:00+00:00",
    labels: list[str] | None = None,
    metadata: dict | None = None,
    merged_at: str | None = None,
) -> int:
    return db.upsert_github_activity(
        repo_name=repo_name,
        activity_type=activity_type,
        number=number,
        title=title,
        body=body,
        state=state,
        author="taka",
        url=f"https://github.com/taka/{repo_name}/{activity_type}/{number}",
        updated_at=updated_at,
        created_at="2026-04-01T12:00:00+00:00",
        labels=labels or [],
        metadata=metadata or {},
        merged_at=merged_at,
    )


def test_labels_and_terms_classify_user_facing_feature(db):
    _insert_activity(
        db,
        activity_type="pull_request",
        number=12,
        title="Add customer dashboard export",
        state="closed",
        labels=["enhancement"],
        metadata={"merged": True, "changed_files": 6, "additions": 120, "deletions": 20},
        merged_at="2026-04-24T12:00:00+00:00",
    )

    item = build_github_activity_classification_report(db, now=NOW)["items"][0]

    assert item["category"] == CATEGORY_USER_FACING_FEATURE
    assert item["impact_score"] >= 80
    assert item["synthesis_worthy"] is True
    assert "label:enhancement" in item["rationale"]
    assert "merged pull request" in item["rationale"]


def test_bug_label_closed_issue_classifies_bug_fix(db):
    _insert_activity(
        db,
        number=3,
        title="Fix login crash after token refresh",
        state="closed",
        labels=["bug"],
    )

    classification = classify_github_activity_row(db.get_recent_github_activity(days=30, now=NOW)[0])

    assert classification.category == CATEGORY_BUG_FIX
    assert classification.impact_score >= 70
    assert "closed issue" in classification.rationale


def test_documentation_and_release_categories_are_separate(db):
    _insert_activity(
        db,
        activity_type="pull_request",
        number=4,
        title="Update README guide examples",
        labels=["documentation"],
    )
    _insert_activity(
        db,
        activity_type="release",
        number=101,
        title="Release v1.4.0",
        state="published",
        updated_at="2026-04-25T10:00:00+00:00",
        metadata={"tag_name": "v1.4.0", "prerelease": False},
    )

    items = build_github_activity_classification_report(db, now=NOW)["items"]

    assert [item["category"] for item in items] == [CATEGORY_RELEASE, CATEGORY_DOCUMENTATION]
    assert items[0]["impact_score"] > items[1]["impact_score"]


def test_question_uses_discussion_metadata_and_question_text(db):
    _insert_activity(
        db,
        activity_type="discussion",
        number=9,
        title="How should scheduled publishing retries work?",
        state="answered",
        metadata={
            "answer_state": "answered",
            "category": {"name": "Q&A", "slug": "q-a"},
            "comments_count": 4,
        },
    )

    item = build_github_activity_classification_report(db, now=NOW)["items"][0]

    assert item["category"] == CATEGORY_QUESTION
    assert item["impact_score"] >= 70
    assert "discussion category:q&a" in item["rationale"]
    assert "answered discussion" in item["rationale"]


def test_low_signal_activity_is_separated_from_synthesis_worthy_work(db):
    _insert_activity(
        db,
        activity_type="workflow_run",
        number=1001,
        title="CI - test suite (success)",
        state="success",
        metadata={"workflow_name": "CI", "conclusion": "success"},
    )
    _insert_activity(
        db,
        activity_type="issue",
        number=2,
        title="Add stale label",
        labels=["stale"],
        metadata={"issue_event_type": "labeled"},
        updated_at="2026-04-24T13:00:00+00:00",
    )

    items = build_github_activity_classification_report(db, now=NOW)["items"]

    assert [item["category"] for item in items] == [CATEGORY_LOW_SIGNAL, CATEGORY_LOW_SIGNAL]
    assert all(item["synthesis_worthy"] is False for item in items)
    assert items[0]["impact_score"] <= 35
    assert "administrative issue event:labeled" in items[0]["rationale"]
    assert "successful workflow run" in items[1]["rationale"]


def test_report_filters_days_repo_and_min_impact(db):
    _insert_activity(
        db,
        repo_name="alpha",
        activity_type="pull_request",
        number=1,
        title="Add user profile settings",
        labels=["enhancement"],
        metadata={"merged": True},
        merged_at="2026-04-24T12:00:00+00:00",
    )
    _insert_activity(
        db,
        repo_name="beta",
        activity_type="issue",
        number=2,
        title="Fix billing regression",
        state="closed",
        labels=["bug"],
    )
    _insert_activity(
        db,
        repo_name="alpha",
        activity_type="issue",
        number=3,
        title="Old feature",
        labels=["enhancement"],
        updated_at="2026-03-01T12:00:00+00:00",
    )

    report = build_github_activity_classification_report(
        db,
        repo="alpha",
        days=7,
        min_impact=70,
        now=NOW,
    )

    assert [item["activity_id"] for item in report["items"]] == ["alpha#1:pull_request"]
    assert report["counts"]["high_impact"] == 1


def test_invalid_filters_are_rejected(db):
    with pytest.raises(ValueError, match="days"):
        build_github_activity_classification_report(db, days=0, now=NOW)
    with pytest.raises(ValueError, match="repo"):
        build_github_activity_classification_report(db, repo=" ", now=NOW)
    with pytest.raises(ValueError, match="min_impact"):
        build_github_activity_classification_report(db, min_impact=101, now=NOW)


def test_cli_json_output(db, capsys):
    _insert_activity(
        db,
        activity_type="release",
        number=22,
        title="Release v2.0.0",
        state="published",
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("classify_github_activity.script_context", fake_script_context):
        assert main(["--format", "json", "--min-impact", "70"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "github_activity_classification"
    assert payload["items"][0]["category"] == CATEGORY_RELEASE
    assert payload["high_impact"][0]["activity_id"] == "repo#22:release"


def test_text_output_highlights_high_impact_items(db):
    _insert_activity(
        db,
        activity_type="release",
        number=22,
        title="Release v2.0.0",
        state="published",
    )

    output = format_github_activity_classification_text(
        build_github_activity_classification_report(db, now=NOW)
    )

    assert "High-impact synthesis candidates:" in output
    assert "category=release" in output
    assert "Release v2.0.0" in output
