"""Tests for seeding content ideas from labeled GitHub activity."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.github_label_idea_seeder import (
    SOURCE_NAME,
    format_github_label_idea_results_json,
    format_github_label_idea_results_text,
    seed_github_label_ideas,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "seed_github_label_ideas.py"
spec = importlib.util.spec_from_file_location("seed_github_label_ideas_cli", SCRIPT_PATH)
seed_github_label_ideas_cli = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(seed_github_label_ideas_cli)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _activity(
    db,
    *,
    repo: str = "taka/presence",
    activity_type: str = "issue",
    number: int | str = 42,
    title: str = "Investigate slow dashboard query",
    state: str = "open",
    labels: list[str] | None = None,
    updated_at: str = "2026-04-30T12:00:00+00:00",
    body: str = "The dashboard query regressed after the latest release and needs a practical fix.",
) -> int:
    labels = labels if labels is not None else ["performance", "help wanted"]
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type=activity_type,
        number=number,
        title=title,
        state=state,
        author="taka",
        url=f"https://github.com/{repo}/{_path_for_type(activity_type)}/{number}",
        updated_at=updated_at,
        created_at="2026-04-29T10:00:00+00:00",
        body=body,
        labels=labels,
        metadata={"activity_id": f"{repo}#{number}:{activity_type}"},
    )


def _path_for_type(activity_type: str) -> str:
    if activity_type == "pull_request":
        return "pull"
    if "discussion" in activity_type:
        return "discussions"
    return "issues"


def test_seeds_content_ideas_from_default_high_signal_labels(db):
    issue_id = _activity(db, number=42, labels=["performance"])
    pr_id = _activity(
        db,
        activity_type="pull_request",
        number=43,
        title="Fix incident fallback for webhook delivery",
        labels=["incident", "backend"],
    )
    discussion_id = _activity(
        db,
        activity_type="discussion",
        number=44,
        title="Design review for the onboarding flow",
        labels=["design"],
    )

    results = seed_github_label_ideas(db, days=14, now=NOW)

    assert [result.status for result in results] == ["created", "created", "created"]
    assert {result.source_activity_id for result in results} == {
        "taka/presence#42:issue",
        "taka/presence#43:pull_request",
        "taka/presence#44:discussion",
    }
    ideas = db.get_content_ideas(status="open", limit=10)
    assert len(ideas) == 3
    first = ideas[0]
    assert first["source"] == SOURCE_NAME
    metadata = json.loads(first["source_metadata"])
    assert metadata["github_activity_id"] in {issue_id, pr_id, discussion_id}
    assert metadata["repo_name"] == "taka/presence"
    assert metadata["title"]
    assert metadata["url"].startswith("https://github.com/taka/presence/")
    assert metadata["matched_labels"]
    assert metadata["labels"]


def test_filters_by_labels_activity_type_recency_and_state(db):
    _activity(db, number=1, labels=["bug"], updated_at="2026-04-30T12:00:00+00:00")
    _activity(db, number=2, labels=["question"])
    _activity(db, number=3, activity_type="release", labels=["bug"])
    _activity(db, number=4, labels=["bug"], updated_at="2026-03-01T12:00:00+00:00")
    _activity(db, number=5, labels=["bug"], state="closed")

    results = seed_github_label_ideas(db, labels=["bug"], days=14, dry_run=True, now=NOW)

    assert [result.number for result in results] == ["1"]
    assert results[0].status == "proposed"
    assert db.get_content_ideas(status="open") == []


def test_custom_label_filter_and_limit(db):
    _activity(db, number=1, labels=["question"])
    _activity(db, number=2, labels=["docs"], updated_at="2026-04-29T12:00:00+00:00")
    _activity(db, number=3, labels=["docs"], updated_at="2026-04-30T12:00:00+00:00")

    results = seed_github_label_ideas(db, labels=["docs"], days=14, limit=1, dry_run=True, now=NOW)

    assert [result.number for result in results] == ["3"]
    assert results[0].matched_labels == ["docs"]


def test_deduplicates_open_ideas_for_same_github_activity_metadata(db):
    _activity(db, number=7, labels=["security"])

    first = seed_github_label_ideas(db, labels=["security"], days=14, now=NOW)
    second = seed_github_label_ideas(db, labels=["security"], days=14, now=NOW)

    assert [result.status for result in first] == ["created"]
    assert [result.status for result in second] == ["skipped"]
    assert second[0].idea_id == first[0].idea_id
    assert second[0].reason == "open duplicate"
    assert len(db.get_content_ideas(status=None)) == 1


def test_dry_run_returns_proposed_ideas_without_mutating_database(db):
    _activity(db, labels=["bug"])

    results = seed_github_label_ideas(db, dry_run=True, now=NOW)

    assert [(result.status, result.reason) for result in results] == [("proposed", "dry run")]
    assert db.get_content_ideas(status="open") == []


def test_json_text_and_cli_outputs(db, capsys):
    _activity(db, labels=["bug"])
    results = seed_github_label_ideas(db, labels=["bug"], dry_run=True, now=NOW)

    payload = json.loads(format_github_label_idea_results_json(results))
    assert payload[0]["status"] == "proposed"
    assert payload[0]["source_metadata"]["source"] == SOURCE_NAME
    text = format_github_label_idea_results_text(results)
    assert "created=0 proposed=1 skipped=0" in text
    assert "taka/presence#42 issue" in text

    with patch.object(
        seed_github_label_ideas_cli,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        seed_github_label_ideas_cli,
        "seed_github_label_ideas",
        wraps=lambda db, **kwargs: seed_github_label_ideas(db, now=NOW, **kwargs),
    ):
        assert (
            seed_github_label_ideas_cli.main(
                ["--labels", "bug,security", "--days", "14", "--limit", "5", "--dry-run", "--format", "json"]
            )
            == 0
        )

    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload[0]["status"] == "proposed"
    assert cli_payload[0]["matched_labels"] == ["bug"]


def test_invalid_filters_raise_clear_errors():
    for kwargs in ({"days": 0}, {"limit": 0}, {"labels": [" "]}):
        try:
            seed_github_label_ideas(None, **kwargs)
        except ValueError as exc:
            assert str(exc)
        else:
            raise AssertionError(f"expected ValueError for {kwargs}")
