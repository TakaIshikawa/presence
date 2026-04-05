"""Tests for historical theme selection."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from synthesis.theme_selector import ThemeSelector, HistoricalContext


class TestShouldInject:
    def test_returns_true_on_frequency_match(self, db):
        """Every Nth pipeline run should trigger injection."""
        # Insert 3 pipeline runs
        for i in range(3):
            db.insert_pipeline_run(
                batch_id=f"batch-{i}",
                content_type="x_post",
                candidates_generated=3,
                best_candidate_index=0,
                best_score_before_refine=7.0,
                final_score=7.0,
            )

        selector = ThemeSelector(db)
        # 3 runs, frequency 3 → 3 % 3 == 0 → True
        assert selector.should_inject("x_post", frequency=3) is True

    def test_returns_false_between_injections(self, db):
        """Non-Nth runs should not trigger injection."""
        # Insert 4 pipeline runs
        for i in range(4):
            db.insert_pipeline_run(
                batch_id=f"batch-{i}",
                content_type="x_post",
                candidates_generated=3,
                best_candidate_index=0,
                best_score_before_refine=7.0,
                final_score=7.0,
            )

        selector = ThemeSelector(db)
        # 4 runs, frequency 3 → 4 % 3 == 1 → False
        assert selector.should_inject("x_post", frequency=3) is False

    def test_returns_false_when_no_runs(self, db):
        """No pipeline runs should return False (not inject on first run)."""
        selector = ThemeSelector(db)
        assert selector.should_inject("x_post", frequency=3) is False

    def test_content_type_isolation(self, db):
        """Pipeline run count is per content type."""
        # Insert 3 x_post runs and 1 x_thread run
        for i in range(3):
            db.insert_pipeline_run(
                batch_id=f"post-{i}",
                content_type="x_post",
                candidates_generated=3,
                best_candidate_index=0,
                best_score_before_refine=7.0,
                final_score=7.0,
            )
        db.insert_pipeline_run(
            batch_id="thread-0",
            content_type="x_thread",
            candidates_generated=3,
            best_candidate_index=0,
            best_score_before_refine=7.0,
            final_score=7.0,
        )

        selector = ThemeSelector(db)
        # x_post has 3 runs → should inject
        assert selector.should_inject("x_post", frequency=3) is True
        # x_thread has 1 run → should not
        assert selector.should_inject("x_thread", frequency=3) is False


class TestSelectHistoricalContext:
    def _insert_commits(self, db, repo, messages, days_ago):
        """Helper to insert commits at a specific age."""
        for i, msg in enumerate(messages):
            ts = (datetime.now(timezone.utc) - timedelta(days=days_ago + i)).isoformat()
            db.insert_commit(
                repo_name=repo,
                commit_sha=f"sha-{repo}-{days_ago}-{i}",
                commit_message=msg,
                timestamp=ts,
                author="test",
            )

    def test_finds_same_repo_commits(self, db):
        """Should find older commits in same repo as current work."""
        # Historical commits from 60 days ago
        self._insert_commits(db, "my-project", [
            "feat: add user auth",
            "fix: session timeout handling",
        ], days_ago=60)

        # Current commits
        current = [
            {"sha": "current-1", "repo_name": "my-project", "message": "feat: add OAuth2"},
        ]

        selector = ThemeSelector(db)
        ctx = selector.select(
            current, "x_post", lookback_days=180, min_age_days=30, max_commits=5
        )

        assert ctx is not None
        assert ctx.strategy == "same_repo"
        assert len(ctx.commits) == 2
        assert "my-project" in ctx.theme_description

    def test_respects_min_age(self, db):
        """Commits younger than min_age_days should not be included."""
        # Recent commits (5 days ago — below min_age_days=30)
        self._insert_commits(db, "my-project", [
            "feat: very recent change",
        ], days_ago=5)

        current = [
            {"sha": "current-1", "repo_name": "my-project", "message": "fix: bug"},
        ]

        selector = ThemeSelector(db)
        ctx = selector.select(
            current, "x_post", lookback_days=180, min_age_days=30, max_commits=5
        )

        # No historical commits old enough → falls through to anniversary
        # which also finds nothing → returns None
        assert ctx is None

    def test_respects_max_commits(self, db):
        """Should limit the number of returned historical commits."""
        # Insert many historical commits
        self._insert_commits(db, "my-project", [
            f"commit {i}" for i in range(10)
        ], days_ago=60)

        current = [
            {"sha": "current-1", "repo_name": "my-project", "message": "fix: bug"},
        ]

        selector = ThemeSelector(db)
        ctx = selector.select(
            current, "x_post", lookback_days=180, min_age_days=30, max_commits=3
        )

        assert ctx is not None
        assert len(ctx.commits) <= 3

    def test_returns_none_when_no_history(self, db):
        """Brand new repos with no historical commits return None."""
        current = [
            {"sha": "current-1", "repo_name": "brand-new-repo", "message": "initial commit"},
        ]

        selector = ThemeSelector(db)
        ctx = selector.select(
            current, "x_post", lookback_days=180, min_age_days=30, max_commits=5
        )

        assert ctx is None

    def test_commit_dict_format(self, db):
        """Returned commits should have sha, repo_name, message keys."""
        self._insert_commits(db, "my-project", ["feat: old feature"], days_ago=60)

        current = [
            {"sha": "current-1", "repo_name": "my-project", "message": "fix: bug"},
        ]

        selector = ThemeSelector(db)
        ctx = selector.select(
            current, "x_post", lookback_days=180, min_age_days=30, max_commits=5
        )

        assert ctx is not None
        commit = ctx.commits[0]
        assert "sha" in commit
        assert "repo_name" in commit
        assert "message" in commit


class TestFindAnniversaryCommits:
    def test_finds_6_month_old_commits(self, db):
        """Should find commits from approximately 6 months ago."""
        # Insert commits from ~180 days ago
        ts = (datetime.now(timezone.utc) - timedelta(days=180)).isoformat()
        db.insert_commit(
            repo_name="old-project",
            commit_sha="anniversary-sha",
            commit_message="feat: the original version",
            timestamp=ts,
            author="test",
        )

        selector = ThemeSelector(db)
        # No current commits matching any repo → same-repo fails → tries anniversary
        ctx = selector.select(
            [{"sha": "x", "repo_name": "different-repo", "message": "m"}],
            "x_post", lookback_days=180, min_age_days=30, max_commits=5,
        )

        assert ctx is not None
        assert ctx.strategy == "anniversary"

    def test_no_anniversary_commits_returns_none(self, db):
        """When no commits exist at target ages, should return None."""
        selector = ThemeSelector(db)
        ctx = selector.select(
            [{"sha": "x", "repo_name": "no-match", "message": "m"}],
            "x_post", lookback_days=180, min_age_days=30, max_commits=5,
        )

        assert ctx is None


class TestDBMethods:
    """Test the DB methods used by ThemeSelector."""

    def test_get_commits_by_repo(self, db):
        """get_commits_by_repo returns commits within age range."""
        # 60 days ago — within default range
        ts_old = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
        db.insert_commit("my-repo", "sha-old", "old commit", ts_old, "test")

        # 5 days ago — too recent (below min_age_days=30)
        ts_new = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
        db.insert_commit("my-repo", "sha-new", "new commit", ts_new, "test")

        result = db.get_commits_by_repo("my-repo", limit=10, min_age_days=30)
        assert len(result) == 1
        assert result[0]["commit_sha"] == "sha-old"

    def test_count_pipeline_runs(self, db):
        """count_pipeline_runs counts runs within time period."""
        db.insert_pipeline_run(
            batch_id="test-batch",
            content_type="x_post",
            candidates_generated=3,
            best_candidate_index=0,
            best_score_before_refine=7.0,
            final_score=7.0,
        )

        assert db.count_pipeline_runs("x_post", since_days=30) == 1
        assert db.count_pipeline_runs("x_thread", since_days=30) == 0
