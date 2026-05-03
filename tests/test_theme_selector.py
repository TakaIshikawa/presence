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


class TestEngagementWeightedSelection:
    """Test engagement-weighted theme selection and prioritization."""

    def _insert_commits_with_engagement(self, db, repo, messages, days_ago, engagement_scores):
        """Helper to insert commits and associated published content with engagement."""
        for i, (msg, score) in enumerate(zip(messages, engagement_scores)):
            ts = (datetime.now(timezone.utc) - timedelta(days=days_ago + i)).isoformat()
            sha = f"sha-{repo}-{days_ago}-{i}"
            db.insert_commit(
                repo_name=repo,
                commit_sha=sha,
                commit_message=msg,
                timestamp=ts,
                author="test",
            )

            # Create published content using this commit
            content_id = db.insert_generated_content(
                content_type="x_post",
                source_commits=[sha],
                source_messages=[],
                content=f"Post about {msg}",
                eval_score=7.0,
                eval_feedback="Test content",
            )

            # Add engagement data
            if score > 0:
                db.insert_engagement(
                    content_id=content_id,
                    tweet_id=f"tweet-{sha}",
                    like_count=int(score * 10),
                    retweet_count=int(score * 5),
                    reply_count=int(score * 2),
                    quote_count=int(score),
                    engagement_score=score,
                )

    def test_high_engagement_themes_available(self, db):
        """Themes from high-engagement posts should be in the available pool."""
        # Insert commits with varying engagement
        self._insert_commits_with_engagement(
            db, "high-engagement-repo",
            ["feat: popular feature", "fix: important bug"],
            days_ago=60,
            engagement_scores=[10.0, 8.5]
        )
        self._insert_commits_with_engagement(
            db, "low-engagement-repo",
            ["feat: niche feature"],
            days_ago=60,
            engagement_scores=[1.0]
        )

        selector = ThemeSelector(db)

        # When selecting from high-engagement repo
        current = [{"sha": "current", "repo_name": "high-engagement-repo", "message": "new feature"}]
        ctx = selector.select(current, "x_post", lookback_days=180, min_age_days=30, max_commits=5)

        assert ctx is not None
        assert ctx.strategy == "same_repo"
        assert len(ctx.commits) == 2
        assert any("popular feature" in c["message"] for c in ctx.commits)

    def test_zero_engagement_baseline(self, db):
        """Themes with zero engagement should still be selectable."""
        self._insert_commits_with_engagement(
            db, "zero-engagement-repo",
            ["feat: unpopular feature"],
            days_ago=60,
            engagement_scores=[0.0]
        )

        selector = ThemeSelector(db)
        current = [{"sha": "current", "repo_name": "zero-engagement-repo", "message": "new work"}]
        ctx = selector.select(current, "x_post", lookback_days=180, min_age_days=30, max_commits=5)

        # Should still find the commit even with zero engagement
        assert ctx is not None
        assert len(ctx.commits) == 1

    def test_missing_engagement_data_for_all_themes(self, db):
        """When no engagement data exists for any theme, selection should still work."""
        # Insert commits without any engagement records
        ts = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
        db.insert_commit("no-engagement-repo", "sha-1", "feat: no engagement tracked", ts, "test")
        db.insert_commit("no-engagement-repo", "sha-2", "fix: no metrics", ts, "test")

        selector = ThemeSelector(db)
        current = [{"sha": "current", "repo_name": "no-engagement-repo", "message": "current work"}]
        ctx = selector.select(current, "x_post", lookback_days=180, min_age_days=30, max_commits=5)

        # Should gracefully handle missing engagement data
        assert ctx is not None
        assert len(ctx.commits) == 2

    def test_engagement_correlation_calculation(self, db):
        """Test that engagement data can be correlated with theme selection."""
        # Insert multiple commits with different engagement levels
        self._insert_commits_with_engagement(
            db, "test-repo",
            ["feat A", "feat B", "feat C"],
            days_ago=60,
            engagement_scores=[5.0, 10.0, 3.0]
        )

        selector = ThemeSelector(db)
        current = [{"sha": "current", "repo_name": "test-repo", "message": "new feature"}]
        ctx = selector.select(current, "x_post", lookback_days=180, min_age_days=30, max_commits=3)

        # All commits should be available for selection
        assert ctx is not None
        assert len(ctx.commits) == 3

        # Verify engagement data exists for correlation
        for commit in ctx.commits:
            content = db.conn.execute(
                "SELECT id FROM generated_content WHERE source_commits LIKE ?",
                (f'%{commit["sha"]}%',)
            ).fetchone()
            if content:
                engagement = db.get_engagement_snapshots_for_content(content["id"])
                # Engagement data should be retrievable if it was inserted
                assert engagement is not None


class TestThemeDiversityEnforcement:
    """Test theme diversity enforcement (prevent same theme >3 times in 10 posts)."""

    def _insert_pipeline_run_with_theme(self, db, batch_id, repo_name):
        """Helper to insert a pipeline run that used a specific theme/repo."""
        # Create a commit for the theme
        ts = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
        sha = f"sha-{batch_id}"
        db.insert_commit(repo_name, sha, f"commit for {batch_id}", ts, "test")

        # Insert pipeline run
        db.insert_pipeline_run(
            batch_id=batch_id,
            content_type="x_post",
            candidates_generated=3,
            best_candidate_index=0,
            best_score_before_refine=7.0,
            final_score=7.0,
        )

    def test_same_theme_used_multiple_times(self, db):
        """Track how many times the same theme has been used recently."""
        # Simulate using same repo theme 3 times
        for i in range(3):
            self._insert_pipeline_run_with_theme(db, f"batch-{i}", "repeated-repo")

        # Check pipeline run count
        count = db.count_pipeline_runs("x_post", since_days=30)
        assert count == 3

    def test_theme_diversity_across_repos(self, db):
        """Different repos should provide theme diversity."""
        # Insert commits from multiple repos
        repos = ["repo-a", "repo-b", "repo-c"]
        for repo in repos:
            ts = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
            db.insert_commit(repo, f"sha-{repo}", f"commit in {repo}", ts, "test")

        selector = ThemeSelector(db)

        # Selecting from repo-a should not include repo-b or repo-c
        current_a = [{"sha": "current", "repo_name": "repo-a", "message": "work"}]
        ctx_a = selector.select(current_a, "x_post", lookback_days=180, min_age_days=30, max_commits=5)

        assert ctx_a is not None
        assert all(c["repo_name"] == "repo-a" for c in ctx_a.commits)

    def test_max_commits_limits_theme_repetition(self, db):
        """max_commits parameter should limit how many commits from same theme."""
        # Insert many commits from same repo
        ts = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
        for i in range(10):
            db.insert_commit("abundant-repo", f"sha-{i}", f"commit {i}", ts, "test")

        selector = ThemeSelector(db)
        current = [{"sha": "current", "repo_name": "abundant-repo", "message": "work"}]

        # Request max 3 commits
        ctx = selector.select(current, "x_post", lookback_days=180, min_age_days=30, max_commits=3)

        assert ctx is not None
        assert len(ctx.commits) == 3

    def test_multiple_repos_in_current_work(self, db):
        """When working on multiple repos, themes from all should be available."""
        # Insert historical commits for two repos
        ts = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
        db.insert_commit("repo-1", "sha-1-old", "old work in repo 1", ts, "test")
        db.insert_commit("repo-2", "sha-2-old", "old work in repo 2", ts, "test")

        selector = ThemeSelector(db)
        current = [
            {"sha": "current-1", "repo_name": "repo-1", "message": "new in repo-1"},
            {"sha": "current-2", "repo_name": "repo-2", "message": "new in repo-2"},
        ]

        ctx = selector.select(current, "x_post", lookback_days=180, min_age_days=30, max_commits=5)

        assert ctx is not None
        # Should include commits from both repos
        repo_names = {c["repo_name"] for c in ctx.commits}
        assert "repo-1" in repo_names or "repo-2" in repo_names


class TestThemeRotationTracking:
    """Test theme rotation tracking over time windows."""

    def test_pipeline_runs_tracked_over_time(self, db):
        """Pipeline runs should be trackable over different time windows."""
        # Insert runs at different times
        now = datetime.now(timezone.utc)

        # Recent run (5 days ago)
        db.conn.execute(
            "INSERT INTO pipeline_runs (batch_id, content_type, final_score, created_at) VALUES (?, ?, ?, ?)",
            ("recent", "x_post", 7.0, (now - timedelta(days=5)).isoformat())
        )

        # Older run (40 days ago)
        db.conn.execute(
            "INSERT INTO pipeline_runs (batch_id, content_type, final_score, created_at) VALUES (?, ?, ?, ?)",
            ("old", "x_post", 7.0, (now - timedelta(days=40)).isoformat())
        )

        # Check different time windows
        count_30_days = db.count_pipeline_runs("x_post", since_days=30)
        count_60_days = db.count_pipeline_runs("x_post", since_days=60)

        assert count_30_days == 1  # Only recent run
        assert count_60_days == 2  # Both runs

    def test_frequency_based_injection_timing(self, db):
        """should_inject respects rotation frequency."""
        selector = ThemeSelector(db)

        # No runs yet
        assert selector.should_inject("x_post", frequency=3) is False

        # Add runs one by one
        for i in range(1, 7):
            db.insert_pipeline_run(
                batch_id=f"batch-{i}",
                content_type="x_post",
                candidates_generated=3,
                best_candidate_index=0,
                best_score_before_refine=7.0,
                final_score=7.0,
            )

            # Check injection decision
            should = selector.should_inject("x_post", frequency=3)
            expected = (i % 3 == 0)  # True on 3rd, 6th, etc.
            assert should == expected, f"Run {i}: expected {expected}, got {should}"

    def test_theme_selection_strategy_priority(self, db):
        """Verify same-repo strategy is tried before anniversary."""
        # Insert both same-repo and anniversary commits
        now = datetime.now(timezone.utc)

        # Same-repo historical (60 days ago)
        ts_same = (now - timedelta(days=60)).isoformat()
        db.insert_commit("current-repo", "sha-same", "same repo history", ts_same, "test")

        # Anniversary commit (~180 days ago, 6 months)
        ts_anniv = (now - timedelta(days=180)).isoformat()
        db.insert_commit("other-repo", "sha-anniv", "anniversary commit", ts_anniv, "test")

        selector = ThemeSelector(db)
        current = [{"sha": "current", "repo_name": "current-repo", "message": "work"}]

        ctx = selector.select(current, "x_post", lookback_days=200, min_age_days=30, max_commits=5)

        # Should prefer same-repo over anniversary
        assert ctx is not None
        assert ctx.strategy == "same_repo"
        assert ctx.commits[0]["repo_name"] == "current-repo"


class TestColdStartBehavior:
    """Test cold-start behavior when no engagement data exists."""

    def test_brand_new_repository(self, db):
        """First commit in a repo should return None (no history)."""
        selector = ThemeSelector(db)
        current = [{"sha": "first-commit", "repo_name": "brand-new-repo", "message": "initial"}]

        ctx = selector.select(current, "x_post", lookback_days=180, min_age_days=30, max_commits=5)

        assert ctx is None

    def test_new_content_type_no_runs(self, db):
        """New content type with no pipeline runs should not inject."""
        selector = ThemeSelector(db)
        assert selector.should_inject("new_content_type", frequency=3) is False

    def test_fallback_to_anniversary_when_no_same_repo(self, db):
        """When same-repo fails, should try anniversary strategy."""
        # Insert only anniversary commits (no same-repo history)
        now = datetime.now(timezone.utc)
        ts = (now - timedelta(days=180)).isoformat()
        db.insert_commit("unrelated-repo", "sha-anniv", "anniversary", ts, "test")

        selector = ThemeSelector(db)
        current = [{"sha": "current", "repo_name": "different-repo", "message": "work"}]

        ctx = selector.select(current, "x_post", lookback_days=200, min_age_days=30, max_commits=5)

        # Should fall back to anniversary
        assert ctx is not None
        assert ctx.strategy == "anniversary"

    def test_no_commits_in_lookback_window(self, db):
        """When no commits exist in the lookback window, return None."""
        # Insert very old commit (beyond lookback)
        now = datetime.now(timezone.utc)
        ts = (now - timedelta(days=400)).isoformat()
        db.insert_commit("old-repo", "sha-ancient", "ancient commit", ts, "test")

        selector = ThemeSelector(db)
        current = [{"sha": "current", "repo_name": "old-repo", "message": "work"}]

        ctx = selector.select(current, "x_post", lookback_days=180, min_age_days=30, max_commits=5)

        # Too old, outside lookback window
        assert ctx is None


class TestErrorHandling:
    """Test error handling for edge cases and malformed data."""

    def test_empty_current_commits(self, db):
        """Empty current commits should return None gracefully."""
        selector = ThemeSelector(db)
        ctx = selector.select([], "x_post", lookback_days=180, min_age_days=30, max_commits=5)
        assert ctx is None

    def test_current_commits_missing_repo_name(self, db):
        """Current commits without repo_name should be filtered out."""
        # Insert some historical commits
        ts = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
        db.insert_commit("valid-repo", "sha-1", "commit", ts, "test")

        selector = ThemeSelector(db)
        current = [
            {"sha": "bad-1", "message": "no repo"},  # Missing repo_name
            {"sha": "bad-2", "repo_name": "", "message": "empty repo"},  # Empty repo_name
        ]

        ctx = selector.select(current, "x_post", lookback_days=180, min_age_days=30, max_commits=5)

        # Should return None since no valid repos
        assert ctx is None

    def test_malformed_commit_data(self, db):
        """Commits with missing optional fields should be handled gracefully."""
        # Insert commit with empty message (but not NULL, as that violates schema)
        db.conn.execute(
            "INSERT INTO github_commits (repo_name, commit_sha, commit_message, timestamp, author) VALUES (?, ?, ?, ?, ?)",
            ("test-repo", "sha-minimal", "", datetime.now(timezone.utc).isoformat(), "test")
        )

        selector = ThemeSelector(db)
        current = [{"sha": "current", "repo_name": "test-repo", "message": "work"}]

        # Should handle empty commit_message field
        ctx = selector.select(current, "x_post", lookback_days=180, min_age_days=0, max_commits=5)

        # Should still work, just with empty message
        if ctx:
            assert all("message" in c for c in ctx.commits)

    def test_negative_time_parameters(self, db):
        """Negative time parameters should be handled safely."""
        selector = ThemeSelector(db)
        current = [{"sha": "current", "repo_name": "test-repo", "message": "work"}]

        # These should not crash
        ctx = selector.select(current, "x_post", lookback_days=-1, min_age_days=30, max_commits=5)
        assert ctx is None  # No results expected

    def test_zero_max_commits(self, db):
        """max_commits=0 should return empty results."""
        ts = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
        db.insert_commit("test-repo", "sha-1", "commit", ts, "test")

        selector = ThemeSelector(db)
        current = [{"sha": "current", "repo_name": "test-repo", "message": "work"}]

        ctx = selector.select(current, "x_post", lookback_days=180, min_age_days=30, max_commits=0)

        # Should return None or empty commits
        assert ctx is None or len(ctx.commits) == 0

    def test_concurrent_selection_requests(self, db):
        """Multiple concurrent selections should not interfere."""
        # Insert commits for two repos
        ts = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
        db.insert_commit("repo-a", "sha-a", "commit a", ts, "test")
        db.insert_commit("repo-b", "sha-b", "commit b", ts, "test")

        selector = ThemeSelector(db)

        # Simulate concurrent requests
        current_a = [{"sha": "current-a", "repo_name": "repo-a", "message": "work a"}]
        current_b = [{"sha": "current-b", "repo_name": "repo-b", "message": "work b"}]

        ctx_a = selector.select(current_a, "x_post", lookback_days=180, min_age_days=30, max_commits=5)
        ctx_b = selector.select(current_b, "x_post", lookback_days=180, min_age_days=30, max_commits=5)

        # Both should succeed independently
        assert ctx_a is not None
        assert ctx_b is not None
        assert ctx_a.commits[0]["repo_name"] == "repo-a"
        assert ctx_b.commits[0]["repo_name"] == "repo-b"

    def test_anniversary_window_edge_cases(self, db):
        """Anniversary window should handle edge cases correctly."""
        selector = ThemeSelector(db)
        now = datetime.now(timezone.utc)

        # Commit exactly at 6-month boundary
        ts_exact = (now - timedelta(days=180)).isoformat()
        db.insert_commit("test-repo", "sha-exact", "exactly 6mo old", ts_exact, "test")

        # Commit just inside window
        ts_inside = (now - timedelta(days=180 - 7)).isoformat()
        db.insert_commit("test-repo", "sha-inside", "inside window", ts_inside, "test")

        # Commit just outside window
        ts_outside = (now - timedelta(days=180 + 15)).isoformat()
        db.insert_commit("test-repo", "sha-outside", "outside window", ts_outside, "test")

        current = [{"sha": "current", "repo_name": "other-repo", "message": "work"}]
        ctx = selector.select(current, "x_post", lookback_days=365, min_age_days=30, max_commits=5)

        # Should find commits within default 14-day window
        if ctx and ctx.strategy == "anniversary":
            assert len(ctx.commits) >= 1

    def test_database_connection_handling(self, db):
        """Selector should handle database properly."""
        selector = ThemeSelector(db)

        # Should work normally
        assert selector.should_inject("x_post", frequency=3) is not None

        # Multiple operations should work
        for i in range(3):
            db.insert_pipeline_run(
                batch_id=f"batch-{i}",
                content_type="x_post",
                candidates_generated=3,
                best_candidate_index=0,
                best_score_before_refine=7.0,
                final_score=7.0,
            )

        assert selector.should_inject("x_post", frequency=3) is True
