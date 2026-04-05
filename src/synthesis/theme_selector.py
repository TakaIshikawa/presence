"""Historical theme selection for content generation."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from storage.db import Database


@dataclass
class HistoricalContext:
    """Historical commits to inject alongside current source material."""
    commits: list[dict]
    theme_description: str
    strategy: str  # "same_repo" | "anniversary"


class ThemeSelector:
    """Selects historical themes to enrich content generation.

    Strategies (tried in priority order):
    1. Same-repo: older commits in repos being worked on now
    2. Anniversary: commits from ~6 or ~12 months ago
    """

    def __init__(self, db: Database):
        self.db = db

    def should_inject(self, content_type: str, frequency: int = 3) -> bool:
        """Determine if this generation should include historical context.

        Returns True every Nth pipeline run for the given content_type.
        """
        count = self.db.count_pipeline_runs(content_type, since_days=30)
        return count > 0 and count % frequency == 0

    def select(
        self,
        current_commits: list[dict],
        content_type: str,
        lookback_days: int = 180,
        min_age_days: int = 30,
        max_commits: int = 5,
    ) -> Optional[HistoricalContext]:
        """Find interesting historical commits related to current work.

        Tries same-repo first (strongest relevance), then anniversary.
        Returns None if no interesting historical commits found.
        """
        # Strategy 1: Same-repo historical commits
        ctx = self._find_same_repo_historical(
            current_commits, lookback_days, min_age_days, max_commits
        )
        if ctx:
            return ctx

        # Strategy 2: Anniversary commits (6 or 12 months ago)
        ctx = self._find_anniversary_commits(max_commits)
        if ctx:
            return ctx

        return None

    def _find_same_repo_historical(
        self,
        current_commits: list[dict],
        lookback_days: int,
        min_age_days: int,
        max_commits: int,
    ) -> Optional[HistoricalContext]:
        """Find older commits in the same repositories being worked on now."""
        repo_names = list({
            c.get("repo_name", "") for c in current_commits if c.get("repo_name")
        })
        if not repo_names:
            return None

        all_historical = []
        for repo in repo_names:
            commits = self.db.get_commits_by_repo(
                repo_name=repo,
                limit=max_commits,
                min_age_days=min_age_days,
                max_age_days=lookback_days,
            )
            all_historical.extend(commits)

        if not all_historical:
            return None

        # Sort by timestamp descending, take top max_commits
        all_historical.sort(key=lambda c: c.get("timestamp", ""), reverse=True)
        selected = all_historical[:max_commits]

        # Build commit dicts matching the format expected by the pipeline
        commit_dicts = [
            {
                "sha": c.get("commit_sha", ""),
                "repo_name": c.get("repo_name", ""),
                "message": c.get("commit_message", ""),
            }
            for c in selected
        ]

        repos_involved = list({c["repo_name"] for c in commit_dicts if c["repo_name"]})
        return HistoricalContext(
            commits=commit_dicts,
            theme_description=f"Same-repo history from {', '.join(repos_involved)}",
            strategy="same_repo",
        )

    def _find_anniversary_commits(
        self,
        max_commits: int,
        target_months: tuple[int, ...] = (6, 12),
        window_days: int = 14,
    ) -> Optional[HistoricalContext]:
        """Find commits from approximately N months ago."""
        now = datetime.now(timezone.utc)
        all_anniversary = []

        for months in target_months:
            target = now - timedelta(days=months * 30)
            start = target - timedelta(days=window_days)
            end = target + timedelta(days=window_days)
            commits = self.db.get_commits_in_range(start, end)
            all_anniversary.extend(commits)

        if not all_anniversary:
            return None

        # Sort by timestamp descending, take top max_commits
        all_anniversary.sort(key=lambda c: c.get("timestamp", ""), reverse=True)
        selected = all_anniversary[:max_commits]

        commit_dicts = [
            {
                "sha": c.get("commit_sha", ""),
                "repo_name": c.get("repo_name", ""),
                "message": c.get("commit_message", ""),
            }
            for c in selected
        ]

        return HistoricalContext(
            commits=commit_dicts,
            theme_description=f"Anniversary commits ({', '.join(str(m) for m in target_months)}mo ago)",
            strategy="anniversary",
        )
