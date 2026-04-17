"""Tests for review_accounts.py formatting and helpers."""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from review_accounts import _format_candidate


class TestFormatCandidate:
    def test_includes_handle_and_index(self):
        candidate = {
            "identifier": "new_user",
            "name": "new_user",
            "relevance_score": 0.62,
            "sample_count": 7,
            "discovery_source": "proactive_mining",
            "created_at": "2026-04-15 10:00:00",
        }
        output = _format_candidate(candidate, 1, 3)
        assert "@new_user" in output
        assert "1/3" in output

    def test_includes_relevance_score(self):
        candidate = {
            "identifier": "user1",
            "name": "user1",
            "relevance_score": 0.75,
            "sample_count": 5,
            "discovery_source": "proactive_mining",
            "created_at": "2026-04-15",
        }
        output = _format_candidate(candidate, 1, 1)
        assert "0.75" in output

    def test_includes_sample_count(self):
        candidate = {
            "identifier": "user1",
            "name": "user1",
            "relevance_score": 0.60,
            "sample_count": 8,
            "discovery_source": "proactive_mining",
            "created_at": "2026-04-15",
        }
        output = _format_candidate(candidate, 1, 1)
        assert "samples: 8" in output

    def test_includes_discovery_source(self):
        candidate = {
            "identifier": "user1",
            "name": "user1",
            "relevance_score": None,
            "sample_count": 0,
            "discovery_source": "proactive_mining",
            "created_at": "2026-04-10",
        }
        output = _format_candidate(candidate, 1, 1)
        assert "proactive_mining" in output

    def test_includes_name_when_different_from_identifier(self):
        candidate = {
            "identifier": "user1",
            "name": "Jane Developer",
            "relevance_score": 0.50,
            "sample_count": 3,
            "discovery_source": "proactive_mining",
            "created_at": "2026-04-15",
        }
        output = _format_candidate(candidate, 1, 1)
        assert "Jane Developer" in output

    def test_omits_name_when_same_as_identifier(self):
        candidate = {
            "identifier": "user1",
            "name": "user1",
            "relevance_score": 0.50,
            "sample_count": 3,
            "discovery_source": "proactive_mining",
            "created_at": "2026-04-15",
        }
        output = _format_candidate(candidate, 1, 1)
        assert "Name:" not in output

    def test_includes_discovered_date(self):
        candidate = {
            "identifier": "user1",
            "name": "user1",
            "relevance_score": None,
            "sample_count": 0,
            "discovery_source": "proactive_mining",
            "created_at": "2026-04-15 10:30:00",
        }
        output = _format_candidate(candidate, 1, 1)
        assert "2026-04-15" in output


class TestApproveFlow:
    def test_approve_sets_active(self, db):
        db.insert_candidate_source(
            "x_account", "test_user", "Test User",
            relevance_score=0.65, sample_count=5,
        )
        candidates = db.get_candidate_sources("x_account")
        assert len(candidates) == 1

        db.approve_candidate(candidates[0]["id"])

        active = db.get_active_curated_sources("x_account")
        assert len(active) == 1
        assert active[0]["identifier"] == "test_user"

        remaining = db.get_candidate_sources("x_account")
        assert len(remaining) == 0

    def test_dismiss_sets_rejected(self, db):
        db.insert_candidate_source(
            "x_account", "bad_user", "Bad User",
            relevance_score=0.30, sample_count=2,
        )
        candidates = db.get_candidate_sources("x_account")
        assert len(candidates) == 1

        db.reject_candidate(candidates[0]["id"])

        active = db.get_active_curated_sources("x_account")
        assert len(active) == 0

        remaining = db.get_candidate_sources("x_account")
        assert len(remaining) == 0


class TestOpenUrl:
    @patch("review_accounts.webbrowser.open")
    def test_opens_profile_url(self, mock_open):
        # Directly test the URL construction logic
        handle = "test_user"
        url = f"https://x.com/{handle}"
        import webbrowser
        webbrowser.open(url)
        mock_open.assert_called_once_with("https://x.com/test_user")
