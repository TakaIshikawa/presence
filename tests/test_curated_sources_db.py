"""Tests for curated_sources DB methods (account discovery)."""

import pytest


class TestSyncConfigSources:
    def test_inserts_new_sources(self, db):
        sources = [
            {"identifier": "karpathy", "name": "Andrej Karpathy", "license": "attribution_required"},
            {"identifier": "swyx", "name": "swyx", "license": "attribution_required"},
        ]
        count = db.sync_config_sources(sources, "x_account")
        assert count == 2

        rows = db.get_active_curated_sources("x_account")
        assert len(rows) == 2
        identifiers = {r["identifier"] for r in rows}
        assert identifiers == {"karpathy", "swyx"}

    def test_idempotent(self, db):
        sources = [{"identifier": "karpathy", "name": "AK", "license": "open"}]
        db.sync_config_sources(sources, "x_account")
        db.sync_config_sources(sources, "x_account")

        rows = db.get_active_curated_sources("x_account")
        assert len(rows) == 1

    def test_updates_name_on_conflict(self, db):
        db.sync_config_sources(
            [{"identifier": "karpathy", "name": "Old Name"}], "x_account"
        )
        db.sync_config_sources(
            [{"identifier": "karpathy", "name": "Andrej Karpathy"}], "x_account"
        )

        rows = db.get_active_curated_sources("x_account")
        assert rows[0]["name"] == "Andrej Karpathy"

    def test_preserves_non_config_rows(self, db):
        # Insert a candidate from discovery
        db.insert_candidate_source("x_account", "new_user", "New User")

        # Sync config sources
        db.sync_config_sources(
            [{"identifier": "karpathy", "name": "AK"}], "x_account"
        )

        # Candidate should still exist
        candidates = db.get_candidate_sources("x_account")
        assert len(candidates) == 1
        assert candidates[0]["identifier"] == "new_user"

    def test_sets_discovery_source_config(self, db):
        db.sync_config_sources(
            [{"identifier": "karpathy", "name": "AK"}], "x_account"
        )
        rows = db.get_active_curated_sources("x_account")
        assert rows[0]["discovery_source"] == "config"


class TestGetActiveCuratedSources:
    def test_returns_only_active(self, db):
        db.sync_config_sources(
            [{"identifier": "karpathy", "name": "AK"}], "x_account"
        )
        db.insert_candidate_source("x_account", "candidate_user")

        rows = db.get_active_curated_sources("x_account")
        assert len(rows) == 1
        assert rows[0]["identifier"] == "karpathy"

    def test_filters_by_source_type(self, db):
        db.sync_config_sources(
            [{"identifier": "karpathy", "name": "AK"}], "x_account"
        )
        db.sync_config_sources(
            [{"identifier": "example.com", "name": "Example Blog"}], "blog"
        )

        x_rows = db.get_active_curated_sources("x_account")
        assert len(x_rows) == 1
        assert x_rows[0]["identifier"] == "karpathy"

        blog_rows = db.get_active_curated_sources("blog")
        assert len(blog_rows) == 1
        assert blog_rows[0]["identifier"] == "example.com"

    def test_empty_when_none_active(self, db):
        rows = db.get_active_curated_sources("x_account")
        assert rows == []


class TestInsertCandidateSource:
    def test_inserts_with_candidate_status(self, db):
        sid = db.insert_candidate_source(
            "x_account", "new_user", "New User",
            discovery_source="proactive_mining",
            relevance_score=0.65, sample_count=7,
        )
        assert sid is not None

        candidates = db.get_candidate_sources("x_account")
        assert len(candidates) == 1
        assert candidates[0]["identifier"] == "new_user"
        assert candidates[0]["status"] == "candidate"
        assert candidates[0]["discovery_source"] == "proactive_mining"
        assert candidates[0]["relevance_score"] == 0.65
        assert candidates[0]["sample_count"] == 7

    def test_duplicate_returns_none(self, db):
        db.insert_candidate_source("x_account", "user1")
        result = db.insert_candidate_source("x_account", "user1")
        assert result is None

    def test_duplicate_does_not_overwrite(self, db):
        db.insert_candidate_source(
            "x_account", "user1", relevance_score=0.50
        )
        db.insert_candidate_source(
            "x_account", "user1", relevance_score=0.90
        )

        candidates = db.get_candidate_sources("x_account")
        assert len(candidates) == 1
        assert candidates[0]["relevance_score"] == 0.50


class TestGetCandidateSources:
    def test_ordered_by_relevance_desc(self, db):
        db.insert_candidate_source("x_account", "low", relevance_score=0.30)
        db.insert_candidate_source("x_account", "high", relevance_score=0.80)
        db.insert_candidate_source("x_account", "mid", relevance_score=0.55)

        candidates = db.get_candidate_sources("x_account")
        scores = [c["relevance_score"] for c in candidates]
        assert scores == [0.80, 0.55, 0.30]

    def test_respects_limit(self, db):
        for i in range(5):
            db.insert_candidate_source("x_account", f"user_{i}", relevance_score=float(i))
        candidates = db.get_candidate_sources("x_account", limit=2)
        assert len(candidates) == 2

    def test_excludes_non_candidates(self, db):
        db.sync_config_sources(
            [{"identifier": "active_user", "name": "Active"}], "x_account"
        )
        db.insert_candidate_source("x_account", "candidate_user")

        candidates = db.get_candidate_sources("x_account")
        assert len(candidates) == 1
        assert candidates[0]["identifier"] == "candidate_user"


class TestApproveCandidate:
    def test_sets_status_active(self, db):
        db.insert_candidate_source("x_account", "user1")
        candidates = db.get_candidate_sources("x_account")
        source_id = candidates[0]["id"]

        db.approve_candidate(source_id)

        # Should now appear in active sources
        active = db.get_active_curated_sources("x_account")
        assert len(active) == 1
        assert active[0]["identifier"] == "user1"

        # Should no longer be a candidate
        candidates = db.get_candidate_sources("x_account")
        assert len(candidates) == 0

    def test_sets_reviewed_at(self, db):
        db.insert_candidate_source("x_account", "user1")
        candidates = db.get_candidate_sources("x_account")
        source_id = candidates[0]["id"]

        db.approve_candidate(source_id)

        active = db.get_active_curated_sources("x_account")
        assert active[0]["reviewed_at"] is not None


class TestRejectCandidate:
    def test_sets_status_rejected(self, db):
        db.insert_candidate_source("x_account", "user1")
        candidates = db.get_candidate_sources("x_account")
        source_id = candidates[0]["id"]

        db.reject_candidate(source_id)

        # Should not be in active or candidate lists
        assert db.get_active_curated_sources("x_account") == []
        assert db.get_candidate_sources("x_account") == []

    def test_sets_reviewed_at(self, db):
        db.insert_candidate_source("x_account", "user1")
        candidates = db.get_candidate_sources("x_account")
        source_id = candidates[0]["id"]

        db.reject_candidate(source_id)

        # Verify via direct query
        row = db.conn.execute(
            "SELECT status, reviewed_at FROM curated_sources WHERE id = ?",
            (source_id,),
        ).fetchone()
        assert row[0] == "rejected"
        assert row[1] is not None


class TestCandidateExists:
    def test_returns_true_for_candidate(self, db):
        db.insert_candidate_source("x_account", "user1")
        assert db.candidate_exists("x_account", "user1") is True

    def test_returns_true_for_active(self, db):
        db.sync_config_sources(
            [{"identifier": "karpathy", "name": "AK"}], "x_account"
        )
        assert db.candidate_exists("x_account", "karpathy") is True

    def test_returns_true_for_rejected(self, db):
        db.insert_candidate_source("x_account", "user1")
        candidates = db.get_candidate_sources("x_account")
        db.reject_candidate(candidates[0]["id"])
        assert db.candidate_exists("x_account", "user1") is True

    def test_returns_false_for_nonexistent(self, db):
        assert db.candidate_exists("x_account", "nobody") is False

    def test_different_source_type_not_found(self, db):
        db.insert_candidate_source("x_account", "user1")
        assert db.candidate_exists("blog", "user1") is False


class TestSyncFollowingSources:
    def test_inserts_new_accounts(self, db):
        accounts = [
            {"id": "1", "username": "alice", "name": "Alice"},
            {"id": "2", "username": "bob", "name": "Bob"},
        ]
        inserted = db.sync_following_sources(accounts)
        assert inserted == 2

        active = db.get_active_curated_sources("x_account")
        assert len(active) == 2
        assert {a["identifier"] for a in active} == {"alice", "bob"}
        for a in active:
            assert a["discovery_source"] == "following"
            assert a["status"] == "active"

    def test_skips_existing_entries(self, db):
        db.sync_config_sources(
            [{"identifier": "alice", "name": "Alice"}], "x_account"
        )
        accounts = [
            {"id": "1", "username": "alice", "name": "Alice Updated"},
            {"id": "2", "username": "bob", "name": "Bob"},
        ]
        inserted = db.sync_following_sources(accounts)
        assert inserted == 1  # Only bob

        # Alice should retain original config data
        active = db.get_active_curated_sources("x_account")
        alice = [a for a in active if a["identifier"] == "alice"][0]
        assert alice["discovery_source"] == "config"
        assert alice["name"] == "Alice"  # Not overwritten

    def test_skips_candidates_and_rejected(self, db):
        db.insert_candidate_source("x_account", "candidate_user")
        db.insert_candidate_source("x_account", "rejected_user")
        cands = db.get_candidate_sources("x_account")
        for c in cands:
            if c["identifier"] == "rejected_user":
                db.reject_candidate(c["id"])

        accounts = [
            {"id": "1", "username": "candidate_user", "name": "C"},
            {"id": "2", "username": "rejected_user", "name": "R"},
            {"id": "3", "username": "new_user", "name": "New"},
        ]
        inserted = db.sync_following_sources(accounts)
        assert inserted == 1  # Only new_user

    def test_uses_username_as_name_fallback(self, db):
        accounts = [{"id": "1", "username": "alice"}]
        db.sync_following_sources(accounts)

        active = db.get_active_curated_sources("x_account")
        assert active[0]["name"] == "alice"

    def test_returns_zero_for_empty_list(self, db):
        assert db.sync_following_sources([]) == 0
