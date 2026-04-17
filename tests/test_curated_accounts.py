"""Tests for merged curated account list (config + DB)."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.curated_accounts import get_active_x_accounts


def _make_config(x_accounts=None, blogs=None):
    """Build a minimal config with curated_sources."""
    curated = None
    if x_accounts is not None or blogs is not None:
        curated = SimpleNamespace(
            x_accounts=x_accounts or [],
            blogs=blogs or [],
        )
    return SimpleNamespace(curated_sources=curated)


def _make_account(identifier, name=None, license="attribution_required"):
    return SimpleNamespace(
        identifier=identifier,
        name=name or identifier,
        license=license,
    )


class TestGetActiveXAccounts:
    def test_config_only(self, db):
        config = _make_config(x_accounts=[
            _make_account("karpathy", "Andrej Karpathy"),
            _make_account("swyx"),
        ])

        accounts = get_active_x_accounts(config, db)
        assert len(accounts) == 2
        identifiers = [a.identifier for a in accounts]
        assert identifiers == ["karpathy", "swyx"]

    def test_db_accounts_merged(self, db):
        config = _make_config(x_accounts=[_make_account("karpathy")])

        # Add an approved account via DB
        db.insert_candidate_source("x_account", "new_user", "New User")
        candidates = db.get_candidate_sources("x_account")
        db.approve_candidate(candidates[0]["id"])

        accounts = get_active_x_accounts(config, db)
        identifiers = [a.identifier for a in accounts]
        assert "karpathy" in identifiers
        assert "new_user" in identifiers
        assert len(accounts) == 2

    def test_dedup_config_and_db(self, db):
        """Same identifier in config and DB should appear only once."""
        config = _make_config(x_accounts=[_make_account("karpathy")])

        # Sync config sources (creates DB row for karpathy)
        db.sync_config_sources(
            [{"identifier": "karpathy", "name": "AK"}], "x_account"
        )

        accounts = get_active_x_accounts(config, db)
        identifiers = [a.identifier for a in accounts]
        assert identifiers.count("karpathy") == 1
        assert len(accounts) == 1

    def test_rejected_excluded(self, db):
        config = _make_config(x_accounts=[])

        # Insert and reject a candidate
        db.insert_candidate_source("x_account", "rejected_user")
        candidates = db.get_candidate_sources("x_account")
        db.reject_candidate(candidates[0]["id"])

        accounts = get_active_x_accounts(config, db)
        assert len(accounts) == 0

    def test_candidates_excluded(self, db):
        """Candidate (not yet approved) sources should not be included."""
        config = _make_config(x_accounts=[])

        db.insert_candidate_source("x_account", "pending_user")

        accounts = get_active_x_accounts(config, db)
        assert len(accounts) == 0

    def test_no_curated_sources_config(self, db):
        config = _make_config()  # curated_sources = None
        accounts = get_active_x_accounts(config, db)
        assert accounts == []

    def test_case_insensitive_dedup(self, db):
        """Config 'Karpathy' and DB 'karpathy' should dedup."""
        config = _make_config(x_accounts=[_make_account("Karpathy")])

        # Insert a DB-active account with lowercase
        db.sync_config_sources(
            [{"identifier": "karpathy", "name": "AK"}], "x_account"
        )

        accounts = get_active_x_accounts(config, db)
        assert len(accounts) == 1

    def test_db_account_has_required_attributes(self, db):
        """DB-sourced accounts should have identifier, name, license."""
        config = _make_config(x_accounts=[])

        db.insert_candidate_source(
            "x_account", "new_user", "New User",
            relevance_score=0.60,
        )
        candidates = db.get_candidate_sources("x_account")
        db.approve_candidate(candidates[0]["id"])

        accounts = get_active_x_accounts(config, db)
        assert len(accounts) == 1
        acc = accounts[0]
        assert acc.identifier == "new_user"
        assert acc.name == "New User"
        assert acc.license == "attribution_required"
