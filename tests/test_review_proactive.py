"""Tests for review_proactive.py formatting and helper functions."""

import sqlite3
import sys
from pathlib import Path

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.cultivate_bridge import PersonContext, ProactiveAction
from review_proactive import _format_action_context, _get_x_user_id


def _make_person_context(**overrides):
    defaults = dict(
        x_handle="dev_jane",
        display_name="Jane Dev",
        bio="Building AI tools",
        relationship_strength=0.42,
        engagement_stage=3,
        dunbar_tier=2,
        authenticity_score=0.85,
        content_quality_score=0.7,
        content_relevance_score=0.6,
        is_known=True,
    )
    defaults.update(overrides)
    return PersonContext(**defaults)


def _make_action(**overrides):
    defaults = dict(
        action_id="act-1",
        action_type="engage",
        target_handle="dev_jane",
        target_person_id="person-1",
        description="Engage with their latest post",
        payload=None,
        person_context=_make_person_context(),
    )
    defaults.update(overrides)
    return ProactiveAction(**defaults)


# --- _format_action_context ---


class TestFormatActionContext:
    def test_full_context(self):
        action = _make_action()
        result = _format_action_context(action)
        assert "ENGAGE -> @dev_jane" in result
        assert "Active (stage 3)" in result
        assert "Key Network (tier 2)" in result
        assert "strength: 0.42" in result
        assert "Bio: Building AI tools" in result

    def test_no_person_context(self):
        action = _make_action(person_context=None)
        result = _format_action_context(action)
        assert "ENGAGE -> @dev_jane" in result
        assert "stage" not in result
        assert "Bio:" not in result

    def test_minimal_person_context(self):
        ctx = _make_person_context(
            bio=None,
            engagement_stage=None,
            dunbar_tier=None,
            relationship_strength=None,
        )
        action = _make_action(person_context=ctx)
        result = _format_action_context(action)
        assert "ENGAGE -> @dev_jane" in result
        # No context lines added
        assert "[" not in result

    def test_different_action_type(self):
        action = _make_action(action_type="strengthen", target_handle="alice")
        result = _format_action_context(action)
        assert "STRENGTHEN -> @alice" in result


# --- _get_x_user_id ---


class TestGetXUserId:
    @pytest.fixture
    def cultivate_db(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""CREATE TABLE people (
            id TEXT PRIMARY KEY,
            x_user_id TEXT,
            x_handle TEXT NOT NULL,
            display_name TEXT NOT NULL,
            is_self INTEGER DEFAULT 0,
            first_seen TEXT NOT NULL,
            last_updated TEXT NOT NULL
        )""")
        conn.execute(
            "INSERT INTO people VALUES (?, ?, ?, ?, 0, '2026-01-01', '2026-01-01')",
            ("person-1", "12345", "jane", "Jane"),
        )
        conn.execute(
            "INSERT INTO people VALUES (?, ?, ?, ?, 0, '2026-01-01', '2026-01-01')",
            ("person-2", None, "bob", "Bob"),
        )
        conn.commit()
        yield conn
        conn.close()

    @pytest.fixture
    def bridge(self, cultivate_db):
        """Minimal bridge-like object with conn attribute."""
        class FakeBridge:
            def __init__(self, conn):
                self.conn = conn
        return FakeBridge(cultivate_db)

    def test_found(self, bridge):
        result = _get_x_user_id(bridge, "person-1")
        assert result == "12345"

    def test_not_found(self, bridge):
        result = _get_x_user_id(bridge, "person-unknown")
        assert result is None

    def test_null_x_user_id(self, bridge):
        result = _get_x_user_id(bridge, "person-2")
        assert result is None
