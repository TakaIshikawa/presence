"""Tests for expire_proactive_actions.py."""

import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from expire_proactive_actions import _draft_ttl_hours, main


def _mock_script_context(config, db):
    @contextmanager
    def _ctx():
        yield (config, db)

    return _ctx


def _row(row_id=1):
    return {
        "id": row_id,
        "action_type": "reply",
        "target_author_handle": "alice",
        "created_at": "2026-04-20 12:00:00",
        "target_tweet_id": f"tweet-{row_id}",
    }


def test_draft_ttl_hours_uses_config_value():
    config = SimpleNamespace(proactive=SimpleNamespace(draft_ttl_hours=36))

    assert _draft_ttl_hours(config) == 36


def test_draft_ttl_hours_uses_default_when_missing():
    config = SimpleNamespace(proactive=SimpleNamespace())

    assert _draft_ttl_hours(config) == 48


def test_draft_ttl_hours_override_wins():
    config = SimpleNamespace(proactive=SimpleNamespace(draft_ttl_hours=36))

    assert _draft_ttl_hours(config, override=12) == 12


@pytest.mark.parametrize("value", [0, -4, 1.5, "48"])
def test_draft_ttl_hours_rejects_invalid_values(value):
    config = SimpleNamespace(proactive=SimpleNamespace(draft_ttl_hours=value))

    with pytest.raises(ValueError, match="positive integer"):
        _draft_ttl_hours(config)


def test_main_dry_run_lists_expired_without_dismissing():
    config = SimpleNamespace(proactive=SimpleNamespace(draft_ttl_hours=24))
    db = MagicMock()
    db.get_expired_proactive_drafts.return_value = [_row(1), _row(2)]

    with patch("expire_proactive_actions.script_context", _mock_script_context(config, db)):
        assert main(["--dry-run"]) == 0

    db.get_expired_proactive_drafts.assert_called_once_with(24, limit=None)
    db.dismiss_expired_proactive_drafts.assert_not_called()


def test_main_defaults_to_apply():
    config = SimpleNamespace(proactive=SimpleNamespace(draft_ttl_hours=24))
    db = MagicMock()
    db.get_expired_proactive_drafts.return_value = [_row()]
    db.dismiss_expired_proactive_drafts.return_value = 1

    with patch("expire_proactive_actions.script_context", _mock_script_context(config, db)):
        assert main([]) == 0

    db.get_expired_proactive_drafts.assert_called_once_with(24, limit=None)
    db.dismiss_expired_proactive_drafts.assert_called_once_with(24, limit=None)


def test_main_limit_is_used():
    config = SimpleNamespace(proactive=SimpleNamespace(draft_ttl_hours=24))
    db = MagicMock()
    db.get_expired_proactive_drafts.return_value = [_row()]
    db.dismiss_expired_proactive_drafts.return_value = 1

    with patch("expire_proactive_actions.script_context", _mock_script_context(config, db)):
        assert main(["--limit", "10"]) == 0

    db.get_expired_proactive_drafts.assert_called_once_with(24, limit=10)
    db.dismiss_expired_proactive_drafts.assert_called_once_with(24, limit=10)


def test_main_ttl_override_is_used():
    config = SimpleNamespace(proactive=SimpleNamespace(draft_ttl_hours=24))
    db = MagicMock()
    db.get_expired_proactive_drafts.return_value = []

    with patch("expire_proactive_actions.script_context", _mock_script_context(config, db)):
        assert main(["--ttl-hours", "6"]) == 0

    db.get_expired_proactive_drafts.assert_called_once_with(6, limit=None)
