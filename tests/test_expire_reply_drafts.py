"""Tests for expire_reply_drafts.py."""

import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from expire_reply_drafts import _draft_ttl_hours, main


def _mock_script_context(config, db):
    @contextmanager
    def _ctx():
        yield (config, db)

    return _ctx


def _row(row_id=1, platform="x"):
    return {
        "id": row_id,
        "platform": platform,
        "inbound_author_handle": "alice",
        "detected_at": "2026-04-20 12:00:00",
        "inbound_tweet_id": f"{platform}-inbound-{row_id}",
    }


def test_draft_ttl_hours_uses_config_value():
    config = SimpleNamespace(replies=SimpleNamespace(draft_ttl_hours=36))

    assert _draft_ttl_hours(config) == 36


def test_draft_ttl_hours_uses_default_when_missing():
    config = SimpleNamespace(replies=SimpleNamespace())

    assert _draft_ttl_hours(config) == 48


def test_draft_ttl_hours_override_wins():
    config = SimpleNamespace(replies=SimpleNamespace(draft_ttl_hours=36))

    assert _draft_ttl_hours(config, override=12) == 12


@pytest.mark.parametrize("value", [0, -4, 1.5, "48"])
def test_draft_ttl_hours_rejects_invalid_values(value):
    config = SimpleNamespace(replies=SimpleNamespace(draft_ttl_hours=value))

    with pytest.raises(ValueError, match="positive integer"):
        _draft_ttl_hours(config)


def test_main_dry_run_lists_expired_without_dismissing():
    config = SimpleNamespace(replies=SimpleNamespace(draft_ttl_hours=24))
    db = MagicMock()
    db.get_expired_reply_drafts.return_value = [_row(1, "x"), _row(2, "bluesky")]

    with patch("expire_reply_drafts.script_context", _mock_script_context(config, db)):
        assert main(["--dry-run"]) == 0

    db.get_expired_reply_drafts.assert_called_once_with(24)
    db.dismiss_expired_reply_drafts.assert_not_called()


def test_main_defaults_to_dry_run():
    config = SimpleNamespace(replies=SimpleNamespace(draft_ttl_hours=24))
    db = MagicMock()
    db.get_expired_reply_drafts.return_value = [_row()]

    with patch("expire_reply_drafts.script_context", _mock_script_context(config, db)):
        assert main([]) == 0

    db.dismiss_expired_reply_drafts.assert_not_called()


def test_main_apply_dismisses_expired():
    config = SimpleNamespace(replies=SimpleNamespace(draft_ttl_hours=24))
    db = MagicMock()
    db.get_expired_reply_drafts.return_value = [_row()]
    db.dismiss_expired_reply_drafts.return_value = 1

    with patch("expire_reply_drafts.script_context", _mock_script_context(config, db)):
        assert main(["--apply"]) == 0

    db.get_expired_reply_drafts.assert_called_once_with(24)
    db.dismiss_expired_reply_drafts.assert_called_once_with(24)


def test_main_ttl_override_is_used():
    config = SimpleNamespace(replies=SimpleNamespace(draft_ttl_hours=24))
    db = MagicMock()
    db.get_expired_reply_drafts.return_value = []

    with patch("expire_reply_drafts.script_context", _mock_script_context(config, db)):
        assert main(["--ttl-hours", "6"]) == 0

    db.get_expired_reply_drafts.assert_called_once_with(6)
