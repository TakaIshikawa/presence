"""Tests for retry_unpublished.py main orchestration."""

import sys
import logging
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def _make_config():
    config = MagicMock()
    config.paths.database = ":memory:"
    config.x.api_key = "key"
    config.x.api_secret = "secret"
    config.x.access_token = "at"
    config.x.access_token_secret = "ats"
    config.synthesis.eval_threshold = 0.7
    return config


def _mock_script_context(config, db):
    @contextmanager
    def _ctx():
        yield (config, db)
    return _ctx


def _make_post_result(success=True, url="https://x.com/user/status/123",
                      tweet_id="123", error=None):
    return SimpleNamespace(success=success, url=url, tweet_id=tweet_id, error=error)


def _make_content_item(content_id=1, content="Test post about AI and coding", retry_count=0):
    return {
        "id": content_id,
        "content": content,
        "retry_count": retry_count,
    }


@pytest.fixture
def mocks():
    """Set up common mocks for retry_unpublished tests."""
    config = _make_config()
    db = MagicMock()

    with patch("retry_unpublished.time.sleep") as mock_sleep, \
         patch("retry_unpublished.XClient") as MockXClient, \
         patch("retry_unpublished.script_context") as mock_ctx:
        mock_ctx.return_value = _mock_script_context(config, db)()
        yield SimpleNamespace(
            config=config,
            db=db,
            x_client=MockXClient.return_value,
            sleep=mock_sleep,
        )


class TestMain:
    def test_no_unpublished_posts(self, mocks):
        mocks.db.get_unpublished_content.return_value = []

        from retry_unpublished import main
        main()

        mocks.x_client.post.assert_not_called()

    def test_success_marks_published(self, mocks):
        mocks.db.get_unpublished_content.return_value = [_make_content_item()]
        mocks.x_client.post.return_value = _make_post_result()

        from retry_unpublished import main
        main()

        mocks.db.mark_published.assert_called_once()
        args = mocks.db.mark_published.call_args
        assert args[0][1] == "https://x.com/user/status/123"

    def test_failure_increments_retry(self, mocks):
        mocks.db.get_unpublished_content.return_value = [_make_content_item()]
        mocks.x_client.post.return_value = _make_post_result(
            success=False, error="Server error"
        )
        mocks.db.increment_retry.return_value = 1

        from retry_unpublished import main
        main()

        mocks.db.increment_retry.assert_called_once()
        mocks.db.mark_published.assert_not_called()

    def test_abandoned_after_three_attempts(self, mocks, caplog):
        caplog.set_level(logging.INFO)
        mocks.db.get_unpublished_content.return_value = [
            _make_content_item(retry_count=2)
        ]
        mocks.x_client.post.return_value = _make_post_result(
            success=False, error="Server error"
        )
        mocks.db.increment_retry.return_value = 3

        from retry_unpublished import main
        main()

        output = caplog.text
        assert "abandoned after 3 attempts" in output

    def test_rate_limit_breaks_loop(self, mocks, caplog):
        caplog.set_level(logging.INFO)
        mocks.db.get_unpublished_content.return_value = [
            _make_content_item(content_id=1),
            _make_content_item(content_id=2),
        ]
        mocks.x_client.post.return_value = _make_post_result(
            success=False, error="HTTP 429 Too Many Requests"
        )
        mocks.db.increment_retry.return_value = 1

        from retry_unpublished import main
        main()

        output = caplog.text
        assert "Rate limited" in output
        # Only first post attempted (loop breaks on 429)
        assert mocks.x_client.post.call_count == 1

    def test_delay_between_successful_posts(self, mocks):
        mocks.db.get_unpublished_content.return_value = [
            _make_content_item(content_id=1),
            _make_content_item(content_id=2),
        ]
        mocks.x_client.post.return_value = _make_post_result()

        from retry_unpublished import main
        main()

        # First post: no delay; second post: 30s delay
        mocks.sleep.assert_called_once_with(30)

    def test_no_delay_for_first_post(self, mocks):
        mocks.db.get_unpublished_content.return_value = [_make_content_item()]
        mocks.x_client.post.return_value = _make_post_result()

        from retry_unpublished import main
        main()

        mocks.sleep.assert_not_called()

    def test_null_retry_count_treated_as_zero(self, mocks, caplog):
        caplog.set_level(logging.INFO)
        mocks.db.get_unpublished_content.return_value = [
            _make_content_item(retry_count=None)
        ]
        mocks.x_client.post.return_value = _make_post_result(
            success=False, error="Fail"
        )
        mocks.db.increment_retry.return_value = 1

        from retry_unpublished import main
        main()

        output = caplog.text
        assert "attempt 1/3" in output
