"""Tests for scripts/fetch_newsletter_metrics.py."""

import importlib.util
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from output.newsletter import NewsletterMetrics


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "fetch_newsletter_metrics.py"
spec = importlib.util.spec_from_file_location("fetch_newsletter_metrics_script", SCRIPT_PATH)
fetch_newsletter_metrics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_newsletter_metrics)


def _config(enabled=True, api_key="test-key"):
    return SimpleNamespace(
        newsletter=SimpleNamespace(enabled=enabled, api_key=api_key),
        timeouts=SimpleNamespace(http_seconds=10),
    )


@contextmanager
def _script_context(config, db):
    yield config, db


def test_exits_when_newsletter_disabled(db):
    with patch.object(
        fetch_newsletter_metrics,
        "script_context",
        return_value=_script_context(_config(enabled=False), db),
    ), patch.object(fetch_newsletter_metrics, "ButtondownClient") as client:
        fetch_newsletter_metrics.main()

    client.assert_not_called()


def test_fetches_metrics_and_classifies_send(db):
    send_id = db.insert_newsletter_send(
        issue_id="issue-1",
        subject="Weekly",
        content_ids=[1],
        subscriber_count=100,
    )
    mock_client = MagicMock()
    mock_client.get_email_analytics.return_value = NewsletterMetrics(
        issue_id="issue-1",
        opens=45,
        clicks=2,
        unsubscribes=0,
    )

    with patch.object(
        fetch_newsletter_metrics,
        "script_context",
        return_value=_script_context(_config(), db),
    ), patch.object(
        fetch_newsletter_metrics,
        "ButtondownClient",
        return_value=mock_client,
    ), patch.object(fetch_newsletter_metrics, "update_monitoring"):
        fetch_newsletter_metrics.main()

    row = db.conn.execute(
        "SELECT status FROM newsletter_sends WHERE id = ?",
        (send_id,),
    ).fetchone()
    assert row["status"] == "resonated"
    mock_client.get_email_analytics.assert_called_once_with("issue-1")
