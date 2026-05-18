"""Shared test fixtures for Presence test suite."""

import sys
import sqlite3
import types
import importlib.util
import datetime as _datetime
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_REAL_DATETIME = _datetime.datetime
_DEFAULT_STABLE_NOW = _REAL_DATETIME(2026, 4, 23, 12, 0, tzinfo=_datetime.timezone.utc)
_DATE_WINDOW_TEST_FILES = {
    "test_blog_excerpt_duplication.py",
    "test_blog_visual_opportunities.py",
    "test_bluesky_reply_context_gaps.py",
    "test_campaign_evidence_gap_seeder.py",
    "test_campaign_evidence_readiness.py",
    "test_claude_blocker_idea_seeder.py",
    "test_claude_command_retry_backoff.py",
    "test_claude_command_retry_effectiveness.py",
    "test_claude_command_retry_recovery.py",
    "test_claude_session_approval_latency.py",
    "test_claude_session_command_duration_buckets.py",
    "test_claude_session_command_duration_outliers.py",
    "test_claude_session_command_exit_codes.py",
    "test_claude_session_context_switches.py",
    "test_claude_session_env_context_report.py",
    "test_claude_session_idle_gaps.py",
    "test_claude_session_interruption_resumption.py",
    "test_claude_session_model_switches.py",
    "test_claude_session_tool_bursts.py",
    "test_claude_session_tool_result_size.py",
    "test_claude_session_tool_timeout_report.py",
    "test_claude_tool_error_taxonomy.py",
    "test_cost_forecast.py",
    "test_discussion_digest.py",
    "test_eval_batch_report.py",
    "test_feedback_rejection_motifs.py",
    "test_github_activity_conversion.py",
    "test_hotfix_commit_idea_seeder.py",
    "test_image_prompt_reuse.py",
    "test_issue_idea_seeder.py",
    "test_newsletter_image_alt_text_report.py",
    "test_newsletter_section_balancer.py",
    "test_newsletter_source_reference_audit.py",
    "test_newsletter_topic_balance.py",
    "test_newsletter_topic_planner.py",
    "test_presence_context.py",
    "test_proactive_action_outcomes.py",
    "test_proactive_action_target_audit.py",
    "test_publication_ledger.py",
    "test_publication_parity.py",
    "test_publication_url_audit.py",
    "test_publish_failover.py",
    "test_publish_failure_reasons.py",
    "test_quote_opportunities.py",
    "test_release_coverage.py",
    "test_reply_context_gap_report.py",
    "test_reply_duplicate_drafts.py",
    "test_reply_followup_promise_audit.py",
    "test_reply_knowledge_gaps.py",
    "test_reply_privacy_audit.py",
    "test_reply_question_coverage.py",
    "test_reply_review_latency.py",
    "test_thread_hook_performance.py",
    "test_visual_alt_text_coverage.py",
    "test_visual_asset_ledger.py",
    "test_x_hashtag_density.py",
}


class _StableDateTimeMeta(type):
    def __instancecheck__(cls, instance):
        return isinstance(instance, _REAL_DATETIME)


def _stable_datetime_class(now: _datetime.datetime) -> type[_datetime.datetime]:
    class StableDateTime(_REAL_DATETIME, metaclass=_StableDateTimeMeta):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return now.replace(tzinfo=None)
            return now.astimezone(tz)

        @classmethod
        def utcnow(cls):
            return now.astimezone(_datetime.timezone.utc).replace(tzinfo=None)

    return StableDateTime


def _is_project_runtime_module(module: object) -> bool:
    module_file = getattr(module, "__file__", None)
    if not module_file:
        return False
    try:
        path = Path(module_file).resolve()
    except (OSError, RuntimeError):
        return False
    return path.is_relative_to(_PROJECT_ROOT / "src") or path.is_relative_to(_PROJECT_ROOT / "scripts")


@pytest.fixture(autouse=True)
def _stable_project_clock_for_date_window_tests(request, monkeypatch):
    if Path(str(request.fspath)).name not in _DATE_WINDOW_TEST_FILES:
        return
    if (
        Path(str(request.fspath)).name == "test_presence_context.py"
        and request.node.name == "test_idea_inbox_excludes_currently_snoozed_ideas_until_they_expire"
    ):
        return

    module_now = getattr(request.module, "NOW", _DEFAULT_STABLE_NOW)
    if isinstance(module_now, _datetime.date) and not isinstance(module_now, _datetime.datetime):
        module_now = _REAL_DATETIME.combine(module_now, _datetime.time.min, tzinfo=_datetime.timezone.utc)
    if not isinstance(module_now, _datetime.datetime):
        module_now = _DEFAULT_STABLE_NOW
    if module_now.tzinfo is None:
        module_now = module_now.replace(tzinfo=_datetime.timezone.utc)
    stable_datetime = _stable_datetime_class(module_now)

    for module in list(sys.modules.values()):
        if _is_project_runtime_module(module) and getattr(module, "datetime", None) is _REAL_DATETIME:
            monkeypatch.setattr(module, "datetime", stable_datetime, raising=False)
    for value in vars(request.module).values():
        if _is_project_runtime_module(value) and getattr(value, "datetime", None) is _REAL_DATETIME:
            monkeypatch.setattr(value, "datetime", stable_datetime, raising=False)

# Add src/ to import path
# Also configured in pyproject.toml [tool.pytest.ini_options] pythonpath
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

if importlib.util.find_spec("tweepy") is None:
    tweepy_stub = types.ModuleType("tweepy")

    class TweepyException(Exception):
        pass

    class Client:
        def __init__(self, *args, **kwargs):
            pass

    class API:
        def __init__(self, *args, **kwargs):
            pass

    class OAuth1UserHandler:
        def __init__(self, *args, **kwargs):
            pass

    tweepy_stub.TweepyException = TweepyException
    tweepy_stub.Client = Client
    tweepy_stub.API = API
    tweepy_stub.OAuth1UserHandler = OAuth1UserHandler
    sys.modules["tweepy"] = tweepy_stub

if importlib.util.find_spec("requests") is None:
    requests_stub = types.ModuleType("requests")

    class HTTPError(Exception):
        def __init__(self, *args, response=None, **kwargs):
            super().__init__(*args)
            self.response = response

    class RequestException(Exception):
        pass

    class ConnectionError(RequestException):
        pass

    class Response:
        status_code = 200

        def json(self):
            return {}

        def raise_for_status(self):
            return None

    def get(*args, **kwargs):
        raise NotImplementedError("requests.get stub must be patched in tests")

    def post(*args, **kwargs):
        raise NotImplementedError("requests.post stub must be patched in tests")

    exceptions_stub = types.SimpleNamespace(
        ConnectionError=ConnectionError,
        HTTPError=HTTPError,
        RequestException=RequestException,
    )
    requests_stub.Response = Response
    requests_stub.HTTPError = HTTPError
    requests_stub.ConnectionError = ConnectionError
    requests_stub.RequestException = RequestException
    requests_stub.exceptions = exceptions_stub
    requests_stub.get = get
    requests_stub.post = post
    sys.modules["requests"] = requests_stub

if importlib.util.find_spec("atproto") is None:
    atproto_stub = types.ModuleType("atproto")
    atproto_exceptions_stub = types.ModuleType("atproto.exceptions")

    class AtProtocolError(Exception):
        pass

    class NetworkError(AtProtocolError):
        pass

    class UnauthorizedError(AtProtocolError):
        pass

    class Client:
        pass

    atproto_stub.Client = Client
    atproto_exceptions_stub.AtProtocolError = AtProtocolError
    atproto_exceptions_stub.NetworkError = NetworkError
    atproto_exceptions_stub.UnauthorizedError = UnauthorizedError
    sys.modules["atproto"] = atproto_stub
    sys.modules["atproto.exceptions"] = atproto_exceptions_stub

from storage.db import Database

SCHEMA_PATH = str(Path(__file__).resolve().parent.parent / "schema.sql")


@pytest.fixture
def schema_path():
    return str(Path(__file__).parent.parent / "schema.sql")


@pytest.fixture
def db(schema_path):
    """In-memory SQLite database with schema applied."""
    database = Database(":memory:")
    database.connect()
    database.init_schema(schema_path)
    yield database
    database.close()


@pytest.fixture()
def file_db(tmp_path):
    """Yield a connected Database backed by a temporary SQLite file."""
    db_path = str(tmp_path / "test.db")
    database = Database(db_path)
    database.connect()
    database.init_schema(schema_path=SCHEMA_PATH)
    yield database
    database.close()


@pytest.fixture()
def sample_message():
    """A minimal claude_messages row dict."""
    return {
        "session_id": "sess-001",
        "message_uuid": "uuid-aaa",
        "project_path": "/home/user/project",
        "timestamp": "2026-03-20T10:00:00+00:00",
        "prompt_text": "Explain the auth module",
    }


@pytest.fixture()
def sample_commit():
    """A minimal github_commits row dict."""
    return {
        "repo_name": "acme/widget",
        "commit_sha": "abc123",
        "commit_message": "fix: resolve race condition",
        "timestamp": "2026-03-20T11:00:00+00:00",
        "author": "dev@acme.io",
    }


@pytest.fixture()
def sample_content():
    """A minimal generated_content row dict."""
    return {
        "content_type": "x_post",
        "source_commits": ["abc123"],
        "source_messages": ["uuid-aaa"],
        "content": "Shipped a fix for the race condition today.",
        "eval_score": 7.5,
        "eval_feedback": "Good conciseness",
    }
