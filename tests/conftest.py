"""Shared test fixtures for Presence test suite."""

import sys
import sqlite3
import types
import importlib.util
from pathlib import Path

import pytest

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
        pass

    class ConnectionError(Exception):
        pass

    def post(*args, **kwargs):
        raise NotImplementedError("requests.post stub must be patched in tests")

    requests_stub.HTTPError = HTTPError
    requests_stub.ConnectionError = ConnectionError
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
