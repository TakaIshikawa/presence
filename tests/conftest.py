"""Shared test fixtures for Presence test suite."""

import sys
import sqlite3
from pathlib import Path

import pytest

# Add src/ to import path
# Also configured in pyproject.toml [tool.pytest.ini_options] pythonpath
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

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
