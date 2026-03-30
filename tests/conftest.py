"""Shared test fixtures for Presence test suite."""

import sys
import sqlite3
from pathlib import Path

import pytest

# Add src/ to import path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from storage.db import Database


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
