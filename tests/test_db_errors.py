"""Tests for database error handling."""

import os
import tempfile
from pathlib import Path

import pytest

from src.storage.db import Database, ConnectionError, DatabaseError


def test_connection_error_read_only_path():
    """Test that connecting to a read-only path raises ConnectionError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a directory with read-only permissions
        readonly_dir = Path(tmpdir) / "readonly"
        readonly_dir.mkdir()
        readonly_dir.chmod(0o444)  # Read-only directory

        db_path = readonly_dir / "test.db"
        db = Database(str(db_path))

        # Attempt to connect should raise ConnectionError
        with pytest.raises(ConnectionError) as exc_info:
            db.connect()

        # Verify the error message contains the path
        assert str(db_path) in str(exc_info.value)

        # Clean up: restore write permissions so tmpdir can be deleted
        readonly_dir.chmod(0o755)


def test_connection_error_invalid_path():
    """Test that connecting to an invalid path raises ConnectionError."""
    # Try to connect to a path that cannot be created (e.g., inside a file)
    with tempfile.NamedTemporaryFile() as tmpfile:
        # Create a regular file, then try to create a DB "inside" it
        db_path = Path(tmpfile.name) / "invalid.db"
        db = Database(str(db_path))

        with pytest.raises(ConnectionError) as exc_info:
            db.connect()

        assert str(db_path) in str(exc_info.value)


def test_init_schema_corrupt_file():
    """Test that init_schema on a corrupt file raises DatabaseError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "corrupt.db"

        # Create a file with invalid SQLite content
        db_path.write_text("This is not a valid SQLite database file\n" * 100)

        db = Database(str(db_path))

        # Connection might succeed (sqlite will try to work with it)
        # but schema initialization should fail
        try:
            db.connect()
            with pytest.raises(DatabaseError) as exc_info:
                db.init_schema("./schema.sql")

            # Verify the error message contains the path
            assert str(db_path) in str(exc_info.value)
        finally:
            db.close()


def test_init_schema_missing_schema_file():
    """Test that init_schema with missing schema file raises DatabaseError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        db = Database(str(db_path))

        try:
            db.connect()

            # Try to initialize with a non-existent schema file
            with pytest.raises((DatabaseError, FileNotFoundError)):
                db.init_schema("/nonexistent/schema.sql")
        finally:
            db.close()


def test_successful_connection_and_init():
    """Test that normal operation still works with error handling."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        db = Database(str(db_path))

        # This should work without raising our custom exceptions
        db.connect()
        assert db.conn is not None

        # We can't easily test init_schema without the full schema.sql file,
        # but we can verify that connection works and basic operations don't
        # raise our custom exceptions unnecessarily

        # Create a simple table directly
        db.conn.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)
        db.conn.commit()

        # Verify the table was created
        cursor = db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'"
        )
        assert cursor.fetchone() is not None

        db.close()


def test_connection_error_inheritance():
    """Test that ConnectionError inherits from DatabaseError."""
    assert issubclass(ConnectionError, DatabaseError)
    assert issubclass(ConnectionError, Exception)


def test_database_error_with_context_manager():
    """Test error handling when using Database as context manager."""
    with tempfile.TemporaryDirectory() as tmpdir:
        readonly_dir = Path(tmpdir) / "readonly"
        readonly_dir.mkdir()
        readonly_dir.chmod(0o444)

        db_path = readonly_dir / "test.db"

        # Context manager should propagate ConnectionError
        with pytest.raises(ConnectionError):
            with Database(str(db_path)):
                pass  # Should fail before reaching here

        # Clean up
        readonly_dir.chmod(0o755)


def test_error_chaining():
    """Test that our exceptions properly chain from sqlite3 exceptions."""
    with tempfile.TemporaryDirectory() as tmpdir:
        readonly_dir = Path(tmpdir) / "readonly"
        readonly_dir.mkdir()
        readonly_dir.chmod(0o444)

        db_path = readonly_dir / "test.db"
        db = Database(str(db_path))

        try:
            db.connect()
        except ConnectionError as e:
            # Verify exception chaining
            assert e.__cause__ is not None
            assert "sqlite3" in str(type(e.__cause__)).lower()
        finally:
            readonly_dir.chmod(0o755)
