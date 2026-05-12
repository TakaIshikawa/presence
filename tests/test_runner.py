"""Tests for src/runner.py shared script utilities."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


class TestScriptContextHappyPath:
    """Test script_context() context manager in normal operation."""

    @patch("runner.Database")
    @patch("runner.load_config")
    def test_yields_config_and_db_tuple(self, mock_load_config, MockDatabase):
        """Verify script_context yields (Config, Database) and calls lifecycle methods."""
        mock_config = MagicMock()
        mock_config.paths.database = "/tmp/test.db"
        mock_load_config.return_value = mock_config

        mock_db = MockDatabase.return_value

        from runner import script_context

        with script_context() as (config, db):
            assert config is mock_config
            assert db is mock_db
            mock_db.connect.assert_called_once()
            mock_db.init_schema.assert_called_once()
            # Verify schema path is passed
            schema_arg = mock_db.init_schema.call_args[0][0]
            assert schema_arg.endswith("schema.sql")

        # Verify cleanup is called after exit
        mock_db.close.assert_called_once()

    @patch("runner.Database")
    @patch("runner.load_config")
    def test_database_instantiated_with_config_path(self, mock_load_config, MockDatabase):
        """Verify Database is instantiated with the database path from config."""
        mock_config = MagicMock()
        mock_config.paths.database = "/custom/path/db.sqlite"
        mock_load_config.return_value = mock_config

        from runner import script_context

        with script_context():
            pass

        MockDatabase.assert_called_once_with("/custom/path/db.sqlite")


class TestScriptContextExceptionHandling:
    """Test script_context() closes DB even when exception occurs in body."""

    @patch("runner.Database")
    @patch("runner.load_config")
    def test_closes_db_when_body_raises_exception(self, mock_load_config, MockDatabase):
        """Verify db.close() is called even when context body raises."""
        mock_config = MagicMock()
        mock_config.paths.database = ":memory:"
        mock_load_config.return_value = mock_config

        mock_db = MockDatabase.return_value

        from runner import script_context

        with pytest.raises(ValueError, match="test error"):
            with script_context() as (config, db):
                raise ValueError("test error")

        # Database cleanup must still happen
        mock_db.close.assert_called_once()


class TestUpdateMonitoringNoOp:
    """Test update_monitoring() remains a safe compatibility hook."""

    def test_update_monitoring_does_not_raise(self):
        from runner import update_monitoring

        update_monitoring("daily_digest")
