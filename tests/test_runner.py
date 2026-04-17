"""Tests for src/runner.py shared script utilities."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

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


class TestUpdateMonitoringCallsSubprocess:
    """Test update_monitoring() subprocess invocation when script exists."""

    @patch("runner.subprocess.run")
    @patch("runner.PROJECT_ROOT", Path("/fake/project"))
    def test_calls_subprocess_when_script_exists(self, mock_run):
        """Verify subprocess.run is called with correct args when sync script exists."""
        from runner import update_monitoring

        with patch("runner.Path.exists", return_value=True):
            update_monitoring("daily_digest")

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert call_args[0] == sys.executable
        assert call_args[1] == "/fake/project/scripts/update_operations_state.py"
        assert call_args[2:] == ["--operation", "daily_digest"]

        # Verify subprocess flags
        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["check"] is False
        assert call_kwargs["capture_output"] is True


class TestUpdateMonitoringNoOpWhenScriptMissing:
    """Test update_monitoring() does nothing when script doesn't exist."""

    @patch("runner.subprocess.run")
    @patch("runner.PROJECT_ROOT", Path("/fake/project"))
    def test_no_subprocess_call_when_script_missing(self, mock_run):
        """Verify no subprocess call when update_operations_state.py doesn't exist."""
        from runner import update_monitoring

        with patch("runner.Path.exists", return_value=False):
            update_monitoring("poll_replies")

        mock_run.assert_not_called()


class TestUpdateMonitoringSwallowsExceptions:
    """Test update_monitoring() silently catches all exceptions."""

    @patch("runner.subprocess.run")
    @patch("runner.PROJECT_ROOT", Path("/fake/project"))
    def test_swallows_subprocess_error(self, mock_run):
        """Verify exceptions from subprocess.run are silently caught."""
        mock_run.side_effect = RuntimeError("subprocess failed")

        from runner import update_monitoring

        with patch("runner.Path.exists", return_value=True):
            # Should not raise despite subprocess error
            update_monitoring("build_knowledge")

        mock_run.assert_called_once()

    @patch("runner.Path.exists")
    @patch("runner.subprocess.run")
    @patch("runner.PROJECT_ROOT", Path("/fake/project"))
    def test_swallows_path_exists_error(self, mock_run, mock_exists):
        """Verify exceptions from Path.exists() are silently caught."""
        mock_exists.side_effect = OSError("filesystem error")

        from runner import update_monitoring

        # Should not raise despite filesystem error
        update_monitoring("cross_post")

        mock_exists.assert_called_once()
        mock_run.assert_not_called()
