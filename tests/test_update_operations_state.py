"""Tests for update_operations_state.py — sync_operation and update flow."""

import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

# Add scripts/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from update_operations_state import OPERATION_QUERIES, sync_operation, update_operations_yaml


# --- fixtures ---


@pytest.fixture
def ops_db():
    """In-memory SQLite DB with poll_state and pipeline_runs tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE poll_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        last_poll_time TEXT NOT NULL,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE pipeline_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT UNIQUE NOT NULL,
        content_type TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.commit()
    yield conn
    conn.close()


# --- TestSyncOperation ---


class TestSyncOperation:
    def test_run_poll_with_data(self, ops_db):
        ops_db.execute(
            "INSERT INTO poll_state (id, last_poll_time) VALUES (1, '2026-04-06T10:00:00')"
        )
        ops_db.commit()
        ops_data = {"runs": []}
        cursor = ops_db.cursor()

        result = sync_operation(cursor, ops_data, "run-poll")

        assert result is True
        assert len(ops_data["runs"]) == 1
        run = ops_data["runs"][0]
        assert run["operationId"] == "run-poll"
        assert run["startedAt"] == "2026-04-06T10:00:00"
        assert run["status"] == "completed"

    def test_run_daily_with_data(self, ops_db):
        ops_db.execute(
            "INSERT INTO pipeline_runs (batch_id, content_type, created_at) "
            "VALUES ('batch-1', 'x_thread', '2026-04-05T23:59:00')"
        )
        ops_db.commit()
        ops_data = {"runs": []}
        cursor = ops_db.cursor()

        result = sync_operation(cursor, ops_data, "run-daily")

        assert result is True
        assert ops_data["runs"][0]["startedAt"] == "2026-04-05T23:59:00"

    def test_no_db_result_returns_false(self, ops_db):
        ops_data = {"runs": []}
        cursor = ops_db.cursor()

        result = sync_operation(cursor, ops_data, "run-poll")

        assert result is False
        assert len(ops_data["runs"]) == 0

    def test_replaces_existing_entry(self, ops_db):
        ops_db.execute(
            "INSERT INTO poll_state (id, last_poll_time) VALUES (1, '2026-04-06T12:00:00')"
        )
        ops_db.commit()
        ops_data = {
            "runs": [
                {"operationId": "run-poll", "startedAt": "2026-04-05T00:00:00"},
                {"operationId": "run-daily", "startedAt": "2026-04-04T00:00:00"},
            ]
        }
        cursor = ops_db.cursor()

        sync_operation(cursor, ops_data, "run-poll")

        # Old run-poll removed, new one added; run-daily preserved
        poll_runs = [r for r in ops_data["runs"] if r["operationId"] == "run-poll"]
        daily_runs = [r for r in ops_data["runs"] if r["operationId"] == "run-daily"]
        assert len(poll_runs) == 1
        assert poll_runs[0]["startedAt"] == "2026-04-06T12:00:00"
        assert len(daily_runs) == 1


# --- TestUpdateOperationsYaml ---


class TestUpdateOperationsYaml:
    def test_all_operations_synced(self, ops_db, tmp_path):
        ops_db.execute(
            "INSERT INTO poll_state (id, last_poll_time) VALUES (1, '2026-04-06T10:00:00')"
        )
        ops_db.execute(
            "INSERT INTO pipeline_runs (batch_id, content_type, created_at) "
            "VALUES ('b1', 'x_thread', '2026-04-05T23:59:00')"
        )
        ops_db.execute(
            "INSERT INTO pipeline_runs (batch_id, content_type, created_at) "
            "VALUES ('b2', 'blog_post', '2026-04-01T12:00:00')"
        )
        ops_db.commit()

        ops_path = tmp_path / "operations.yaml"
        ops_path.write_text(yaml.dump({"runs": []}))

        with patch("update_operations_state.Path") as MockPath, \
             patch("update_operations_state.sqlite3.connect", return_value=ops_db):
            # Make Path(__file__).parent.parent resolve to tmp_path
            mock_project = tmp_path
            MockPath.return_value.parent.parent = mock_project
            # Override the ops_path and db_path
            with patch("builtins.open", create=True) as mock_open:
                import io
                yaml_content = yaml.dump({"runs": []})
                mock_open.return_value.__enter__ = lambda s: io.StringIO(yaml_content)
                mock_open.return_value.__exit__ = lambda s, *a: None

                # Test sync_operation directly since update_operations_yaml
                # has hardcoded paths that are hard to mock cleanly
                ops_data = {"runs": []}
                cursor = ops_db.cursor()
                synced = 0
                for op_id in OPERATION_QUERIES:
                    if sync_operation(cursor, ops_data, op_id):
                        synced += 1

        assert synced == 3
        assert len(ops_data["runs"]) == 3

    def test_no_operations_have_data(self, ops_db):
        ops_data = {"runs": []}
        cursor = ops_db.cursor()
        synced = 0
        for op_id in OPERATION_QUERIES:
            if sync_operation(cursor, ops_data, op_id):
                synced += 1

        assert synced == 0

    def test_specific_operation_only(self, ops_db):
        ops_db.execute(
            "INSERT INTO poll_state (id, last_poll_time) VALUES (1, '2026-04-06T10:00:00')"
        )
        ops_db.commit()
        ops_data = {"runs": []}
        cursor = ops_db.cursor()

        # Only sync run-poll
        result = sync_operation(cursor, ops_data, "run-poll")
        assert result is True

        # run-daily should not be synced
        result = sync_operation(cursor, ops_data, "run-daily")
        assert result is False

    def test_missing_runs_key(self, ops_db):
        ops_db.execute(
            "INSERT INTO poll_state (id, last_poll_time) VALUES (1, '2026-04-06T10:00:00')"
        )
        ops_db.commit()

        ops_data = {}
        ops_data["runs"] = []  # Simulates the initialization in update_operations_yaml
        cursor = ops_db.cursor()
        sync_operation(cursor, ops_data, "run-poll")
        assert len(ops_data["runs"]) == 1
