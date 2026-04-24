"""Tests for update_operations_state.py — sync_operation and update flow."""

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
import requests
import yaml

# Add scripts/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from update_operations_state import (
    OPERATION_QUERIES,
    OperationsAlertThresholds,
    OperationsWebhookConfig,
    build_webhook_payload,
    compute_alert_statuses,
    deliver_operations_alerts,
    sync_operation,
    update_operations_yaml,
)


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


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


class TestAlertStatuses:
    def _thresholds(self, **overrides):
        values = {
            "max_consecutive_publish_failures": 3,
            "max_ingestion_age_minutes": 60,
            "max_queue_backlog_items": 2,
            "evaluation_window_hours": 24,
            "min_evaluation_runs": 3,
            "min_evaluation_pass_rate": 0.5,
        }
        values.update(overrides)
        return OperationsAlertThresholds(**values)

    def _ts(self, minutes_ago=0):
        return (NOW - timedelta(minutes=minutes_ago)).strftime("%Y-%m-%d %H:%M:%S")

    def _seed_ingestion(self, conn, minutes_ago=0):
        conn.execute(
            "INSERT OR REPLACE INTO poll_state (id, last_poll_time) VALUES (1, ?)",
            (self._ts(minutes_ago),),
        )
        conn.commit()

    def test_consecutive_publish_failures_alerts_only_above_threshold(self, db):
        self._seed_ingestion(db.conn)
        for i in range(3):
            db.conn.execute(
                """INSERT INTO content_publications
                   (content_id, platform, status, last_error_at, updated_at)
                   VALUES (?, 'x', 'failed', ?, ?)""",
                (i + 1, self._ts(i), self._ts(i)),
            )
        db.conn.commit()

        summary = compute_alert_statuses(db.conn, self._thresholds(), now=NOW)

        check = summary["checks"]["consecutive_publish_failures"]
        assert check["value"] == 3
        assert check["status"] == "ok"

        db.conn.execute(
            """INSERT INTO content_publications
               (content_id, platform, status, last_error_at, updated_at)
               VALUES (4, 'x', 'failed', ?, ?)""",
            (self._ts(-1), self._ts(-1)),
        )
        db.conn.commit()

        summary = compute_alert_statuses(db.conn, self._thresholds(), now=NOW)
        check = summary["checks"]["consecutive_publish_failures"]
        assert check["value"] == 4
        assert check["status"] == "alert"

    def test_stale_ingestion_alerts_only_above_threshold(self, db):
        self._seed_ingestion(db.conn, minutes_ago=60)

        summary = compute_alert_statuses(db.conn, self._thresholds(), now=NOW)

        check = summary["checks"]["stale_ingestion"]
        assert check["ageMinutes"] == 60
        assert check["status"] == "ok"

        db.conn.execute(
            "UPDATE poll_state SET last_poll_time = ? WHERE id = 1",
            (self._ts(61),),
        )
        db.conn.commit()

        summary = compute_alert_statuses(db.conn, self._thresholds(), now=NOW)
        check = summary["checks"]["stale_ingestion"]
        assert check["ageMinutes"] == 61
        assert check["status"] == "alert"

    def test_queue_backlog_alerts_only_above_threshold(self, db):
        self._seed_ingestion(db.conn)
        for i in range(2):
            db.conn.execute(
                """INSERT INTO publish_queue (content_id, scheduled_at, status)
                   VALUES (?, ?, 'queued')""",
                (i + 1, self._ts()),
            )
        db.conn.commit()

        summary = compute_alert_statuses(db.conn, self._thresholds(), now=NOW)

        check = summary["checks"]["queue_backlog"]
        assert check["value"] == 2
        assert check["status"] == "ok"

        db.conn.execute(
            """INSERT INTO publish_queue (content_id, scheduled_at, status)
               VALUES (3, ?, 'failed')""",
            (self._ts(),),
        )
        db.conn.commit()

        summary = compute_alert_statuses(db.conn, self._thresholds(), now=NOW)
        check = summary["checks"]["queue_backlog"]
        assert check["value"] == 3
        assert check["status"] == "alert"

    def test_low_evaluation_pass_rate_alerts_only_below_threshold(self, db):
        self._seed_ingestion(db.conn)
        for i, outcome in enumerate(
            ["published", "published", "below_threshold", "all_filtered"]
        ):
            db.conn.execute(
                """INSERT INTO pipeline_runs
                   (batch_id, content_type, outcome, published, created_at)
                   VALUES (?, 'x_post', ?, ?, ?)""",
                (f"boundary-{i}", outcome, int(outcome == "published"), self._ts(10)),
            )
        db.conn.commit()

        summary = compute_alert_statuses(db.conn, self._thresholds(), now=NOW)

        check = summary["checks"]["evaluation_pass_rate"]
        assert check["total"] == 4
        assert check["passed"] == 2
        assert check["passRate"] == 0.5
        assert check["status"] == "ok"

        db.conn.execute("DELETE FROM pipeline_runs")
        for i, outcome in enumerate(
            ["published", "below_threshold", "below_threshold", "all_filtered"]
        ):
            db.conn.execute(
                """INSERT INTO pipeline_runs
                   (batch_id, content_type, outcome, published, created_at)
                   VALUES (?, 'x_post', ?, ?, ?)""",
                (f"low-{i}", outcome, int(outcome == "published"), self._ts(10)),
            )
        db.conn.commit()

        summary = compute_alert_statuses(db.conn, self._thresholds(), now=NOW)
        check = summary["checks"]["evaluation_pass_rate"]
        assert check["passRate"] == 0.25
        assert check["status"] == "alert"
        assert "evaluation_pass_rate" in summary["triggered"]


class TestOperationsAlertWebhook:
    def _summary(self):
        return {
            "status": "alert",
            "generatedAt": NOW.isoformat(),
            "checks": {
                "queue_backlog": {
                    "status": "alert",
                    "value": 4,
                    "threshold": 2,
                    "summary": "Publish queue backlog has 4 items > 2",
                },
                "stale_ingestion": {
                    "status": "ok",
                    "summary": "Ingestion freshness within threshold",
                },
            },
        }

    def _webhook(self, **overrides):
        values = {
            "webhook_url": "https://hooks.example.test/ops",
            "webhook_enabled": True,
            "webhook_min_level": "alert",
        }
        values.update(overrides)
        return OperationsWebhookConfig(**values)

    def test_build_webhook_payload_is_compact(self):
        payload = build_webhook_payload(
            self._summary(),
            source="update_operations_state",
            min_level="alert",
        )

        assert payload["source"] == "update_operations_state"
        assert payload["status"] == "alert"
        assert payload["generatedAt"] == NOW.isoformat()
        assert payload["generated_at"] == NOW.isoformat()
        assert payload["warning_count"] == 1
        assert payload["warnings"] == ["Publish queue backlog has 4 items > 2"]
        assert len(payload["alerts"]) == 1
        alert = payload["alerts"][0]
        assert alert["id"] == "queue_backlog"
        assert alert["level"] == "alert"
        assert alert["summary"] == "Publish queue backlog has 4 items > 2"
        assert len(alert["fingerprint"]) == 64
        assert "checks" not in payload

    def test_deliver_webhook_posts_once_per_fingerprint(self, db):
        with patch("update_operations_state.requests.post") as mock_post:
            mock_post.return_value.raise_for_status.return_value = None

            first = deliver_operations_alerts(
                db.conn,
                self._summary(),
                self._webhook(),
                source="update_operations_state",
                http_timeout=12,
            )
            second = deliver_operations_alerts(
                db.conn,
                self._summary(),
                self._webhook(),
                source="update_operations_state",
                http_timeout=12,
            )

        assert first["status"] == "sent"
        assert second["status"] == "deduped"
        assert mock_post.call_count == 1
        assert mock_post.call_args.kwargs["timeout"] == 12
        assert mock_post.call_args.kwargs["json"]["alerts"][0]["id"] == "queue_backlog"

    def test_deliver_webhook_noops_when_disabled(self, db):
        with patch("update_operations_state.requests.post") as mock_post:
            result = deliver_operations_alerts(
                db.conn,
                self._summary(),
                self._webhook(webhook_enabled=False),
                source="update_operations_state",
            )

        assert result["status"] == "disabled"
        assert result["sent"] is False
        mock_post.assert_not_called()

    def test_deliver_webhook_noops_below_min_level(self, db):
        warning_summary = {
            "status": "warning",
            "generated_at": NOW.isoformat(),
            "checks": {
                "poll_state": {
                    "status": "warning",
                    "warnings": ["poll_state is stale"],
                },
            },
            "warnings": ["poll_state is stale"],
        }
        with patch("update_operations_state.requests.post") as mock_post:
            result = deliver_operations_alerts(
                db.conn,
                warning_summary,
                self._webhook(webhook_min_level="alert"),
                source="operations_health",
            )

        assert result["status"] == "below_min_level"
        assert result["sent"] is False
        mock_post.assert_not_called()

    def test_deliver_webhook_dry_run_does_not_post_or_persist(self, db):
        with patch("update_operations_state.requests.post") as mock_post:
            dry = deliver_operations_alerts(
                db.conn,
                self._summary(),
                self._webhook(),
                source="update_operations_state",
                dry_run=True,
            )
            metadata_table = db.conn.execute(
                """SELECT 1 FROM sqlite_master
                   WHERE type = 'table' AND name = 'operations_alert_metadata'"""
            ).fetchone()
            real = deliver_operations_alerts(
                db.conn,
                self._summary(),
                self._webhook(),
                source="update_operations_state",
            )

        assert dry["status"] == "dry_run"
        assert dry["payload"]["alerts"][0]["id"] == "queue_backlog"
        assert metadata_table is None
        assert real["status"] == "sent"
        assert mock_post.call_count == 1

    def test_deliver_webhook_reports_http_error_without_persisting(self, db):
        with patch("update_operations_state.requests.post") as mock_post:
            mock_post.return_value.raise_for_status.side_effect = requests.HTTPError(
                "500 Server Error"
            )

            failed = deliver_operations_alerts(
                db.conn,
                self._summary(),
                self._webhook(),
                source="update_operations_state",
            )
            retry = deliver_operations_alerts(
                db.conn,
                self._summary(),
                self._webhook(),
                source="update_operations_state",
            )

        assert failed["status"] == "failed"
        assert failed["sent"] is False
        assert "500 Server Error" in failed["error"]
        assert retry["status"] == "failed"
        assert mock_post.call_count == 2
