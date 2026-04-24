"""Tests for curated source health enforcement."""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_health import (
    build_pause_decisions,
    pause_failing_sources,
    source_failure_threshold_from_config,
)
from source_health import main


def _insert_source(
    db,
    identifier: str,
    *,
    source_type: str = "blog",
    failures: int = 0,
    last_failure_at: str | None = None,
    last_success_at: str | None = None,
    last_error: str | None = None,
    status: str = "active",
) -> int:
    db.sync_config_sources(
        [{"identifier": identifier, "name": identifier, "license": "open"}],
        source_type,
    )
    row = db.get_curated_source(source_type, identifier)
    db.conn.execute(
        """UPDATE curated_sources
           SET consecutive_failures = ?,
               last_failure_at = ?,
               last_success_at = ?,
               last_error = ?,
               status = ?
           WHERE id = ?""",
        (failures, last_failure_at, last_success_at, last_error, status, row["id"]),
    )
    db.conn.commit()
    return row["id"]


class TestSourceHealthDecisions:
    def test_threshold_and_last_failure_recency_are_required(self):
        now = datetime.now(timezone.utc)
        stale_failure = (now - timedelta(hours=2)).isoformat()
        newer_success = now.isoformat()
        recent_failure = now.isoformat()
        older_success = (now - timedelta(hours=2)).isoformat()

        rows = [
            {
                "id": 1,
                "source_type": "blog",
                "identifier": "below.example",
                "consecutive_failures": 2,
                "last_failure_at": recent_failure,
                "last_success_at": None,
                "status": "active",
            },
            {
                "id": 2,
                "source_type": "blog",
                "identifier": "recovered.example",
                "consecutive_failures": 4,
                "last_failure_at": stale_failure,
                "last_success_at": newer_success,
                "status": "active",
            },
            {
                "id": 3,
                "source_type": "blog",
                "identifier": "failing.example",
                "consecutive_failures": 3,
                "last_failure_at": recent_failure,
                "last_success_at": older_success,
                "last_error": "bad xml",
                "status": "active",
            },
        ]

        decisions = build_pause_decisions(rows, threshold=3)

        assert [decision.identifier for decision in decisions] == ["failing.example"]
        assert decisions[0].threshold == 3
        assert decisions[0].last_error == "bad xml"

    def test_config_threshold_default(self):
        assert source_failure_threshold_from_config(SimpleNamespace(curated_sources=None)) == 3

        config = SimpleNamespace(
            curated_sources=SimpleNamespace(source_failure_threshold=2)
        )
        assert source_failure_threshold_from_config(config) == 2


class TestSourceHealthDatabase:
    def test_pause_failing_sources_updates_status_and_reviewed_at(self, db):
        now = datetime.now(timezone.utc).isoformat()
        source_id = _insert_source(
            db,
            "example.com",
            failures=3,
            last_failure_at=now,
            last_error="timeout",
        )

        decisions = pause_failing_sources(db, 3)

        assert [decision.id for decision in decisions] == [source_id]
        row = db.get_curated_source("blog", "example.com")
        assert row["status"] == "paused"
        assert row["reviewed_at"] is not None

    def test_dry_run_does_not_pause(self, db):
        now = datetime.now(timezone.utc).isoformat()
        _insert_source(
            db,
            "example.com",
            failures=3,
            last_failure_at=now,
            last_error="timeout",
        )

        decisions = pause_failing_sources(db, 3, dry_run=True)

        assert len(decisions) == 1
        assert db.get_curated_source("blog", "example.com")["status"] == "active"

    def test_restore_by_id_or_identifier(self, db):
        first_id = _insert_source(db, "one.example", status="paused")
        _insert_source(db, "two.example", status="paused")

        restored = db.restore_curated_sources(
            source_ids=[first_id],
            identifiers=["two.example"],
        )

        assert restored == 2
        assert db.get_curated_source("blog", "one.example")["status"] == "active"
        assert db.get_curated_source("blog", "two.example")["status"] == "active"


class TestSourceHealthCli:
    def test_check_json_lists_candidates(self, db, capsys):
        now = datetime.now(timezone.utc).isoformat()
        _insert_source(
            db,
            "example.com",
            failures=4,
            last_failure_at=now,
            last_error="timeout",
        )
        config = SimpleNamespace(
            curated_sources=SimpleNamespace(source_failure_threshold=4)
        )

        with (
            patch("source_health.script_context") as mock_context,
            patch("sys.argv", ["source_health.py", "check", "--dry-run", "--json"]),
        ):
            mock_context.return_value.__enter__.return_value = (config, db)
            mock_context.return_value.__exit__.return_value = None

            main()

        payload = json.loads(capsys.readouterr().out)
        assert payload["threshold"] == 4
        assert payload["candidates"][0]["identifier"] == "example.com"
        assert payload["candidates"][0]["consecutive_failures"] == 4
        assert payload["candidates"][0]["last_error"] == "timeout"

    def test_pause_command_updates_matching_sources(self, db, capsys):
        now = datetime.now(timezone.utc).isoformat()
        _insert_source(db, "example.com", failures=3, last_failure_at=now)
        config = SimpleNamespace(
            curated_sources=SimpleNamespace(source_failure_threshold=3)
        )

        with (
            patch("source_health.script_context") as mock_context,
            patch("source_health.update_monitoring") as mock_monitoring,
            patch("sys.argv", ["source_health.py", "pause"]),
        ):
            mock_context.return_value.__enter__.return_value = (config, db)
            mock_context.return_value.__exit__.return_value = None

            main()

        assert "paused" in capsys.readouterr().out
        assert db.get_curated_source("blog", "example.com")["status"] == "paused"
        mock_monitoring.assert_called_once_with("source_health")

    def test_restore_command_reactivates_by_identifier(self, db, capsys):
        _insert_source(db, "example.com", status="paused")
        config = SimpleNamespace(
            curated_sources=SimpleNamespace(source_failure_threshold=3)
        )

        with (
            patch("source_health.script_context") as mock_context,
            patch("source_health.update_monitoring") as mock_monitoring,
            patch("sys.argv", ["source_health.py", "restore", "example.com"]),
        ):
            mock_context.return_value.__enter__.return_value = (config, db)
            mock_context.return_value.__exit__.return_value = None

            main()

        assert "restored" in capsys.readouterr().out
        assert db.get_curated_source("blog", "example.com")["status"] == "active"
        mock_monitoring.assert_called_once_with("source_health")
