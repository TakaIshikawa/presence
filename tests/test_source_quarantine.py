"""Tests for curated source quarantine classification and CLI output."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_quarantine import quarantine_curated_sources
from quarantine_sources import main


NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def _set_health(
    db,
    source_type: str,
    identifier: str,
    *,
    failures: int = 0,
    status: str | None = None,
    last_success_at: str | None = None,
) -> None:
    db.conn.execute(
        """UPDATE curated_sources
           SET consecutive_failures = ?,
               last_fetch_status = ?,
               last_success_at = ?
           WHERE source_type = ? AND identifier = ?""",
        (failures, status, last_success_at, source_type, identifier),
    )
    db.conn.commit()


def test_classification_thresholds(db):
    db.sync_config_sources(
        [
            {"identifier": "healthy", "name": "Healthy"},
            {"identifier": "watch", "name": "Watch"},
            {"identifier": "bad", "name": "Bad"},
        ],
        "x_account",
    )
    fresh = (NOW - timedelta(days=2)).isoformat()
    _set_health(db, "x_account", "healthy", last_success_at=fresh)
    _set_health(db, "x_account", "watch", failures=1, status="failure", last_success_at=fresh)
    _set_health(db, "x_account", "bad", failures=3, status="failure", last_success_at=fresh)

    report = quarantine_curated_sources(
        db,
        failure_threshold=3,
        stale_days=30,
        now=NOW,
    )

    by_identifier = {source["identifier"]: source for source in report["sources"]}
    assert by_identifier["healthy"]["classification"] == "healthy"
    assert by_identifier["watch"]["classification"] == "watch"
    assert by_identifier["bad"]["classification"] == "quarantine"
    assert "consecutive failures 3 >= threshold 3" == by_identifier["bad"]["reason"]
    assert report["counts"] == {"healthy": 1, "watch": 1, "quarantine": 1}


def test_stale_active_source_is_quarantined(db):
    db.sync_config_sources(
        [{"identifier": "example.com", "name": "Example", "feed_url": "https://example.com/rss"}],
        "blog",
    )
    _set_health(
        db,
        "blog",
        "example.com",
        last_success_at=(NOW - timedelta(days=45)).isoformat(),
        status="success",
    )

    report = quarantine_curated_sources(
        db,
        failure_threshold=3,
        stale_days=30,
        now=NOW,
    )

    source = report["sources"][0]
    assert source["classification"] == "quarantine"
    assert source["would_pause"] is True
    assert source["reason"] == "last success older than 30 days"


def test_apply_pauses_only_quarantined_active_sources(db):
    db.sync_config_sources(
        [
            {"identifier": "bad_active", "name": "Bad Active"},
            {"identifier": "watch_active", "name": "Watch Active"},
        ],
        "x_account",
    )
    db.insert_candidate_source("x_account", "candidate")
    db.insert_candidate_source("x_account", "rejected")
    rejected = [
        row for row in db.get_candidate_sources("x_account") if row["identifier"] == "rejected"
    ][0]
    db.reject_candidate(rejected["id"])

    fresh = (NOW - timedelta(days=1)).isoformat()
    for identifier in ["bad_active", "candidate", "rejected"]:
        _set_health(
            db,
            "x_account",
            identifier,
            failures=5,
            status="failure",
            last_success_at=fresh,
        )
    _set_health(
        db,
        "x_account",
        "watch_active",
        failures=1,
        status="failure",
        last_success_at=fresh,
    )

    report = quarantine_curated_sources(
        db,
        failure_threshold=3,
        stale_days=30,
        apply=True,
        now=NOW,
    )

    assert report["planned_pauses"] == 1
    assert report["updated"] == 1
    assert db.get_curated_source("x_account", "bad_active")["status"] == "paused"
    assert db.get_curated_source("x_account", "bad_active")["active"] == 0
    assert db.get_curated_source("x_account", "watch_active")["status"] == "active"
    assert db.get_curated_source("x_account", "candidate")["status"] == "candidate"
    assert db.get_curated_source("x_account", "rejected")["status"] == "rejected"


def test_dry_run_reports_json_without_modifying_database(db, capsys):
    db.sync_config_sources(
        [{"identifier": "example.com", "name": "Example", "feed_url": "https://example.com/rss"}],
        "blog",
    )
    _set_health(
        db,
        "blog",
        "example.com",
        failures=4,
        status="failure",
        last_success_at=(NOW - timedelta(days=1)).isoformat(),
    )
    row = db.get_curated_source("blog", "example.com")

    mock_config = MagicMock()
    with patch("quarantine_sources.script_context") as mock_context, patch(
        "knowledge.source_quarantine.datetime"
    ) as mock_datetime:
        mock_datetime.now.return_value = NOW
        mock_datetime.fromisoformat.side_effect = datetime.fromisoformat
        mock_context.return_value.__enter__ = lambda self: (mock_config, db)
        mock_context.return_value.__exit__ = lambda self, *args: None

        main(
            [
                "report",
                "--failure-threshold",
                "3",
                "--stale-days",
                "30",
                "--dry-run",
                "--source-type",
                "blog",
                "--json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["applied"] is False
    assert payload["planned_pauses"] == 1
    assert payload["updated"] == 0
    source = payload["sources"][0]
    assert source["id"] == row["id"]
    assert source["identifier"] == "example.com"
    assert source["classification"] == "quarantine"
    assert source["reason"] == "consecutive failures 4 >= threshold 3"
    assert source["risk"] == "quarantine"
    assert source["score"] == 100
    row = db.get_curated_source("blog", "example.com")
    assert row["status"] == "active"
    assert row["active"] == 1


def test_pause_dry_run_prints_candidates_without_modifying_database(db, capsys):
    db.sync_config_sources(
        [{"identifier": "example.com", "name": "Example"}],
        "blog",
    )
    _set_health(
        db,
        "blog",
        "example.com",
        failures=4,
        status="failure",
        last_success_at=(NOW - timedelta(days=1)).isoformat(),
    )

    mock_config = MagicMock()
    with patch("quarantine_sources.script_context") as mock_context:
        mock_context.return_value.__enter__ = lambda self: (mock_config, db)
        mock_context.return_value.__exit__ = lambda self, *args: None

        main(["pause", "--failure-threshold", "3", "--dry-run", "--source-type", "blog"])

    output = capsys.readouterr().out
    assert "CURATED SOURCE QUARANTINE (DRY RUN)" in output
    assert "Planned pauses: 1; updated: 0" in output
    assert "example.com" in output
    assert "consecutive failures 4 >= threshold 3" in output
    row = db.get_curated_source("blog", "example.com")
    assert row["status"] == "active"
    assert row["active"] == 1


@pytest.mark.parametrize(
    "argv",
    [
        ["report", "--failure-threshold", "-1"],
        ["pause", "--stale-days", "-1"],
        ["report", "--source-type", "podcast"],
    ],
)
def test_invalid_cli_arguments_fail_with_argparse_error(argv):
    with patch("quarantine_sources.script_context") as mock_context, pytest.raises(
        SystemExit
    ) as exc:
        main(argv)

    assert exc.value.code == 2
    mock_context.assert_not_called()


def test_pause_subcommand_marks_active_source_inactive(db, capsys):
    db.sync_config_sources(
        [{"identifier": "example.com", "name": "Example"}],
        "blog",
    )
    _set_health(
        db,
        "blog",
        "example.com",
        failures=4,
        status="failure",
        last_success_at=(NOW - timedelta(days=1)).isoformat(),
    )
    db.conn.execute(
        """UPDATE curated_sources
           SET last_failure_at = ?
           WHERE source_type = 'blog' AND identifier = 'example.com'""",
        (NOW.isoformat(),),
    )
    db.conn.commit()

    mock_config = MagicMock()
    with patch("quarantine_sources.script_context") as mock_context, patch(
        "sys.argv",
        ["quarantine_sources.py", "pause", "--failure-threshold", "3"],
    ):
        mock_context.return_value.__enter__ = lambda self: (mock_config, db)
        mock_context.return_value.__exit__ = lambda self, *args: None

        main()

    row = db.get_curated_source("blog", "example.com")
    assert row["status"] == "paused"
    assert row["active"] == 0
    assert row["reviewed_at"] is not None


def test_resume_dry_run_does_not_reactivate_paused_source(db, capsys):
    db.sync_config_sources(
        [{"identifier": "example.com", "name": "Example"}],
        "blog",
    )
    row = db.get_curated_source("blog", "example.com")
    db.pause_curated_sources_by_ids([row["id"]])

    mock_config = MagicMock()
    with patch("quarantine_sources.script_context") as mock_context, patch(
        "sys.argv",
        ["quarantine_sources.py", "resume", "example.com", "--dry-run", "--json"],
    ):
        mock_context.return_value.__enter__ = lambda self: (mock_config, db)
        mock_context.return_value.__exit__ = lambda self, *args: None

        main()

    payload = json.loads(capsys.readouterr().out)
    assert payload["planned"] == 1
    assert payload["updated"] == 0
    row = db.get_curated_source("blog", "example.com")
    assert row["status"] == "paused"
    assert row["active"] == 0


def test_resume_reactivates_and_clears_reviewed_at(db, capsys):
    db.sync_config_sources(
        [{"identifier": "example.com", "name": "Example"}],
        "blog",
    )
    row = db.get_curated_source("blog", "example.com")
    db.pause_curated_sources_by_ids([row["id"]])

    mock_config = MagicMock()
    with patch("quarantine_sources.script_context") as mock_context, patch(
        "sys.argv",
        ["quarantine_sources.py", "resume", str(row["id"])],
    ):
        mock_context.return_value.__enter__ = lambda self: (mock_config, db)
        mock_context.return_value.__exit__ = lambda self, *args: None

        main()

    row = db.get_curated_source("blog", "example.com")
    assert row["status"] == "active"
    assert row["active"] == 1
    assert row["reviewed_at"] is None


def test_reject_marks_source_inactive(db, capsys):
    db.sync_config_sources(
        [{"identifier": "example.com", "name": "Example"}],
        "blog",
    )

    mock_config = MagicMock()
    with patch("quarantine_sources.script_context") as mock_context, patch(
        "sys.argv",
        ["quarantine_sources.py", "reject", "example.com"],
    ):
        mock_context.return_value.__enter__ = lambda self: (mock_config, db)
        mock_context.return_value.__exit__ = lambda self, *args: None

        main()

    row = db.get_curated_source("blog", "example.com")
    assert row["status"] == "rejected"
    assert row["active"] == 0
    assert row["reviewed_at"] is not None
