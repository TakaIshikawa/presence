"""Tests for publication channel SLA breach reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.publication_channel_sla_breach import (
    build_publication_channel_sla_breach_report,
    build_publication_channel_sla_breach_report_from_db,
    format_publication_channel_sla_breach_json,
    format_publication_channel_sla_breach_text,
)


NOW = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_channel_sla_breach.py"
spec = importlib.util.spec_from_file_location("publication_channel_sla_breach_script", SCRIPT_PATH)
publication_channel_sla_breach_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_channel_sla_breach_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_computes_breaches_and_oldest_by_channel_state():
    rows = [
        {"item_id": "q1", "channel": "x", "state": "queued", "scheduled_at": (NOW - timedelta(hours=30)).isoformat()},
        {"item_id": "q2", "channel": "x", "state": "queued", "scheduled_at": (NOW - timedelta(hours=50)).isoformat()},
        {"item_id": "q3", "channel": "bluesky", "state": "failed", "created_at": (NOW - timedelta(hours=13)).isoformat()},
        {"item_id": "q4", "channel": "x", "state": "published", "created_at": (NOW - timedelta(hours=100)).isoformat()},
    ]

    report = build_publication_channel_sla_breach_report(rows, now=NOW)

    assert report["total_breaches"] == 3
    summary = {(row["channel"], row["state"]): row for row in report["channel_summary"]}
    assert summary[("x", "queued")]["breach_count"] == 2
    assert summary[("x", "queued")]["oldest_item_id"] == "q2"
    assert json.loads(format_publication_channel_sla_breach_json(report))["threshold_hours"]["queued"] == 24.0


def test_supports_default_and_channel_threshold_overrides():
    rows = [
        {"item_id": "q1", "channel": "x", "state": "queued", "scheduled_at": (NOW - timedelta(hours=20)).isoformat()},
        {"item_id": "q2", "channel": "bluesky", "state": "queued", "scheduled_at": (NOW - timedelta(hours=20)).isoformat()},
    ]

    report = build_publication_channel_sla_breach_report(
        rows,
        threshold_hours={"queued": 24},
        channel_threshold_hours={"x": {"queued": 12}},
        now=NOW,
    )

    assert [row["item_id"] for row in report["breached_items"]] == ["q1"]
    assert "Publication Channel SLA Breach" in format_publication_channel_sla_breach_text(report)


def test_db_loader_reads_publish_queue(db):
    content_id = db.conn.execute(
        """INSERT INTO generated_content (content, content_type, published, created_at)
           VALUES ('Copy', 'x_post', 0, ?)""",
        (NOW.isoformat(),),
    ).lastrowid
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, created_at)
           VALUES (?, ?, 'x', 'queued', ?)""",
        (content_id, (NOW - timedelta(hours=40)).isoformat(), (NOW - timedelta(hours=41)).isoformat()),
    ).lastrowid
    db.conn.commit()

    report = build_publication_channel_sla_breach_report_from_db(db, now=NOW)

    assert report["breached_items"][0]["item_id"] == str(queue_id)
    assert report["total_breaches"] == 1


def test_cli_outputs_json_and_text(db, file_db, capsys):
    for database in (db, file_db):
        content_id = database.conn.execute(
            """INSERT INTO generated_content (content, content_type, published, created_at)
               VALUES ('Copy', 'x_post', 0, ?)""",
            (NOW.isoformat(),),
        ).lastrowid
        database.conn.execute(
            """INSERT INTO publish_queue
               (content_id, scheduled_at, platform, status, created_at)
               VALUES (?, ?, 'x', 'queued', ?)""",
            (content_id, (NOW - timedelta(hours=30)).isoformat(), NOW.isoformat()),
        )
        database.conn.commit()

    with patch.object(
        publication_channel_sla_breach_script,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        publication_channel_sla_breach_script,
        "build_publication_channel_sla_breach_report_from_db",
        wraps=lambda db, **kwargs: build_publication_channel_sla_breach_report_from_db(db, now=NOW, **kwargs),
    ):
        assert publication_channel_sla_breach_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["total_breaches"] == 1

    assert (
        publication_channel_sla_breach_script.main(
            ["--db", str(file_db.db_path), "--threshold-hours", '{"queued": 12}', "--format", "text"]
        )
        == 0
    )
    assert "Total breaches: 1" in capsys.readouterr().out
    assert publication_channel_sla_breach_script.main(["--threshold-hours", "[]"]) == 2
