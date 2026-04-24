"""Tests for audit_publish_queue.py."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from audit_publish_queue import main, parse_args


def _insert_content(db, content: str = "Queued post") -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, 'x_post', 7.0, 0)""",
        (content,),
    ).lastrowid


def _queue_item(db, *, scheduled_at: datetime, platform: str = "x") -> int:
    content_id = _insert_content(db)
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, 'queued')""",
        (content_id, scheduled_at.isoformat(), platform),
    ).lastrowid
    db.conn.commit()
    return queue_id


@contextmanager
def _script_context(db, config=None):
    config = config or SimpleNamespace(
        publishing=SimpleNamespace(
            daily_platform_limits={"x": 3, "bluesky": 3},
            embargo_windows=[],
        )
    )
    yield config, db


def test_parse_args_defaults_to_dry_run():
    args = parse_args([])

    assert args.window_minutes == 30
    assert args.dry_run is False
    assert args.apply_holds is False


def test_main_dry_run_reports_collisions_without_writing(db, capsys):
    base = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    _queue_item(db, scheduled_at=base, platform="x")
    second = _queue_item(db, scheduled_at=base + timedelta(minutes=10), platform="x")

    with patch("audit_publish_queue.script_context", return_value=_script_context(db)):
        result = main(["--dry-run", "--window-minutes", "30"])

    output = capsys.readouterr().out
    assert result == 0
    assert "Found 1 publish queue scheduling collision group" in output
    assert "Dry run: no queue rows were changed." in output
    assert db.get_publish_queue_item(second)["status"] == "queued"


def test_main_apply_holds_marks_affected_rows(db, capsys):
    base = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    first = _queue_item(db, scheduled_at=base, platform="x")
    second = _queue_item(db, scheduled_at=base + timedelta(minutes=10), platform="x")

    with patch("audit_publish_queue.script_context", return_value=_script_context(db)):
        result = main(["--apply-holds", "--window-minutes", "30"])

    output = capsys.readouterr().out
    assert result == 0
    assert "Applied holds to 1 queued item" in output
    assert db.get_publish_queue_item(first)["status"] == "queued"
    assert db.get_publish_queue_item(second)["status"] == "held"


def test_main_dry_run_overrides_apply_holds(db, capsys):
    base = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    _queue_item(db, scheduled_at=base, platform="x")
    second = _queue_item(db, scheduled_at=base + timedelta(minutes=10), platform="x")

    with patch("audit_publish_queue.script_context", return_value=_script_context(db)):
        result = main(["--apply-holds", "--dry-run", "--window-minutes", "30"])

    output = capsys.readouterr().out
    assert result == 0
    assert "Dry run: no queue rows were changed." in output
    assert db.get_publish_queue_item(second)["status"] == "queued"


def test_main_json_outputs_collision_payload(db, capsys):
    base = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    _queue_item(db, scheduled_at=base, platform="all")
    _queue_item(db, scheduled_at=base + timedelta(minutes=10), platform="x")

    with patch("audit_publish_queue.script_context", return_value=_script_context(db)):
        result = main(["--json", "--window-minutes", "30"])

    payload = json.loads(capsys.readouterr().out)
    assert result == 0
    assert payload["collision_count"] == 1
    assert payload["collision_groups"][0]["platform"] == "x"
    assert payload["applied_holds"] == []


def test_main_rejects_non_positive_window(capsys):
    result = main(["--window-minutes", "0"])

    captured = capsys.readouterr()
    assert result == 2
    assert "--window-minutes must be positive" in captured.err
