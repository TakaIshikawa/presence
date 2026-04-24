from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from export_calendar import main, parse_args


@contextmanager
def _script_context(db):
    yield None, db


def test_parse_args_defaults_to_planned_topics_only():
    args = parse_args(["--start-date", "2026-04-24"])

    assert args.days == 30
    assert args.output is None
    assert args.include_queue is False
    assert args.start_date.isoformat() == "2026-04-24"


def test_parse_args_rejects_non_positive_days():
    with pytest.raises(SystemExit):
        parse_args(["--days", "0"])


def test_main_writes_stdout_with_deterministic_start_date(db, capsys):
    topic_id = db.insert_planned_topic(
        topic="testing",
        angle="CLI stdout",
        target_date="2026-04-25",
    )

    with patch("export_calendar.script_context", return_value=_script_context(db)):
        main(["--start-date", "2026-04-24", "--days", "3"])

    out = capsys.readouterr().out
    assert out.startswith("BEGIN:VCALENDAR\r\n")
    assert out.count("BEGIN:VEVENT") == 1
    assert f"UID:planned-topic-{topic_id}@presence.local" in out
    assert "DTSTART;VALUE=DATE:20260425" in out


def test_main_writes_file_and_includes_queue_when_requested(db, tmp_path, capsys):
    content_id = db.conn.execute(
        """INSERT INTO generated_content
           (content_type, content, eval_score, published)
           VALUES ('x_post', 'Queued from CLI', 7.0, 0)"""
    ).lastrowid
    queue_id = db.queue_for_publishing(
        content_id,
        "2026-04-25T10:00:00+00:00",
        platform="bluesky",
    )
    output_path = tmp_path / "calendar.ics"

    with patch("export_calendar.script_context", return_value=_script_context(db)):
        main(
            [
                "--start-date",
                "2026-04-24",
                "--days",
                "3",
                "--include-queue",
                "--output",
                str(output_path),
            ]
        )

    assert capsys.readouterr().out == ""
    exported = output_path.read_text(encoding="utf-8")
    assert f"UID:publish-queue-{queue_id}@presence.local" in exported
    assert "DTSTART:20260425T100000Z" in exported
    assert "SUMMARY:Publish (bluesky): x_post" in exported
