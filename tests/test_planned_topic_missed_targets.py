"""Tests for planned topic missed target reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.planned_topic_missed_targets import build_planned_topic_missed_targets_report_from_db


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "planned_topic_missed_targets.py"
spec = importlib.util.spec_from_file_location("planned_topic_missed_targets_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE content_campaigns (id INTEGER PRIMARY KEY, name TEXT, status TEXT)")
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY, published INTEGER)")
    conn.execute(
        """CREATE TABLE planned_topics (
               id INTEGER PRIMARY KEY,
               campaign_id INTEGER,
               topic TEXT,
               angle TEXT,
               target_date TEXT,
               status TEXT,
               content_id INTEGER
           )"""
    )
    conn.execute("INSERT INTO content_campaigns VALUES (1, 'Launch', 'active')")
    conn.execute("INSERT INTO content_campaigns VALUES (2, 'Archive', 'paused')")
    conn.execute("INSERT INTO generated_content VALUES (10, 1)")
    conn.execute("INSERT INTO generated_content VALUES (11, 0)")
    return conn


def test_report_returns_overdue_planned_skipped_and_unpublished_content_rows():
    conn = _conn()
    conn.executemany(
        "INSERT INTO planned_topics VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "Topic A", "Angle A", "2026-05-01", "planned", None),
            (2, 1, "Topic B", "Angle B", "2026-05-02", "skipped", None),
            (3, 1, "Topic C", "Angle C", "2026-05-03", "generated", 11),
            (4, 1, "Topic D", "Angle D", "2026-05-03", "generated", 10),
            (5, 1, "Topic E", "Angle E", "2026-05-10", "planned", None),
        ],
    )

    report = build_planned_topic_missed_targets_report_from_db(conn, as_of="2026-05-05", min_days_overdue=1)

    assert [item["planned_topic_id"] for item in report["missed_targets"]] == [1, 2, 3]
    first = report["missed_targets"][0]
    assert first["campaign_name"] == "Launch"
    assert first["topic"] == "Topic A"
    assert first["angle"] == "Angle A"
    assert first["target_date"] == "2026-05-01"
    assert first["status"] == "planned"
    assert first["content_id"] is None
    assert first["published"] is False
    assert first["days_overdue"] == 4


def test_campaign_status_filter_and_cli(tmp_path, capsys):
    conn = _conn()
    conn.executemany(
        "INSERT INTO planned_topics VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "Active", "A", "2026-05-01", "planned", None),
            (2, 2, "Paused", "P", "2026-05-01", "planned", None),
        ],
    )
    conn.commit()
    db_path = tmp_path / "planned.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--as-of", "2026-05-05", "--campaign-status", "paused"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["campaign_name"] for item in payload["missed_targets"]] == ["Archive"]

    assert script.main(["--db", str(db_path), "--as-of", "2026-05-05", "--format", "text"]) == 0
    assert "Planned Topic Missed Targets" in capsys.readouterr().out

    assert script.main(["--db", str(db_path), "--min-days-overdue", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err


def test_missing_table_and_datetime_as_of():
    report = build_planned_topic_missed_targets_report_from_db(
        sqlite3.connect(":memory:"),
        as_of=datetime(2026, 5, 5, tzinfo=timezone.utc),
    )
    assert report["missing_tables"] == ["planned_topics"]
    assert report["missed_targets"] == []
