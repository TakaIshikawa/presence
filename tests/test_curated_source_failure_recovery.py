"""Tests for curated source failure recovery reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.curated_source_failure_recovery import build_curated_source_failure_recovery_report_from_db


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "curated_source_failure_recovery.py"
spec = importlib.util.spec_from_file_location("curated_source_failure_recovery_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE curated_sources (
               id INTEGER PRIMARY KEY,
               source_type TEXT,
               identifier TEXT,
               status TEXT,
               active INTEGER,
               last_fetch_at TEXT,
               last_success_at TEXT,
               last_failure_at TEXT,
               consecutive_failures INTEGER
           )"""
    )
    return conn


def test_classifies_recovered_recovering_and_still_failing_sources():
    conn = _conn()
    conn.executemany(
        "INSERT INTO curated_sources VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, "rss", "recovered", "active", 1, "2026-05-19T00:00:00+00:00", "2026-05-19T00:00:00+00:00", "2026-05-18T00:00:00+00:00", 0),
            (2, "rss", "recovering", "active", 1, "2026-05-19T00:00:00+00:00", None, "2026-05-18T00:00:00+00:00", 0),
            (3, "api", "failing", "failed", 1, "2026-04-01T00:00:00+00:00", "2026-03-01T00:00:00+00:00", "2026-04-01T00:00:00+00:00", 4),
            (4, "rss", "inactive", "failed", 0, None, None, "2026-04-01T00:00:00+00:00", 4),
        ],
    )

    report = build_curated_source_failure_recovery_report_from_db(conn, lookback_days=30, now=NOW)

    statuses = {item["identifier"]: item["status"] for item in report["sources"]}
    assert statuses == {"recovered": "recovered", "recovering": "recovering", "failing": "still_failing"}
    failing = next(item for item in report["sources"] if item["identifier"] == "failing")
    assert failing["source_type"] == "api"
    assert failing["active"] is True
    assert failing["last_failure_at"] == "2026-04-01T00:00:00+00:00"
    assert failing["consecutive_failures"] == 4
    assert failing["recovery_age_days"] == 49


def test_cli_source_type_filter_and_validation(tmp_path, capsys):
    conn = _conn()
    conn.executemany(
        "INSERT INTO curated_sources VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, "rss", "rss-one", "active", 1, None, "2026-05-19T00:00:00+00:00", "2026-05-18T00:00:00+00:00", 0),
            (2, "api", "api-one", "failed", 1, None, None, "2026-04-01T00:00:00+00:00", 2),
        ],
    )
    conn.commit()
    db_path = tmp_path / "curated.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--source-type", "api"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["identifier"] for item in payload["sources"]] == ["api-one"]

    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Curated Source Failure Recovery" in capsys.readouterr().out

    assert script.main(["--db", str(db_path), "--lookback-days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_missing_table():
    report = build_curated_source_failure_recovery_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert report["missing_tables"] == ["curated_sources"]
    assert report["sources"] == []
