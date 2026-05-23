from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.reply_platform_cursor_integrity import build_reply_platform_cursor_integrity_report, build_reply_platform_cursor_integrity_report_from_db

NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_platform_cursor_integrity.py"
spec = importlib.util.spec_from_file_location("reply_platform_cursor_integrity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_cursor_integrity():
    report = build_reply_platform_cursor_integrity_report([
        {"platform": "x", "cursor": "10", "last_mention_id": "9", "updated_at": "2026-05-18T00:00:00+00:00"},
        {"platform": "bluesky", "cursor": "", "updated_at": "2026-05-20T00:00:00+00:00"},
    ], max_age_hours=24, now=NOW)
    assert report["artifact_type"] == "reply_platform_cursor_integrity"
    assert report["totals"]["by_reason"]["missing_cursor"] == 1
    assert report["totals"]["by_reason"]["stale_platform_cursor"] == 1
    assert report["totals"]["by_reason"]["legacy_cursor_divergence"] == 1


def test_db_loader_and_cli(tmp_path, capsys):
    path = tmp_path / "reply.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript("""CREATE TABLE reply_state (id INTEGER, last_mention_id TEXT);
    CREATE TABLE platform_reply_state (platform TEXT, cursor TEXT, updated_at TEXT);
    INSERT INTO reply_state VALUES (1, '1');
    INSERT INTO platform_reply_state VALUES ('x', '2', '2026-05-20T00:00:00+00:00');""")
    conn.commit()
    assert build_reply_platform_cursor_integrity_report_from_db(conn, now=NOW)["totals"]["by_reason"]["legacy_cursor_divergence"] == 1
    assert script.main(["--db", str(path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "reply_platform_cursor_integrity"
