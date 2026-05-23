from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.mastodon_engagement_snapshot_drift import build_mastodon_engagement_snapshot_drift_report, build_mastodon_engagement_snapshot_drift_report_from_db


NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "mastodon_engagement_snapshot_drift.py"
spec = importlib.util.spec_from_file_location("mastodon_engagement_snapshot_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_snapshot_drift_reasons():
    report = build_mastodon_engagement_snapshot_drift_report([
        {"id": 1, "content_id": 1, "post_id": "p", "favourite_count": 4, "boost_count": 1, "reply_count": 1, "raw_metrics": "{}", "fetched_at": "2026-05-18T00:00:00+00:00"},
        {"id": 2, "content_id": 1, "post_id": "p", "favourite_count": 2, "boost_count": 1, "reply_count": 1, "raw_metrics": "{bad", "fetched_at": "2026-05-18T00:00:00+00:00"},
        {"id": 3, "content_id": 1, "post_id": "p", "favourite_count": 5, "boost_count": 1, "reply_count": 1, "raw_metrics": "{}", "fetched_at": "2026-05-20T12:00:00+00:00"},
    ], max_gap_hours=24, now=NOW)
    assert report["artifact_type"] == "mastodon_engagement_snapshot_drift"
    assert report["totals"]["by_reason"] == {"count_drop": 1, "duplicate_fetched_at": 1, "invalid_raw_metrics": 1, "stale_gap": 1}


def test_db_loader_missing_schema_and_cli(tmp_path, capsys):
    path = tmp_path / "m.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript("""CREATE TABLE mastodon_engagement (content_id INTEGER, post_id TEXT, favourite_count INTEGER, boost_count INTEGER, reply_count INTEGER, raw_metrics TEXT, fetched_at TEXT);
    INSERT INTO mastodon_engagement VALUES (1, 'p', 2, 0, 0, '{}', '2026-05-18T00:00:00+00:00');""")
    conn.commit()
    assert build_mastodon_engagement_snapshot_drift_report_from_db(conn, now=NOW)["totals"]["row_count"] == 1
    assert build_mastodon_engagement_snapshot_drift_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["mastodon_engagement"]
    assert script.main(["--db", str(path), "--format", "json", "--max-gap-hours", "1"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "mastodon_engagement_snapshot_drift"
    assert script.main(["--limit", "0"]) == 2
