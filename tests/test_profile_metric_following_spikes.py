from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.profile_metric_following_spikes import build_profile_metric_following_spikes_report, build_profile_metric_following_spikes_report_from_db

NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "profile_metric_following_spikes.py"
spec = importlib.util.spec_from_file_location("profile_metric_following_spikes_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_profile_metric_spikes():
    report = build_profile_metric_following_spikes_report([
        {"id": 1, "platform": "x", "follower_count": 1000, "following_count": 100, "tweet_count": 10, "fetched_at": "2026-05-18T00:00:00+00:00"},
        {"id": 2, "platform": "x", "follower_count": 1000, "following_count": 500, "tweet_count": 9, "fetched_at": "2026-05-18T00:00:00+00:00"},
        {"id": 3, "platform": "x", "follower_count": 100, "following_count": 1000, "tweet_count": 11, "fetched_at": "2026-05-19T00:00:00+00:00"},
        {"id": 4, "platform": "x", "follower_count": 100, "following_count": 100, "tweet_count": 12, "fetched_at": "2026-05-20T00:00:00+00:00"},
    ], following_delta_threshold=100, ratio_threshold=0.5, now=NOW)
    assert report["artifact_type"] == "profile_metric_following_spikes"
    assert report["totals"]["by_reason"]["following_spike"] >= 1
    assert report["totals"]["by_reason"]["following_drop"] == 1
    assert report["totals"]["by_reason"]["ratio_collapse"] >= 1
    assert report["totals"]["by_reason"]["duplicate_fetched_at"] == 1
    assert report["totals"]["by_reason"]["tweet_count_decrease"] == 1


def test_db_loader_and_cli(tmp_path, capsys):
    path = tmp_path / "profile.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript("""CREATE TABLE profile_metrics (platform TEXT, follower_count INTEGER, following_count INTEGER, tweet_count INTEGER, fetched_at TEXT);
    INSERT INTO profile_metrics VALUES ('x', 100, 10, 10, '2026-05-18T00:00:00+00:00');
    INSERT INTO profile_metrics VALUES ('x', 100, 200, 9, '2026-05-19T00:00:00+00:00');""")
    conn.commit()
    assert build_profile_metric_following_spikes_report_from_db(conn, following_delta_threshold=100, now=NOW)["totals"]["by_reason"]["following_spike"] == 1
    assert script.main(["--db", str(path), "--format", "json", "--following-delta-threshold", "100", "--ratio-threshold", "0.5"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "profile_metric_following_spikes"
