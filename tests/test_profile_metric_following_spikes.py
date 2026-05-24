from datetime import datetime, timezone
import sqlite3

from evaluation.profile_metric_following_spikes import build_profile_metric_following_spikes_report, build_profile_metric_following_spikes_report_from_db

NOW = datetime(2026, 5, 1, tzinfo=timezone.utc)


def test_profile_metric_following_spikes():
    report = build_profile_metric_following_spikes_report([
        {"id": 1, "platform": "x", "fetched_at": "2026-05-01T00:00:00+00:00", "following_count": 100, "follower_count": 1000, "tweet_count": 10},
        {"id": 2, "platform": "x", "fetched_at": "2026-05-01T00:00:00+00:00", "following_count": 300, "follower_count": 100, "tweet_count": 9},
        {"id": 3, "platform": "x", "fetched_at": "2026-05-02T00:00:00+00:00", "following_count": 50, "follower_count": 100, "tweet_count": 9},
    ], following_delta_threshold=100, ratio_threshold=0.5, now=NOW)
    assert {"following_spike", "following_drop", "ratio_collapse", "duplicate_fetched_at", "tweet_count_decrease"} <= {f["reason"] for f in report["findings"]}
    assert build_profile_metric_following_spikes_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["profile_metrics"]
