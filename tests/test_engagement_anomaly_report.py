"""Tests for engagement metric anomaly reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.engagement_anomaly_report import (
    EngagementMetricSnapshot,
    analyze_engagement_metric_snapshots,
    build_engagement_anomaly_report,
    format_engagement_anomaly_report_json,
    format_engagement_anomaly_report_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "engagement_anomaly_report.py"
spec = importlib.util.spec_from_file_location("engagement_anomaly_report_script", SCRIPT_PATH)
engagement_anomaly_report_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(engagement_anomaly_report_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, *, tweet_id: str = "tweet-1") -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=f"Published post {tweet_id}",
        eval_score=8.0,
        eval_feedback="usable",
    )
    db.conn.execute(
        """UPDATE generated_content
           SET published = 1, tweet_id = ?, published_at = ?
           WHERE id = ?""",
        (tweet_id, (NOW - timedelta(days=1)).isoformat(), content_id),
    )
    db.conn.commit()
    return content_id


def _engagement(
    db,
    content_id: int,
    *,
    tweet_id: str = "tweet-1",
    fetched_at: datetime,
    likes: int,
    replies: int = 0,
    reposts: int = 0,
) -> None:
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, reply_count, retweet_count, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (content_id, tweet_id, likes, replies, reposts, fetched_at.isoformat()),
    )
    db.conn.commit()


def test_normal_growth_has_no_anomalies():
    snapshots = [
        EngagementMetricSnapshot(
            post_id="post-a",
            fetched_at="2026-05-02T10:00:00+00:00",
            likes=10,
            replies=2,
            reposts=1,
            impressions=100,
            bookmarks=0,
        ),
        EngagementMetricSnapshot(
            post_id="post-a",
            fetched_at="2026-05-02T12:00:00+00:00",
            likes=13,
            replies=3,
            reposts=1,
            impressions=140,
            bookmarks=1,
        ),
    ]

    assert analyze_engagement_metric_snapshots(
        snapshots,
        jump_thresholds={"likes": 100, "replies": 50, "reposts": 50, "impressions": 1000, "bookmarks": 20},
        rate_thresholds_per_hour={"likes": 100, "replies": 50, "reposts": 50, "impressions": 1000, "bookmarks": 20},
    ) == ()


def test_negative_deltas_are_flagged_with_metric_names():
    anomalies = analyze_engagement_metric_snapshots(
        [
            {"post_id": "post-a", "fetched_at": "2026-05-02T10:00:00+00:00", "likes": 50, "replies": 6},
            {"post_id": "post-a", "fetched_at": "2026-05-02T11:00:00+00:00", "likes": 49, "replies": 3},
        ]
    )

    assert [(item.anomaly_type, item.metric, item.delta, item.severity) for item in anomalies] == [
        ("negative_delta", "likes", -1, "critical"),
        ("negative_delta", "replies", -3, "critical"),
    ]


def test_large_jumps_and_impossible_rates_are_flagged():
    anomalies = analyze_engagement_metric_snapshots(
        [
            {"post_id": "post-a", "fetched_at": "2026-05-02T10:00:00+00:00", "likes": 10},
            {"post_id": "post-a", "fetched_at": "2026-05-02T10:30:00+00:00", "likes": 250},
        ],
        jump_thresholds={"likes": 100},
        rate_thresholds_per_hour={"likes": 300},
    )
    payload = [item.to_dict() for item in anomalies]

    assert [item["anomaly_type"] for item in payload] == ["impossible_rate", "large_jump"]
    assert {item["metric"] for item in payload} == {"likes"}
    assert payload[0]["rate_per_hour"] == 480
    assert payload[1]["delta"] == 240


def test_out_of_order_input_is_sorted_before_comparing():
    anomalies = analyze_engagement_metric_snapshots(
        [
            {"post_id": "post-a", "fetched_at": "2026-05-02T12:00:00+00:00", "likes": 30},
            {"post_id": "post-a", "fetched_at": "2026-05-02T10:00:00+00:00", "likes": 20},
        ]
    )

    assert anomalies == ()


def test_database_report_and_formatters_include_required_summary_fields(db):
    content_id = _content(db, tweet_id="tweet-db")
    _engagement(db, content_id, tweet_id="tweet-db", fetched_at=NOW - timedelta(hours=2), likes=100)
    _engagement(db, content_id, tweet_id="tweet-db", fetched_at=NOW - timedelta(hours=1), likes=40)

    report = build_engagement_anomaly_report(db, days=7, now=NOW)
    payload = json.loads(format_engagement_anomaly_report_json(report))
    text = format_engagement_anomaly_report_text(report)

    assert payload["artifact_type"] == "engagement_anomaly_report"
    assert payload["has_issues"] is True
    assert payload["totals"]["snapshots_scanned"] == 2
    assert payload["totals"]["posts_scanned"] == 1
    anomaly = payload["anomalies"][0]
    assert anomaly["post_id"] == "tweet-db"
    assert anomaly["content_id"] == content_id
    assert anomaly["metric"] == "likes"
    assert anomaly["previous_fetched_at"] == (NOW - timedelta(hours=2)).isoformat()
    assert anomaly["current_fetched_at"] == (NOW - timedelta(hours=1)).isoformat()
    assert anomaly["delta"] == -60
    assert anomaly["severity"] == "critical"
    assert "post_id=tweet-db" in text
    assert "metric=likes" in text
    assert "delta=-60" in text
    assert "severity=critical" in text


def test_cli_json_validation_thresholds_and_fail_on_issues(db, monkeypatch, capsys):
    content_id = _content(db, tweet_id="tweet-cli")
    _engagement(db, content_id, tweet_id="tweet-cli", fetched_at=NOW - timedelta(hours=2), likes=10)
    _engagement(db, content_id, tweet_id="tweet-cli", fetched_at=NOW - timedelta(hours=1), likes=250)
    monkeypatch.setattr(
        engagement_anomaly_report_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        engagement_anomaly_report_script,
        "build_engagement_anomaly_report",
        lambda db, **kwargs: build_engagement_anomaly_report(db, now=NOW, **kwargs),
    )

    assert engagement_anomaly_report_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert engagement_anomaly_report_script.main(["--likes-jump-threshold", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err

    exit_code = engagement_anomaly_report_script.main(
        [
            "--days",
            "7",
            "--likes-jump-threshold",
            "100",
            "--likes-rate-threshold",
            "200",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["filters"]["jump_thresholds"]["likes"] == 100
    assert payload["filters"]["rate_thresholds_per_hour"]["likes"] == 200
    assert {item["anomaly_type"] for item in payload["anomalies"]} == {
        "impossible_rate",
        "large_jump",
    }

    assert engagement_anomaly_report_script.main(["--likes-jump-threshold", "100", "--fail-on-issues"]) == 1
    assert "post_id=tweet-cli" in capsys.readouterr().out
