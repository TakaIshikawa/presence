"""Tests for source freshness distribution reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.source_freshness_distribution import (
    build_source_freshness_distribution_report,
    build_source_freshness_distribution_report_from_db,
    format_source_freshness_distribution_json,
    format_source_freshness_distribution_text,
)


NOW = datetime(2026, 5, 18, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_freshness_distribution.py"
spec = importlib.util.spec_from_file_location("source_freshness_distribution_script", SCRIPT_PATH)
source_freshness_distribution_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(source_freshness_distribution_script)


@contextmanager
def _script_context(conn):
    yield SimpleNamespace(), conn


def _row(content_id: str, age_days: int, *, channel: str = "x", content_type: str = "post") -> dict:
    return {
        "content_id": content_id,
        "channel": channel,
        "content_type": content_type,
        "generated_at": NOW.isoformat(),
        "source_timestamp": (NOW - timedelta(days=age_days)).isoformat(),
        "source_id": f"src-{content_id}",
    }


def test_empty_input_has_zero_buckets():
    report = build_source_freshness_distribution_report([], now=NOW)

    assert report["empty_state"]["is_empty"] is True
    assert [row["count"] for row in report["buckets"]] == [0, 0, 0, 0]
    assert report["totals"]["evidence_count"] == 0


def test_multiple_buckets_counts_percentages_and_stale_examples():
    report = build_source_freshness_distribution_report(
        [_row("a", 0), _row("b", 2), _row("c", 8), _row("d", 31)],
        now=NOW,
    )

    buckets = {row["bucket"]: row for row in report["buckets"]}

    assert buckets["0-1d"]["count"] == 1
    assert buckets["2-7d"]["count"] == 1
    assert buckets["8-30d"]["count"] == 1
    assert buckets["31d+"]["count"] == 1
    assert buckets["31d+"]["percentage"] == 0.25
    assert report["examples"][0]["content_id"] == "d"


def test_filters_by_channel_and_content_type_when_metadata_exists():
    report = build_source_freshness_distribution_report(
        [
            _row("a", 1, channel="newsletter", content_type="digest"),
            _row("b", 40, channel="x", content_type="post"),
            _row("c", 5, channel="newsletter", content_type="post"),
        ],
        channel="newsletter",
        content_type="digest",
        now=NOW,
    )

    assert report["totals"]["evidence_count"] == 1
    assert report["groups"]["by_channel"][0]["channel"] == "newsletter"
    assert report["groups"]["by_content_type"][0]["content_type"] == "digest"


def test_db_loader_json_text_and_cli_formatting(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE source_evidence (
            id INTEGER PRIMARY KEY,
            content_id TEXT,
            content_type TEXT,
            channel TEXT,
            source_id TEXT,
            generated_at TEXT,
            source_timestamp TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO source_evidence
           (content_id, content_type, channel, source_id, generated_at, source_timestamp)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("post-1", "post", "x", "src-1", NOW.isoformat(), (NOW - timedelta(days=45)).isoformat()),
    )
    conn.commit()

    report = build_source_freshness_distribution_report_from_db(conn, now=NOW)

    assert json.loads(format_source_freshness_distribution_json(report))["artifact_type"] == "source_freshness_distribution"
    assert "31d+: count=1" in format_source_freshness_distribution_text(report)
    monkeypatch.setattr(source_freshness_distribution_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        source_freshness_distribution_script,
        "build_source_freshness_distribution_report_from_db",
        lambda db, **kwargs: build_source_freshness_distribution_report_from_db(db, now=NOW, **kwargs),
    )
    assert source_freshness_distribution_script.main(["--table", "--channel", "x"]) == 0
    assert "Source Freshness Distribution" in capsys.readouterr().out
