"""Tests for content source freshness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import pytest
import sqlite3
from types import SimpleNamespace

from evaluation.content_source_freshness import (
    build_content_source_freshness_report,
    format_content_source_freshness_csv,
    format_content_source_freshness_json,
)


NOW = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "content_source_freshness.py"
)
spec = importlib.util.spec_from_file_location(
    "content_source_freshness_script",
    SCRIPT_PATH,
)
content_source_freshness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_source_freshness_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_claude_message(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    message_uuid: str,
    timestamp: datetime,
) -> int:
    cursor = conn.execute(
        """INSERT INTO claude_messages (session_id, message_uuid, project_path, timestamp, prompt_text)
           VALUES (?, ?, ?, ?, ?)""",
        (session_id, message_uuid, "/test", timestamp.isoformat(), "test prompt"),
    )
    conn.commit()
    return cursor.lastrowid


def _add_github_commit(
    conn: sqlite3.Connection,
    *,
    repo_name: str,
    commit_sha: str,
    timestamp: datetime,
) -> int:
    cursor = conn.execute(
        """INSERT INTO github_commits (repo_name, commit_sha, commit_message, timestamp, author)
           VALUES (?, ?, ?, ?, ?)""",
        (repo_name, commit_sha, "test commit", timestamp.isoformat(), "test-author"),
    )
    conn.commit()
    return cursor.lastrowid


def _add_github_activity(
    conn: sqlite3.Connection,
    *,
    repo_name: str,
    activity_type: str,
    number: str,
    ingested_at: datetime,
) -> int:
    cursor = conn.execute(
        """INSERT INTO github_activity (repo_name, activity_type, number, title, updated_at, ingested_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (repo_name, activity_type, number, "test activity", ingested_at.isoformat(), ingested_at.isoformat()),
    )
    conn.commit()
    return cursor.lastrowid


def _add_knowledge(
    conn: sqlite3.Connection,
    *,
    source_type: str,
    source_id: str,
    ingested_at: datetime,
) -> int:
    cursor = conn.execute(
        """INSERT INTO knowledge (source_type, source_id, content, ingested_at)
           VALUES (?, ?, ?, ?)""",
        (source_type, source_id, "test content", ingested_at.isoformat()),
    )
    conn.commit()
    return cursor.lastrowid


def _add_curated_source(
    conn: sqlite3.Connection,
    *,
    source_type: str,
    identifier: str,
    last_success_at: datetime | None = None,
    status: str = "active",
) -> int:
    cursor = conn.execute(
        """INSERT INTO curated_sources (source_type, identifier, last_success_at, status)
           VALUES (?, ?, ?, ?)""",
        (source_type, identifier, last_success_at.isoformat() if last_success_at else None, status),
    )
    conn.commit()
    return cursor.lastrowid


def test_reports_fresh_and_stale_sources():
    """Sources are marked as stale based on threshold."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    # Fresh source (ingested 2 days ago)
    _add_claude_message(
        conn,
        session_id="fresh-session",
        message_uuid="msg-1",
        timestamp=NOW - timedelta(days=2),
    )

    # Stale source (ingested 10 days ago)
    _add_claude_message(
        conn,
        session_id="stale-session",
        message_uuid="msg-2",
        timestamp=NOW - timedelta(days=10),
    )

    report = build_content_source_freshness_report(
        conn,
        stale_threshold_days=7,
        now=NOW,
    )

    assert len(report.rows) == 2
    rows_by_id = {row.source_identifier: row for row in report.rows}

    assert rows_by_id["fresh-session"].is_stale is False
    assert rows_by_id["fresh-session"].days_since_ingestion == 2

    assert rows_by_id["stale-session"].is_stale is True
    assert rows_by_id["stale-session"].days_since_ingestion == 10

    # Stale sources should come first in sorted output
    assert report.rows[0].source_identifier == "stale-session"
    assert report.rows[1].source_identifier == "fresh-session"


def test_groups_sources_by_type():
    """Report groups sources by their type."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    _add_claude_message(conn, session_id="s1", message_uuid="m1", timestamp=NOW - timedelta(days=1))
    _add_claude_message(conn, session_id="s2", message_uuid="m2", timestamp=NOW - timedelta(days=2))
    _add_github_commit(conn, repo_name="repo1", commit_sha="sha1", timestamp=NOW - timedelta(days=1))
    _add_knowledge(conn, source_type="own_post", source_id="post-1", ingested_at=NOW - timedelta(days=1))

    report = build_content_source_freshness_report(conn, now=NOW)

    assert report.grouped_by_source_type["claude_messages"] == 2
    assert report.grouped_by_source_type["github_commits"] == 1
    assert report.grouped_by_source_type["knowledge"] == 1


def test_filters_by_source_type():
    """Report can filter by specific source type."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    _add_claude_message(conn, session_id="s1", message_uuid="m1", timestamp=NOW - timedelta(days=1))
    _add_github_commit(conn, repo_name="repo1", commit_sha="sha1", timestamp=NOW - timedelta(days=1))

    report = build_content_source_freshness_report(
        conn,
        source_type="claude_messages",
        now=NOW,
    )

    assert len(report.rows) == 1
    assert report.rows[0].source_type == "claude_messages"


def test_calculates_totals_accurately():
    """Report totals count sources and staleness correctly."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    # 3 fresh sources
    _add_claude_message(conn, session_id="s1", message_uuid="m1", timestamp=NOW - timedelta(days=1))
    _add_claude_message(conn, session_id="s2", message_uuid="m2", timestamp=NOW - timedelta(days=2))
    _add_github_commit(conn, repo_name="repo1", commit_sha="sha1", timestamp=NOW - timedelta(days=3))

    # 2 stale sources
    _add_github_commit(conn, repo_name="repo2", commit_sha="sha2", timestamp=NOW - timedelta(days=10))
    _add_knowledge(conn, source_type="own_post", source_id="post-1", ingested_at=NOW - timedelta(days=15))

    report = build_content_source_freshness_report(
        conn,
        stale_threshold_days=7,
        now=NOW,
    )

    assert report.totals["source_count"] == 5
    assert report.totals["active_count"] == 3
    assert report.totals["stale_count"] == 2


def test_handles_github_activity_by_type():
    """GitHub activity is tracked per repo and activity type."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    _add_github_activity(
        conn,
        repo_name="myrepo",
        activity_type="issue",
        number="1",
        ingested_at=NOW - timedelta(days=2),
    )
    _add_github_activity(
        conn,
        repo_name="myrepo",
        activity_type="pull_request",
        number="10",
        ingested_at=NOW - timedelta(days=3),
    )

    report = build_content_source_freshness_report(conn, now=NOW)

    assert len(report.rows) == 2
    identifiers = {row.source_identifier for row in report.rows}
    assert "myrepo:issue" in identifiers
    assert "myrepo:pull_request" in identifiers


def test_handles_knowledge_by_source_type():
    """Knowledge entries are grouped by source_type."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    _add_knowledge(conn, source_type="own_post", source_id="p1", ingested_at=NOW - timedelta(days=1))
    _add_knowledge(conn, source_type="own_post", source_id="p2", ingested_at=NOW - timedelta(days=5))
    _add_knowledge(conn, source_type="curated_article", source_id="a1", ingested_at=NOW - timedelta(days=3))

    report = build_content_source_freshness_report(conn, now=NOW)

    # Should have 2 groups: own_post and curated_article
    assert len(report.rows) == 2
    rows_by_id = {row.source_identifier: row for row in report.rows}

    # own_post should show most recent of the 2 (1 day ago, not 5)
    assert rows_by_id["own_post"].days_since_ingestion == 1
    assert rows_by_id["own_post"].record_count == 2

    assert rows_by_id["curated_article"].days_since_ingestion == 3
    assert rows_by_id["curated_article"].record_count == 1


def test_handles_curated_sources():
    """Curated sources track feed fetch status."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    _add_curated_source(
        conn,
        source_type="blog",
        identifier="example.com",
        last_success_at=NOW - timedelta(days=2),
        status="active",
    )
    _add_curated_source(
        conn,
        source_type="newsletter",
        identifier="newsletter.com",
        last_success_at=NOW - timedelta(days=20),
        status="paused",
    )

    report = build_content_source_freshness_report(
        conn,
        stale_threshold_days=7,
        now=NOW,
    )

    assert len(report.rows) == 2
    rows_by_id = {row.source_identifier: row for row in report.rows}

    blog_row = rows_by_id["blog:example.com"]
    assert blog_row.is_stale is False
    assert blog_row.days_since_ingestion == 2
    assert blog_row.status == "active"

    newsletter_row = rows_by_id["newsletter:newsletter.com"]
    assert newsletter_row.is_stale is True
    assert newsletter_row.days_since_ingestion == 20
    assert newsletter_row.status == "paused"


def test_json_output_format():
    """JSON output is valid and includes all required fields."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    _add_claude_message(conn, session_id="test", message_uuid="m1", timestamp=NOW - timedelta(days=5))

    report = build_content_source_freshness_report(conn, now=NOW)
    json_output = format_content_source_freshness_json(report)

    data = json.loads(json_output)
    assert data["artifact_type"] == "content_source_freshness"
    assert "generated_at" in data
    assert "filters" in data
    assert "totals" in data
    assert "rows" in data
    assert "grouped_by_source_type" in data

    assert len(data["rows"]) == 1
    row = data["rows"][0]
    assert row["source_type"] == "claude_messages"
    assert row["source_identifier"] == "test"
    assert row["days_since_ingestion"] == 5
    assert row["is_stale"] is False


def test_csv_output_format():
    """CSV output includes headers and properly formatted rows."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    _add_github_commit(
        conn,
        repo_name="test,repo",
        commit_sha="sha1",
        timestamp=NOW - timedelta(days=3),
    )

    report = build_content_source_freshness_report(conn, now=NOW)
    csv_output = format_content_source_freshness_csv(report)

    lines = csv_output.split("\n")
    assert lines[0] == "source_type,source_identifier,last_ingestion_at,days_since_ingestion,record_count,is_stale,status"
    assert len(lines) == 2  # header + 1 data row

    # CSV should properly escape commas
    assert '"test,repo"' in lines[1]


def test_empty_database():
    """Report handles empty database gracefully."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    report = build_content_source_freshness_report(conn, now=NOW)

    assert len(report.rows) == 0
    assert report.totals["source_count"] == 0
    assert report.totals["active_count"] == 0
    assert report.totals["stale_count"] == 0
    assert report.grouped_by_source_type == {}


def test_missing_all_source_tables():
    """Report handles missing source tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_content_source_freshness_report(conn, now=NOW)

    assert len(report.rows) == 0
    assert len(report.missing_tables) > 0


def test_stale_threshold_validation():
    """Negative stale threshold raises ValueError."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    with pytest.raises(ValueError, match="stale_threshold_days must be non-negative"):
        build_content_source_freshness_report(conn, stale_threshold_days=-1, now=NOW)


def test_sorting_prioritizes_stale_sources():
    """Stale sources appear first, sorted by days descending."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    # Create sources with varying staleness
    _add_claude_message(conn, session_id="s1", message_uuid="m1", timestamp=NOW - timedelta(days=1))  # fresh
    _add_claude_message(conn, session_id="s2", message_uuid="m2", timestamp=NOW - timedelta(days=15))  # stale
    _add_claude_message(conn, session_id="s3", message_uuid="m3", timestamp=NOW - timedelta(days=3))  # fresh
    _add_claude_message(conn, session_id="s4", message_uuid="m4", timestamp=NOW - timedelta(days=30))  # very stale

    report = build_content_source_freshness_report(
        conn,
        stale_threshold_days=7,
        now=NOW,
    )

    # Stale sources should be first, ordered by staleness
    assert report.rows[0].source_identifier == "s4"  # 30 days
    assert report.rows[0].is_stale is True
    assert report.rows[1].source_identifier == "s2"  # 15 days
    assert report.rows[1].is_stale is True
    # Then fresh sources
    assert report.rows[2].source_identifier == "s3"  # 3 days
    assert report.rows[2].is_stale is False
    assert report.rows[3].source_identifier == "s1"  # 1 day
    assert report.rows[3].is_stale is False


def test_script_json_output(monkeypatch):
    """Script produces valid JSON output."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    monkeypatch.setattr(
        content_source_freshness_script,
        "script_context",
        lambda: _script_context(conn),
    )

    _add_claude_message(conn, session_id="test", message_uuid="m1", timestamp=NOW - timedelta(days=5))

    exit_code = content_source_freshness_script.main(["--format", "json"])
    assert exit_code == 0


def test_script_csv_output(monkeypatch):
    """Script produces valid CSV output."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    monkeypatch.setattr(
        content_source_freshness_script,
        "script_context",
        lambda: _script_context(conn),
    )

    _add_github_commit(conn, repo_name="test", commit_sha="sha1", timestamp=NOW - timedelta(days=3))

    exit_code = content_source_freshness_script.main(["--format", "csv"])
    assert exit_code == 0


def test_script_with_filters(monkeypatch):
    """Script applies filters correctly."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    monkeypatch.setattr(
        content_source_freshness_script,
        "script_context",
        lambda: _script_context(conn),
    )

    _add_claude_message(conn, session_id="s1", message_uuid="m1", timestamp=NOW - timedelta(days=5))
    _add_github_commit(conn, repo_name="repo1", commit_sha="sha1", timestamp=NOW - timedelta(days=10))

    exit_code = content_source_freshness_script.main([
        "--stale-threshold-days", "7",
        "--source-type", "github_commits",
        "--format", "json",
    ])
    assert exit_code == 0


def test_multiple_records_show_latest_ingestion():
    """When multiple records exist, show the most recent ingestion."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    # Same session, multiple messages
    _add_claude_message(conn, session_id="session1", message_uuid="m1", timestamp=NOW - timedelta(days=10))
    _add_claude_message(conn, session_id="session1", message_uuid="m2", timestamp=NOW - timedelta(days=5))
    _add_claude_message(conn, session_id="session1", message_uuid="m3", timestamp=NOW - timedelta(days=2))

    report = build_content_source_freshness_report(conn, now=NOW)

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.source_identifier == "session1"
    assert row.days_since_ingestion == 2  # Most recent
    assert row.record_count == 3


def test_boundary_case_exactly_at_threshold():
    """Source exactly at threshold is marked as stale."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    _add_claude_message(conn, session_id="s1", message_uuid="m1", timestamp=NOW - timedelta(days=7))

    report = build_content_source_freshness_report(
        conn,
        stale_threshold_days=7,
        now=NOW,
    )

    assert report.rows[0].days_since_ingestion == 7
    assert report.rows[0].is_stale is True


def test_no_ingestion_data_marked_as_stale():
    """Sources with NULL ingestion timestamps are marked stale."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    # Curated source with no last_success_at
    _add_curated_source(
        conn,
        source_type="blog",
        identifier="never-fetched.com",
        last_success_at=None,
        status="active",
    )

    report = build_content_source_freshness_report(conn, now=NOW)

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.last_ingestion_at is None
    assert row.days_since_ingestion is None
    assert row.is_stale is True


def test_zero_threshold_marks_all_as_stale():
    """Threshold of 0 marks everything older than today as stale."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    _add_claude_message(conn, session_id="s1", message_uuid="m1", timestamp=NOW - timedelta(hours=1))
    _add_claude_message(conn, session_id="s2", message_uuid="m2", timestamp=NOW - timedelta(days=1))

    report = build_content_source_freshness_report(
        conn,
        stale_threshold_days=0,
        now=NOW,
    )

    rows_by_id = {row.source_identifier: row for row in report.rows}

    # Both should be stale (anything with days >= 0 and at the threshold)
    # Actually, hours=1 should be 0 days, and days=1 should be 1 day
    assert rows_by_id["s1"].days_since_ingestion == 0
    assert rows_by_id["s1"].is_stale is True  # 0 >= 0
    assert rows_by_id["s2"].days_since_ingestion == 1
    assert rows_by_id["s2"].is_stale is True  # 1 >= 0


def test_filters_preserve_grouping():
    """Filtering by source type doesn't affect grouping accuracy."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    _add_claude_message(conn, session_id="s1", message_uuid="m1", timestamp=NOW - timedelta(days=1))
    _add_claude_message(conn, session_id="s2", message_uuid="m2", timestamp=NOW - timedelta(days=2))
    _add_github_commit(conn, repo_name="repo1", commit_sha="sha1", timestamp=NOW - timedelta(days=1))

    report = build_content_source_freshness_report(
        conn,
        source_type="claude_messages",
        now=NOW,
    )

    # Should only show claude_messages
    assert report.totals["source_count"] == 2
    assert report.grouped_by_source_type == {"claude_messages": 2}
