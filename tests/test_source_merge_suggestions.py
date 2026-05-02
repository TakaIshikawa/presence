"""Tests for curated source merge suggestions."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from knowledge.source_merge_suggestions import (
    build_source_merge_suggestion_report,
    format_source_merge_suggestion_json,
    format_source_merge_suggestion_text,
    normalize_domain,
    normalize_handle,
    normalize_url,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "suggest_source_merges.py"
spec = importlib.util.spec_from_file_location("suggest_source_merges", SCRIPT_PATH)
suggest_source_merges = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(suggest_source_merges)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    schema_path = Path(__file__).parent.parent / "schema.sql"
    connection.executescript(schema_path.read_text())
    yield connection
    connection.close()


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_source(
    conn,
    *,
    source_type: str = "blog",
    identifier: str,
    name: str | None = None,
    license_value: str = "attribution_required",
    feed_url: str | None = None,
    canonical_url: str | None = None,
    link_title: str | None = None,
    site_name: str | None = None,
    status: str = "active",
    active: int = 1,
) -> int:
    cursor = conn.execute(
        """INSERT INTO curated_sources
           (source_type, identifier, name, license, feed_url, canonical_url,
            link_title, site_name, status, active)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            source_type,
            identifier,
            name,
            license_value,
            feed_url,
            canonical_url,
            link_title,
            site_name,
            status,
            active,
        ),
    )
    conn.commit()
    return cursor.lastrowid


def test_normalization_groups_domains_handles_and_tracking_urls(conn):
    assert normalize_domain("HTTPS://WWW.Example.com/posts/") == "example.com"
    assert normalize_handle("@Example_User ") == "example_user"
    assert (
        normalize_url("http://www.example.com/feed/?utm_source=mail&b=2&a=1#frag")
        == "https://example.com/feed?a=1&b=2"
    )

    survivor_id = _insert_source(
        conn,
        identifier="HTTPS://WWW.Example.com/",
        name="Example",
        license_value="open",
        feed_url="https://www.example.com/feed/?utm_source=newsletter",
        canonical_url="https://example.com/",
    )
    duplicate_id = _insert_source(
        conn,
        identifier="example.com",
        name="Example Blog",
        license_value="restricted",
        feed_url="http://example.com/feed",
        canonical_url="http://www.example.com/?ref=x",
        status="paused",
    )

    report = build_source_merge_suggestion_report(conn, now=NOW)

    assert report.totals == {"source_count": 2, "suggestion_count": 1}
    suggestion = report.suggestions[0]
    assert suggestion.confidence == 1.0
    assert suggestion.canonical_survivor_candidates[0].id == survivor_id
    assert suggestion.duplicate_ids == (duplicate_id,)
    assert suggestion.conflicting_fields["license"] == ["open", "restricted"]
    assert suggestion.conflicting_fields["status"] == ["active", "paused"]
    assert "canonical_url:https://example.com" in suggestion.evidence
    assert "feed_url:https://example.com/feed" in suggestion.evidence


def test_equivalent_handles_group_despite_case_and_at_prefix(conn):
    first_id = _insert_source(
        conn,
        source_type="x_account",
        identifier="@SomeHandle",
        name="Some Handle",
    )
    second_id = _insert_source(
        conn,
        source_type="x_account",
        identifier="somehandle",
        name="SomeHandle",
        status="candidate",
    )

    report = build_source_merge_suggestion_report(conn, source_type="x_account", now=NOW)

    assert len(report.suggestions) == 1
    suggestion = report.suggestions[0]
    assert suggestion.confidence == 0.9
    assert suggestion.canonical_survivor_candidates[0].id == first_id
    assert suggestion.duplicate_ids == (second_id,)
    assert suggestion.conflicting_fields["status"] == ["active", "candidate"]


def test_link_metadata_can_suggest_lower_confidence_merge(conn):
    _insert_source(
        conn,
        identifier="alpha.example",
        link_title="The AI Systems Review",
        site_name="Systems Weekly",
    )
    _insert_source(
        conn,
        identifier="beta.example",
        link_title="  the ai systems review ",
        site_name="SYSTEMS WEEKLY",
    )

    below_threshold = build_source_merge_suggestion_report(
        conn,
        min_confidence=0.8,
        now=NOW,
    )
    assert below_threshold.suggestions == ()

    report = build_source_merge_suggestion_report(conn, min_confidence=0.7, now=NOW)
    assert len(report.suggestions) == 1
    assert report.suggestions[0].confidence == 0.72
    assert report.suggestions[0].evidence == (
        "link_metadata:systems weekly|the ai systems review",
    )


def test_filters_json_text_and_cli_are_deterministic(db, capsys):
    survivor_id = _insert_source(
        db.conn,
        source_type="newsletter",
        identifier="https://News.Example/",
        name="News",
        feed_url="https://news.example/rss?utm_campaign=spring",
        status="active",
    )
    duplicate_id = _insert_source(
        db.conn,
        source_type="newsletter",
        identifier="news.example/",
        name="News Alt",
        feed_url="http://www.news.example/rss",
        status="active",
    )
    _insert_source(
        db.conn,
        source_type="blog",
        identifier="news.example",
        feed_url="http://www.news.example/rss",
        status="candidate",
    )

    report = build_source_merge_suggestion_report(
        db,
        source_type="newsletter",
        status="active",
        min_confidence=0.9,
        now=NOW,
    )

    assert len(report.suggestions) == 1
    assert report.suggestions[0].canonical_survivor_candidates[0].id == survivor_id
    assert report.suggestions[0].duplicate_ids == (duplicate_id,)
    assert format_source_merge_suggestion_json(report) == format_source_merge_suggestion_json(report)
    payload = json.loads(format_source_merge_suggestion_json(report))
    assert sorted(payload) == [
        "filters",
        "generated_at",
        "missing_tables",
        "suggestions",
        "totals",
    ]
    assert payload["filters"]["source_type"] == "newsletter"
    assert payload["suggestions"][0]["sources"][0]["license"] == "attribution_required"
    text = format_source_merge_suggestion_text(report)
    assert "Curated Source Merge Suggestions" in text
    assert f"survivor_candidates={survivor_id}" in text
    assert f"duplicate_ids={duplicate_id}" in text

    with patch.object(
        suggest_source_merges,
        "script_context",
        wraps=lambda: _script_context(db),
    ):
        exit_code = suggest_source_merges.main(
            [
                "--source-type",
                "newsletter",
                "--status",
                "active",
                "--min-confidence",
                "0.9",
                "--format",
                "json",
            ]
        )

    assert exit_code == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["totals"]["suggestion_count"] == 1
    assert cli_payload["suggestions"][0]["duplicate_ids"] == [duplicate_id]


def test_missing_curated_sources_table_reports_empty():
    connection = sqlite3.connect(":memory:")
    report = build_source_merge_suggestion_report(connection, now=NOW)
    assert report.missing_tables == ("curated_sources",)
    assert report.suggestions == ()
