"""Tests for planned topic source_material resolution."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.planned_topic_source_resolver import (
    build_planned_topic_source_resolver_report,
    format_planned_topic_source_resolver_json,
    format_planned_topic_source_resolver_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "resolve_planned_topic_sources.py"
)
spec = importlib.util.spec_from_file_location("resolve_planned_topic_sources_script", SCRIPT_PATH)
resolve_planned_topic_sources_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(resolve_planned_topic_sources_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _campaign(db, name: str = "Launch") -> int:
    return db.create_campaign(
        name=name,
        goal="Ship launch material",
        start_date="2026-05-01",
        end_date="2026-05-31",
        status="active",
    )


def _topic(
    db,
    *,
    source_material: str | None,
    campaign_id: int | None = None,
    status: str = "planned",
    target_date: str = "2026-05-04",
    topic: str = "source readiness",
) -> int:
    return db.insert_planned_topic(
        topic=topic,
        angle="resolver",
        target_date=target_date,
        source_material=source_material,
        campaign_id=campaign_id,
        status=status,
    )


def _seed_sources(db) -> dict[str, str | int]:
    commit_id = db.insert_commit(
        repo_name="taka/presence",
        commit_sha="abcdef1234567890",
        commit_message="Add resolver",
        timestamp="2026-05-01T10:00:00+00:00",
        author="taka",
    )
    message_id = db.insert_claude_message(
        session_id="sess-123",
        message_uuid="msg-abc",
        project_path="/repo/presence",
        timestamp="2026-05-01T11:00:00+00:00",
        prompt_text="Implement the resolver",
    )
    activity_id = db.upsert_github_activity(
        repo_name="taka/presence",
        activity_type="issue",
        number=42,
        title="Track source resolver",
        state="open",
        author="taka",
        url="https://github.com/taka/presence/issues/42",
        updated_at="2026-05-01T12:00:00+00:00",
    )
    return {"commit_id": commit_id, "message_id": message_id, "activity_id": activity_id}


def test_mixed_valid_and_missing_references_are_partial_with_diagnostics(db):
    ids = _seed_sources(db)
    topic_id = _topic(
        db,
        source_material=(
            "abcdef1234567890 msg-abc sess-123 "
            "taka/presence#42:issue missing-token"
        ),
    )

    report = build_planned_topic_source_resolver_report(db, days=7, now=NOW)
    row = report.planned_topics[0]
    payload = json.loads(format_planned_topic_source_resolver_json(report))

    assert row.planned_topic_id == topic_id
    assert row.resolution_status == "partial"
    assert row.reference_count == 5
    assert row.resolved_reference_count == 4
    assert row.missing_reference_count == 1
    assert report.totals["partial"] == 1
    assert report.totals["resolved_references"] == 4
    assert {lookup.reference for lookup in row.lookups if lookup.lookup_status == "missing"} == {
        "missing-token"
    }
    assert {
        (lookup.reference_type, lookup.artifact_table, lookup.artifact_id)
        for lookup in row.lookups
        if lookup.lookup_status == "resolved"
    } == {
        ("commit", "github_commits", ids["commit_id"]),
        ("claude_message", "claude_messages", ids["message_id"]),
        ("claude_session", "claude_messages", ids["message_id"]),
        ("github_activity", "github_activity", ids["activity_id"]),
    }
    assert payload["artifact_type"] == "planned_topic_source_resolution"
    assert list(payload) == sorted(payload)


def test_json_array_and_comma_separated_source_material_formats(db):
    _seed_sources(db)
    json_topic_id = _topic(
        db,
        source_material=json.dumps(["abcdef1234567890", "claude_message:msg-abc"]),
        topic="json",
    )
    csv_topic_id = _topic(
        db,
        source_material="commit:abcdef1234567890, github_activity:taka/presence#42:issue",
        topic="csv",
    )

    report = build_planned_topic_source_resolver_report(db, days=7, now=NOW)
    by_id = {row.planned_topic_id: row for row in report.planned_topics}

    assert by_id[json_topic_id].resolution_status == "resolved"
    assert by_id[csv_topic_id].resolution_status == "resolved"
    assert by_id[csv_topic_id].reference_count == 2


def test_campaign_status_and_lookback_filters_are_applied(db):
    _seed_sources(db)
    campaign_a = _campaign(db, "A")
    campaign_b = _campaign(db, "B")
    included = _topic(
        db,
        campaign_id=campaign_a,
        status="planned",
        source_material="abcdef1234567890",
        target_date="2026-05-03",
    )
    _topic(
        db,
        campaign_id=campaign_b,
        status="planned",
        source_material="abcdef1234567890",
        target_date="2026-05-03",
    )
    _topic(
        db,
        campaign_id=campaign_a,
        status="generated",
        source_material="abcdef1234567890",
        target_date="2026-05-03",
    )
    old_topic = _topic(
        db,
        campaign_id=campaign_a,
        status="planned",
        source_material="abcdef1234567890",
        target_date=(NOW - timedelta(days=20)).date().isoformat(),
    )
    db.conn.execute(
        "UPDATE planned_topics SET created_at = ? WHERE id = ?",
        ((NOW - timedelta(days=20)).isoformat(), old_topic),
    )
    db.conn.commit()

    report = build_planned_topic_source_resolver_report(
        db,
        campaign_id=campaign_a,
        status="planned",
        days=7,
        now=NOW,
    )

    assert [row.planned_topic_id for row in report.planned_topics] == [included]
    assert report.filters["campaign_id"] == campaign_a
    assert report.filters["status"] == "planned"


def test_absent_optional_source_tables_are_reported_without_crashing():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            topic TEXT,
            source_material TEXT,
            target_date TEXT,
            status TEXT,
            created_at TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO planned_topics
           (id, campaign_id, topic, source_material, target_date, status, created_at)
           VALUES (1, NULL, 'missing tables', 'commit:abcdef1234567890', '2026-05-04', 'planned', '2026-05-01')"""
    )

    report = build_planned_topic_source_resolver_report(conn, days=7, now=NOW)
    row = report.planned_topics[0]

    assert set(report.missing_tables) == {"github_commits", "claude_messages", "github_activity"}
    assert row.resolution_status == "missing"
    assert row.lookups[0].diagnostic == "github_commits table is missing"


def test_malformed_source_material_is_unparseable_and_text_is_deterministic(db):
    _topic(db, source_material='["unterminated"', topic="bad")

    report = build_planned_topic_source_resolver_report(db, days=7, now=NOW)
    row = report.planned_topics[0]
    text = format_planned_topic_source_resolver_text(report)

    assert row.resolution_status == "unparseable"
    assert row.parse_status == "unparseable"
    assert "malformed JSON" in (row.parse_diagnostic or "")
    assert report.totals["unparseable"] == 1
    assert "Planned Topic Source Resolution" in text
    assert "resolution=unparseable" in text
    assert text == format_planned_topic_source_resolver_text(report)


def test_cli_supports_filters_json_and_argument_validation(db, monkeypatch, capsys):
    _seed_sources(db)
    campaign_id = _campaign(db)
    topic_id = _topic(db, campaign_id=campaign_id, source_material="abcdef1234567890")
    monkeypatch.setattr(
        resolve_planned_topic_sources_script,
        "script_context",
        lambda: _script_context(db),
    )

    assert resolve_planned_topic_sources_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    exit_code = resolve_planned_topic_sources_script.main(
        [
            "--campaign-id",
            str(campaign_id),
            "--status",
            "planned",
            "--days",
            "7",
            "--limit",
            "5",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert [row["planned_topic_id"] for row in payload["planned_topics"]] == [topic_id]
    assert payload["planned_topics"][0]["resolution_status"] == "resolved"
