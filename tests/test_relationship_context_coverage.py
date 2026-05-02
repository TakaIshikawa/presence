"""Tests for relationship context coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.relationship_context_coverage import (
    build_relationship_context_coverage_report,
    format_relationship_context_coverage_json,
    format_relationship_context_coverage_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "relationship_context_coverage.py"
spec = importlib.util.spec_from_file_location("relationship_context_coverage_script", SCRIPT_PATH)
relationship_context_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(relationship_context_coverage_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _context(**overrides) -> str:
    payload = {
        "engagement_stage": 3,
        "stage_name": "Active",
        "dunbar_tier": 2,
        "tier_name": "Key Network",
        "relationship_strength": 0.72,
        "last_interaction_at": "2026-05-01T10:00:00+00:00",
        "relationship_notes": "Usually asks precise implementation questions.",
    }
    payload.update(overrides)
    return json.dumps(payload, sort_keys=True)


def _insert_reply(db, inbound_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=inbound_id,
        inbound_author_handle="alice",
        inbound_author_id=f"{inbound_id}-author",
        inbound_text="Can you clarify this?",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="Thanks for asking.",
        status="pending",
        platform="x",
        relationship_context=_context(),
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute("UPDATE reply_queue SET detected_at = ? WHERE id = ?", (detected_at, reply_id))
    db.conn.commit()


def test_complete_context_has_no_coverage_gaps(db):
    reply_id = _insert_reply(db, "complete")
    _set_detected_at(db, reply_id, "2026-05-02T10:00:00+00:00")

    report = build_relationship_context_coverage_report(db, days=7, now=NOW)

    assert report["totals"]["scanned_count"] == 1
    assert report["totals"]["complete_count"] == 1
    assert report["affected_reply_queue_ids"] == []
    assert report["totals"]["by_missing_field"] == {
        "stage": 0,
        "tier": 0,
        "strength": 0,
        "last_interaction_at": 0,
        "notes": 0,
    }


def test_missing_fields_include_per_field_counts_and_reply_ids(db):
    missing_stage = _insert_reply(db, "missing-stage", relationship_context=_context(engagement_stage=None))
    missing_notes = _insert_reply(db, "missing-notes", relationship_context=_context(relationship_notes=""))
    weak = _insert_reply(db, "weak", relationship_context=_context(relationship_strength=0.2))
    _set_detected_at(db, missing_stage, "2026-05-02T08:00:00+00:00")
    _set_detected_at(db, missing_notes, "2026-05-02T09:00:00+00:00")
    _set_detected_at(db, weak, "2026-05-02T10:00:00+00:00")

    report = build_relationship_context_coverage_report(
        db,
        days=7,
        min_strength=0.5,
        now=NOW,
    )

    assert report["affected_reply_queue_ids"] == [missing_stage, missing_notes, weak]
    assert report["totals"]["by_missing_field"]["stage"] == 1
    assert report["totals"]["by_missing_field"]["notes"] == 1
    assert report["totals"]["by_missing_field"]["strength"] == 1
    fields = {item["field"]: item for item in report["missing_fields"]}
    assert fields["stage"]["reply_queue_ids"] == [missing_stage]
    assert fields["notes"]["reply_queue_ids"] == [missing_notes]
    assert fields["strength"]["reply_queue_ids"] == [weak]


def test_malformed_relationship_context_is_reported_without_crashing(db):
    bad = _insert_reply(db, "bad-json", relationship_context="{bad-json")
    _set_detected_at(db, bad, "2026-05-02T10:00:00+00:00")

    report = build_relationship_context_coverage_report(db, days=7, now=NOW)

    assert report["totals"]["malformed_count"] == 1
    assert report["affected_reply_queue_ids"] == [bad]
    assert report["rows"][0]["malformed_context"] is True
    assert report["rows"][0]["missing_fields"] == [
        "stage",
        "tier",
        "strength",
        "last_interaction_at",
        "notes",
    ]


def test_status_platform_and_detected_at_lookback_filters_apply_consistently(db):
    included = _insert_reply(db, "included", platform="x", status="pending", relationship_context=None)
    other_platform = _insert_reply(
        db,
        "other-platform",
        platform="bluesky",
        status="pending",
        relationship_context=None,
    )
    reviewed = _insert_reply(db, "reviewed", platform="x", status="reviewed", relationship_context=None)
    old = _insert_reply(db, "old", platform="x", status="pending", relationship_context=None)
    _set_detected_at(db, included, "2026-05-02T10:00:00+00:00")
    _set_detected_at(db, other_platform, "2026-05-02T10:00:00+00:00")
    _set_detected_at(db, reviewed, "2026-05-02T10:00:00+00:00")
    _set_detected_at(db, old, "2026-04-20T10:00:00+00:00")

    report = build_relationship_context_coverage_report(
        db,
        status="pending",
        platform="x",
        days=7,
        now=NOW,
    )

    assert report["totals"]["scanned_count"] == 1
    assert report["affected_reply_queue_ids"] == [included]


def test_json_text_and_cli_output_include_summary_counts(db, monkeypatch, capsys):
    reply_id = _insert_reply(db, "cli", relationship_context=None)
    _set_detected_at(db, reply_id, "2026-05-02T10:00:00+00:00")
    report = build_relationship_context_coverage_report(db, days=7, now=NOW)

    payload = json.loads(format_relationship_context_coverage_json(report))
    text = format_relationship_context_coverage_text(report)

    assert list(payload) == sorted(payload)
    assert payload["totals"]["by_missing_field"]["notes"] == 1
    assert "Relationship Context Coverage" in text
    assert "notes=1" in text

    monkeypatch.setattr(
        relationship_context_coverage_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        relationship_context_coverage_script,
        "build_relationship_context_coverage_report",
        lambda db_arg, **kwargs: build_relationship_context_coverage_report(
            db_arg,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = relationship_context_coverage_script.main(
        ["--status", "pending", "--platform", "x", "--days", "7", "--format", "json"]
    )
    cli_payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert cli_payload["affected_reply_queue_ids"] == [reply_id]
    assert cli_payload["filters"]["status"] == ["pending"]
    assert cli_payload["filters"]["platform"] == ["x"]
