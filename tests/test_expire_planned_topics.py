"""Tests for stale planned topic expiration."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from expire_planned_topics import main
from synthesis.planned_topic_expiration import expire_planned_topics


NOW = datetime(2026, 4, 25, 12, tzinfo=timezone.utc)


def _planned(db, topic: str, **kwargs) -> int:
    return db.insert_planned_topic(topic=topic, angle=f"{topic} angle", **kwargs)


def _content(db) -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha"],
        source_messages=["message"],
        content="Generated post",
        eval_score=8.0,
        eval_feedback="Good",
    )


def _topic_row(db, topic_id: int) -> dict:
    row = db.conn.execute(
        "SELECT * FROM planned_topics WHERE id = ?",
        (topic_id,),
    ).fetchone()
    return dict(row)


def test_dry_run_returns_eligible_topics_without_updates(db):
    old_id = _planned(db, "old planned", target_date="2026-04-10")

    results = expire_planned_topics(
        db,
        older_than_days=7,
        dry_run=True,
        now=NOW,
    )

    assert [result.topic_id for result in results] == [old_id]
    assert "older than cutoff 2026-04-18" in results[0].reason
    row = _topic_row(db, old_id)
    assert row["status"] == "planned"
    assert row["source_material"] is None


def test_apply_marks_eligible_topics_skipped_and_records_json_metadata(db):
    source = {"source": "manual", "notes": ["keep this"]}
    old_id = _planned(
        db,
        "json source",
        target_date="2026-04-01",
        source_material=json.dumps(source),
    )

    results = expire_planned_topics(db, older_than_days=14, now=NOW)

    assert [(result.topic_id, result.status) for result in results] == [(old_id, "expired")]
    row = _topic_row(db, old_id)
    assert row["status"] == "skipped"
    payload = json.loads(row["source_material"])
    assert payload["source"] == "manual"
    assert payload["notes"] == ["keep this"]
    assert payload["expiration"]["source"] == "planned_topic_expiration"
    assert payload["expiration"]["older_than_days"] == 14
    assert payload["expiration"]["cutoff_date"] == "2026-04-11"
    assert "target_date 2026-04-01" in payload["expiration"]["reason"]


def test_apply_tolerates_plain_text_source_material(db):
    old_id = _planned(
        db,
        "plain source",
        target_date="2026-03-20",
        source_material="commit abc123 and notes",
    )

    expire_planned_topics(db, older_than_days=14, now=NOW)

    payload = json.loads(_topic_row(db, old_id)["source_material"])
    assert payload["original_source_material"] == "commit abc123 and notes"
    assert payload["expiration"]["source"] == "planned_topic_expiration"


def test_ineligible_topics_are_not_expired(db):
    no_target_id = _planned(db, "no target")
    future_id = _planned(db, "future", target_date="2026-05-01")
    generated_id = _planned(db, "generated", target_date="2026-04-01")
    db.mark_planned_topic_generated(generated_id, _content(db))
    linked_planned_id = _planned(db, "linked but planned", target_date="2026-04-01")
    db.conn.execute(
        "UPDATE planned_topics SET content_id = ? WHERE id = ?",
        (_content(db), linked_planned_id),
    )
    db.conn.commit()

    results = expire_planned_topics(db, older_than_days=7, now=NOW)

    assert results == []
    assert _topic_row(db, no_target_id)["status"] == "planned"
    assert _topic_row(db, future_id)["status"] == "planned"
    assert _topic_row(db, generated_id)["status"] == "generated"
    assert _topic_row(db, linked_planned_id)["status"] == "planned"


def test_campaign_filter_limits_expiration_to_requested_campaign(db):
    campaign_id = db.create_campaign(name="Launch")
    other_campaign_id = db.create_campaign(name="Other")
    scoped_id = _planned(db, "scoped", target_date="2026-04-01", campaign_id=campaign_id)
    other_id = _planned(db, "other", target_date="2026-04-01", campaign_id=other_campaign_id)
    uncampaigned_id = _planned(db, "uncampaigned", target_date="2026-04-01")

    results = expire_planned_topics(
        db,
        older_than_days=7,
        campaign_id=campaign_id,
        now=NOW,
    )

    assert [result.topic_id for result in results] == [scoped_id]
    assert _topic_row(db, scoped_id)["status"] == "skipped"
    assert _topic_row(db, other_id)["status"] == "planned"
    assert _topic_row(db, uncampaigned_id)["status"] == "planned"


def test_missing_campaign_fails(db):
    with pytest.raises(ValueError, match="Campaign 999 does not exist"):
        expire_planned_topics(db, older_than_days=7, campaign_id=999, now=NOW)


def test_cli_json_dry_run(db, capsys):
    old_id = _planned(db, "cli", target_date="2026-04-01")

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("expire_planned_topics.script_context", fake_script_context):
        main(["--older-than-days", "7", "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["topic_id"] == old_id
    assert payload[0]["status"] == "eligible"
    assert _topic_row(db, old_id)["status"] == "planned"
