"""Tests for campaign topic backlog imports."""

from __future__ import annotations

import csv
import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from import_campaign_topics import main
from synthesis.campaign_topic_import import (
    build_campaign_topic_import_plan,
    format_campaign_topic_import_json,
    import_campaign_topics,
    normalize_target_date,
)


def test_validation_reports_row_errors_with_csv_line_numbers(tmp_path, db):
    campaign_id = db.create_campaign(name="Backlog", status="active")
    path = tmp_path / "topics.csv"
    _write_csv(
        path,
        [
            {"topic": "", "angle": "Testing", "target_date": "2026-05-03"},
            {"topic": "Launch", "angle": "Bad date", "target_date": "next week"},
            {"topic": "Ops", "angle": "Bad priority", "target_date": "2026-05-05", "priority": "urgent"},
        ],
    )

    report = import_campaign_topics(db, campaign_id=campaign_id, file_path=path)

    assert [(row.reference, row.errors) for row in report.invalid] == [
        ("line 2", ("topic is required",)),
        ("line 3", ("target_date must be an ISO date or datetime",)),
        ("line 4", ("priority must be one of: high, normal, low",)),
    ]
    assert db.get_planned_topics(status="planned") == []


def test_json_import_reports_indexes_and_normalizes_target_dates(tmp_path, db):
    campaign_id = db.create_campaign(name="JSON backlog", status="active")
    path = tmp_path / "topics.json"
    path.write_text(
        json.dumps(
            [
                {
                    "topic": "Launch",
                    "angle": "Founders",
                    "target_date": "2026-05-03T10:30:00+09:00",
                    "priority": "high",
                    "source": "planning doc",
                    "notes": "Lead with customer proof",
                },
                {"topic": "Broken", "angle": "No date"},
            ]
        ),
        encoding="utf-8",
    )

    report = import_campaign_topics(db, campaign_id=campaign_id, file_path=path)

    assert [(row.reference, row.status, row.target_date) for row in report.rows] == [
        ("index 0", "planned", "2026-05-03"),
        ("index 1", "invalid", None),
    ]
    assert report.invalid[0].errors == ("target_date is required",)


def test_duplicate_detection_uses_campaign_topic_angle(db):
    campaign_id = db.create_campaign(name="Dupes", status="active")
    other_campaign_id = db.create_campaign(name="Other", status="active")
    existing_id = db.insert_planned_topic(
        topic="Launch Plan",
        angle="Founder Story",
        target_date="2026-05-01",
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="Launch Plan",
        angle="Founder Story",
        target_date="2026-05-01",
        campaign_id=other_campaign_id,
    )

    report = build_campaign_topic_import_plan(
        db,
        campaign_id=campaign_id,
        source_rows=[
            ("index 0", {"topic": " launch   plan ", "angle": "FOUNDER story", "target_date": "2026-06-01"}),
            ("index 1", {"topic": "Launch Plan", "angle": "New Proof", "target_date": "2026-06-02"}),
            ("index 2", {"topic": "launch plan", "angle": "new proof", "target_date": "2026-06-03"}),
        ],
    )

    assert [(row.reference, row.status, row.duplicate_id, row.reason) for row in report.rows] == [
        ("index 0", "duplicate", existing_id, f"matches planned_topic #{existing_id}"),
        ("index 1", "planned", None, "ready"),
        ("index 2", "duplicate", None, "duplicates input row index 1"),
    ]


def test_dry_run_reports_planned_inserts_without_database_writes(tmp_path, db):
    campaign_id = db.create_campaign(name="Dry run", status="active")
    path = tmp_path / "topics.csv"
    _write_csv(
        path,
        [
            {
                "topic": "Architecture",
                "angle": "Tradeoffs",
                "target_date": "2026-05-07",
                "priority": "low",
                "source": "roadmap",
                "notes": "Mention migration",
            }
        ],
    )

    report = import_campaign_topics(db, campaign_id=campaign_id, file_path=path)

    assert report.dry_run is True
    assert report.to_dict()["summary"] == {
        "planned": 1,
        "inserted": 0,
        "duplicates": 0,
        "invalid": 0,
        "blocked": 0,
        "total": 1,
    }
    assert db.get_planned_topics(status="planned") == []


def test_apply_inserts_valid_rows_with_metadata(tmp_path, db):
    campaign_id = db.create_campaign(name="Apply", status="active")
    path = tmp_path / "topics.json"
    path.write_text(
        json.dumps(
            {
                "topics": [
                    {
                        "topic": "Reliability",
                        "angle": "Incident review",
                        "target_date": "2026-05-08",
                        "priority": "high",
                        "source": "retro",
                        "notes": "Tie to customer trust",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    report = import_campaign_topics(db, campaign_id=campaign_id, file_path=path, apply=True)
    planned = db.get_planned_topics(status="planned")
    metadata = json.loads(planned[0]["source_material"])

    assert report.inserted[0].record_id == planned[0]["id"]
    assert planned[0]["campaign_id"] == campaign_id
    assert planned[0]["topic"] == "Reliability"
    assert planned[0]["angle"] == "Incident review"
    assert planned[0]["target_date"] == "2026-05-08"
    assert metadata["campaign_topic_import"] == {
        "priority": "high",
        "source": "retro",
        "notes": "Tie to customer trust",
    }


def test_apply_blocks_duplicates_unless_skip_duplicates(tmp_path, db):
    campaign_id = db.create_campaign(name="Duplicate apply", status="active")
    db.insert_planned_topic(
        topic="Launch",
        angle="Proof",
        target_date="2026-05-01",
        campaign_id=campaign_id,
    )
    path = tmp_path / "topics.csv"
    _write_csv(
        path,
        [
            {"topic": "Launch", "angle": "Proof", "target_date": "2026-05-10"},
            {"topic": "Launch", "angle": "Timeline", "target_date": "2026-05-11"},
        ],
    )

    blocked = import_campaign_topics(db, campaign_id=campaign_id, file_path=path, apply=True)
    skip_campaign_id = db.create_campaign(name="Duplicate skip", status="active")
    db.insert_planned_topic(
        topic="Launch",
        angle="Proof",
        target_date="2026-05-01",
        campaign_id=skip_campaign_id,
    )
    skipped = import_campaign_topics(
        db,
        campaign_id=skip_campaign_id,
        file_path=path,
        apply=True,
        skip_duplicates=True,
    )

    assert blocked.to_dict()["summary"] == {
        "planned": 0,
        "inserted": 1,
        "duplicates": 0,
        "invalid": 0,
        "blocked": 1,
        "total": 2,
    }
    assert len([row for row in db.get_planned_topics(status="planned") if row["campaign_id"] == campaign_id]) == 2
    assert [row.status for row in skipped.rows] == ["skipped_duplicate", "inserted"]


def test_format_json_is_stable(tmp_path, db):
    campaign_id = db.create_campaign(name="JSON format", status="active")
    path = tmp_path / "topics.json"
    path.write_text(json.dumps([{"topic": "Ops", "target_date": "2026-05-09"}]), encoding="utf-8")

    payload = json.loads(format_campaign_topic_import_json(import_campaign_topics(db, campaign_id=campaign_id, file_path=path)))

    assert payload["summary"]["planned"] == 1
    assert payload["rows"][0]["reference"] == "index 0"


def test_cli_wiring_json_output(tmp_path, db, capsys):
    campaign_id = db.create_campaign(name="CLI import", status="active")
    path = tmp_path / "topics.csv"
    _write_csv(path, [{"topic": "CLI", "angle": "", "target_date": "2026-05-12"}])

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("import_campaign_topics.script_context", fake_script_context):
        exit_code = main(
            [
                "--campaign-id",
                str(campaign_id),
                "--file",
                str(path),
                "--apply",
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["summary"]["inserted"] == 1
    assert db.get_planned_topics(status="planned")[0]["topic"] == "CLI"


def test_normalize_target_date_accepts_date_and_datetime_values():
    assert normalize_target_date("2026-05-01") == "2026-05-01"
    assert normalize_target_date("2026-05-01T23:30:00Z") == "2026-05-01"


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fields = ["topic", "angle", "target_date", "priority", "source", "notes"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
