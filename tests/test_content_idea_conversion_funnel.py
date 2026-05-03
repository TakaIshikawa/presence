"""Tests for content idea conversion funnel export."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import csv
from io import StringIO
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from synthesis.content_idea_conversion_funnel import (
    build_content_idea_conversion_funnel_report,
    format_content_idea_conversion_funnel_csv,
    format_content_idea_conversion_funnel_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_idea_conversion_funnel.py"
spec = importlib.util.spec_from_file_location("content_idea_conversion_funnel_script", SCRIPT_PATH)
content_idea_conversion_funnel_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_idea_conversion_funnel_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _set_idea_created_at(db, idea_id: int, created_at: str) -> None:
    db.conn.execute(
        "UPDATE content_ideas SET created_at = ?, updated_at = ? WHERE id = ?",
        (created_at, created_at, idea_id),
    )
    db.conn.commit()


def _content(
    db,
    text: str,
    *,
    published: bool = False,
    abandoned: bool = False,
    curation_quality: str | None = None,
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        """UPDATE generated_content
              SET published = ?, published_at = ?, published_url = ?, curation_quality = ?
            WHERE id = ?""",
        (
            -1 if abandoned else (1 if published else 0),
            "2026-04-25T12:00:00+00:00" if published else None,
            "https://example.com/post/1" if published else None,
            curation_quality,
            content_id,
        ),
    )
    db.conn.commit()
    return content_id


def _row(report, source_type: str):
    return next(row for row in report.rows if row.source_type == source_type)


def test_normal_progression_counts_by_source_type(db):
    published_idea = db.add_content_idea(
        "Release lesson",
        topic="release",
        source="release_digest",
    )
    _set_idea_created_at(db, published_idea, "2026-04-20T12:00:00+00:00")
    published_content = _content(
        db,
        "Published release post",
        published=True,
        curation_quality="good",
    )
    planned_id = db.promote_content_idea(published_idea, "2026-04-24", topic="release")
    db.mark_planned_topic_generated(planned_id, published_content)

    approved_idea = db.add_content_idea("Approved draft", source="manual")
    _set_idea_created_at(db, approved_idea, "2026-04-21T12:00:00+00:00")
    approved_content = _content(db, "Approved but unpublished", curation_quality="approved")
    db.conn.execute(
        "UPDATE content_ideas SET source_metadata = ? WHERE id = ?",
        (json.dumps({"content_id": approved_content}), approved_idea),
    )
    db.conn.commit()

    report = build_content_idea_conversion_funnel_report(db, min_age_days=14, now=NOW)

    release = _row(report, "release_digest")
    manual = _row(report, "manual")
    assert release.counts == {
        "created": 1,
        "candidate_generated": 1,
        "approved": 1,
        "published": 1,
        "abandoned": 0,
        "stale": 0,
    }
    assert manual.counts["candidate_generated"] == 1
    assert manual.counts["approved"] == 1
    assert manual.counts["published"] == 0
    assert report.totals["created"] == 2
    assert report.totals["approved"] == 2


def test_stale_and_abandoned_classification_are_derived_without_schema_changes(db):
    stale_idea = db.add_content_idea("Old open idea", source="manual")
    _set_idea_created_at(db, stale_idea, "2026-04-01T12:00:00+00:00")
    young_idea = db.add_content_idea("Young open idea", source="manual")
    _set_idea_created_at(db, young_idea, "2026-04-28T12:00:00+00:00")
    dismissed_idea = db.add_content_idea(
        "Dismissed idea",
        source="manual",
        status="dismissed",
    )
    _set_idea_created_at(db, dismissed_idea, "2026-04-01T12:00:00+00:00")
    abandoned_content_idea = db.add_content_idea("Failed generated idea", source="manual")
    _set_idea_created_at(db, abandoned_content_idea, "2026-04-01T12:00:00+00:00")
    content_id = _content(db, "Retries exhausted", abandoned=True)
    db.conn.execute(
        "UPDATE content_ideas SET source_metadata = ? WHERE id = ?",
        (json.dumps({"content_id": content_id}), abandoned_content_idea),
    )
    db.conn.commit()

    report = build_content_idea_conversion_funnel_report(db, min_age_days=14, now=NOW)
    row = _row(report, "manual")

    assert row.counts["created"] == 4
    assert row.counts["candidate_generated"] == 1
    assert row.counts["stale"] == 1
    assert row.counts["abandoned"] == 2
    assert row.stale_idea_ids == (stale_idea,)
    assert row.abandoned_idea_ids == (dismissed_idea, abandoned_content_idea)


def test_date_and_source_type_filters_are_applied(db):
    included = db.add_content_idea("Included", source="manual")
    _set_idea_created_at(db, included, "2026-04-15T12:00:00+00:00")
    old = db.add_content_idea("Old", source="manual")
    _set_idea_created_at(db, old, "2026-04-01T12:00:00+00:00")
    other_source = db.add_content_idea("Other source", source="release_digest")
    _set_idea_created_at(db, other_source, "2026-04-15T12:00:00+00:00")

    report = build_content_idea_conversion_funnel_report(
        db,
        start_date="2026-04-10",
        end_date="2026-04-20",
        source_type="manual",
        min_age_days=30,
        now=NOW,
    )

    assert [row.source_type for row in report.rows] == ["manual"]
    assert report.rows[0].idea_ids == (included,)
    assert report.filters == {
        "start_date": "2026-04-10",
        "end_date": "2026-04-20",
        "source_type": "manual",
        "min_age_days": 30,
    }


def test_json_and_csv_outputs_are_deterministically_ordered(db):
    beta = db.add_content_idea("Beta", source="beta")
    alpha = db.add_content_idea("Alpha", source="alpha")
    _set_idea_created_at(db, beta, "2026-04-15T12:00:00+00:00")
    _set_idea_created_at(db, alpha, "2026-04-14T12:00:00+00:00")

    report = build_content_idea_conversion_funnel_report(db, min_age_days=60, now=NOW)
    payload = json.loads(format_content_idea_conversion_funnel_json(report))
    csv_rows = list(csv.DictReader(StringIO(format_content_idea_conversion_funnel_csv(report))))

    assert [row.source_type for row in report.rows] == ["alpha", "beta"]
    assert payload["artifact_type"] == "content_idea_conversion_funnel"
    assert payload["generated_at"] == "2026-05-03T12:00:00+00:00"
    assert [row["source_type"] for row in payload["rows"]] == ["alpha", "beta"]
    assert [row["source_type"] for row in csv_rows] == ["alpha", "beta"]
    assert csv_rows[0]["created"] == "1"
    assert csv_rows[0]["idea_ids"] == str(alpha)


def test_cli_supports_filters_format_and_argument_validation(db, monkeypatch, capsys):
    idea_id = db.add_content_idea("CLI idea", source="manual")
    _set_idea_created_at(db, idea_id, "2026-04-15T12:00:00+00:00")
    monkeypatch.setattr(
        content_idea_conversion_funnel_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        content_idea_conversion_funnel_script,
        "build_content_idea_conversion_funnel_report",
        lambda db, **kwargs: build_content_idea_conversion_funnel_report(db, now=NOW, **kwargs),
    )

    assert content_idea_conversion_funnel_script.main(["--min-age-days", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err

    exit_code = content_idea_conversion_funnel_script.main(
        [
            "--start-date",
            "2026-04-01",
            "--end-date",
            "2026-04-30",
            "--source-type",
            "manual",
            "--min-age-days",
            "14",
            "--format",
            "csv",
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert output.splitlines()[0].startswith("source_type,created,candidate_generated")
    assert "manual,1,0,0,0,0,1" in output
