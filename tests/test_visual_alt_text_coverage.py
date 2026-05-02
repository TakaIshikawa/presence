"""Tests for generated visual alt-text coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.visual_alt_text_coverage import (
    build_visual_alt_text_coverage_report,
    format_visual_alt_text_coverage_json,
    format_visual_alt_text_coverage_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "visual_alt_text_coverage.py"
spec = importlib.util.spec_from_file_location("visual_alt_text_coverage_script", SCRIPT_PATH)
visual_alt_text_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(visual_alt_text_coverage_script)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_content(
    db,
    *,
    content_type: str = "x_visual",
    image_path: str | None = "/tmp/chart.png",
    image_prompt: str | None = "Pipeline dashboard with review status labels",
    image_alt_text: str | None = "Pipeline dashboard with review status labels and throughput trend.",
    created_at: str = "2026-04-30T12:00:00+00:00",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content="Generated content",
        eval_score=8,
        eval_feedback="ok",
        image_path=image_path,
        image_prompt=image_prompt,
        image_alt_text=image_alt_text,
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at, content_id),
    )
    db.conn.commit()
    return content_id


def test_empty_schema_degrades_to_empty_report():
    report = build_visual_alt_text_coverage_report(sqlite3.connect(":memory:"), now=NOW)
    payload = json.loads(format_visual_alt_text_coverage_json(report))

    assert payload["artifact_type"] == "visual_alt_text_coverage"
    assert payload["generated_at"] == "2026-05-01T12:00:00+00:00"
    assert payload["totals"]["visual_content"] == 0
    assert payload["findings"] == []


def test_each_finding_bucket_and_totals_are_reported(db):
    missing = _add_content(db, image_alt_text="")
    short = _add_content(db, image_alt_text="Tiny")
    duplicate = _add_content(
        db,
        image_prompt="Release chart with latency by week",
        image_alt_text="Release chart with latency by week",
    )
    ok = _add_content(db)
    _add_content(
        db,
        content_type="x_post",
        image_path=None,
        image_alt_text="Text-only post should not be counted.",
    )

    report = build_visual_alt_text_coverage_report(db, min_chars=12, now=NOW)
    by_id = {finding.content_id: finding for finding in report.findings}

    assert report.totals == {
        "visual_content": 4,
        "ok": 1,
        "findings": 3,
        "missing_alt_text": 1,
        "too_short_alt_text": 1,
        "duplicate_prompt_alt_text": 1,
    }
    assert by_id[missing].finding_type == "missing_alt_text"
    assert by_id[missing].recommended_action == "write_descriptive_alt_text"
    assert by_id[short].finding_type == "too_short_alt_text"
    assert by_id[short].recommended_action == "expand_alt_text"
    assert by_id[duplicate].finding_type == "duplicate_prompt_alt_text"
    assert by_id[duplicate].recommended_action == "rewrite_alt_text_for_accessibility"
    assert ok not in by_id


def test_visual_content_type_without_image_path_is_audited(db):
    content_id = _add_content(
        db,
        content_type="visual",
        image_path=None,
        image_alt_text=None,
    )

    report = build_visual_alt_text_coverage_report(db, now=NOW)

    assert report.totals["visual_content"] == 1
    assert report.findings[0].content_id == content_id
    assert report.findings[0].finding_type == "missing_alt_text"


def test_deterministic_sorting_and_output(db):
    warning = _add_content(db, image_alt_text="Short")
    error = _add_content(db, image_alt_text="")
    earlier_error = _add_content(db, image_alt_text="")

    report = build_visual_alt_text_coverage_report(db, min_chars=20, now=NOW)
    payload = json.loads(format_visual_alt_text_coverage_json(report))
    text = format_visual_alt_text_coverage_text(report)

    assert [finding.content_id for finding in report.findings] == [
        error,
        earlier_error,
        warning,
    ]
    assert [item["severity"] for item in payload["findings"]] == [
        "error",
        "error",
        "warning",
    ]
    assert "Visual Alt Text Coverage" in text
    assert "missing=2 too_short=1 duplicate_prompt=0" in text


def test_window_days_filters_created_at(db):
    recent = _add_content(db, created_at="2026-04-30T12:00:00+00:00", image_alt_text="")
    old = _add_content(db, created_at="2026-03-01T12:00:00+00:00", image_alt_text="")

    report = build_visual_alt_text_coverage_report(db, days=7, now=NOW)

    assert [finding.content_id for finding in report.findings] == [recent]
    assert old not in {finding.content_id for finding in report.findings}


def test_partial_generated_content_schema_degrades_to_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content TEXT)")

    report = build_visual_alt_text_coverage_report(conn, now=NOW)

    assert report.totals["visual_content"] == 0
    assert report.findings == ()
    assert report.missing_optional_columns == (
        "content_type",
        "image_path",
        "image_alt_text",
    )


def test_invalid_builder_arguments_raise():
    with pytest.raises(ValueError, match="days"):
        build_visual_alt_text_coverage_report(sqlite3.connect(":memory:"), days=0)
    with pytest.raises(ValueError, match="min_chars"):
        build_visual_alt_text_coverage_report(sqlite3.connect(":memory:"), min_chars=0)


def test_cli_outputs_json_and_validates_positive_arguments(db, capsys, monkeypatch):
    _add_content(db, image_alt_text="")
    monkeypatch.setattr(
        visual_alt_text_coverage_script,
        "script_context",
        lambda: _script_context(db),
    )

    assert visual_alt_text_coverage_script.main(
        ["--format", "json", "--days", "7", "--min-chars", "12"]
    ) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["window_days"] == 7
    assert payload["min_chars"] == 12
    assert payload["findings"][0]["finding_type"] == "missing_alt_text"
    with pytest.raises(SystemExit):
        visual_alt_text_coverage_script.parse_args(["--days", "0"])
    with pytest.raises(SystemExit):
        visual_alt_text_coverage_script.parse_args(["--min-chars", "-1"])
