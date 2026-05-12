"""Tests for generated visual alt-text backlog reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from output.visual_alt_text_backlog import (
    build_visual_alt_text_backlog_report,
    format_visual_alt_text_backlog_json,
    format_visual_alt_text_backlog_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "visual_alt_text_backlog.py"
spec = importlib.util.spec_from_file_location("visual_alt_text_backlog_script", SCRIPT_PATH)
visual_alt_text_backlog_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(visual_alt_text_backlog_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    image_path: str | None = "/tmp/chart.png",
    image_prompt: str | None = "A chart showing weekly publication throughput",
    image_alt_text: str | None = "Line chart showing weekly publication throughput rising across May.",
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        content="Visual post",
        eval_score=8,
        eval_feedback="ok",
        image_path=image_path,
        image_prompt=image_prompt,
        image_alt_text=image_alt_text,
    )
    db.conn.commit()
    return content_id


def test_reports_image_path_with_empty_alt_text_as_missing(db):
    content_id = _content(db, image_alt_text="")

    report = build_visual_alt_text_backlog_report(db, now=NOW)

    assert len(report.rows) == 1
    assert report.rows[0].content_id == content_id
    assert report.rows[0].issue_type == "missing_alt_text"
    assert report.rows[0].severity == "error"


def test_reports_generic_placeholder_alt_text(db):
    content_id = _content(db, image_alt_text="Generated image")

    report = build_visual_alt_text_backlog_report(db, now=NOW)

    assert report.rows[0].content_id == content_id
    assert report.rows[0].issue_type == "placeholder_alt_text"
    assert report.rows[0].severity == "warning"


def test_reports_overly_short_alt_text_and_excludes_descriptive_rows(db):
    short = _content(db, image_alt_text="Small chart")
    ok = _content(db, image_alt_text="Bar chart comparing retry failures by platform and week.")
    prompt_only = _content(db, image_path=None, image_prompt="Prompt creates a visual", image_alt_text=None)
    _content(db, image_path=None, image_prompt=None, image_alt_text="")

    report = build_visual_alt_text_backlog_report(db, min_chars=20, now=NOW)
    by_id = {row.content_id: row for row in report.rows}

    assert by_id[short].issue_type == "too_short_alt_text"
    assert by_id[prompt_only].issue_type == "missing_alt_text"
    assert ok not in by_id
    assert report.totals == {
        "row_count": 2,
        "missing_alt_text": 1,
        "placeholder_alt_text": 0,
        "too_short_alt_text": 1,
    }


def test_json_and_text_output_are_deterministic(db):
    _content(db, image_alt_text="Image")

    report = build_visual_alt_text_backlog_report(db, now=NOW)
    payload = json.loads(format_visual_alt_text_backlog_json(report))
    text = format_visual_alt_text_backlog_text(report)

    assert payload["artifact_type"] == "visual_alt_text_backlog"
    assert payload["rows"][0]["issue_type"] == "placeholder_alt_text"
    assert "Visual Alt Text Backlog" in text


def test_cli_outputs_json(db, capsys, monkeypatch):
    _content(db, image_alt_text="")
    monkeypatch.setattr(visual_alt_text_backlog_script, "script_context", lambda: _script_context(db))

    assert visual_alt_text_backlog_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["totals"]["missing_alt_text"] == 1


def test_invalid_min_chars_raises(db):
    with pytest.raises(ValueError, match="min_chars"):
        build_visual_alt_text_backlog_report(db, min_chars=0)
