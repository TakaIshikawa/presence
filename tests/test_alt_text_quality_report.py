"""Tests for generated visual alt-text quality reports."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from synthesis.alt_text_quality_report import (
    GENERIC,
    MISSING,
    PASS,
    REDUNDANT,
    TOO_SHORT,
    build_alt_text_quality_report,
    format_alt_text_quality_json,
    format_alt_text_quality_text,
    score_alt_text_row,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "alt_text_quality_report.py"
spec = importlib.util.spec_from_file_location("alt_text_quality_report_script", SCRIPT_PATH)
alt_text_quality_report_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(alt_text_quality_report_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _visual(
    db,
    *,
    content: str = "Launch metrics improved after a dashboard cleanup.",
    content_type: str = "x_visual",
    image_path: str | None = "/tmp/presence-images/launch-dashboard.png",
    image_alt_text: str | None = "Launch dashboard chart with conversion trend labels and rollout notes.",
    created_at: str = "2026-04-30T12:00:00+00:00",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="usable",
        image_path=image_path,
        image_alt_text=image_alt_text,
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at, content_id),
    )
    db.conn.commit()
    return content_id


def test_score_flags_missing_short_generic_filename_and_redundant_alt_text():
    rows = [
        {"id": 1, "content_type": "x_visual", "image_path": "/tmp/a.png", "image_alt_text": ""},
        {"id": 2, "content_type": "x_visual", "image_path": "/tmp/a.png", "image_alt_text": "Chart"},
        {
            "id": 3,
            "content_type": "x_visual",
            "image_path": "/tmp/a.png",
            "image_alt_text": "A screenshot",
        },
        {
            "id": 4,
            "content_type": "x_visual",
            "image_path": "/tmp/launch-dashboard-42.png",
            "image_alt_text": "launch-dashboard-42.png",
        },
        {
            "id": 5,
            "content_type": "x_visual",
            "content": "Launch dashboard chart with conversion trend labels and rollout notes.",
            "image_path": "/tmp/a.png",
            "image_alt_text": "Launch dashboard chart with conversion trend labels and rollout notes.",
        },
    ]

    scored = [score_alt_text_row(row) for row in rows]

    assert [row.status for row in scored] == [MISSING, TOO_SHORT, TOO_SHORT, GENERIC, REDUNDANT]
    assert scored[0].quality_flags == ("missing_alt_text",)
    assert "generic_wording" in scored[2].quality_flags
    assert "filename_leakage" in scored[3].quality_flags
    assert scored[4].remediation.startswith("Rewrite the alt text")


def test_build_report_inspects_rows_with_image_path_or_visual_content_type_and_filters_days(db):
    recent_visual_without_path = _visual(
        db,
        content_type="x_visual",
        image_path=None,
        image_alt_text="Release card showing status labels and deployment timing.",
    )
    image_post = _visual(
        db,
        content_type="x_post",
        image_path="/tmp/image.png",
        image_alt_text="Image post chart with clear annotations and trend labels.",
    )
    old_visual = _visual(
        db,
        image_path="/tmp/old.png",
        image_alt_text="Old chart with clear annotations and trend labels.",
        created_at="2026-04-01T12:00:00+00:00",
    )
    text_only = _visual(
        db,
        content_type="x_post",
        image_path=None,
        image_alt_text=None,
    )

    rows = build_alt_text_quality_report(db, days=7, now=NOW)
    ids = {row.content_id for row in rows}

    assert recent_visual_without_path in ids
    assert image_post in ids
    assert old_visual not in ids
    assert text_only not in ids


def test_status_filter_and_stable_json_output(db):
    missing_id = _visual(db, image_alt_text="")
    _visual(
        db,
        image_path="/tmp/pass.png",
        image_alt_text="Dashboard panel showing labeled conversion trends and release status.",
    )

    rows = build_alt_text_quality_report(db, days=7, status=MISSING, now=NOW)
    payload = json.loads(format_alt_text_quality_json(rows))
    text = format_alt_text_quality_text(rows)

    assert [row.content_id for row in rows] == [missing_id]
    assert payload == [
        {
            "content_id": missing_id,
            "content_type": "x_visual",
            "created_at": "2026-04-30T12:00:00+00:00",
            "image_alt_text": None,
            "image_path": "/tmp/presence-images/launch-dashboard.png",
            "quality_flags": ["missing_alt_text"],
            "remediation": "Add concise alt text describing the visual's key information and context.",
            "status": "missing",
        }
    ]
    assert list(payload[0].keys()) == sorted(payload[0].keys())
    assert "missing=1" in text


def test_cli_supports_status_filter_and_json_output(db, monkeypatch, capsys):
    missing_id = _visual(db, image_alt_text="")
    _visual(
        db,
        image_path="/tmp/pass.png",
        image_alt_text="Dashboard panel showing labeled conversion trends and release status.",
    )
    monkeypatch.setattr(alt_text_quality_report_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        alt_text_quality_report_script,
        "build_alt_text_quality_report",
        lambda db, **kwargs: build_alt_text_quality_report(db, now=NOW, **kwargs),
    )

    exit_code = alt_text_quality_report_script.main(
        ["--days", "7", "--status", "missing", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert [item["content_id"] for item in payload] == [missing_id]
    assert payload[0]["status"] == MISSING


def test_passing_row_status_is_pass():
    row = score_alt_text_row(
        {
            "id": 10,
            "content_type": "x_visual",
            "content": "A short post about launch instrumentation.",
            "image_path": "/tmp/chart.png",
            "image_alt_text": "Dashboard chart showing launch instrumentation trends and labeled review checkpoints.",
        }
    )

    assert row.status == PASS
    assert row.quality_flags == ()
    assert row.remediation == "No remediation needed."
