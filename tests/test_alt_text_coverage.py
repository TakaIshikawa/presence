"""Tests for generated visual alt-text coverage audits."""

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from alt_text_coverage import fetch_visual_content_rows, main
from synthesis.alt_text_coverage import audit_alt_text_coverage


def _visual_row(**overrides):
    row = {
        "content_id": 1,
        "content": "Launch metrics improved after a dashboard cleanup.",
        "content_type": "x_visual",
        "image_path": "/tmp/presence-images/launch.png",
        "image_prompt": "Launch metrics dashboard with conversion trend annotations",
        "image_alt_text": (
            "Launch metrics dashboard with conversion trend annotations and status labels."
        ),
        "created_at": "2026-04-24 10:00:00",
    }
    row.update(overrides)
    return row


def test_audit_classifies_missing_too_short_duplicate_and_ok():
    rows = [
        _visual_row(content_id=1, image_alt_text=""),
        _visual_row(content_id=2, image_alt_text="Tiny"),
        _visual_row(
            content_id=3,
            content="Launch metrics dashboard with conversion trend annotations.",
            image_alt_text="Launch metrics dashboard with conversion trend annotations.",
        ),
        _visual_row(content_id=4),
    ]

    report = audit_alt_text_coverage(rows, include_ok=True)
    statuses = {item.content_id: item.status for item in report.items}

    assert report.total == 4
    assert report.missing == 1
    assert report.too_short == 1
    assert report.duplicate_content == 1
    assert report.ok == 1
    assert statuses == {
        1: "missing",
        2: "too_short",
        3: "duplicate_content",
        4: "ok",
    }


def test_audit_flags_reused_alt_text_as_duplicate_content():
    alt = "Launch metrics dashboard with conversion trend annotations and status labels."
    rows = [
        _visual_row(content_id=1, image_alt_text=alt),
        _visual_row(content_id=2, image_alt_text=alt),
    ]

    report = audit_alt_text_coverage(rows, include_ok=True)

    assert report.duplicate_content == 2
    assert {item.status for item in report.items} == {"duplicate_content"}
    assert all("duplicate_content" in item.issue_codes for item in report.items)


def test_audit_excludes_ok_items_by_default():
    report = audit_alt_text_coverage([_visual_row()])

    assert report.ok == 1
    assert report.items == ()


def test_min_length_threshold_changes_too_short_result():
    row = _visual_row(image_alt_text="Launch metrics visual summary")

    passing = audit_alt_text_coverage([row], min_length=10, include_ok=True)
    failing = audit_alt_text_coverage([row], min_length=40, include_ok=True)

    assert passing.items[0].status == "ok"
    assert failing.items[0].status == "too_short"


def test_fetch_visual_content_rows_filters_by_date_and_visual_shape(db):
    recent_visual = db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        content="Recent visual",
        eval_score=8,
        eval_feedback="ok",
        image_path=None,
        image_alt_text="Recent visual with clear status labels.",
    )
    old_visual = db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        content="Old visual",
        eval_score=8,
        eval_feedback="ok",
        image_path="/tmp/old.png",
        image_alt_text="Old visual with clear status labels.",
    )
    non_visual = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Text only",
        eval_score=8,
        eval_feedback="ok",
    )
    image_post = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Image attached",
        eval_score=8,
        eval_feedback="ok",
        image_path="/tmp/image.png",
        image_alt_text="Image attached with clear status labels.",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = datetime('now', '-10 days') WHERE id = ?",
        (old_visual,),
    )
    db.conn.commit()

    rows = fetch_visual_content_rows(db, days=2)
    ids = {row["content_id"] for row in rows}

    assert recent_visual in ids
    assert image_post in ids
    assert old_visual not in ids
    assert non_visual not in ids


def test_cli_json_report(db, capsys):
    db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        content="Missing alt",
        eval_score=8,
        eval_feedback="ok",
        image_path="/tmp/missing.png",
        image_alt_text="",
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("alt_text_coverage.script_context", fake_script_context), patch(
        "sys.argv", ["alt_text_coverage.py", "--days", "1", "--json"]
    ):
        main()

    payload = json.loads(capsys.readouterr().out)
    assert payload["days"] == 1
    assert payload["totals"]["missing"] == 1
    assert payload["items"][0]["content_id"] is not None
    assert payload["items"][0]["issue_codes"] == ["missing_alt_text"]
