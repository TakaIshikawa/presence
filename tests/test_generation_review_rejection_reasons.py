"""Tests for generation review rejection reason reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.generation_review_rejection_reasons import (
    build_generation_review_rejection_reasons_report,
    build_generation_review_rejection_reasons_report_from_db,
    format_generation_review_rejection_reasons_text,
    normalize_generation_rejection_reason,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "generation_review_rejection_reasons.py"
spec = importlib.util.spec_from_file_location("generation_review_rejection_reasons_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_reason_normalization_groups_code_text_and_blanks():
    assert normalize_generation_rejection_reason("Below threshold score (6.2 < 7)", None) == "below_threshold"
    assert normalize_generation_rejection_reason("", "score_threshold") == "below_threshold"
    assert normalize_generation_rejection_reason("Unsupported claim in output", None) == "unsupported_claims"
    assert normalize_generation_rejection_reason("All candidates filtered", None) == "all_filtered"
    assert normalize_generation_rejection_reason(None, None) == "unknown"


def test_ranked_reason_summaries_include_breakdowns_and_examples():
    rows = [
        {
            "id": 1,
            "content_id": 101,
            "status": "rejected",
            "gate_name": "quality_gate",
            "reason_text": "Below threshold score (6.4 < 7.0)",
            "model": "gpt-5.4",
            "prompt_version": "v2",
            "content_format": "blog_post",
            "created_at": "2026-05-15T09:00:00+00:00",
        },
        {
            "id": 2,
            "content_id": 102,
            "status": "rejected",
            "gate_name": "quality_gate",
            "reason_code": "score_threshold",
            "model": "gpt-5.4",
            "prompt_version": "v2",
            "content_format": "blog_post",
            "created_at": "2026-05-15T10:00:00+00:00",
        },
        {
            "id": 3,
            "content_id": 103,
            "status": "rejected",
            "gate_name": "claim_check",
            "reason_text": "Unsupported claims detected",
            "model": "gpt-5.3",
            "prompt_version": "v1",
            "content_format": "newsletter",
            "created_at": "2026-05-15T11:00:00+00:00",
        },
        {
            "id": 4,
            "content_id": 104,
            "status": "approved",
            "reason_text": "",
            "content_format": "blog_post",
        },
    ]

    report = build_generation_review_rejection_reasons_report(rows, examples_limit=1, now=NOW)

    assert [item["reason_label"] for item in report["reasons"]] == ["below_threshold", "unsupported_claims"]
    threshold = report["reasons"][0]
    assert threshold["count"] == 2
    assert threshold["affected_gates"] == {"quality_gate": 2}
    assert threshold["models"] == {"gpt-5.4": 2}
    assert threshold["prompt_versions"] == {"v2": 2}
    assert threshold["content_formats"] == {"blog_post": 2}
    assert len(threshold["affected_examples"]) == 1
    assert threshold["affected_examples"][0]["content_id"] == "102"
    assert report["totals"]["rows_scanned"] == 4
    assert report["totals"]["rejected_record_count"] == 3
    assert "Generation Review Rejection Reasons" in format_generation_review_rejection_reasons_text(report)


def test_blank_rejected_reason_is_grouped_as_unknown():
    report = build_generation_review_rejection_reasons_report(
        [{"id": 1, "status": "rejected", "gate_name": "manual_review", "reason_text": ""}],
        now=NOW,
    )

    assert report["reasons"][0]["reason_label"] == "unknown"
    assert report["reasons"][0]["affected_gates"] == {"manual_review": 1}


def test_db_loader_and_cli_json_output(db, monkeypatch, capsys):
    content_id = db.insert_generated_content(
        content_type="blog_post",
        source_commits=[],
        source_messages=[],
        content="A generated draft",
        eval_score=5,
        eval_feedback="needs work",
    )
    db.conn.execute(
        "UPDATE generated_content SET content_format = ? WHERE id = ?",
        ("case_study", content_id),
    )
    db.conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, content_id, outcome, published, rejection_reason, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "batch-1",
            "blog_post",
            content_id,
            "below_threshold",
            0,
            "Below threshold score",
            NOW.isoformat(),
        ),
    )
    db.conn.commit()

    report = build_generation_review_rejection_reasons_report_from_db(db, now=NOW)
    assert report["reasons"][0]["reason_label"] == "below_threshold"
    assert report["reasons"][0]["content_formats"] == {"case_study": 1}

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_generation_review_rejection_reasons_report_from_db",
        lambda db, **kwargs: build_generation_review_rejection_reasons_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "generation_review_rejection_reasons"

    assert script.main(["--table"]) == 0
    assert "Generation Review Rejection Reasons" in capsys.readouterr().out
