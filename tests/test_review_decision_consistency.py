"""Tests for review decision consistency reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.review_decision_consistency import (
    build_review_decision_consistency_report,
    format_review_decision_consistency_json,
    format_review_decision_consistency_table,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "review_decision_consistency.py"
spec = importlib.util.spec_from_file_location("review_decision_consistency_script", SCRIPT_PATH)
review_decision_consistency_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(review_decision_consistency_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, score: float, decision: str, *, gate_passed: int) -> int:
    content_id = db.insert_generated_content("x_post", [], [], "post", score, "ok")
    db.conn.execute("UPDATE generated_content SET curation_quality = ? WHERE id = ?", (decision, content_id))
    db.conn.execute(
        """INSERT INTO content_persona_guard (content_id, checked, passed, status, score)
           VALUES (?, 1, ?, ?, ?)""",
        (content_id, gate_passed, "passed" if gate_passed else "failed", score),
    )
    db.conn.commit()
    return int(content_id)


def test_flags_low_score_approval_high_score_rejection_and_gate_mismatch(db):
    approved_low = _content(db, 4.0, "good", gate_passed=1)
    rejected_high = _content(db, 9.0, "too_specific", gate_passed=0)
    mismatch = _content(db, 9.0, "good", gate_passed=0)
    consistent = _content(db, 8.5, "good", gate_passed=1)

    report = build_review_decision_consistency_report(db, low_score_threshold=5.0, high_score_threshold=8.0, now=NOW)
    rows = {row.item_id: row.to_dict() for row in report.rows}

    assert rows[approved_low]["inconsistency_codes"] == ["approved_low_score"]
    assert rows[rejected_high]["inconsistency_codes"] == ["rejected_high_score"]
    assert rows[mismatch]["inconsistency_codes"] == ["final_gate_mismatch"]
    assert rows[consistent]["consistency_status"] == "consistent"


def test_threshold_json_table_and_cli(db, monkeypatch, capsys):
    content_id = _content(db, 4.5, "good", gate_passed=1)
    report = build_review_decision_consistency_report(db, low_score_threshold=5.0, high_score_threshold=8.0, now=NOW)
    payload = json.loads(format_review_decision_consistency_json(report))

    assert payload["artifact_type"] == "review_decision_consistency"
    assert payload["rows"][0]["item_id"] == content_id
    assert "Review Decision Consistency" in format_review_decision_consistency_table(report)

    monkeypatch.setattr(review_decision_consistency_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        review_decision_consistency_script,
        "build_review_decision_consistency_report",
        lambda db, **kwargs: build_review_decision_consistency_report(db, now=NOW, **kwargs),
    )
    assert review_decision_consistency_script.main(["--format", "table", "--low-score-threshold", "6"]) == 0
    assert "item_id | review_decision" in capsys.readouterr().out
