"""Tests for persona drift reporting."""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.persona_drift_report import (  # noqa: E402
    PersonaDriftReporter,
    format_json_report,
    format_text_report,
)
from persona_drift_report import main  # noqa: E402


def _insert_guarded_content(
    db,
    *,
    content: str,
    passed: bool,
    score: float,
    reasons: list[str],
    created_at: datetime,
    content_type: str = "x_post",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=score,
        eval_feedback="test",
    )
    db.save_persona_guard_summary(
        content_id,
        {
            "checked": True,
            "passed": passed,
            "status": "passed" if passed else "failed",
            "score": score,
            "reasons": reasons,
            "metrics": {"grounding_score": score},
        },
    )
    timestamp = created_at.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
    db.conn.execute(
        "UPDATE content_persona_guard SET created_at = ?, updated_at = ? WHERE content_id = ?",
        (timestamp, timestamp, content_id),
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (timestamp, content_id),
    )
    db.conn.commit()
    return content_id


def test_report_aggregates_persona_guard_outcomes(db):
    now = datetime.now(timezone.utc)
    passed_id = _insert_guarded_content(
        db,
        content="Grounded post",
        passed=True,
        score=0.82,
        reasons=[],
        created_at=now - timedelta(days=1),
    )
    failed_old_id = _insert_guarded_content(
        db,
        content="Generic post",
        passed=False,
        score=0.31,
        reasons=["generic abstraction", "banned tone marker"],
        created_at=now - timedelta(days=2),
    )
    failed_recent_id = _insert_guarded_content(
        db,
        content="Another generic post",
        passed=False,
        score=0.41,
        reasons=["generic abstraction"],
        created_at=now - timedelta(hours=3),
        content_type="x_thread",
    )
    _insert_guarded_content(
        db,
        content="Outside range",
        passed=False,
        score=0.1,
        reasons=["old reason"],
        created_at=now - timedelta(days=15),
    )

    report = PersonaDriftReporter(db).build_report(days=7, limit_failures=1)

    assert report.total == 3
    assert report.passed == 1
    assert report.failed == 2
    assert report.pass_rate == 0.333
    assert report.average_score == 0.513
    assert report.reason_counts == {
        "generic abstraction": 2,
        "banned tone marker": 1,
    }
    assert [failure.content_id for failure in report.recent_failures] == [failed_recent_id]
    assert passed_id not in [failure.content_id for failure in report.recent_failures]
    assert failed_old_id not in [failure.content_id for failure in report.recent_failures]


def test_empty_dataset_returns_zero_report(db):
    report = PersonaDriftReporter(db).build_report(days=7)

    assert report.to_dict() == {
        "days": 7,
        "totals": {"total": 0, "passed": 0, "failed": 0},
        "pass_rate": 0.0,
        "average_score": 0.0,
        "reason_counts": {},
        "recent_failures": [],
    }
    assert "No persona guard rows found." in format_text_report(report)


def test_json_output_is_stable_and_automation_friendly(db):
    now = datetime.now(timezone.utc)
    content_id = _insert_guarded_content(
        db,
        content="Failed post",
        passed=False,
        score=0.25,
        reasons=["banned tone marker"],
        created_at=now,
    )

    report = PersonaDriftReporter(db).build_report(days=7)
    payload = json.loads(format_json_report(report))

    assert list(payload.keys()) == [
        "average_score",
        "days",
        "pass_rate",
        "reason_counts",
        "recent_failures",
        "totals",
    ]
    assert payload["totals"] == {"failed": 1, "passed": 0, "total": 1}
    assert payload["recent_failures"][0]["content_id"] == content_id


def test_text_output_includes_failed_content_ids(db):
    now = datetime.now(timezone.utc)
    content_id = _insert_guarded_content(
        db,
        content="Failed post",
        passed=False,
        score=0.25,
        reasons=["banned tone marker"],
        created_at=now,
    )

    output = format_text_report(PersonaDriftReporter(db).build_report(days=7))

    assert f"content_id={content_id}" in output
    assert "banned tone marker" in output


def test_cli_supports_json_and_limit_failures(db, capsys):
    now = datetime.now(timezone.utc)
    _insert_guarded_content(
        db,
        content="Failed post one",
        passed=False,
        score=0.25,
        reasons=["banned tone marker"],
        created_at=now - timedelta(minutes=2),
    )
    recent_id = _insert_guarded_content(
        db,
        content="Failed post two",
        passed=False,
        score=0.35,
        reasons=["generic abstraction"],
        created_at=now,
    )

    context = MagicMock()
    context.__enter__.return_value = (MagicMock(), db)
    context.__exit__.return_value = None

    with patch("persona_drift_report.script_context", return_value=context):
        main(["--days", "7", "--json", "--limit-failures", "1"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["totals"] == {"failed": 2, "passed": 0, "total": 2}
    assert [item["content_id"] for item in payload["recent_failures"]] == [recent_id]
