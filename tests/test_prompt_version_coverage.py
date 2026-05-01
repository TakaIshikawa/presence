"""Tests for prompt version coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from evaluation.prompt_version_coverage import (
    build_prompt_version_coverage_report,
    format_prompt_version_coverage_json,
    format_prompt_version_coverage_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "prompt_version_coverage.py"
spec = importlib.util.spec_from_file_location("prompt_version_coverage_script", SCRIPT_PATH)
prompt_version_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(prompt_version_coverage_script)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _write_prompt(tmp_path: Path, name: str, text: str) -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def _content(db, content_type: str, created_at: datetime, eval_score: float = 8.0) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=f"{content_type} generated content",
        eval_score=eval_score,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at.isoformat(), content_id),
    )
    db.conn.commit()
    return content_id


def _prediction(
    db,
    content_id: int,
    *,
    prompt_type: str,
    prompt_version: str | None,
    prompt_hash: str | None,
    created_at: datetime,
    predicted_score: float = 8.0,
    actual_score: float | None = None,
) -> int:
    prediction_id = db.insert_prediction(
        content_id=content_id,
        predicted_score=predicted_score,
        prompt_type=prompt_type,
        prompt_version=prompt_version,
        prompt_hash=prompt_hash,
    )
    error = None if actual_score is None else actual_score - predicted_score
    db.conn.execute(
        """UPDATE engagement_predictions
           SET created_at = ?, actual_engagement_score = ?, prediction_error = ?
           WHERE id = ?""",
        (created_at.isoformat(), actual_score, error, prediction_id),
    )
    db.conn.commit()
    return prediction_id


def test_report_lists_prompt_files_usage_and_prediction_metadata(db, tmp_path):
    v2 = _write_prompt(tmp_path, "x_post_v2.txt", "version two prompt")
    _write_prompt(tmp_path, "final_gate.txt", "unused gate prompt")
    prompt_record = db.register_prompt_version("x_post", v2.read_text(encoding="utf-8"))
    content_id = _content(db, "x_post", NOW - timedelta(days=1))
    _prediction(
        db,
        content_id,
        prompt_type="x_post",
        prompt_version="2",
        prompt_hash=prompt_record["prompt_hash"],
        created_at=NOW - timedelta(hours=12),
        predicted_score=8.0,
        actual_score=9.0,
    )

    report = build_prompt_version_coverage_report(db, days=7, prompts_dir=tmp_path, now=NOW)
    payload = report.to_dict()

    assert payload["artifact_type"] == "prompt_version_coverage"
    assert payload["counts"]["prompt_files"] == 2
    x_post = next(row for row in payload["prompts"] if row["prompt_file"] == "x_post_v2.txt")
    assert x_post["prompt_type"] == "x_post"
    assert x_post["inferred_version"] == 2
    assert x_post["registered_version"] == prompt_record["version"]
    assert x_post["recent_usage_count"] == 1
    assert x_post["latest_usage_at"] == (NOW - timedelta(days=1)).isoformat()
    assert x_post["recent_prediction_count"] == 1
    assert x_post["avg_actual_engagement_score"] == 9.0
    assert "recent_usage" in x_post["statuses"]
    unused = next(row for row in payload["prompts"] if row["prompt_file"] == "final_gate.txt")
    assert unused["statuses"] == ["unvalidated"]


def test_stale_and_weak_outcome_flags_are_reported(db, tmp_path):
    old_prompt = _write_prompt(tmp_path, "refiner.txt", "refine prompt")
    weak_prompt = _write_prompt(tmp_path, "x_thread.txt", "thread prompt")
    old_content_id = _content(db, "refiner", NOW - timedelta(days=20))
    weak_content_id = _content(db, "x_thread", NOW - timedelta(days=1))
    _prediction(
        db,
        old_content_id,
        prompt_type="refiner",
        prompt_version="1",
        prompt_hash=None,
        created_at=NOW - timedelta(days=20),
    )
    _prediction(
        db,
        weak_content_id,
        prompt_type="x_thread",
        prompt_version="1",
        prompt_hash=None,
        created_at=NOW - timedelta(hours=6),
        predicted_score=9.0,
        actual_score=3.0,
    )

    assert old_prompt.exists()
    assert weak_prompt.exists()
    report = build_prompt_version_coverage_report(db, days=7, prompts_dir=tmp_path, now=NOW)

    stale = next(row for row in report.prompts if row.prompt_file == "refiner.txt")
    weak = next(row for row in report.prompts if row.prompt_file == "x_thread.txt")
    assert stale.recent_usage_count == 0
    assert stale.total_usage_count == 1
    assert "stale" in stale.statuses
    assert "weak_outcomes" in weak.statuses
    assert weak.mean_absolute_prediction_error == 6.0
    assert report.counts["stale"] == 1
    assert report.counts["weak_outcomes"] == 1


def test_json_and_text_output_are_stable(db, tmp_path):
    _write_prompt(tmp_path, "blog_post.txt", "blog prompt")
    _content(db, "blog_post", NOW - timedelta(days=1))

    report = build_prompt_version_coverage_report(db, days=7, prompts_dir=tmp_path, now=NOW)
    payload = json.loads(format_prompt_version_coverage_json(report))
    text = format_prompt_version_coverage_text(report)

    assert list(payload) == sorted(payload)
    assert payload["filters"]["days"] == 7
    assert "Prompt Version Coverage" in text
    assert "blog_post.txt type=blog_post inferred=v1" in text
    assert "recent_usage=1" in text


def test_invalid_days_are_rejected(db, tmp_path):
    with pytest.raises(ValueError, match="days"):
        build_prompt_version_coverage_report(db, days=0, prompts_dir=tmp_path, now=NOW)


def test_cli_supports_days_and_json_format(db, tmp_path, capsys):
    _write_prompt(tmp_path, "x_post.txt", "post prompt")
    _content(db, "x_post", datetime.now(timezone.utc) - timedelta(days=1))

    with patch.object(
        prompt_version_coverage_script,
        "script_context",
        return_value=_script_context(db),
    ), patch.object(
        prompt_version_coverage_script,
        "build_prompt_version_coverage_report",
        side_effect=lambda database, days: build_prompt_version_coverage_report(
            database,
            days=days,
            prompts_dir=tmp_path,
        ),
    ):
        exit_code = prompt_version_coverage_script.main(["--days", "7", "--format", "json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["filters"]["days"] == 7
    assert payload["prompts"][0]["prompt_file"] == "x_post.txt"
