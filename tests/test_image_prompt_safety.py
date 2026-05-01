"""Tests for generated image prompt safety linting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from synthesis.image_prompt_safety import (
    RULE_BRAND_LOGO_IMPERSONATION,
    RULE_DENSE_RENDERED_TEXT,
    RULE_MISSING_ALT_TEXT,
    RULE_PRIVATE_DATA,
    RULE_REAL_PERSON_LIKENESS,
    build_image_prompt_safety_report,
    format_image_prompt_safety_json,
    format_image_prompt_safety_text,
    lint_image_prompt_row,
    lint_image_prompts,
    should_fail,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "lint_image_prompts.py"
spec = importlib.util.spec_from_file_location("lint_image_prompts_script", SCRIPT_PATH)
lint_image_prompts_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(lint_image_prompts_script)

NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_content(
    db,
    *,
    image_path: str | None = None,
    image_prompt: str | None = "Annotated deployment checklist card",
    image_alt_text: str | None = "Annotated deployment checklist card with three review steps.",
    content_type: str = "x_visual",
) -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content="Visual post",
        eval_score=8.0,
        eval_feedback="ok",
        image_path=image_path,
        image_prompt=image_prompt,
        image_alt_text=image_alt_text,
    )


def test_missing_alt_text_is_error_for_image_path_or_prompt():
    path_findings = lint_image_prompt_row(
        {
            "id": 1,
            "content_type": "x_post",
            "image_path": "/tmp/visual.png",
            "image_prompt": "",
            "image_alt_text": "",
        }
    )
    prompt_findings = lint_image_prompt_row(
        {
            "id": 2,
            "content_type": "x_post",
            "image_path": "",
            "image_prompt": "Abstract queue dashboard",
            "image_alt_text": None,
        }
    )

    assert path_findings[0].rule_id == RULE_MISSING_ALT_TEXT
    assert path_findings[0].severity == "error"
    assert prompt_findings[0].rule_id == RULE_MISSING_ALT_TEXT


def test_prompt_rules_flag_risky_requests():
    findings = lint_image_prompt_row(
        {
            "id": 7,
            "content_type": "x_visual",
            "image_prompt": (
                "Photorealistic portrait of Ada Lovelace holding a fake OpenAI logo, "
                "with a visible SSN and lots of tiny text in the background"
            ),
            "image_alt_text": "Portrait scene with background annotations.",
        }
    )
    by_rule = {finding.rule_id: finding for finding in findings}

    assert by_rule[RULE_REAL_PERSON_LIKENESS].severity == "error"
    assert by_rule[RULE_PRIVATE_DATA].severity == "error"
    assert by_rule[RULE_BRAND_LOGO_IMPERSONATION].severity == "error"
    assert by_rule[RULE_DENSE_RENDERED_TEXT].severity == "warn"
    assert all(finding.content_id == 7 for finding in findings)
    assert all(finding.remediation for finding in findings)


def test_negated_logo_and_dense_text_requests_are_not_flagged():
    findings = lint_image_prompt_row(
        {
            "id": 9,
            "content_type": "x_visual",
            "image_prompt": "Generic product dashboard without logos and no dense text.",
            "image_alt_text": "Generic product dashboard with clean blocks and charts.",
        }
    )

    assert findings == []


def test_content_id_and_days_filters_are_applied(db):
    old_id = _insert_content(db, image_prompt="A clean release chart")
    recent_id = _insert_content(db, image_prompt="A dashboard with lots of tiny text")
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        ("2026-04-01T00:00:00+00:00", old_id),
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        ("2026-04-24T00:00:00+00:00", recent_id),
    )
    db.conn.commit()

    findings = lint_image_prompts(db, days=7, now=NOW)
    content_findings = lint_image_prompts(db, content_id=old_id, days=7, now=NOW)

    assert [finding.content_id for finding in findings] == [recent_id]
    assert content_findings == []


def test_report_formatters_and_fail_on_logic(db):
    content_id = _insert_content(
        db,
        image_prompt="Dashboard with lots of tiny text",
        image_alt_text="Dashboard packed with small rendered text.",
    )

    report = build_image_prompt_safety_report(db, content_id=content_id, fail_on="warn")
    payload = json.loads(format_image_prompt_safety_json(report))
    text = format_image_prompt_safety_text(report)

    assert report["status"] == "failed"
    assert payload["artifact_type"] == "image_prompt_safety_lint"
    assert payload["findings"][0]["rule_id"] == RULE_DENSE_RENDERED_TEXT
    assert "warn dense_rendered_text" in text
    assert should_fail(report["findings"], fail_on="warn") is True
    assert should_fail(report["findings"], fail_on="error") is False


def test_cli_json_output_uses_db_path_and_fail_on_error(file_db, capsys):
    _insert_content(
        file_db,
        image_prompt="Photorealistic portrait of Grace Hopper",
        image_alt_text="Portrait-style technical illustration.",
    )

    exit_code = lint_image_prompts_script.main(
        [
            "--db",
            str(file_db.db_path),
            "--days",
            "30",
            "--format",
            "json",
            "--fail-on",
            "error",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["filters"]["days"] == 30
    assert payload["findings"][0]["rule_id"] == RULE_REAL_PERSON_LIKENESS


def test_cli_text_output_uses_script_context_and_fail_on_warn(db, monkeypatch, capsys):
    content_id = _insert_content(
        db,
        image_prompt="Dashboard with lots of tiny text",
        image_alt_text="Dashboard packed with small rendered text.",
    )
    monkeypatch.setattr(
        lint_image_prompts_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = lint_image_prompts_script.main(
        [
            "--content-id",
            str(content_id),
            "--fail-on",
            "warn",
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "Image Prompt Safety Lint" in output
    assert f"content={content_id}" in output
