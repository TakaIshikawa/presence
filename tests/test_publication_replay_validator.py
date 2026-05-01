"""Tests for publication replay dry-run validation."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.publication_replay_bundle import build_publication_replay_bundle
from output.publication_replay_validator import (
    export_to_json,
    format_text_report,
    validate_publication_replay_bundle,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "validate_publication_replay.py"
spec = importlib.util.spec_from_file_location("validate_publication_replay_script", SCRIPT_PATH)
validate_publication_replay_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(validate_publication_replay_script)

BASE_TIME = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str = "Replay this post") -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )


def _valid_bundle(db) -> dict:
    content_id = _content(db)
    queue_id = db.queue_for_publishing(
        content_id,
        BASE_TIME.isoformat(),
        platform="x",
    )
    db.upsert_content_variant(content_id, "x", "post", "Variant post")
    db.select_content_variant(content_id, "x", "post")
    db.upsert_publication_failure(content_id, "x", "temporary failure")
    db.conn.execute(
        "UPDATE content_publications SET platform_url = ? WHERE content_id = ? AND platform = ?",
        ("https://x.com/me/status/1", content_id, "x"),
    )
    db.record_publication_attempt(
        queue_id,
        content_id,
        "x",
        False,
        attempted_at=BASE_TIME.isoformat(),
        platform_url="https://x.com/me/status/1",
        error="temporary failure",
    )
    return build_publication_replay_bundle(
        db,
        content_id=content_id,
        platform="x",
        generated_at=BASE_TIME,
    )


def _codes(report) -> list[str]:
    return sorted(
        issue.code
        for target in report.targets
        for issue in target.issues
    ) + sorted(issue.code for issue in report.bundle_issues)


def test_valid_bundle_passes_with_checked_counts_and_stable_output(db):
    bundle = _valid_bundle(db)
    before = db.conn.total_changes

    report = validate_publication_replay_bundle(bundle, db=db)
    payload = json.loads(export_to_json(report))
    text = format_text_report(report)

    assert report.checked_content_count == 1
    assert report.checked_target_count == 1
    assert report.blocked_count == 0
    assert report.passed_count == 1
    assert report.targets[0].attempt_count == 1
    assert report.targets[0].selected_variant_count == 1
    assert payload["artifact_type"] == "publication_replay_validation"
    assert list(payload.keys()) == sorted(payload.keys())
    assert "Publication Replay Validation" in text
    assert "content #1 x: passed attempts=1 states=1 variants=1" in text
    assert db.conn.total_changes == before


def test_detects_duplicate_targets_missing_content_stale_variants_bad_platforms_and_url_mismatch(db):
    bundle = _valid_bundle(db)
    content_id = bundle["contents"][0]["content"]["id"]
    bundle["contents"][0]["attempts"].append(dict(bundle["contents"][0]["attempts"][0], id=999))
    bundle["contents"][0]["attempts"].append(
        {
            "id": 1000,
            "content_id": content_id,
            "platform": "mastodon",
            "attempted_at": BASE_TIME.isoformat(),
            "success": False,
        }
    )
    db.upsert_content_variant(content_id, "x", "post", "Changed variant")
    db.conn.execute(
        "UPDATE content_publications SET platform_url = ? WHERE content_id = ? AND platform = ?",
        ("https://x.com/me/status/changed", content_id, "x"),
    )
    missing_entry = json.loads(json.dumps(bundle["contents"][0]))
    missing_entry["content"]["id"] = 9999
    missing_entry["attempts"] = [
        dict(missing_entry["attempts"][0], id=2000, content_id=9999, platform="bluesky")
    ]
    missing_entry["platform_states"] = []
    missing_entry["selected_variants"] = []
    bundle["contents"].append(missing_entry)

    report = validate_publication_replay_bundle(bundle, db=db)

    assert report.blocked_count == 3
    assert _codes(report) == [
        "duplicate_replay_target",
        "invalid_platform",
        "missing_content",
        "publication_url_mismatch",
        "stale_variant_content",
    ]


def test_strict_mode_treats_warnings_as_blocking(db):
    bundle = _valid_bundle(db)
    content_id = bundle["contents"][0]["content"]["id"]
    db.conn.execute(
        "UPDATE generated_content SET content = ? WHERE id = ?",
        ("Edited after export", content_id),
    )

    default_report = validate_publication_replay_bundle(bundle, db=db)
    strict_report = validate_publication_replay_bundle(bundle, db=db, strict=True)

    assert _codes(default_report) == ["content_mismatch"]
    assert default_report.warning_count == 1
    assert default_report.blocked_count == 0
    assert strict_report.warning_count == 1
    assert strict_report.blocked_count == 1


def test_cli_reads_bundle_and_outputs_text_and_json(db, tmp_path, capsys):
    bundle = _valid_bundle(db)
    path = tmp_path / "replay.json"
    path.write_text(json.dumps(bundle), encoding="utf-8")

    with patch.object(
        validate_publication_replay_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = validate_publication_replay_script.main(
            [str(path), "--format", "json"]
        )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["checked_target_count"] == 1
    assert payload["blocked_count"] == 0

    bundle["contents"][0]["platform_states"][0]["platform_url"] = "https://stale.example/post"
    path.write_text(json.dumps(bundle), encoding="utf-8")
    with patch.object(
        validate_publication_replay_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = validate_publication_replay_script.main([str(path)])

    assert exit_code == 1
    output = capsys.readouterr().out
    assert "Publication Replay Validation" in output
    assert "publication_url_mismatch" in output
