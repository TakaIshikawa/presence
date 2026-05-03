"""Tests for publication attempt payload anomaly reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from output.publication_attempt_payload_anomalies import (
    build_publication_attempt_payload_anomalies_report,
    format_publication_attempt_payload_anomalies_json,
    format_publication_attempt_payload_anomalies_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "publication_attempt_payload_anomalies.py"
)
spec = importlib.util.spec_from_file_location(
    "publication_attempt_payload_anomalies_script",
    SCRIPT_PATH,
)
publication_attempt_payload_anomalies_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_attempt_payload_anomalies_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str = "post") -> int:
    return int(
        db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content=text,
            eval_score=8.0,
            eval_feedback="ok",
        )
    )


def _attempt(
    db,
    *,
    content_id: int | None = None,
    platform: str = "x",
    attempted_at: str = "2026-05-01T10:00:00+00:00",
    success: bool,
    platform_post_id: str | None = None,
    platform_url: str | None = None,
    error: str | None = None,
    error_category: str | None = None,
    response_metadata: dict | None = None,
) -> int:
    return int(
        db.record_publication_attempt(
            queue_id=None,
            content_id=content_id or _content(db),
            platform=platform,
            attempted_at=attempted_at,
            success=success,
            platform_post_id=platform_post_id,
            platform_url=platform_url,
            error=error,
            error_category=error_category,
            response_metadata=response_metadata,
        )
    )


def test_detects_each_anomaly_type_and_totals(db):
    success_id = _attempt(
        db,
        success=True,
        error="publisher returned 200 with warning",
        error_category="network",
        response_metadata={"ok": True},
    )
    malformed_id = _attempt(
        db,
        success=False,
        error="rate limit exceeded",
        error_category="auth",
    )
    db.conn.execute(
        "UPDATE publication_attempts SET response_metadata = ? WHERE id = ?",
        ("{not-json", malformed_id),
    )
    oversized_id = _attempt(
        db,
        success=False,
        platform="bluesky",
        error="temporary network timeout",
        error_category="network",
        response_metadata={"body": "x" * 40},
    )
    db.conn.commit()

    report = build_publication_attempt_payload_anomalies_report(
        db,
        days=7,
        max_metadata_bytes=20,
        now=NOW,
    )
    payload = json.loads(format_publication_attempt_payload_anomalies_json(report))

    assert payload["artifact_type"] == "publication_attempt_payload_anomalies"
    assert payload["totals"]["anomaly_count"] == 6
    assert payload["totals"]["by_type"] == {
        "category_mismatch": 1,
        "malformed_response_metadata": 1,
        "missing_url_for_success": 1,
        "oversized_response_metadata": 1,
        "success_with_error": 1,
        "success_without_post_id": 1,
    }
    assert payload["totals"]["by_platform"] == {"bluesky": 1, "x": 5}
    assert payload["totals"]["by_severity"] == {"high": 3, "low": 1, "medium": 2}

    by_type = {item["type"]: item for item in payload["items"]}
    assert by_type["success_without_post_id"]["attempt_id"] == success_id
    assert by_type["success_with_error"]["attempt_id"] == success_id
    assert by_type["missing_url_for_success"]["attempt_id"] == success_id
    assert by_type["malformed_response_metadata"]["attempt_id"] == malformed_id
    assert by_type["category_mismatch"]["details"] == {
        "classified_error_category": "rate_limit",
        "stored_error_category": "auth",
    }
    assert by_type["oversized_response_metadata"]["attempt_id"] == oversized_id
    assert by_type["oversized_response_metadata"]["details"]["metadata_bytes"] > 20
    assert all(item["fix_hint"] for item in payload["items"])


def test_malformed_response_metadata_is_captured_instead_of_raising(db):
    attempt_id = _attempt(
        db,
        success=False,
        error="bad request",
        error_category="validation",
    )
    db.conn.execute(
        "UPDATE publication_attempts SET response_metadata = ? WHERE id = ?",
        ("[", attempt_id),
    )
    db.conn.commit()

    report = build_publication_attempt_payload_anomalies_report(db, now=NOW)

    assert [item["type"] for item in report["items"]] == ["malformed_response_metadata"]
    assert report["items"][0]["attempt_id"] == attempt_id


def test_platform_and_severity_filters_work_through_cli(db, monkeypatch, capsys):
    x_id = _attempt(db, platform="x", success=True)
    _attempt(
        db,
        platform="bluesky",
        success=False,
        error="rate limit exceeded",
        error_category="auth",
    )
    monkeypatch.setattr(
        publication_attempt_payload_anomalies_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        publication_attempt_payload_anomalies_script,
        "build_publication_attempt_payload_anomalies_report",
        lambda db, **kwargs: build_publication_attempt_payload_anomalies_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = publication_attempt_payload_anomalies_script.main(
        [
            "--format",
            "json",
            "--platform",
            "x",
            "--severity",
            "high",
            "--days",
            "7",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert {item["attempt_id"] for item in payload["items"]} == {x_id}
    assert {item["severity"] for item in payload["items"]} == {"high"}
    assert payload["totals"]["by_platform"] == {"x": 1}

    assert publication_attempt_payload_anomalies_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_text_output_and_deterministic_ordering(db):
    later_id = _attempt(
        db,
        platform="bluesky",
        attempted_at="2026-05-01T11:00:00+00:00",
        success=False,
        error="rate limit exceeded",
        error_category="auth",
    )
    earlier_id = _attempt(
        db,
        platform="x",
        attempted_at="2026-05-01T09:00:00+00:00",
        success=True,
    )
    report = build_publication_attempt_payload_anomalies_report(db, now=NOW)
    text = format_publication_attempt_payload_anomalies_text(report)

    ordered = [(item["severity"], item["platform"], item["type"], item["attempt_id"]) for item in report["items"]]

    assert ordered == [
        ("high", "x", "success_without_post_id", earlier_id),
        ("medium", "bluesky", "category_mismatch", later_id),
        ("medium", "x", "missing_url_for_success", earlier_id),
    ]
    assert "Publication Attempt Payload Anomaly Report" in text
    assert f"attempt={earlier_id}" in text
    assert "fix_hint:" in text
