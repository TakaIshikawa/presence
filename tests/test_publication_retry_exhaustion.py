"""Tests for publication retry exhaustion reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.publication_retry_exhaustion import (
    build_publication_retry_exhaustion_report,
    build_publication_retry_exhaustion_report_from_db,
    format_publication_retry_exhaustion_table,
)


NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_retry_exhaustion.py"
spec = importlib.util.spec_from_file_location("publication_retry_exhaustion_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str) -> int:
    return db.insert_generated_content("x_post", [], [], text, 7, "ok")


def _publication(db, content_id: int, *, platform: str = "x", status: str = "failed", attempts: int = 0, category: str | None = None) -> int:
    cursor = db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, error, error_category, attempt_count, last_error_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (content_id, platform, status, "rate limit exceeded", category, attempts, NOW.isoformat()),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_retry_budget_statuses_are_separated_and_grouped():
    report = build_publication_retry_exhaustion_report(
        [
            {"id": 1, "content_id": 10, "platform": "x", "status": "failed", "attempt_count": 3, "error_category": "rate_limit"},
            {"id": 2, "content_id": 11, "platform": "x", "status": "failed", "attempt_count": 2, "error_category": "media"},
            {"id": 3, "content_id": 12, "platform": "bluesky", "status": "queued", "attempt_count": 0, "error": "unauthorized"},
        ],
        retry_limit=3,
        nearly_exhausted_retries=1,
        now=NOW,
    )
    by_id = {row["publication_id"]: row for row in report["rows"]}

    assert by_id[1]["remaining_retries"] == 0
    assert by_id[1]["exhaustion_status"] == "exhausted"
    assert by_id[2]["remaining_retries"] == 1
    assert by_id[2]["exhaustion_status"] == "nearly_exhausted"
    assert by_id[3]["remaining_retries"] == 3
    assert by_id[3]["exhaustion_status"] == "retryable"
    assert report["summary"]["exhausted_count"] == 1
    assert report["summary"]["nearly_exhausted_count"] == 1
    assert any(group["channel"] == "x" and group["last_error_category"] == "rate_limit" for group in report["groups"])


def test_db_loader_excludes_published_rows_and_cli_outputs_json_and_table(db, monkeypatch, capsys):
    exhausted_id = _publication(db, _content(db, "exhausted"), attempts=3, category="rate_limit")
    near_id = _publication(db, _content(db, "near"), attempts=2, category="media")
    _publication(db, _content(db, "published"), status="published", attempts=3)

    report = build_publication_retry_exhaustion_report_from_db(db, retry_limit=3, nearly_exhausted_retries=1, now=NOW)
    by_id = {row["publication_id"]: row for row in report["rows"]}
    assert set(by_id) == {exhausted_id, near_id}
    assert by_id[exhausted_id]["exhaustion_status"] == "exhausted"
    assert by_id[near_id]["exhaustion_status"] == "nearly_exhausted"
    assert "Publication Retry Exhaustion" in format_publication_retry_exhaustion_table(report)

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_publication_retry_exhaustion_report_from_db",
        lambda db, **kwargs: build_publication_retry_exhaustion_report_from_db(db, now=NOW, **kwargs),
    )

    assert script.main(["--retry-limit", "3"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "publication_retry_exhaustion"
    assert payload["summary"]["exhausted_count"] == 1

    assert script.main(["--table"]) == 0
    assert "nearly_exhausted" in capsys.readouterr().out
