from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.claim_review_aging import (
    build_claim_review_aging_report,
    format_claim_review_aging_json,
    format_claim_review_aging_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claim_review_aging.py"
spec = importlib.util.spec_from_file_location("claim_review_aging_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, *, days_ago: int, unsupported: int = 0, published: int = 0, content_type: str = "x_post") -> int:
    cid = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content="claimy draft",
        eval_score=7,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published = ?, published_at = ? WHERE id = ?",
        (
            (NOW - timedelta(days=days_ago)).isoformat(),
            published,
            (NOW - timedelta(days=days_ago - 1)).isoformat() if published == 1 else None,
            cid,
        ),
    )
    if unsupported:
        db.save_claim_check_summary(cid, supported_count=1, unsupported_count=unsupported, annotation_text="bad")
    return cid


def test_detects_claim_check_table_and_sorts_by_risk_age_and_id(db):
    newer_high = _content(db, days_ago=5, unsupported=3, content_type="blog_post")
    older_low = _content(db, days_ago=20, unsupported=1, published=1)
    ignored = _content(db, days_ago=12, unsupported=0)

    report = build_claim_review_aging_report(db, now=NOW)

    assert [item["content_id"] for item in report["items"]] == [newer_high, older_low]
    assert ignored not in [item["content_id"] for item in report["items"]]
    assert report["items"][0]["unsupported_claim_count"] == 3
    assert report["items"][1]["publish_status"] == "published"
    assert report["totals"]["age_buckets"]["0_7_days"] == 1
    assert report["totals"]["age_buckets"]["15_30_days"] == 1
    assert report["totals"]["by_content_type"] == {"blog_post": 1, "x_post": 1}


def test_detects_optional_generated_content_metadata_and_bucket_filter(db):
    db.conn.execute("ALTER TABLE generated_content ADD COLUMN metadata JSON")
    cid = _content(db, days_ago=40, unsupported=0)
    db.conn.execute(
        "UPDATE generated_content SET metadata = ? WHERE id = ?",
        (json.dumps({"claim_check": {"unsupported_count": 2}}), cid),
    )
    db.conn.commit()

    excluded = build_claim_review_aging_report(db, days=60, max_age_bucket="15_30_days", now=NOW)
    included = build_claim_review_aging_report(db, days=60, max_age_bucket="31_plus_days", now=NOW)

    assert excluded["items"] == []
    assert included["items"][0]["content_id"] == cid
    assert included["items"][0]["age_bucket"] == "31_plus_days"


def test_formatters_and_cli(db, monkeypatch, capsys):
    cid = _content(db, days_ago=9, unsupported=1)
    report = build_claim_review_aging_report(db, now=NOW)
    payload = json.loads(format_claim_review_aging_json(report))
    text = format_claim_review_aging_text(report)

    assert list(payload) == sorted(payload)
    assert f"content_id={cid}" in text

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_claim_review_aging_report",
        lambda db, **kwargs: build_claim_review_aging_report(db, now=NOW, **kwargs),
    )
    assert script.main(["--days", "30", "--limit", "1", "--max-age-bucket", "31_plus_days", "--format", "json"]) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["limit"] == 1
    assert cli_payload["items"][0]["content_id"] == cid
