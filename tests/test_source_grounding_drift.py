from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.source_grounding_drift import (
    build_source_grounding_drift_report,
    format_source_grounding_drift_json,
    format_source_grounding_drift_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_grounding_drift.py"
spec = importlib.util.spec_from_file_location("source_grounding_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, *, commits=None, messages=None, days_ago=1, content_type="x_post") -> int:
    cid = db.insert_generated_content(
        content_type=content_type,
        source_commits=[] if commits is None else commits,
        source_messages=[] if messages is None else messages,
        content="draft",
        eval_score=7,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        ((NOW - timedelta(days=days_ago)).isoformat(), cid),
    )
    db.conn.commit()
    return cid


def test_flags_no_source_evidence_weak_family_and_malformed_json(db):
    no_sources = _content(db)
    weak = _content(db)
    malformed = _content(db, commits=["sha"])
    db.conn.execute(
        "INSERT INTO knowledge (source_type, source_id, content, approved) VALUES ('curated_article', 'k1', 'k', 1)"
    )
    kid = db.conn.execute("SELECT id FROM knowledge").fetchone()["id"]
    db.insert_content_knowledge_links(weak, [(kid, 0.8)])
    db.conn.execute("UPDATE generated_content SET source_commits = ? WHERE id = ?", ("not-json", malformed))
    db.conn.commit()

    report = build_source_grounding_drift_report(db, now=NOW)
    by_id = {item["content_id"]: item for item in report["items"]}

    assert by_id[no_sources]["flag_codes"] == ["no_source_evidence"]
    assert by_id[weak]["flag_codes"] == ["single_weak_source_family"]
    assert by_id[malformed]["flag_codes"] == ["malformed_source_json", "no_source_evidence"]
    assert report["weekly_drift"][0]["representative_content_ids"] == sorted([no_sources, weak, malformed])


def test_strong_source_refs_and_newsletter_refs_are_handled(db):
    strong = _content(db, commits=["abc"], messages=["msg"])
    weak_newsletter = _content(db)
    db.conn.execute(
        """INSERT INTO newsletter_sends (issue_id, subject, source_content_ids, status, metadata, sent_at)
           VALUES ('i1', 'Subject', ?, 'sent', '{}', ?)""",
        (json.dumps([weak_newsletter]), NOW.isoformat()),
    )
    db.conn.commit()

    report = build_source_grounding_drift_report(db, now=NOW)
    ids = [item["content_id"] for item in report["items"]]

    assert strong not in ids
    assert ids == [weak_newsletter]
    assert report["items"][0]["evidence_families"] == ["newsletter_refs"]


def test_formatters_and_cli(db, monkeypatch, capsys):
    cid = _content(db)
    report = build_source_grounding_drift_report(db, now=NOW)
    payload = json.loads(format_source_grounding_drift_json(report))
    text = format_source_grounding_drift_text(report)

    assert list(payload) == sorted(payload)
    assert f"content_id={cid}" in text

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_source_grounding_drift_report",
        lambda db, **kwargs: build_source_grounding_drift_report(db, now=NOW, **kwargs),
    )
    assert script.main(["--days", "30", "--limit", "1", "--format", "json"]) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["limit"] == 1
    assert cli_payload["items"][0]["content_id"] == cid
