"""Tests for reply relationship warmth drift reporting."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_relationship_warmth_drift import build_reply_relationship_warmth_drift_report


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_relationship_warmth_drift.py"
spec = importlib.util.spec_from_file_location("reply_relationship_warmth_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_groups_replies_and_flags_colder_relationship_drift():
    rows = [
        {"relationship_id": "rel-1", "reply_id": "1", "reply_text": "Thanks, I really appreciate your thoughtful launch notes!", "created_at": "2026-05-01"},
        {"relationship_id": "rel-1", "reply_id": "2", "reply_text": "Great work, glad you shared this with us!", "created_at": "2026-05-02"},
        {"relationship_id": "rel-1", "reply_id": "3", "reply_text": "Interesting.", "created_at": "2026-05-03"},
        {"relationship_id": "rel-1", "reply_id": "4", "reply_text": "Nice.", "created_at": "2026-05-04"},
    ]

    report = build_reply_relationship_warmth_drift_report(rows, min_replies=4)

    assert report["relationships"][0]["relationship_id"] == "rel-1"
    assert report["relationships"][0]["drift_direction"] == "colder"
    assert report["relationships"][0]["confidence"] > 0
    assert len(report["relationships"][0]["recent_examples"]) == 3


def test_min_replies_filters_sparse_relationships():
    report = build_reply_relationship_warmth_drift_report(
        [{"relationship_id": "rel-1", "reply_text": "Thanks!"}, {"relationship_id": "rel-1", "reply_text": "Nice."}],
        min_replies=3,
    )

    assert report["relationships"] == []
    assert report["empty_state"]["is_empty"] is True


def test_cli_supports_json_text_and_min_replies(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_reply_relationship_warmth_drift_report_from_db",
        lambda _db, **kwargs: build_reply_relationship_warmth_drift_report(
            [
                {"relationship_id": "rel", "reply_text": "Thanks for your thoughtful note!", "created_at": "1"},
                {"relationship_id": "rel", "reply_text": "I appreciate your details.", "created_at": "2"},
                {"relationship_id": "rel", "reply_text": "Nice.", "created_at": "3"},
            ],
            **kwargs,
        ),
    )

    assert script.main(["--min-replies", "3", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "reply_relationship_warmth_drift"
    assert script.main(["--format", "text"]) == 0
    assert "Reply Relationship Warmth Drift" in capsys.readouterr().out
