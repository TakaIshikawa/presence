"""Tests for newsletter dead link impact reporting."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.newsletter_dead_link_impact import build_newsletter_dead_link_impact_report


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_dead_link_impact.py"
spec = importlib.util.spec_from_file_location("newsletter_dead_link_impact_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_ranks_broken_links_by_affected_sends_and_clicks():
    report = build_newsletter_dead_link_impact_report(
        [
            {"url": "https://ok.example", "status": "200", "clicks": 50, "content_id": "issue-ok"},
            {"url": "https://low.example", "status": "404", "clicks": 1, "content_ids": ["issue-3"]},
            {"url": "https://high.example", "status": "500", "clicks": 8, "content_ids": ["issue-1", "issue-2"]},
        ]
    )

    assert [item["url"] for item in report["links"]] == ["https://high.example", "https://low.example"]
    assert report["links"][0]["affected_sends"] == 2
    assert report["links"][0]["click_count"] == 8
    assert "readers have clicked" in report["links"][0]["remediation_reason"]


def test_missing_optional_click_metrics_do_not_fail():
    report = build_newsletter_dead_link_impact_report([{"url": "https://dead.example", "error": "timeout", "content_id": "send-1"}])

    assert report["links"][0]["click_count"] == 0
    assert report["links"][0]["affected_content_ids"] == ["send-1"]
    assert report["links"][0]["impact_score"] == 10


def test_empty_data_returns_structured_empty_result():
    report = build_newsletter_dead_link_impact_report([])

    assert report["totals"]["broken_link_count"] == 0
    assert report["empty_state"]["is_empty"] is True
    assert report["links"] == []


def test_cli_supports_json_and_text(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_newsletter_dead_link_impact_report_from_db",
        lambda _db: build_newsletter_dead_link_impact_report([{"url": "https://dead.example", "status": "404"}]),
    )

    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "newsletter_dead_link_impact"
    assert script.main(["--format", "text"]) == 0
    assert "Newsletter Dead Link Impact" in capsys.readouterr().out
