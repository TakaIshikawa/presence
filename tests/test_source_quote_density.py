"""Tests for source quote density reporting."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.source_quote_density import build_source_quote_density_report


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_quote_density.py"
spec = importlib.util.spec_from_file_location("source_quote_density_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_flags_items_above_quote_density_limit():
    report = build_source_quote_density_report(
        [
            {
                "content_id": "post-1",
                "content_type": "blog",
                "content": 'Intro. "This quoted source paragraph is long and central." Outro.',
            },
            {"content_id": "post-2", "content_type": "newsletter", "content": "Mostly original synthesis with no quoted spans."},
        ],
        max_quote_density=0.2,
    )

    assert report["flagged_items"][0]["content_id"] == "post-1"
    assert report["flagged_items"][0]["quoted_span_count"] == 1
    assert report["flagged_items"][0]["quote_density"] > 0.2
    assert "Quote-like spans" in report["flagged_items"][0]["reason"]


def test_source_excerpt_matches_count_toward_density():
    report = build_source_quote_density_report(
        [
            {
                "content_id": "post-1",
                "content_type": "blog",
                "content": "Original setup. copied source language appears directly here.",
                "source_excerpt": "copied source language appears directly here",
            }
        ],
        max_quote_density=0.3,
    )

    assert report["flagged_items"][0]["quoted_span_count"] == 1


def test_cli_supports_json_text_and_density(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_source_quote_density_report_from_db",
        lambda _db, **kwargs: build_source_quote_density_report(
            [{"content_id": "a", "content_type": "blog", "content": '"quoted content dominates"'}],
            **kwargs,
        ),
    )

    assert script.main(["--max-quote-density", "0.1", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "source_quote_density"
    assert script.main(["--format", "text"]) == 0
    assert "Source Quote Density" in capsys.readouterr().out
