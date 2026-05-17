"""Tests for blog excerpt similarity reporting."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.blog_excerpt_similarity import build_blog_excerpt_similarity_report


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_excerpt_similarity.py"
spec = importlib.util.spec_from_file_location("blog_excerpt_similarity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_flags_excerpt_pairs_above_threshold():
    report = build_blog_excerpt_similarity_report(
        [
            {"id": "a", "excerpt": "How teams turn noisy customer feedback into focused product bets."},
            {"id": "b", "excerpt": "How teams turn noisy customer feedback into focused product bets!"},
            {"id": "c", "excerpt": "A launch checklist for release notes and changelog hygiene."},
        ],
        threshold=0.9,
    )

    assert report["totals"]["flagged_pair_count"] == 1
    assert report["pairs"][0]["left_id"] == "a"
    assert report["pairs"][0]["right_id"] == "b"
    assert report["pairs"][0]["similarity"] >= 0.9
    assert "above the 90%" in report["pairs"][0]["reason"]


def test_below_threshold_pairs_are_not_flagged():
    report = build_blog_excerpt_similarity_report(
        [
            {"id": "a", "excerpt": "A concise guide to source curation workflows."},
            {"id": "b", "excerpt": "How publication retries recover after platform failures."},
        ],
        threshold=0.75,
    )

    assert report["pairs"] == []
    assert report["empty_state"]["is_empty"] is True


def test_cli_supports_json_text_and_threshold(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_blog_excerpt_similarity_report_from_db",
        lambda _db, **kwargs: build_blog_excerpt_similarity_report(
            [
                {"id": "a", "excerpt": "Same excerpt for a launch note"},
                {"id": "b", "excerpt": "Same excerpt for a launch note"},
            ],
            **kwargs,
        ),
    )

    assert script.main(["--threshold", "0.8", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["pairs"][0]["similarity"] == 1.0
    assert script.main(["--format", "text"]) == 0
    assert "Blog Excerpt Similarity" in capsys.readouterr().out
