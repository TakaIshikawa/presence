"""Tests for generation candidate novelty reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.generation_candidate_novelty import (
    build_generation_candidate_novelty_report,
    build_generation_candidate_novelty_report_from_db,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "generation_candidate_novelty.py"
spec = importlib.util.spec_from_file_location("generation_candidate_novelty_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_diverse_near_duplicate_and_single_candidate_groups():
    rows = [
        {"id": "a1", "generation_run_id": "run-a", "text": "Ship the parser with structured validation and retry logging."},
        {"id": "a2", "generation_run_id": "run-a", "text": "Launch the dashboard with customer evidence and rollout notes."},
        {"id": "b1", "generation_run_id": "run-b", "text": "Queue retries need timeout logging and migration safeguards."},
        {"id": "b2", "generation_run_id": "run-b", "text": "Queue retries need timeout logging and migration safeguards today."},
        {"id": "c1", "generation_run_id": "run-c", "text": "Only candidate in this run."},
    ]

    report = build_generation_candidate_novelty_report(rows, similarity_threshold=0.75, now=NOW)
    statuses = {group["group_key"]: group["novelty_status"] for group in report["groups"]}

    assert statuses["run-a"] == "diverse"
    assert statuses["run-b"] == "near_duplicates"
    assert statuses["run-c"] == "single_candidate"
    assert report["totals"]["near_duplicate_pair_count"] == 1


def test_threshold_affects_near_duplicate_pairs():
    rows = [
        {"id": "1", "generation_run_id": "run", "text": "Retry timeout logging migration queue"},
        {"id": "2", "generation_run_id": "run", "text": "Retry timeout logging migration queue dashboard"},
    ]

    lenient = build_generation_candidate_novelty_report(rows, similarity_threshold=0.7, now=NOW)
    strict = build_generation_candidate_novelty_report(rows, similarity_threshold=0.9, now=NOW)

    assert lenient["totals"]["near_duplicate_pair_count"] == 1
    assert strict["totals"]["near_duplicate_pair_count"] == 0


def test_db_loader_and_cli_outputs(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE generation_candidates (id TEXT, generation_run_id TEXT, source_item_id TEXT, text TEXT)")
    conn.execute("INSERT INTO generation_candidates VALUES (?, ?, ?, ?)", ("1", "run", "src", "Same candidate text"))
    conn.execute("INSERT INTO generation_candidates VALUES (?, ?, ?, ?)", ("2", "run", "src", "Same candidate text"))
    conn.commit()
    db = SimpleNamespace(conn=conn)

    report = build_generation_candidate_novelty_report_from_db(db, now=NOW)
    assert report["totals"]["near_duplicate_pair_count"] == 1

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_generation_candidate_novelty_report_from_db",
        lambda db, **kwargs: build_generation_candidate_novelty_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "generation_candidate_novelty"
    assert script.main(["--table"]) == 0
    assert "Generation Candidate Novelty" in capsys.readouterr().out
