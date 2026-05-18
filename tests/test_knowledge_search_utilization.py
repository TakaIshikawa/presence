"""Tests for knowledge search utilization reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.knowledge_search_utilization import (
    build_knowledge_search_utilization_report,
    build_knowledge_search_utilization_report_from_db,
    format_knowledge_search_utilization_table,
)


NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_search_utilization.py"
spec = importlib.util.spec_from_file_location("knowledge_search_utilization_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_report_scores_utilization_and_unused_top_results():
    report = build_knowledge_search_utilization_report(
        [
            {"generation_id": 1, "content_type": "x_post"},
            {"generation_id": 2, "content_type": "blog_post"},
            {"generation_id": 3, "content_type": "newsletter"},
        ],
        [
            {"generation_id": 1, "source_id": "a", "rank": 1},
            {"generation_id": 1, "source_id": "b", "rank": 2},
            {"generation_id": 1, "source_id": "c", "rank": 3},
            {"generation_id": 2, "source_id": "d", "rank": 1},
            {"generation_id": 2, "source_id": "e", "rank": 2},
        ],
        [
            {"generation_id": 1, "source_id": "a"},
            {"generation_id": 2, "source_id": "d"},
            {"generation_id": 2, "source_id": "e"},
        ],
        low_utilization_rate=0.5,
        top_rank=2,
        now=NOW,
    )
    by_id = {row["generation_id"]: row for row in report["rows"]}

    assert by_id[1]["retrieved_source_count"] == 3
    assert by_id[1]["used_source_count"] == 1
    assert by_id[1]["utilization_rate"] == 0.3333
    assert by_id[1]["utilization_bucket"] == "low_utilization"
    assert by_id[1]["unused_top_result_ids"] == ["b"]
    assert by_id[2]["utilization_bucket"] == "high_utilization"
    assert by_id[3]["retrieved_source_count"] == 0
    assert by_id[3]["utilization_bucket"] == "no_retrievals"


def test_db_loader_and_cli_support_json_and_table(db, monkeypatch, capsys):
    low_id = db.insert_generated_content("x_post", [], [], "post", 7, "ok")
    none_id = db.insert_generated_content("blog_post", [], [], "blog", 7, "ok")
    db.conn.execute(
        """CREATE TABLE knowledge_search_results (
             id INTEGER PRIMARY KEY,
             generation_id INTEGER,
             knowledge_id INTEGER,
             rank INTEGER
           )"""
    )
    db.conn.executemany(
        "INSERT INTO knowledge_search_results (generation_id, knowledge_id, rank) VALUES (?, ?, ?)",
        [(low_id, 101, 1), (low_id, 102, 2), (low_id, 103, 3)],
    )
    db.conn.execute(
        "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, 0.9)",
        (low_id, 101),
    )
    db.conn.commit()

    report = build_knowledge_search_utilization_report_from_db(db, low_utilization_rate=0.5, top_rank=2, now=NOW)
    by_id = {row["generation_id"]: row for row in report["rows"]}
    assert by_id[low_id]["utilization_bucket"] == "low_utilization"
    assert by_id[low_id]["unused_top_result_ids"] == ["102"]
    assert by_id[none_id]["utilization_bucket"] == "no_retrievals"
    assert "Knowledge Search Utilization" in format_knowledge_search_utilization_table(report)

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_knowledge_search_utilization_report_from_db",
        lambda db, **kwargs: build_knowledge_search_utilization_report_from_db(db, now=NOW, **kwargs),
    )

    assert script.main(["--low-utilization-rate", "0.5", "--top-rank", "2"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "knowledge_search_utilization"
    assert payload["summary"]["low_utilization_count"] == 1

    assert script.main(["--table"]) == 0
    assert "low_utilization" in capsys.readouterr().out
