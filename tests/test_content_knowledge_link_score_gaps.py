from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.content_knowledge_link_score_gaps import build_content_knowledge_link_score_gaps_report, build_content_knowledge_link_score_gaps_report_from_db


NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_knowledge_link_score_gaps.py"
spec = importlib.util.spec_from_file_location("content_knowledge_link_score_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_link_gaps():
    report = build_content_knowledge_link_score_gaps_report([
        {"id": 1, "content_id": 1, "knowledge_id": 1, "resolved_content_id": None, "resolved_knowledge_id": 1, "relevance_score": 2},
        {"id": 2, "content_id": 2, "knowledge_id": 2, "resolved_content_id": 2, "resolved_knowledge_id": None, "relevance_score": 0.1, "content_published": 1},
        {"id": 3, "content_id": 2, "knowledge_id": 2, "resolved_content_id": 2, "resolved_knowledge_id": 2, "relevance_score": 0.1, "content_published": 1},
    ], weak_threshold=0.3, min_links=1, now=NOW)
    assert report["artifact_type"] == "content_knowledge_link_score_gaps"
    assert report["totals"]["by_reason"]["missing_content"] == 1
    assert report["totals"]["by_reason"]["missing_knowledge"] == 1
    assert report["totals"]["by_reason"]["invalid_relevance_score"] == 1
    assert report["totals"]["by_reason"]["duplicate_link"] == 1
    assert report["totals"]["by_reason"]["weak_published_grounding"] == 1


def test_db_loader_and_cli(tmp_path, capsys):
    path = tmp_path / "ckl.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript("""CREATE TABLE generated_content (id INTEGER PRIMARY KEY, published INTEGER);
    CREATE TABLE knowledge (id INTEGER PRIMARY KEY);
    CREATE TABLE content_knowledge_links (content_id INTEGER, knowledge_id INTEGER, relevance_score REAL);
    INSERT INTO generated_content VALUES (1, 1);
    INSERT INTO knowledge VALUES (1);
    INSERT INTO content_knowledge_links VALUES (1, 1, 0.1);""")
    conn.commit()
    assert build_content_knowledge_link_score_gaps_report_from_db(conn, weak_threshold=0.5, now=NOW)["totals"]["by_reason"]["weak_published_grounding"] == 1
    assert script.main(["--db", str(path), "--format", "json", "--weak-threshold", "0.5", "--min-links", "1"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "content_knowledge_link_score_gaps"
