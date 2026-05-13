from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sqlite3

from evaluation.few_shot_influence_drift import build_few_shot_influence_drift_report


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "few_shot_influence_drift.py"
spec = importlib.util.spec_from_file_location("few_shot_influence_drift_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY, content_type TEXT, eval_score REAL, created_at TEXT
        );
        CREATE TABLE few_shot_usages (
            id INTEGER PRIMARY KEY, example_id TEXT, example_text TEXT, content_id INTEGER, used_at TEXT
        );"""
    )
    return conn


def _usage(conn: sqlite3.Connection, idx: int, example: str, score: float, ctype: str = "x_post") -> None:
    conn.execute("INSERT INTO generated_content VALUES (?, ?, ?, '2026-04-25T00:00:00+00:00')", (idx, ctype, score))
    conn.execute("INSERT INTO few_shot_usages VALUES (?, ?, ?, ?, '2026-04-25T00:00:00+00:00')", (idx, example, f"example {example}", idx))


def test_overused_low_performing_examples():
    conn = _conn()
    for idx in range(1, 4):
        _usage(conn, idx, "bad", 2.0)
    for idx in range(4, 7):
        _usage(conn, idx, "good", 10.0)

    report = build_few_shot_influence_drift_report(conn, min_uses=3, min_underperformance_pct=30, now=NOW)

    assert report.issues[0].example_id == "bad"
    assert report.issues[0].issue_type == "overused_low_performing_example"


def test_healthy_examples_are_not_flagged():
    conn = _conn()
    for idx in range(1, 4):
        _usage(conn, idx, "ok", 8.0)

    report = build_few_shot_influence_drift_report(conn, min_uses=3, now=NOW)

    assert report.issues == ()


def test_content_type_filtering():
    conn = _conn()
    for idx in range(1, 4):
        _usage(conn, idx, "bad", 1.0, "x_post")
    for idx in range(4, 7):
        _usage(conn, idx, "good", 10.0, "x_post")
    for idx in range(7, 10):
        _usage(conn, idx, "bad", 10.0, "blog_post")

    report = build_few_shot_influence_drift_report(conn, content_type="blog_post", min_uses=3, now=NOW)

    assert report.issues == ()


def test_missing_schema_empty_state():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_few_shot_influence_drift_report(conn, now=NOW)

    assert report.empty_state["is_empty"] is True
    assert report.missing_tables


def test_cli_text_output(capsys, tmp_path):
    db_path = tmp_path / "fewshot.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content_type TEXT, eval_score REAL, created_at TEXT);
        CREATE TABLE few_shot_usages (id INTEGER PRIMARY KEY, example_id TEXT, example_text TEXT, content_id INTEGER, used_at TEXT);
        INSERT INTO generated_content VALUES (1, 'x_post', 1, '2026-04-25T00:00:00+00:00');
        INSERT INTO generated_content VALUES (2, 'x_post', 1, '2026-04-25T00:00:00+00:00');
        INSERT INTO generated_content VALUES (3, 'x_post', 10, '2026-04-25T00:00:00+00:00');
        INSERT INTO few_shot_usages VALUES (1, 'bad', 'bad example', 1, '2026-04-25T00:00:00+00:00');
        INSERT INTO few_shot_usages VALUES (2, 'bad', 'bad example', 2, '2026-04-25T00:00:00+00:00');
        INSERT INTO few_shot_usages VALUES (3, 'good', 'good example', 3, '2026-04-25T00:00:00+00:00');"""
    )
    conn.close()

    assert script.main(["--db", str(db_path), "--days", "60", "--min-uses", "2", "--format", "text"]) == 0
    assert "Few-Shot Influence Drift" in capsys.readouterr().out
