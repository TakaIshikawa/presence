from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.cross_post_reuse_risk import build_cross_post_reuse_risk_report


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "cross_post_reuse_risk.py"
spec = importlib.util.spec_from_file_location("cross_post_reuse_risk_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            content_type TEXT,
            content TEXT,
            created_at TEXT
        )"""
    )
    return conn


def test_exact_duplicate_reuse_is_grouped():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, 'x', 'post', 'Launch notes are live today.', '2026-05-01T10:00:00+00:00')")
    conn.execute("INSERT INTO generated_content VALUES (2, 'linkedin', 'post', 'Launch notes are live today.', '2026-05-01T11:00:00+00:00')")

    report = build_cross_post_reuse_risk_report(conn, now=NOW)

    pair = report.risky_pairs[0]
    assert (pair.left_content_id, pair.right_content_id) == (1, 2)
    assert pair.similarity_score == 1.0
    assert pair.age_delta_hours == 1.0
    assert pair.recommended_action


def test_below_threshold_is_not_flagged():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, 'x', 'post', 'Short product update.', '2026-05-01T10:00:00+00:00')")
    conn.execute("INSERT INTO generated_content VALUES (2, 'linkedin', 'post', 'A long founder story with different details.', '2026-05-01T11:00:00+00:00')")

    report = build_cross_post_reuse_risk_report(conn, min_similarity=0.9, now=NOW)

    assert report.risky_pairs == ()
    assert report.empty_state["is_empty"] is True


def test_platform_filter_limits_candidates():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, 'x', 'post', 'Same reused copy.', '2026-05-01T10:00:00+00:00')")
    conn.execute("INSERT INTO generated_content VALUES (2, 'linkedin', 'post', 'Same reused copy.', '2026-05-01T11:00:00+00:00')")
    conn.execute("INSERT INTO generated_content VALUES (3, 'x', 'thread', 'Same reused copy.', '2026-05-01T12:00:00+00:00')")

    report = build_cross_post_reuse_risk_report(conn, platform="x", now=NOW)

    assert [(p.left_content_id, p.right_content_id) for p in report.risky_pairs] == [(1, 3)]


def test_cli_json_output(capsys, tmp_path):
    db_path = tmp_path / "reuse.db"
    created_at = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=2)
    reused_at = created_at + timedelta(hours=1)
    conn = sqlite3.connect(db_path)
    conn.executescript(
        f"""CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY, platform TEXT, content_type TEXT, content TEXT, created_at TEXT
        );
        INSERT INTO generated_content VALUES (1, 'x', 'post', 'Duplicate copy', '{created_at.isoformat()}');
        INSERT INTO generated_content VALUES (2, 'linkedin', 'post', 'Duplicate copy', '{reused_at.isoformat()}');"""
    )
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--min-similarity", "0.95"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["artifact_type"] == "cross_post_reuse_risk"
    assert payload["risky_pairs"][0]["similarity_score"] == 1.0
